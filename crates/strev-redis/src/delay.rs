use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use strev::{Delay, Message, Metadata, PublishError, Publisher, Topic};
use tokio_util::sync::CancellationToken;

use crate::marshaller::{DefaultMarshaller, Marshaller};
use crate::publisher::{RedisPublisher, RedisPublisherConfig};

pub(crate) const DELAYED_TOPICS_SET: &str = "strev:delayed-topics";

pub(crate) fn delayed_zset_key(topic: &str) -> String {
    format!("strev:delayed:{topic}")
}

pub(crate) fn due_millis(delay: Delay) -> f64 {
    delay
        .not_before()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as f64)
        .unwrap_or(0.0)
}

fn now_millis() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as f64)
        .unwrap_or(0.0)
}

#[derive(Serialize, Deserialize)]
struct StagedMessage {
    uuid: String,
    payload: Vec<u8>,
    metadata: BTreeMap<String, String>,
}

/// Encode a message into a unique ZSET member. The `uuid` keeps otherwise-identical
/// messages distinct so neither overwrites the other in the sorted set.
pub(crate) fn encode(message: &Message) -> Result<String, serde_json::Error> {
    let metadata = message
        .metadata()
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    serde_json::to_string(&StagedMessage {
        uuid: message.uuid().to_string(),
        payload: message.payload().to_vec(),
        metadata,
    })
}

fn decode(member: &str) -> Result<Message, serde_json::Error> {
    let staged: StagedMessage = serde_json::from_str(member)?;
    let mut metadata = Metadata::new();
    for (key, value) in staged.metadata {
        metadata.set(key, value);
    }
    Ok(Message::with_metadata(
        Bytes::from(staged.payload),
        metadata,
    ))
}

/// Configuration for a [`RedisDelayPromoter`].
pub struct RedisDelayPromoterConfig {
    pub client: redis::Client,
    pub marshaller: Arc<dyn Marshaller>,
    pub max_stream_len: Option<usize>,
    pub poll_interval: Duration,
    pub batch_size: usize,
}

impl RedisDelayPromoterConfig {
    pub fn new(client: redis::Client) -> Self {
        Self {
            client,
            marshaller: Arc::new(DefaultMarshaller),
            max_stream_len: None,
            poll_interval: Duration::from_millis(200),
            batch_size: 100,
        }
    }
}

/// Moves due messages staged by [`publish_after`](strev::DelayedPublisher::publish_after)
/// into their live streams. Run one for exactly-once promotion, or several for high
/// availability (delivery is then at-least-once; pair with the `Deduplicator` middleware).
pub struct RedisDelayPromoter {
    conn: redis::aio::MultiplexedConnection,
    publisher: RedisPublisher,
    poll_interval: Duration,
    batch_size: usize,
}

impl RedisDelayPromoter {
    pub async fn new(config: RedisDelayPromoterConfig) -> Result<Self, PublishError> {
        let conn = config
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| PublishError::Backend(Box::new(e)))?;

        let mut publisher_config = RedisPublisherConfig::new(config.client);
        publisher_config.marshaller = config.marshaller;
        publisher_config.max_stream_len = config.max_stream_len;
        let publisher = RedisPublisher::new(publisher_config).await?;

        Ok(Self {
            conn,
            publisher,
            poll_interval: config.poll_interval,
            batch_size: config.batch_size,
        })
    }

    pub async fn run(self, shutdown: CancellationToken) {
        loop {
            match self.promote_once().await {
                Ok(count) if count > 0 => continue,
                _ => {}
            }

            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(self.poll_interval) => {}
            }
        }
    }

    async fn promote_once(&self) -> Result<usize, PublishError> {
        let mut conn = self.conn.clone();
        let now = now_millis();

        let topics: Vec<String> = conn
            .smembers(DELAYED_TOPICS_SET)
            .await
            .map_err(|e| PublishError::Backend(Box::new(e)))?;

        let mut promoted = 0;
        for topic in topics {
            let key = delayed_zset_key(&topic);
            let due: Vec<String> = redis::cmd("ZRANGEBYSCORE")
                .arg(&key)
                .arg("-inf")
                .arg(now)
                .arg("LIMIT")
                .arg(0)
                .arg(self.batch_size)
                .query_async(&mut conn)
                .await
                .map_err(|e| PublishError::Backend(Box::new(e)))?;

            for member in due {
                let message = decode(&member).map_err(|e| PublishError::Backend(Box::new(e)))?;
                self.publisher
                    .publish(&Topic::new(topic.clone()), vec![message])
                    .await?;
                let _: () = conn
                    .zrem(&key, &member)
                    .await
                    .map_err(|e| PublishError::Backend(Box::new(e)))?;
                promoted += 1;
            }
        }

        Ok(promoted)
    }
}
