use std::sync::Arc;

use async_trait::async_trait;
use redis::AsyncCommands;
use strev::{CloseError, Message, Outcome, PublishError, Topic};

use crate::marshaller::{DefaultMarshaller, Marshaller};

pub struct RedisPublisherConfig {
    pub client: redis::Client,
    pub marshaller: Arc<dyn Marshaller>,
    pub max_stream_len: Option<usize>,
}

impl RedisPublisherConfig {
    pub fn new(client: redis::Client) -> Self {
        Self {
            client,
            marshaller: Arc::new(DefaultMarshaller),
            max_stream_len: None,
        }
    }
}

pub struct RedisPublisher {
    conn: redis::aio::MultiplexedConnection,
    marshaller: Arc<dyn Marshaller>,
    max_stream_len: Option<usize>,
}

impl RedisPublisher {
    pub async fn new(config: RedisPublisherConfig) -> Result<Self, PublishError> {
        let conn = config
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| PublishError::Backend(Box::new(e)))?;

        Ok(Self {
            conn,
            marshaller: config.marshaller,
            max_stream_len: config.max_stream_len,
        })
    }
}

#[async_trait]
impl strev::Publisher for RedisPublisher {
    async fn publish(
        &self,
        topic: &Topic,
        messages: Vec<Message>,
    ) -> Result<Vec<Outcome>, PublishError> {
        let mut conn = self.conn.clone();
        let stream_key = topic.as_str();
        let mut outcomes = Vec::with_capacity(messages.len());

        for msg in messages {
            let fields = self.marshaller.marshal(&msg);

            let items: Vec<(&str, &[u8])> = fields
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_slice()))
                .collect();

            let result: Result<String, _> = if let Some(maxlen) = self.max_stream_len {
                redis::cmd("XADD")
                    .arg(stream_key)
                    .arg("MAXLEN")
                    .arg("~")
                    .arg(maxlen)
                    .arg("*")
                    .arg(&items)
                    .query_async(&mut conn)
                    .await
            } else {
                conn.xadd(stream_key, "*", &items).await
            };

            match result {
                Ok(_) => outcomes.push(msg.ack()),
                Err(e) => {
                    let _ = msg.nack();
                    return Err(PublishError::Backend(Box::new(e)));
                }
            }
        }

        Ok(outcomes)
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        Ok(())
    }
}
