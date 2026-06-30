use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use redis::AsyncCommands;
use strev::{
    AckReceiver, CloseError, ConsumerLag, Disposition, LagError, Message, MessageStream,
    SubscribeError, Topic,
};

use crate::marshaller::{DefaultMarshaller, Marshaller};

pub struct RedisSubscriberConfig {
    pub client: redis::Client,
    pub consumer_group: String,
    pub consumer_name: String,
    pub marshaller: Arc<dyn Marshaller>,
    pub block_duration: Duration,
    pub batch_size: usize,
    pub buffer_size: usize,
    pub claim_idle: Duration,
}

impl RedisSubscriberConfig {
    pub fn new(client: redis::Client, consumer_group: impl Into<String>) -> Self {
        let consumer_name = format!("strev-{}", uuid::Uuid::new_v4());
        Self {
            client,
            consumer_group: consumer_group.into(),
            consumer_name,
            marshaller: Arc::new(DefaultMarshaller),
            block_duration: Duration::from_secs(2),
            batch_size: 10,
            buffer_size: 64,
            claim_idle: Duration::from_secs(60),
        }
    }
}

pub struct RedisSubscriber {
    config: Arc<RedisSubscriberConfig>,
}

impl RedisSubscriber {
    pub fn new(config: RedisSubscriberConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

#[async_trait]
impl strev::Subscriber for RedisSubscriber {
    async fn subscribe(&self, topic: &Topic) -> Result<MessageStream, SubscribeError> {
        let (tx, stream) = MessageStream::channel(self.config.buffer_size);
        let config = self.config.clone();
        let stream_key = topic.as_str().to_string();

        let conn = config
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| SubscribeError::Backend(Box::new(e)))?;

        ensure_consumer_group(&conn, &stream_key, &config.consumer_group)
            .await
            .map_err(|e| SubscribeError::Backend(Box::new(e)))?;

        tokio::spawn(async move {
            let mut conn = conn;
            let group = config.consumer_group.clone();
            let consumer = config.consumer_name.clone();
            let block_ms = config.block_duration.as_millis() as usize;
            let claim_idle_ms = config.claim_idle.as_millis() as usize;
            let count = config.batch_size;

            loop {
                if tx.is_closed() {
                    break;
                }

                let result: Result<redis::Value, _> = redis::cmd("XREADGROUP")
                    .arg("GROUP")
                    .arg(&group)
                    .arg(&consumer)
                    .arg("COUNT")
                    .arg(count)
                    .arg("BLOCK")
                    .arg(block_ms)
                    .arg("STREAMS")
                    .arg(&stream_key)
                    .arg(">")
                    .query_async(&mut conn)
                    .await;

                let mut entries = match result {
                    Ok(val) => parse_stream_response(val),
                    Err(_) => {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

                let claimed: Result<redis::Value, _> = redis::cmd("XAUTOCLAIM")
                    .arg(&stream_key)
                    .arg(&group)
                    .arg(&consumer)
                    .arg(claim_idle_ms)
                    .arg("0")
                    .query_async(&mut conn)
                    .await;
                if let Ok(val) = claimed {
                    entries.extend(parse_claim_response(val));
                }

                for (entry_id, fields) in entries {
                    let (payload, mut metadata) = match config.marshaller.unmarshal(&fields) {
                        Some(v) => v,
                        None => {
                            xack(&mut conn, &stream_key, &group, &entry_id).await;
                            continue;
                        }
                    };

                    metadata.set("redis_stream_id", entry_id.clone());

                    let msg = Message::with_metadata(payload, metadata);
                    let retry = msg.copy();
                    let (leased, ack) = msg.leased();

                    if tx.send(leased).await.is_err() {
                        reenqueue(
                            &mut conn,
                            &config.marshaller,
                            &stream_key,
                            &group,
                            &entry_id,
                            &retry,
                        )
                        .await;
                        break;
                    }

                    tokio::spawn(resolve_ack(
                        conn.clone(),
                        config.marshaller.clone(),
                        stream_key.clone(),
                        group.clone(),
                        entry_id,
                        retry,
                        ack,
                    ));
                }
            }
        });

        Ok(stream)
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        Ok(())
    }
}

#[async_trait]
impl ConsumerLag for RedisSubscriber {
    async fn lag(&self, topic: &Topic) -> Result<u64, LagError> {
        let mut conn = self
            .config
            .client
            .get_multiplexed_async_connection()
            .await?;

        let groups: redis::Value = match redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(topic.as_str())
            .query_async(&mut conn)
            .await
        {
            Ok(value) => value,
            Err(_) => return Ok(0),
        };

        Ok(group_lag(&groups, &self.config.consumer_group))
    }
}

fn value_to_text(value: &redis::Value) -> Option<String> {
    match value {
        redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
        redis::Value::SimpleString(text) => Some(text.clone()),
        _ => None,
    }
}

fn value_to_i64(value: &redis::Value) -> Option<i64> {
    match value {
        redis::Value::Int(n) => Some(*n),
        other => value_to_text(other).and_then(|s| s.parse().ok()),
    }
}

fn group_lag(value: &redis::Value, group: &str) -> u64 {
    let redis::Value::Array(entries) = value else {
        return 0;
    };

    for entry in entries {
        let redis::Value::Array(fields) = entry else {
            continue;
        };

        let mut name = None;
        let mut lag = None;
        let mut i = 0;
        while i + 1 < fields.len() {
            match value_to_text(&fields[i]).as_deref() {
                Some("name") => name = value_to_text(&fields[i + 1]),
                Some("lag") => lag = value_to_i64(&fields[i + 1]),
                _ => {}
            }
            i += 2;
        }

        if name.as_deref() == Some(group) {
            return lag.unwrap_or(0).max(0) as u64;
        }
    }

    0
}

async fn ensure_consumer_group(
    conn: &redis::aio::MultiplexedConnection,
    stream_key: &str,
    group: &str,
) -> Result<(), redis::RedisError> {
    let mut conn = conn.clone();
    let result: Result<String, redis::RedisError> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(stream_key)
        .arg(group)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(&mut conn)
        .await;

    match result {
        Ok(_) => Ok(()),
        Err(e) if e.to_string().contains("BUSYGROUP") => Ok(()),
        Err(e) => Err(e),
    }
}

async fn xack(conn: &mut redis::aio::MultiplexedConnection, key: &str, group: &str, id: &str) {
    let _: Result<(), _> = redis::cmd("XACK")
        .arg(key)
        .arg(group)
        .arg(id)
        .query_async(conn)
        .await;
}

/// Republish a message as a new stream entry and clear the original from the pending list,
/// so a nacked (or undeliverable) message is redelivered immediately rather than waiting for
/// `XAUTOCLAIM`.
async fn reenqueue(
    conn: &mut redis::aio::MultiplexedConnection,
    marshaller: &Arc<dyn Marshaller>,
    stream_key: &str,
    group: &str,
    entry_id: &str,
    message: &Message,
) {
    let fields = marshaller.marshal(message);
    let items: Vec<(&str, &[u8])> = fields
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_slice()))
        .collect();
    let _: Result<String, _> = conn.xadd(stream_key, "*", &items).await;
    xack(conn, stream_key, group, entry_id).await;
}

async fn resolve_ack(
    mut conn: redis::aio::MultiplexedConnection,
    marshaller: Arc<dyn Marshaller>,
    stream_key: String,
    group: String,
    entry_id: String,
    retry: Message,
    ack: AckReceiver,
) {
    match ack.recv().await {
        Disposition::Ack => {
            xack(&mut conn, &stream_key, &group, &entry_id).await;
        }
        Disposition::Nack => {
            reenqueue(
                &mut conn,
                &marshaller,
                &stream_key,
                &group,
                &entry_id,
                &retry,
            )
            .await;
        }
    }
}

fn parse_claim_response(val: redis::Value) -> Vec<(String, Vec<(String, redis::Value)>)> {
    let array = match val {
        redis::Value::Array(array) => array,
        _ => return Vec::new(),
    };

    match array.into_iter().nth(1) {
        Some(messages) => parse_entries(messages),
        None => Vec::new(),
    }
}

fn parse_stream_response(val: redis::Value) -> Vec<(String, Vec<(String, redis::Value)>)> {
    let mut entries = Vec::new();

    let streams = match val {
        redis::Value::Array(streams) => streams,
        _ => return entries,
    };

    for stream in streams {
        let stream_arr = match stream {
            redis::Value::Array(arr) => arr,
            _ => continue,
        };

        if let Some(messages) = stream_arr.into_iter().nth(1) {
            entries.extend(parse_entries(messages));
        }
    }

    entries
}

fn parse_entries(messages: redis::Value) -> Vec<(String, Vec<(String, redis::Value)>)> {
    let messages = match messages {
        redis::Value::Array(messages) => messages,
        _ => return Vec::new(),
    };

    let mut entries = Vec::new();
    for message in messages {
        let msg_arr = match message {
            redis::Value::Array(arr) => arr,
            _ => continue,
        };

        if msg_arr.len() < 2 {
            continue;
        }

        let entry_id = match &msg_arr[0] {
            redis::Value::BulkString(b) => String::from_utf8_lossy(b).to_string(),
            redis::Value::SimpleString(s) => s.clone(),
            _ => continue,
        };

        let field_arr = match &msg_arr[1] {
            redis::Value::Array(arr) => arr,
            _ => continue,
        };

        let mut fields = Vec::new();
        let mut i = 0;
        while i + 1 < field_arr.len() {
            let key = match &field_arr[i] {
                redis::Value::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                redis::Value::SimpleString(s) => s.clone(),
                _ => {
                    i += 2;
                    continue;
                }
            };
            fields.push((key, field_arr[i + 1].clone()));
            i += 2;
        }

        entries.push((entry_id, fields));
    }

    entries
}
