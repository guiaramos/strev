use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use strev::{CloseError, Message, MessageStream, SubscribeError, Topic};

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
            let group = &config.consumer_group;
            let consumer = &config.consumer_name;
            let block_ms = config.block_duration.as_millis() as usize;
            let count = config.batch_size;

            loop {
                if tx.is_closed() {
                    break;
                }

                let result: Result<redis::Value, _> = redis::cmd("XREADGROUP")
                    .arg("GROUP")
                    .arg(group)
                    .arg(consumer)
                    .arg("COUNT")
                    .arg(count)
                    .arg("BLOCK")
                    .arg(block_ms)
                    .arg("STREAMS")
                    .arg(&stream_key)
                    .arg(">")
                    .query_async(&mut conn)
                    .await;

                let entries = match result {
                    Ok(val) => parse_stream_response(val),
                    Err(_) => {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

                for (entry_id, fields) in entries {
                    let (payload, mut metadata) = match config.marshaller.unmarshal(&fields) {
                        Some(v) => v,
                        None => {
                            let _: Result<(), _> = redis::cmd("XACK")
                                .arg(&stream_key)
                                .arg(group)
                                .arg(&entry_id)
                                .query_async(&mut conn)
                                .await;
                            continue;
                        }
                    };

                    metadata.set("redis_stream_id", entry_id.clone());

                    let msg = Message::with_metadata(payload, metadata);

                    if tx.send(msg).await.is_err() {
                        break;
                    }

                    let _: Result<(), _> = redis::cmd("XACK")
                        .arg(&stream_key)
                        .arg(group)
                        .arg(&entry_id)
                        .query_async(&mut conn)
                        .await;
                }
            }
        });

        Ok(stream)
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        Ok(())
    }
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

fn parse_stream_response(val: redis::Value) -> Vec<(String, Vec<(String, redis::Value)>)> {
    let mut entries = Vec::new();

    let streams = match val {
        redis::Value::Array(streams) => streams,
        redis::Value::Nil => return entries,
        _ => return entries,
    };

    for stream in streams {
        let stream_arr = match stream {
            redis::Value::Array(arr) => arr,
            _ => continue,
        };

        if stream_arr.len() < 2 {
            continue;
        }

        let messages = match &stream_arr[1] {
            redis::Value::Array(msgs) => msgs,
            _ => continue,
        };

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
    }

    entries
}
