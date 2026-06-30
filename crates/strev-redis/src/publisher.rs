use std::sync::Arc;

use async_trait::async_trait;
use redis::AsyncCommands;
use strev::{CloseError, Delay, DelayedPublisher, Message, Outcome, PublishError, Topic};

use crate::delay::{DELAYED_TOPICS_SET, delayed_zset_key, due_millis, encode};
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
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.conn.clone();
        let stream_key = topic.as_str();

        // Pipeline every XADD into a single round-trip; arg() copies into the command buffer,
        // so per-message field buffers can be dropped each iteration.
        let mut pipe = redis::pipe();
        for msg in &messages {
            let fields = self.marshaller.marshal(msg);
            let command = pipe.cmd("XADD").arg(stream_key);
            if let Some(maxlen) = self.max_stream_len {
                command.arg("MAXLEN").arg("~").arg(maxlen);
            }
            command.arg("*");
            for (key, value) in &fields {
                command.arg(key.as_str()).arg(value.as_slice());
            }
        }

        let result: Result<Vec<redis::Value>, _> = pipe.query_async(&mut conn).await;
        match result {
            Ok(_) => Ok(messages.into_iter().map(Message::ack).collect()),
            Err(e) => {
                for msg in messages {
                    let _ = msg.nack();
                }
                Err(PublishError::Backend(Box::new(e)))
            }
        }
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        Ok(())
    }
}

#[async_trait]
impl DelayedPublisher for RedisPublisher {
    async fn publish_after(
        &self,
        topic: &Topic,
        messages: Vec<Message>,
        delay: Delay,
    ) -> Result<Vec<Outcome>, PublishError> {
        let mut conn = self.conn.clone();
        let key = delayed_zset_key(topic.as_str());
        let score = due_millis(delay);
        let mut outcomes = Vec::with_capacity(messages.len());

        for msg in messages {
            let member = match encode(&msg) {
                Ok(member) => member,
                Err(e) => {
                    let _ = msg.nack();
                    return Err(PublishError::Backend(Box::new(e)));
                }
            };

            let registered: Result<i64, redis::RedisError> =
                conn.sadd(DELAYED_TOPICS_SET, topic.as_str()).await;
            if let Err(e) = registered {
                let _ = msg.nack();
                return Err(PublishError::Backend(Box::new(e)));
            }

            let staged: Result<i64, redis::RedisError> = conn.zadd(&key, &member, score).await;
            match staged {
                Ok(_) => outcomes.push(msg.ack()),
                Err(e) => {
                    let _ = msg.nack();
                    return Err(PublishError::Backend(Box::new(e)));
                }
            }
        }

        Ok(outcomes)
    }
}
