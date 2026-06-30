use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use serde_json::{Map, Value};
use sqlx::PgPool;
use strev::{CloseError, Delay, DelayedPublisher, Message, Outcome, PublishError, Topic};

use crate::schema::ensure_schema;

pub struct PostgresPublisherConfig {
    pub pool: PgPool,
}

impl PostgresPublisherConfig {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

pub struct PostgresPublisher {
    pool: PgPool,
}

impl PostgresPublisher {
    pub async fn new(config: PostgresPublisherConfig) -> Result<Self, PublishError> {
        ensure_schema(&config.pool)
            .await
            .map_err(|e| PublishError::Backend(Box::new(e)))?;
        Ok(Self { pool: config.pool })
    }
}

#[async_trait]
impl strev::Publisher for PostgresPublisher {
    async fn publish(
        &self,
        topic: &Topic,
        messages: Vec<Message>,
    ) -> Result<Vec<Outcome>, PublishError> {
        let mut outcomes = Vec::with_capacity(messages.len());

        for msg in messages {
            let metadata = metadata_to_json(&msg);

            let result =
                sqlx::query("INSERT INTO strev_messages (topic, uuid, payload, metadata) VALUES ($1, $2, $3, $4)")
                    .bind(topic.as_str())
                    .bind(msg.uuid().to_string())
                    .bind(msg.payload().as_ref())
                    .bind(Value::Object(metadata))
                    .execute(&self.pool)
                    .await;

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

#[async_trait]
impl DelayedPublisher for PostgresPublisher {
    async fn publish_after(
        &self,
        topic: &Topic,
        messages: Vec<Message>,
        delay: Delay,
    ) -> Result<Vec<Outcome>, PublishError> {
        let deliver_after = delay
            .not_before()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        let mut outcomes = Vec::with_capacity(messages.len());

        for msg in messages {
            let metadata = metadata_to_json(&msg);

            let result = sqlx::query(
                "INSERT INTO strev_delayed_messages (topic, uuid, payload, metadata, deliver_after) VALUES ($1, $2, $3, $4, to_timestamp($5))",
            )
            .bind(topic.as_str())
            .bind(msg.uuid().to_string())
            .bind(msg.payload().as_ref())
            .bind(Value::Object(metadata))
            .bind(deliver_after)
            .execute(&self.pool)
            .await;

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
}

fn metadata_to_json(msg: &Message) -> Map<String, Value> {
    let mut map = Map::new();
    for (key, value) in msg.metadata().iter() {
        map.insert(key.to_string(), Value::String(value.to_string()));
    }
    map
}
