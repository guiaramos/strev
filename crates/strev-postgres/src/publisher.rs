use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use serde_json::{Map, Value};
use sqlx::{PgPool, Postgres, QueryBuilder};
use strev::{CloseError, Delay, DelayedPublisher, Message, Outcome, PublishError, Topic};

use crate::schema::ensure_schema;

/// Rows per multi-row INSERT. Keeps bind parameters well under Postgres's 65535 cap while
/// amortizing round-trips for high publish rates.
const INSERT_CHUNK: usize = 1000;

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

    /// Publish within a caller-supplied connection, typically an open transaction
    /// (`&mut *tx`). The messages are inserted in that transaction, so they commit
    /// atomically with the caller's other writes - the transactional outbox pattern. A normal
    /// [`PostgresSubscriber`](crate::PostgresSubscriber) then delivers them once committed.
    pub async fn publish_tx(
        &self,
        conn: &mut sqlx::PgConnection,
        topic: &Topic,
        messages: Vec<Message>,
    ) -> Result<Vec<Outcome>, PublishError> {
        let mut failure = None;
        for chunk in messages.chunks(INSERT_CHUNK) {
            let mut builder = build_messages_insert(topic.as_str(), chunk);
            if let Err(e) = builder.build().execute(&mut *conn).await {
                failure = Some(e);
                break;
            }
        }
        settle(messages, failure)
    }
}

#[async_trait]
impl strev::Publisher for PostgresPublisher {
    async fn publish(
        &self,
        topic: &Topic,
        messages: Vec<Message>,
    ) -> Result<Vec<Outcome>, PublishError> {
        let mut failure = None;
        for chunk in messages.chunks(INSERT_CHUNK) {
            let mut builder = build_messages_insert(topic.as_str(), chunk);
            if let Err(e) = builder.build().execute(&self.pool).await {
                failure = Some(e);
                break;
            }
        }
        settle(messages, failure)
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

        let mut failure = None;
        for chunk in messages.chunks(INSERT_CHUNK) {
            let mut builder = build_delayed_insert(topic.as_str(), chunk, deliver_after);
            if let Err(e) = builder.build().execute(&self.pool).await {
                failure = Some(e);
                break;
            }
        }
        settle(messages, failure)
    }
}

fn build_messages_insert<'a>(topic: &'a str, chunk: &'a [Message]) -> QueryBuilder<'a, Postgres> {
    let mut builder =
        QueryBuilder::new("INSERT INTO strev_messages (topic, uuid, payload, metadata) ");
    builder.push_values(chunk, |mut b, msg| {
        b.push_bind(topic)
            .push_bind(msg.uuid().to_string())
            .push_bind(msg.payload().to_vec())
            .push_bind(Value::Object(metadata_to_json(msg)));
    });
    builder
}

fn build_delayed_insert<'a>(
    topic: &'a str,
    chunk: &'a [Message],
    deliver_after: f64,
) -> QueryBuilder<'a, Postgres> {
    let mut builder = QueryBuilder::new(
        "INSERT INTO strev_delayed_messages (topic, uuid, payload, metadata, deliver_after) ",
    );
    builder.push_values(chunk, |mut b, msg| {
        b.push_bind(topic)
            .push_bind(msg.uuid().to_string())
            .push_bind(msg.payload().to_vec())
            .push_bind(Value::Object(metadata_to_json(msg)))
            .push("to_timestamp(")
            .push_bind_unseparated(deliver_after)
            .push_unseparated(")");
    });
    builder
}

/// Ack every message on success, or nack every message and return the error.
fn settle(
    messages: Vec<Message>,
    failure: Option<sqlx::Error>,
) -> Result<Vec<Outcome>, PublishError> {
    match failure {
        None => Ok(messages.into_iter().map(Message::ack).collect()),
        Some(e) => {
            for msg in messages {
                let _ = msg.nack();
            }
            Err(PublishError::Backend(Box::new(e)))
        }
    }
}

fn metadata_to_json(msg: &Message) -> Map<String, Value> {
    let mut map = Map::new();
    for (key, value) in msg.metadata().iter() {
        map.insert(key.to_string(), Value::String(value.to_string()));
    }
    map
}
