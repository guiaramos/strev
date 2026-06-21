use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;
use sqlx::{PgPool, Row};
use strev::{CloseError, Message, MessageStream, Metadata, SubscribeError, Topic};
use tokio::sync::mpsc::Sender;

use crate::schema::ensure_schema;

pub struct PostgresSubscriberConfig {
    pub pool: PgPool,
    pub consumer_group: String,
    pub poll_interval: Duration,
    pub batch_size: i64,
    pub buffer_size: usize,
}

impl PostgresSubscriberConfig {
    pub fn new(pool: PgPool, consumer_group: impl Into<String>) -> Self {
        Self {
            pool,
            consumer_group: consumer_group.into(),
            poll_interval: Duration::from_millis(200),
            batch_size: 100,
            buffer_size: 64,
        }
    }
}

pub struct PostgresSubscriber {
    config: Arc<PostgresSubscriberConfig>,
}

impl PostgresSubscriber {
    pub fn new(config: PostgresSubscriberConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

#[async_trait]
impl strev::Subscriber for PostgresSubscriber {
    async fn subscribe(&self, topic: &Topic) -> Result<MessageStream, SubscribeError> {
        let config = self.config.clone();
        let topic = topic.as_str().to_string();

        ensure_schema(&config.pool)
            .await
            .map_err(|e| SubscribeError::Backend(Box::new(e)))?;

        sqlx::query(
            "INSERT INTO strev_offsets (consumer_group, topic, last_id) VALUES ($1, $2, 0) ON CONFLICT DO NOTHING",
        )
        .bind(&config.consumer_group)
        .bind(&topic)
        .execute(&config.pool)
        .await
        .map_err(|e| SubscribeError::Backend(Box::new(e)))?;

        let (sender, stream) = MessageStream::channel(config.buffer_size);

        tokio::spawn(async move {
            loop {
                if sender.is_closed() {
                    break;
                }

                match poll_once(&config, &topic, &sender).await {
                    Ok(count) if count > 0 => continue,
                    Ok(_) => tokio::time::sleep(config.poll_interval).await,
                    Err(_) => tokio::time::sleep(config.poll_interval).await,
                }
            }
        });

        Ok(stream)
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        Ok(())
    }
}

async fn poll_once(
    config: &PostgresSubscriberConfig,
    topic: &str,
    sender: &Sender<Message>,
) -> Result<usize, sqlx::Error> {
    let mut tx = config.pool.begin().await?;

    let locked = sqlx::query(
        "SELECT last_id FROM strev_offsets WHERE consumer_group = $1 AND topic = $2 FOR UPDATE SKIP LOCKED",
    )
    .bind(&config.consumer_group)
    .bind(topic)
    .fetch_optional(&mut *tx)
    .await?;

    let last_id: i64 = match locked {
        Some(row) => row.try_get("last_id")?,
        None => {
            tx.rollback().await?;
            return Ok(0);
        }
    };

    let rows = sqlx::query(
        "SELECT id, payload, metadata FROM strev_messages WHERE topic = $1 AND id > $2 ORDER BY id ASC LIMIT $3",
    )
    .bind(topic)
    .bind(last_id)
    .bind(config.batch_size)
    .fetch_all(&mut *tx)
    .await?;

    if rows.is_empty() {
        tx.rollback().await?;
        return Ok(0);
    }

    let mut max_id = last_id;
    for row in &rows {
        let id: i64 = row.try_get("id")?;
        let payload: Vec<u8> = row.try_get("payload")?;
        let metadata_json: Value = row.try_get("metadata")?;

        let mut metadata = Metadata::new();
        if let Value::Object(map) = metadata_json {
            for (key, value) in map {
                if let Value::String(text) = value {
                    metadata.set(key, text);
                }
            }
        }

        let message = Message::with_metadata(Bytes::from(payload), metadata);
        if sender.send(message).await.is_err() {
            tx.rollback().await?;
            return Ok(0);
        }

        max_id = id;
    }

    sqlx::query("UPDATE strev_offsets SET last_id = $1 WHERE consumer_group = $2 AND topic = $3")
        .bind(max_id)
        .bind(&config.consumer_group)
        .bind(topic)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(rows.len())
}
