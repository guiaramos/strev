use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;
use sqlx::{PgPool, Row};
use strev::{
    AckReceiver, CloseError, ConsumerLag, Disposition, LagError, Message, MessageStream, Metadata,
    SubscribeError, Topic,
};
use tokio::sync::mpsc::Sender;

use crate::schema::ensure_schema;

pub struct PostgresSubscriberConfig {
    pub pool: PgPool,
    pub consumer_group: String,
    pub poll_interval: Duration,
    pub batch_size: i64,
    pub buffer_size: usize,
    pub visibility_timeout: Duration,
}

impl PostgresSubscriberConfig {
    pub fn new(pool: PgPool, consumer_group: impl Into<String>) -> Self {
        Self {
            pool,
            consumer_group: consumer_group.into(),
            poll_interval: Duration::from_millis(200),
            batch_size: 100,
            buffer_size: 64,
            visibility_timeout: Duration::from_secs(30),
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
                    _ => tokio::time::sleep(config.poll_interval).await,
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
impl ConsumerLag for PostgresSubscriber {
    async fn lag(&self, topic: &Topic) -> Result<u64, LagError> {
        let lag: i64 = sqlx::query(
            "SELECT COUNT(*) AS lag
             FROM strev_messages m
             LEFT JOIN strev_consume c
                ON c.consumer_group = $2 AND c.topic = $1 AND c.message_id = m.id
             WHERE m.topic = $1
               AND m.id > COALESCE((SELECT last_id FROM strev_offsets WHERE consumer_group = $2 AND topic = $1), 0)
               AND (c.message_id IS NULL OR NOT c.acked)",
        )
        .bind(topic.as_str())
        .bind(&self.config.consumer_group)
        .fetch_one(&self.config.pool)
        .await?
        .try_get("lag")?;

        Ok(lag.max(0) as u64)
    }
}

const ADVANCE_SCAN_LIMIT: i64 = 1000;

const CLAIM_SQL: &str = "WITH claimable AS (
    SELECT m.id, m.payload, m.metadata
    FROM strev_messages m
    LEFT JOIN strev_consume c
        ON c.consumer_group = $1 AND c.topic = $2 AND c.message_id = m.id
    WHERE m.topic = $2
        AND m.id > $3
        AND (c.message_id IS NULL OR (NOT c.acked AND c.locked_until < now()))
    ORDER BY m.id ASC
    LIMIT $4
),
leased AS (
    INSERT INTO strev_consume (consumer_group, topic, message_id, locked_until, acked)
    SELECT $1, $2, id, now() + ($5 * interval '1 millisecond'), false FROM claimable
    ON CONFLICT (consumer_group, topic, message_id)
        DO UPDATE SET locked_until = EXCLUDED.locked_until
    RETURNING message_id
)
SELECT claimable.id, claimable.payload, claimable.metadata
FROM claimable
JOIN leased ON leased.message_id = claimable.id
ORDER BY claimable.id ASC";

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

    let visibility_ms = config.visibility_timeout.as_millis() as i64;
    let rows = sqlx::query(CLAIM_SQL)
        .bind(&config.consumer_group)
        .bind(topic)
        .bind(last_id)
        .bind(config.batch_size)
        .bind(visibility_ms)
        .fetch_all(&mut *tx)
        .await?;

    if rows.is_empty() {
        tx.rollback().await?;
        return Ok(0);
    }

    tx.commit().await?;

    let count = rows.len();
    for row in rows {
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

        let (message, ack) = Message::with_metadata(Bytes::from(payload), metadata).leased();

        if sender.send(message).await.is_err() {
            expire_lease(&config.pool, &config.consumer_group, topic, id).await;
            return Ok(0);
        }

        tokio::spawn(resolve_ack(
            config.pool.clone(),
            config.consumer_group.clone(),
            topic.to_string(),
            id,
            ack,
        ));
    }

    Ok(count)
}

async fn resolve_ack(
    pool: PgPool,
    group: String,
    topic: String,
    message_id: i64,
    ack: AckReceiver,
) {
    match ack.recv().await {
        Disposition::Ack => {
            let _ = commit_ack(&pool, &group, &topic, message_id).await;
        }
        Disposition::Nack => {
            expire_lease(&pool, &group, &topic, message_id).await;
        }
    }
}

/// Mark a message acked and, if it sits at the watermark head, advance the offset over the
/// contiguous acked run and prune the compacted rows.
async fn commit_ack(
    pool: &PgPool,
    group: &str,
    topic: &str,
    message_id: i64,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    let last_id: i64 = sqlx::query(
        "SELECT last_id FROM strev_offsets WHERE consumer_group = $1 AND topic = $2 FOR UPDATE",
    )
    .bind(group)
    .bind(topic)
    .fetch_one(&mut *tx)
    .await?
    .try_get("last_id")?;

    sqlx::query(
        "UPDATE strev_consume SET acked = true WHERE consumer_group = $1 AND topic = $2 AND message_id = $3",
    )
    .bind(group)
    .bind(topic)
    .bind(message_id)
    .execute(&mut *tx)
    .await?;

    // Advance the watermark over the contiguous acked prefix of this topic's messages.
    // Message ids are a global sequence, so a topic's ids are not consecutive integers;
    // walk the topic's actual message order rather than assuming id == last_id + 1.
    let rows = sqlx::query(
        "SELECT m.id, (c.acked IS TRUE) AS acked
         FROM strev_messages m
         LEFT JOIN strev_consume c
            ON c.consumer_group = $1 AND c.topic = $2 AND c.message_id = m.id
         WHERE m.topic = $2 AND m.id > $3
         ORDER BY m.id ASC
         LIMIT $4",
    )
    .bind(group)
    .bind(topic)
    .bind(last_id)
    .bind(ADVANCE_SCAN_LIMIT)
    .fetch_all(&mut *tx)
    .await?;

    let mut new_last = last_id;
    for row in rows {
        let id: i64 = row.try_get("id")?;
        let acked: bool = row.try_get("acked")?;
        if acked {
            new_last = id;
        } else {
            break;
        }
    }

    if new_last > last_id {
        sqlx::query(
            "UPDATE strev_offsets SET last_id = $1 WHERE consumer_group = $2 AND topic = $3",
        )
        .bind(new_last)
        .bind(group)
        .bind(topic)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "DELETE FROM strev_consume WHERE consumer_group = $1 AND topic = $2 AND message_id <= $3",
        )
        .bind(group)
        .bind(topic)
        .bind(new_last)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await
}

/// Expire the lease so the next poll re-claims the message (nack, shutdown, or timeout).
async fn expire_lease(pool: &PgPool, group: &str, topic: &str, message_id: i64) {
    let _ = sqlx::query(
        "UPDATE strev_consume SET locked_until = now() - interval '1 second' WHERE consumer_group = $1 AND topic = $2 AND message_id = $3 AND NOT acked",
    )
    .bind(group)
    .bind(topic)
    .bind(message_id)
    .execute(pool)
    .await;
}
