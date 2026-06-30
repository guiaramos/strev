use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;
use sqlx::{PgPool, Row};
use strev::{
    CloseError, ConsumerLag, Disposition, LagError, Message, MessageStream, Metadata,
    SubscribeError, Topic,
};
use tokio::sync::mpsc::{self, Sender};

use crate::schema::ensure_schema;

/// Verdicts buffered before a flush, and the channel depth feeding the flusher.
const MAX_ACK_BATCH: usize = 500;
const VERDICT_BUFFER: usize = 4096;

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
        let (verdict_tx, verdict_rx) = mpsc::channel::<(i64, Disposition)>(VERDICT_BUFFER);

        tokio::spawn(ack_flusher(
            config.pool.clone(),
            config.consumer_group.clone(),
            topic.clone(),
            verdict_rx,
        ));

        tokio::spawn(async move {
            loop {
                if sender.is_closed() {
                    break;
                }

                match poll_once(&config, &topic, &sender, &verdict_tx).await {
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
    verdicts: &Sender<(i64, Disposition)>,
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

    // Advance the watermark once per poll (under the offset lock) instead of on every ack,
    // so the contiguous-acked scan stays off the high-throughput ack path.
    let last_id = advance_watermark(&mut tx, &config.consumer_group, topic, last_id).await?;

    let visibility_ms = config.visibility_timeout.as_millis() as i64;
    let rows = sqlx::query(CLAIM_SQL)
        .bind(&config.consumer_group)
        .bind(topic)
        .bind(last_id)
        .bind(config.batch_size)
        .bind(visibility_ms)
        .fetch_all(&mut *tx)
        .await?;

    tx.commit().await?;

    if rows.is_empty() {
        return Ok(0);
    }

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

        // Forward the verdict to the flusher, which batches the database writes.
        let verdicts = verdicts.clone();
        tokio::spawn(async move {
            let disposition = ack.recv().await;
            let _ = verdicts.send((id, disposition)).await;
        });
    }

    Ok(count)
}

/// Drain verdicts and apply them in batched updates: one statement per flush for all acks
/// and one for all nacks, instead of a round-trip per message. Bursts are coalesced via
/// `try_recv` up to [`MAX_ACK_BATCH`].
async fn ack_flusher(
    pool: PgPool,
    group: String,
    topic: String,
    mut verdicts: mpsc::Receiver<(i64, Disposition)>,
) {
    while let Some((id, disposition)) = verdicts.recv().await {
        let mut acks = Vec::new();
        let mut nacks = Vec::new();
        match disposition {
            Disposition::Ack => acks.push(id),
            Disposition::Nack => nacks.push(id),
        }

        while acks.len() + nacks.len() < MAX_ACK_BATCH {
            match verdicts.try_recv() {
                Ok((id, Disposition::Ack)) => acks.push(id),
                Ok((id, Disposition::Nack)) => nacks.push(id),
                Err(_) => break,
            }
        }

        if !acks.is_empty() {
            let _ = sqlx::query(
                "UPDATE strev_consume SET acked = true WHERE consumer_group = $1 AND topic = $2 AND message_id = ANY($3)",
            )
            .bind(&group)
            .bind(&topic)
            .bind(&acks)
            .execute(&pool)
            .await;
        }

        if !nacks.is_empty() {
            let _ = sqlx::query(
                "UPDATE strev_consume SET locked_until = now() - interval '1 second' WHERE consumer_group = $1 AND topic = $2 AND message_id = ANY($3) AND NOT acked",
            )
            .bind(&group)
            .bind(&topic)
            .bind(&nacks)
            .execute(&pool)
            .await;
        }
    }
}

/// Advance the offset over the contiguous acked prefix of this topic's messages and prune
/// the compacted consume rows. Message ids are a global sequence, so a topic's ids are not
/// consecutive integers; walk the topic's actual message order. Runs under the offset lock
/// held by the caller. Returns the new watermark.
async fn advance_watermark(
    conn: &mut sqlx::PgConnection,
    group: &str,
    topic: &str,
    last_id: i64,
) -> Result<i64, sqlx::Error> {
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
    .fetch_all(&mut *conn)
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
        .execute(&mut *conn)
        .await?;

        sqlx::query(
            "DELETE FROM strev_consume WHERE consumer_group = $1 AND topic = $2 AND message_id <= $3",
        )
        .bind(group)
        .bind(topic)
        .bind(new_last)
        .execute(&mut *conn)
        .await?;
    }

    Ok(new_last)
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
