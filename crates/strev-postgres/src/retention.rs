use std::time::Duration;

use sqlx::PgPool;
use strev::PublishError;
use tokio_util::sync::CancellationToken;

use crate::schema::ensure_schema;

/// Deletes messages every group on the topic has already acked (id at or below the minimum
/// watermark), in bounded batches.
const PURGE_CONSUMED_SQL: &str = "DELETE FROM strev_messages WHERE ctid IN (
    SELECT m.ctid
    FROM strev_messages m
    JOIN (SELECT topic, MIN(last_id) AS min_id FROM strev_offsets GROUP BY topic) w
        ON m.topic = w.topic
    WHERE m.id <= w.min_id
    LIMIT $1
)";

/// Deletes messages older than a max age regardless of consumption (log-style retention).
const PURGE_AGED_SQL: &str = "DELETE FROM strev_messages WHERE ctid IN (
    SELECT ctid FROM strev_messages
    WHERE created_at < now() - ($1 * interval '1 second')
    LIMIT $2
)";

/// Configuration for [`PostgresRetention`].
pub struct PostgresRetentionConfig {
    pub pool: PgPool,
    pub interval: Duration,
    pub batch_size: i64,
    pub max_age: Option<Duration>,
}

impl PostgresRetentionConfig {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            interval: Duration::from_secs(60),
            batch_size: 10_000,
            max_age: None,
        }
    }

    /// Also delete messages older than `max_age` even if not yet consumed (log-style
    /// retention), bounding growth on topics without an active durable consumer.
    pub fn max_age(mut self, max_age: Duration) -> Self {
        self.max_age = Some(max_age);
        self
    }
}

/// Purges `strev_messages` so the durable log stays bounded under high publish rates.
/// Removes messages acked by every group on their topic, and optionally messages past a
/// max age. Run one instance; it holds no per-topic state.
pub struct PostgresRetention {
    pool: PgPool,
    interval: Duration,
    batch_size: i64,
    max_age: Option<Duration>,
}

impl PostgresRetention {
    pub async fn new(config: PostgresRetentionConfig) -> Result<Self, PublishError> {
        ensure_schema(&config.pool)
            .await
            .map_err(|e| PublishError::Backend(Box::new(e)))?;

        Ok(Self {
            pool: config.pool,
            interval: config.interval,
            batch_size: config.batch_size,
            max_age: config.max_age,
        })
    }

    pub async fn run(self, shutdown: CancellationToken) {
        loop {
            match self.purge_once().await {
                Ok(deleted) if deleted > 0 => continue,
                _ => {}
            }

            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(self.interval) => {}
            }
        }
    }

    async fn purge_once(&self) -> Result<u64, sqlx::Error> {
        let mut deleted = sqlx::query(PURGE_CONSUMED_SQL)
            .bind(self.batch_size)
            .execute(&self.pool)
            .await?
            .rows_affected();

        if let Some(max_age) = self.max_age {
            deleted += sqlx::query(PURGE_AGED_SQL)
                .bind(max_age.as_secs_f64())
                .bind(self.batch_size)
                .execute(&self.pool)
                .await?
                .rows_affected();
        }

        Ok(deleted)
    }
}
