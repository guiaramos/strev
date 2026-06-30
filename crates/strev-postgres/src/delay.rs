use std::time::Duration;

use sqlx::PgPool;
use strev::PublishError;
use tokio_util::sync::CancellationToken;

use crate::schema::ensure_schema;

const PROMOTE_SQL: &str = "WITH due AS (
    DELETE FROM strev_delayed_messages
    WHERE id IN (
        SELECT id FROM strev_delayed_messages
        WHERE deliver_after <= now()
        ORDER BY deliver_after
        LIMIT $1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING topic, uuid, payload, metadata
)
INSERT INTO strev_messages (topic, uuid, payload, metadata)
SELECT topic, uuid, payload, metadata FROM due";

/// Configuration for a [`PostgresDelayPromoter`].
pub struct PostgresDelayPromoterConfig {
    pub pool: PgPool,
    pub poll_interval: Duration,
    pub batch_size: i64,
}

impl PostgresDelayPromoterConfig {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            poll_interval: Duration::from_millis(200),
            batch_size: 100,
        }
    }
}

/// Moves due messages staged by [`publish_after`](strev::DelayedPublisher::publish_after)
/// into their live topics. The claim and the insert happen in one statement, so promotion
/// is exactly-once and several promoters can run concurrently (`FOR UPDATE SKIP LOCKED`).
pub struct PostgresDelayPromoter {
    pool: PgPool,
    poll_interval: Duration,
    batch_size: i64,
}

impl PostgresDelayPromoter {
    pub async fn new(config: PostgresDelayPromoterConfig) -> Result<Self, PublishError> {
        ensure_schema(&config.pool)
            .await
            .map_err(|e| PublishError::Backend(Box::new(e)))?;

        Ok(Self {
            pool: config.pool,
            poll_interval: config.poll_interval,
            batch_size: config.batch_size,
        })
    }

    pub async fn run(self, shutdown: CancellationToken) {
        loop {
            match self.promote_once().await {
                Ok(count) if count > 0 => continue,
                _ => {}
            }

            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(self.poll_interval) => {}
            }
        }
    }

    async fn promote_once(&self) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(PROMOTE_SQL)
            .bind(self.batch_size)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}
