use sqlx::PgPool;

const MESSAGES_DDL: &str = "CREATE TABLE IF NOT EXISTS strev_messages (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    uuid TEXT NOT NULL,
    payload BYTEA NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)";

const MESSAGES_INDEX_DDL: &str =
    "CREATE INDEX IF NOT EXISTS strev_messages_topic_id ON strev_messages (topic, id)";

const OFFSETS_DDL: &str = "CREATE TABLE IF NOT EXISTS strev_offsets (
    consumer_group TEXT NOT NULL,
    topic TEXT NOT NULL,
    last_id BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (consumer_group, topic)
)";

const SCHEMA_LOCK_KEY: i64 = 0x_7374_7265_7600;

pub(crate) async fn ensure_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(SCHEMA_LOCK_KEY)
        .execute(&mut *tx)
        .await?;
    sqlx::query(MESSAGES_DDL).execute(&mut *tx).await?;
    sqlx::query(MESSAGES_INDEX_DDL).execute(&mut *tx).await?;
    sqlx::query(OFFSETS_DDL).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}
