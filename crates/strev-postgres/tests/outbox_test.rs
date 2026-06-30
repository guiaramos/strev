use std::time::Duration;

use bytes::Bytes;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use strev::{Message, Subscriber, Topic};
use strev_postgres::{
    PostgresPublisher, PostgresPublisherConfig, PostgresSubscriber, PostgresSubscriberConfig,
};
use tokio_stream::StreamExt;
use uuid::Uuid;

async fn pg_pool() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@127.0.0.1:5432/postgres".into());
    PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(3))
        .connect(&url)
        .await
        .ok()
}

#[tokio::test]
async fn committed_outbox_message_is_delivered() {
    let Some(pool) = pg_pool().await else {
        return;
    };
    let topic = Topic::new(format!("outbox-{}", Uuid::new_v4()));

    let subscriber = PostgresSubscriber::new(PostgresSubscriberConfig::new(pool.clone(), "g"));
    let mut stream = subscriber.subscribe(&topic).await.unwrap();

    let publisher = PostgresPublisher::new(PostgresPublisherConfig::new(pool.clone()))
        .await
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    publisher
        .publish_tx(
            &mut tx,
            &topic,
            vec![Message::new(Bytes::from("committed"))],
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("timeout")
        .expect("stream ended");
    assert_eq!(received.payload().as_ref(), b"committed");
    let _ = received.ack();
}

#[tokio::test]
async fn rolled_back_outbox_message_is_not_delivered() {
    let Some(pool) = pg_pool().await else {
        return;
    };
    let topic = Topic::new(format!("outbox-{}", Uuid::new_v4()));

    let subscriber = PostgresSubscriber::new(PostgresSubscriberConfig::new(pool.clone(), "g"));
    let mut stream = subscriber.subscribe(&topic).await.unwrap();

    let publisher = PostgresPublisher::new(PostgresPublisherConfig::new(pool.clone()))
        .await
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    publisher
        .publish_tx(
            &mut tx,
            &topic,
            vec![Message::new(Bytes::from("rolled-back"))],
        )
        .await
        .unwrap();
    tx.rollback().await.unwrap();

    let result = tokio::time::timeout(Duration::from_secs(1), stream.next()).await;
    assert!(result.is_err(), "rolled-back message must not be delivered");
}
