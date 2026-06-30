use std::time::Duration;

use bytes::Bytes;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use strev::{Message, Publisher, Subscriber, Topic};
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

#[tokio::test]
async fn lease_timeout_reclaims_unacked_message() {
    let Some(pool) = pg_pool().await else {
        return;
    };
    let topic = Topic::new(format!("lease-{}", Uuid::new_v4()));

    let mut config = PostgresSubscriberConfig::new(pool.clone(), "g");
    config.visibility_timeout = Duration::from_millis(500);
    config.poll_interval = Duration::from_millis(50);
    let subscriber = PostgresSubscriber::new(config);
    let mut stream = subscriber.subscribe(&topic).await.unwrap();

    let publisher = PostgresPublisher::new(PostgresPublisherConfig::new(pool.clone()))
        .await
        .unwrap();
    publisher
        .publish(&topic, vec![Message::new(Bytes::from("hold-me"))])
        .await
        .unwrap();

    let first = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("timeout")
        .expect("stream ended");
    assert_eq!(first.payload().as_ref(), b"hold-me");
    // Hold `first` without acking: the lease must expire and the message be re-claimed.

    let second = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("timeout: lease was not reclaimed")
        .expect("stream ended");
    assert_eq!(second.payload().as_ref(), b"hold-me");
    let _ = second.ack();
}

#[tokio::test]
async fn reports_consumer_lag() {
    use strev::ConsumerLag;

    let Some(pool) = pg_pool().await else {
        return;
    };
    let topic = Topic::new(format!("lag-{}", Uuid::new_v4()));

    let subscriber = PostgresSubscriber::new(PostgresSubscriberConfig::new(pool.clone(), "g"));
    let mut stream = subscriber.subscribe(&topic).await.unwrap();

    let publisher = PostgresPublisher::new(PostgresPublisherConfig::new(pool.clone()))
        .await
        .unwrap();
    let messages = (0..5)
        .map(|i| Message::new(Bytes::from(format!("m-{i}"))))
        .collect();
    publisher.publish(&topic, messages).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(subscriber.lag(&topic).await.unwrap(), 5);

    for _ in 0..5 {
        let msg = tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .expect("timeout")
            .expect("stream ended");
        let _ = msg.ack();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(subscriber.lag(&topic).await.unwrap(), 0);
}

#[tokio::test]
async fn retention_purges_fully_consumed_messages() {
    use strev_postgres::{PostgresRetention, PostgresRetentionConfig};

    let Some(pool) = pg_pool().await else {
        return;
    };
    let topic = Topic::new(format!("retain-{}", Uuid::new_v4()));

    let subscriber = PostgresSubscriber::new(PostgresSubscriberConfig::new(pool.clone(), "g"));
    let mut stream = subscriber.subscribe(&topic).await.unwrap();

    let publisher = PostgresPublisher::new(PostgresPublisherConfig::new(pool.clone()))
        .await
        .unwrap();
    let messages = (0..5)
        .map(|i| Message::new(Bytes::from(format!("m-{i}"))))
        .collect();
    publisher.publish(&topic, messages).await.unwrap();

    for _ in 0..5 {
        let msg = tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .expect("timeout")
            .expect("stream ended");
        let _ = msg.ack();
    }
    // Let the watermark advance over the acked prefix via a poll.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let retention = PostgresRetention::new(PostgresRetentionConfig::new(pool.clone()))
        .await
        .unwrap();
    let token = tokio_util::sync::CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { retention.run(tc).await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    token.cancel();
    handle.await.unwrap();

    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM strev_messages WHERE topic = $1")
        .bind(topic.as_str())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(remaining, 0, "fully-consumed messages should be purged");
}
