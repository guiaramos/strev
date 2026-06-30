use std::time::{Duration, Instant};

use bytes::Bytes;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use strev::{Delay, DelayedPublisher, Message, Subscriber, Topic};
use strev_postgres::{
    PostgresDelayPromoter, PostgresDelayPromoterConfig, PostgresPublisher, PostgresPublisherConfig,
    PostgresSubscriber, PostgresSubscriberConfig,
};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
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
async fn promotes_delayed_message_after_due() {
    let Some(pool) = pg_pool().await else {
        return;
    };
    let topic = Topic::new(format!("delay-{}", Uuid::new_v4()));

    let subscriber =
        PostgresSubscriber::new(PostgresSubscriberConfig::new(pool.clone(), "delay-group"));
    let mut stream = subscriber.subscribe(&topic).await.unwrap();

    let publisher = PostgresPublisher::new(PostgresPublisherConfig::new(pool.clone()))
        .await
        .unwrap();

    let started = Instant::now();
    publisher
        .publish_after(
            &topic,
            vec![Message::new(Bytes::from("payload"))],
            Delay::after(Duration::from_millis(300)),
        )
        .await
        .unwrap();

    let token = CancellationToken::new();
    let promoter = PostgresDelayPromoter::new(PostgresDelayPromoterConfig::new(pool.clone()))
        .await
        .unwrap();
    let tc = token.clone();
    let handle = tokio::spawn(async move { promoter.run(tc).await });

    let received = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("timeout")
        .expect("stream ended");

    assert!(started.elapsed() >= Duration::from_millis(300));
    assert_eq!(received.payload().as_ref(), b"payload");
    let _ = received.ack();

    token.cancel();
    handle.await.unwrap();
}
