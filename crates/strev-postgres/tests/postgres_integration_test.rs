use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use strev::{HandlerResult, Message, Publisher, Router, ShutdownSignal, Subscriber, Topic};
use strev_postgres::{
    PostgresPublisher, PostgresPublisherConfig, PostgresSubscriber, PostgresSubscriberConfig,
};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

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

fn unique_topic() -> String {
    format!("strevtest.{}", uuid::Uuid::new_v4().simple())
}

fn unique_group() -> String {
    format!("grp-{}", uuid::Uuid::new_v4().simple())
}

#[tokio::test]
async fn publish_and_subscribe() {
    let pool = match pg_pool().await {
        Some(p) => p,
        None => {
            eprintln!("skipping: postgres not available");
            return;
        }
    };

    let topic = Topic::new(unique_topic());

    let subscriber =
        PostgresSubscriber::new(PostgresSubscriberConfig::new(pool.clone(), unique_group()));
    let mut msg_stream = subscriber.subscribe(&topic).await.unwrap();

    let publisher = PostgresPublisher::new(PostgresPublisherConfig::new(pool))
        .await
        .unwrap();
    Publisher::publish(
        &publisher,
        &topic,
        vec![Message::new(Bytes::from("hello postgres"))],
    )
    .await
    .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(10), msg_stream.next())
        .await
        .expect("timeout waiting for message")
        .expect("stream ended");

    assert_eq!(msg.payload().as_ref(), b"hello postgres");
    let _ = msg.ack();
}

#[tokio::test]
async fn publish_multiple_messages() {
    let pool = match pg_pool().await {
        Some(p) => p,
        None => {
            eprintln!("skipping: postgres not available");
            return;
        }
    };

    let topic = Topic::new(unique_topic());

    let subscriber =
        PostgresSubscriber::new(PostgresSubscriberConfig::new(pool.clone(), unique_group()));
    let mut msg_stream = subscriber.subscribe(&topic).await.unwrap();

    let publisher = PostgresPublisher::new(PostgresPublisherConfig::new(pool))
        .await
        .unwrap();
    let msgs: Vec<Message> = (0..3)
        .map(|i| Message::new(Bytes::from(format!("msg-{i}"))))
        .collect();

    Publisher::publish(&publisher, &topic, msgs).await.unwrap();

    for i in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(10), msg_stream.next())
            .await
            .expect("timeout")
            .expect("stream ended");
        assert_eq!(msg.payload().as_ref(), format!("msg-{i}").as_bytes());
        let _ = msg.ack();
    }
}

#[tokio::test]
async fn metadata_roundtrip() {
    let pool = match pg_pool().await {
        Some(p) => p,
        None => {
            eprintln!("skipping: postgres not available");
            return;
        }
    };

    let topic = Topic::new(unique_topic());

    let subscriber =
        PostgresSubscriber::new(PostgresSubscriberConfig::new(pool.clone(), unique_group()));
    let mut msg_stream = subscriber.subscribe(&topic).await.unwrap();

    let publisher = PostgresPublisher::new(PostgresPublisherConfig::new(pool))
        .await
        .unwrap();
    let mut msg = Message::new(Bytes::from("with-meta"));
    msg.metadata_mut().set("source", "test");
    msg.metadata_mut().set("version", "1.0");

    Publisher::publish(&publisher, &topic, vec![msg])
        .await
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(10), msg_stream.next())
        .await
        .expect("timeout")
        .expect("stream ended");

    assert_eq!(received.metadata().get("source"), Some("test"));
    assert_eq!(received.metadata().get("version"), Some("1.0"));
    let _ = received.ack();
}

#[tokio::test]
async fn router_with_postgres() {
    let pool = match pg_pool().await {
        Some(p) => p,
        None => {
            eprintln!("skipping: postgres not available");
            return;
        }
    };

    let topic = Topic::new(unique_topic());
    let processed = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();

    let counter = processed.clone();
    let sub_config = PostgresSubscriberConfig::new(pool.clone(), unique_group());
    router.add_consumer(
        "postgres_handler",
        topic.clone(),
        PostgresSubscriber::new(sub_config),
        move |msg: Message| {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(HandlerResult::ack(msg))
            }
        },
    );

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let publisher = PostgresPublisher::new(PostgresPublisherConfig::new(pool))
        .await
        .unwrap();
    for i in 0..3 {
        let msg = Message::new(Bytes::from(format!("router-msg-{i}")));
        Publisher::publish(&publisher, &topic, vec![msg])
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_secs(3)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(processed.load(Ordering::SeqCst), 3);
}
