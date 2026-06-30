use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use strev::{HandlerResult, Message, Publisher, Router, ShutdownSignal, Subscriber, Topic};
use strev_postgres::{
    PostgresPublisher, PostgresPublisherConfig, PostgresSubscriber, PostgresSubscriberConfig,
};
use strev_testsuite::Backend;
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

struct PostgresBackend {
    pool: PgPool,
}

#[async_trait]
impl Backend for PostgresBackend {
    async fn publisher(&self) -> Box<dyn Publisher> {
        Box::new(
            PostgresPublisher::new(PostgresPublisherConfig::new(self.pool.clone()))
                .await
                .unwrap(),
        )
    }

    async fn subscriber(&self, group: &str) -> Box<dyn Subscriber> {
        Box::new(PostgresSubscriber::new(PostgresSubscriberConfig::new(
            self.pool.clone(),
            group,
        )))
    }
}

async fn backend() -> Option<PostgresBackend> {
    Some(PostgresBackend {
        pool: pg_pool().await?,
    })
}

#[tokio::test]
async fn conformance_roundtrip() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: postgres not available");
        return;
    };
    strev_testsuite::roundtrip(&backend).await;
}

#[tokio::test]
async fn conformance_ordering() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: postgres not available");
        return;
    };
    strev_testsuite::ordering(&backend).await;
}

#[tokio::test]
async fn conformance_metadata() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: postgres not available");
        return;
    };
    strev_testsuite::metadata_fidelity(&backend).await;
}

#[tokio::test]
async fn conformance_consumer_group_resume() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: postgres not available");
        return;
    };
    strev_testsuite::consumer_group_resume(&backend).await;
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

    let topic = Topic::new(format!("router.{}", uuid::Uuid::new_v4().simple()));
    let processed = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();
    let counter = processed.clone();
    let group = format!("grp-{}", uuid::Uuid::new_v4().simple());
    router.add_consumer(
        "postgres_handler",
        topic.clone(),
        PostgresSubscriber::new(PostgresSubscriberConfig::new(pool.clone(), group)),
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
        Publisher::publish(
            &publisher,
            &topic,
            vec![Message::new(Bytes::from(format!("m-{i}")))],
        )
        .await
        .unwrap();
    }

    tokio::time::sleep(Duration::from_secs(3)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(processed.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn conformance_nack_redelivery() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: postgres not available");
        return;
    };
    strev_testsuite::nack_redelivery(&backend).await;
}

#[tokio::test]
async fn conformance_competing_consumers() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: postgres not available");
        return;
    };
    strev_testsuite::competing_consumers(&backend).await;
}

#[tokio::test]
async fn conformance_throughput() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: backend not available");
        return;
    };
    strev_testsuite::throughput(&backend).await;
}
