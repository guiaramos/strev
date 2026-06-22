use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use strev::{HandlerResult, Message, Publisher, Router, ShutdownSignal, Topic};
use strev_redis::{RedisPublisher, RedisPublisherConfig, RedisSubscriber, RedisSubscriberConfig};
use strev_testsuite::Backend;
use tokio_util::sync::CancellationToken;

async fn redis_client() -> Option<redis::Client> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());
    let client = redis::Client::open(url).ok()?;
    client.get_multiplexed_async_connection().await.ok()?;
    Some(client)
}

struct RedisBackend {
    client: redis::Client,
}

#[async_trait]
impl Backend for RedisBackend {
    async fn publisher(&self) -> Box<dyn Publisher> {
        Box::new(
            RedisPublisher::new(RedisPublisherConfig::new(self.client.clone()))
                .await
                .unwrap(),
        )
    }

    async fn subscriber(&self, group: &str) -> Box<dyn strev::Subscriber> {
        Box::new(RedisSubscriber::new(RedisSubscriberConfig::new(
            self.client.clone(),
            group,
        )))
    }
}

async fn backend() -> Option<RedisBackend> {
    Some(RedisBackend {
        client: redis_client().await?,
    })
}

#[tokio::test]
async fn conformance_roundtrip() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: redis not available");
        return;
    };
    strev_testsuite::roundtrip(&backend).await;
}

#[tokio::test]
async fn conformance_ordering() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: redis not available");
        return;
    };
    strev_testsuite::ordering(&backend).await;
}

#[tokio::test]
async fn conformance_metadata() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: redis not available");
        return;
    };
    strev_testsuite::metadata_fidelity(&backend).await;
}

#[tokio::test]
async fn conformance_consumer_group_resume() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: redis not available");
        return;
    };
    strev_testsuite::consumer_group_resume(&backend).await;
}

#[tokio::test]
async fn router_with_redis() {
    let client = match redis_client().await {
        Some(c) => c,
        None => {
            eprintln!("skipping: redis not available");
            return;
        }
    };

    let topic = Topic::new(format!("router.{}", uuid::Uuid::new_v4().simple()));
    let processed = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();
    let counter = processed.clone();
    let group = format!("grp-{}", uuid::Uuid::new_v4().simple());
    router.add_consumer(
        "redis_handler",
        topic.clone(),
        RedisSubscriber::new(RedisSubscriberConfig::new(client.clone(), group)),
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

    tokio::time::sleep(Duration::from_secs(1)).await;

    let publisher = RedisPublisher::new(RedisPublisherConfig::new(client))
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

    tokio::time::sleep(Duration::from_secs(2)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(processed.load(Ordering::SeqCst), 3);
}
