use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use strev::{HandlerResult, Message, Publisher, Router, ShutdownSignal, Subscriber, Topic};
use strev_nats::{NatsPublisher, NatsPublisherConfig, NatsSubscriber, NatsSubscriberConfig};
use strev_testsuite::Backend;
use tokio_util::sync::CancellationToken;

const STREAM: &str = "conformance";

async fn nats_client() -> Option<async_nats::Client> {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    async_nats::connect(&url).await.ok()
}

struct NatsBackend {
    client: async_nats::Client,
}

#[async_trait]
impl Backend for NatsBackend {
    async fn publisher(&self) -> Box<dyn Publisher> {
        Box::new(
            NatsPublisher::new(NatsPublisherConfig::new(self.client.clone(), STREAM))
                .await
                .unwrap(),
        )
    }

    async fn subscriber(&self, _group: &str) -> Box<dyn Subscriber> {
        Box::new(NatsSubscriber::new(NatsSubscriberConfig::new(
            self.client.clone(),
            STREAM,
        )))
    }
}

async fn backend() -> Option<NatsBackend> {
    Some(NatsBackend {
        client: nats_client().await?,
    })
}

#[tokio::test]
async fn conformance_roundtrip() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: nats not available");
        return;
    };
    strev_testsuite::roundtrip(&backend).await;
}

#[tokio::test]
async fn conformance_ordering() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: nats not available");
        return;
    };
    strev_testsuite::ordering(&backend).await;
}

#[tokio::test]
async fn conformance_metadata() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: nats not available");
        return;
    };
    strev_testsuite::metadata_fidelity(&backend).await;
}

#[tokio::test]
async fn conformance_consumer_group_resume() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: nats not available");
        return;
    };
    strev_testsuite::consumer_group_resume(&backend).await;
}

#[tokio::test]
async fn router_with_nats() {
    let client = match nats_client().await {
        Some(c) => c,
        None => {
            eprintln!("skipping: nats not available");
            return;
        }
    };

    let stream = format!("router{}", uuid::Uuid::new_v4().simple());
    let topic = Topic::new(format!("{stream}.router"));
    let processed = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();
    let counter = processed.clone();
    router.add_consumer(
        "nats_handler",
        topic.clone(),
        NatsSubscriber::new(NatsSubscriberConfig::new(client.clone(), &stream)),
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

    let publisher = NatsPublisher::new(NatsPublisherConfig::new(client, &stream))
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
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    tokio::time::sleep(Duration::from_secs(5)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(processed.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn conformance_nack_redelivery() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: nats not available");
        return;
    };
    strev_testsuite::nack_redelivery(&backend).await;
}

#[tokio::test]
async fn reports_consumer_lag() {
    use std::time::Duration;

    use bytes::Bytes;
    use strev::{ConsumerLag, Message, Publisher, Subscriber, Topic};
    use uuid::Uuid;

    let Some(client) = nats_client().await else {
        eprintln!("skipping: nats not available");
        return;
    };
    let topic = Topic::new(format!("conformance.lag-{}", Uuid::new_v4().simple()));

    let subscriber = NatsSubscriber::new(NatsSubscriberConfig::new(client.clone(), STREAM));
    let _held = subscriber.subscribe(&topic).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let publisher = NatsPublisher::new(NatsPublisherConfig::new(client.clone(), STREAM))
        .await
        .unwrap();
    let messages = (0..5)
        .map(|i| Message::new(Bytes::from(format!("m-{i}"))))
        .collect();
    Publisher::publish(&publisher, &topic, messages)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let lag = subscriber.lag(&topic).await.unwrap();
    assert!((1..=5).contains(&lag), "unexpected lag: {lag}");
}
