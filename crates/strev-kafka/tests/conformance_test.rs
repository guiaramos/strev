use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use strev::{HandlerResult, Message, Publisher, Router, ShutdownSignal, Subscriber, Topic};
use strev_kafka::{KafkaPublisher, KafkaPublisherConfig, KafkaSubscriber, KafkaSubscriberConfig};
use strev_testsuite::Backend;
use tokio_util::sync::CancellationToken;

fn kafka_brokers() -> String {
    std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into())
}

async fn kafka_available(brokers: &str) -> bool {
    let brokers = brokers.to_string();
    tokio::task::spawn_blocking(move || {
        let consumer: Result<BaseConsumer, _> = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create();
        match consumer {
            Ok(c) => c.fetch_metadata(None, Duration::from_secs(3)).is_ok(),
            Err(_) => false,
        }
    })
    .await
    .unwrap_or(false)
}

struct KafkaBackend {
    brokers: String,
}

#[async_trait]
impl Backend for KafkaBackend {
    async fn publisher(&self) -> Box<dyn Publisher> {
        Box::new(KafkaPublisher::new(KafkaPublisherConfig::new(&self.brokers)).unwrap())
    }

    async fn subscriber(&self, group: &str) -> Box<dyn Subscriber> {
        Box::new(KafkaSubscriber::new(KafkaSubscriberConfig::new(
            &self.brokers,
            group,
        )))
    }

    fn warmup(&self) -> Duration {
        Duration::from_secs(3)
    }
}

async fn backend() -> Option<KafkaBackend> {
    let brokers = kafka_brokers();
    if kafka_available(&brokers).await {
        Some(KafkaBackend { brokers })
    } else {
        None
    }
}

#[tokio::test]
async fn conformance_roundtrip() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: kafka not available");
        return;
    };
    strev_testsuite::roundtrip(&backend).await;
}

#[tokio::test]
async fn conformance_ordering() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: kafka not available");
        return;
    };
    strev_testsuite::ordering(&backend).await;
}

#[tokio::test]
async fn conformance_metadata() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: kafka not available");
        return;
    };
    strev_testsuite::metadata_fidelity(&backend).await;
}

#[tokio::test]
async fn conformance_consumer_group_resume() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: kafka not available");
        return;
    };
    strev_testsuite::consumer_group_resume(&backend).await;
}

#[tokio::test]
async fn router_with_kafka() {
    let brokers = kafka_brokers();
    if !kafka_available(&brokers).await {
        eprintln!("skipping: kafka not available");
        return;
    }

    let topic = Topic::new(format!("router-{}", uuid::Uuid::new_v4().simple()));
    let processed = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();
    let counter = processed.clone();
    let group = format!("grp-{}", uuid::Uuid::new_v4().simple());
    router.add_consumer(
        "kafka_handler",
        topic.clone(),
        KafkaSubscriber::new(KafkaSubscriberConfig::new(&brokers, group)),
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

    tokio::time::sleep(Duration::from_secs(3)).await;

    let publisher = KafkaPublisher::new(KafkaPublisherConfig::new(&brokers)).unwrap();
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

    tokio::time::sleep(Duration::from_secs(8)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(processed.load(Ordering::SeqCst), 3);
}
