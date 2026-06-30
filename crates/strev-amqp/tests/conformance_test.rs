use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use strev::{HandlerResult, Message, Publisher, Router, ShutdownSignal, Subscriber, Topic};
use strev_amqp::{AmqpPublisher, AmqpPublisherConfig, AmqpSubscriber, AmqpSubscriberConfig};
use strev_testsuite::Backend;
use tokio_util::sync::CancellationToken;

fn amqp_uri() -> String {
    std::env::var("AMQP_URI").unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:5672/%2f".into())
}

async fn amqp_available(uri: &str) -> bool {
    AmqpPublisher::new(AmqpPublisherConfig::new(uri))
        .await
        .is_ok()
}

struct AmqpBackend {
    uri: String,
}

#[async_trait]
impl Backend for AmqpBackend {
    async fn publisher(&self) -> Box<dyn Publisher> {
        Box::new(
            AmqpPublisher::new(AmqpPublisherConfig::new(&self.uri))
                .await
                .unwrap(),
        )
    }

    async fn subscriber(&self, group: &str) -> Box<dyn Subscriber> {
        Box::new(AmqpSubscriber::new(AmqpSubscriberConfig::new(
            &self.uri, group,
        )))
    }
}

async fn backend() -> Option<AmqpBackend> {
    let uri = amqp_uri();
    if amqp_available(&uri).await {
        Some(AmqpBackend { uri })
    } else {
        None
    }
}

#[tokio::test]
async fn conformance_roundtrip() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: amqp not available");
        return;
    };
    strev_testsuite::roundtrip(&backend).await;
}

#[tokio::test]
async fn conformance_ordering() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: amqp not available");
        return;
    };
    strev_testsuite::ordering(&backend).await;
}

#[tokio::test]
async fn conformance_metadata() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: amqp not available");
        return;
    };
    strev_testsuite::metadata_fidelity(&backend).await;
}

#[tokio::test]
async fn conformance_consumer_group_resume() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: amqp not available");
        return;
    };
    strev_testsuite::consumer_group_resume(&backend).await;
}

#[tokio::test]
async fn router_with_amqp() {
    let uri = amqp_uri();
    if !amqp_available(&uri).await {
        eprintln!("skipping: amqp not available");
        return;
    }

    let topic = Topic::new(format!("router.{}", uuid::Uuid::new_v4().simple()));
    let processed = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();
    let counter = processed.clone();
    let group = format!("grp-{}", uuid::Uuid::new_v4().simple());
    router.add_consumer(
        "amqp_handler",
        topic.clone(),
        AmqpSubscriber::new(AmqpSubscriberConfig::new(&uri, group)),
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

    tokio::time::sleep(Duration::from_secs(2)).await;

    let publisher = AmqpPublisher::new(AmqpPublisherConfig::new(&uri))
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

    for _ in 0..80 {
        if processed.load(Ordering::SeqCst) >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    token.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(processed.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn conformance_nack_redelivery() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: amqp not available");
        return;
    };
    strev_testsuite::nack_redelivery(&backend).await;
}

#[tokio::test]
async fn conformance_competing_consumers() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: amqp not available");
        return;
    };
    strev_testsuite::competing_consumers(&backend).await;
}

#[tokio::test]
async fn reports_consumer_lag() {
    use bytes::Bytes;
    use strev::{ConsumerLag, Message, Publisher, Subscriber, Topic};
    use strev_amqp::{AmqpPublisher, AmqpPublisherConfig, AmqpSubscriber, AmqpSubscriberConfig};
    use uuid::Uuid;

    if backend().await.is_none() {
        return;
    }
    let uri = amqp_uri();
    let topic = Topic::new(format!("lag.{}", Uuid::new_v4().simple()));
    let group = "g";

    let subscriber = AmqpSubscriber::new(AmqpSubscriberConfig::new(&uri, group));
    let _stream = Subscriber::subscribe(&subscriber, &topic).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let publisher = AmqpPublisher::new(AmqpPublisherConfig::new(&uri))
        .await
        .unwrap();
    let messages = (0..5)
        .map(|i| Message::new(Bytes::from(format!("m-{i}"))))
        .collect();
    Publisher::publish(&publisher, &topic, messages)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let lag = subscriber.lag(&topic).await.unwrap();
    assert!(lag <= 5, "lag {lag} should be bounded by published count");
}
