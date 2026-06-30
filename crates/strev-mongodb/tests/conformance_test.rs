use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use mongodb::Client;
use strev::{HandlerResult, Message, Publisher, Router, ShutdownSignal, Subscriber, Topic};
use strev_mongodb::{MongoPublisher, MongoPublisherConfig, MongoSubscriber, MongoSubscriberConfig};
use strev_testsuite::Backend;
use tokio_util::sync::CancellationToken;

async fn mongo_client() -> Option<Client> {
    let uri = std::env::var("MONGODB_URI")
        .unwrap_or_else(|_| "mongodb://127.0.0.1:27017/?directConnection=true".into());
    let client = Client::with_uri_str(&uri).await.ok()?;
    client
        .database("strev")
        .run_command(mongodb::bson::doc! { "ping": 1 })
        .await
        .ok()?;
    Some(client)
}

struct MongoBackend {
    client: Client,
}

#[async_trait]
impl Backend for MongoBackend {
    async fn publisher(&self) -> Box<dyn Publisher> {
        Box::new(MongoPublisher::new(MongoPublisherConfig::new(
            self.client.clone(),
        )))
    }

    async fn subscriber(&self, group: &str) -> Box<dyn Subscriber> {
        Box::new(MongoSubscriber::new(MongoSubscriberConfig::new(
            self.client.clone(),
            group,
        )))
    }
}

async fn backend() -> Option<MongoBackend> {
    Some(MongoBackend {
        client: mongo_client().await?,
    })
}

#[tokio::test]
async fn conformance_roundtrip() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: mongodb not available");
        return;
    };
    strev_testsuite::roundtrip(&backend).await;
}

#[tokio::test]
async fn conformance_ordering() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: mongodb not available");
        return;
    };
    strev_testsuite::ordering(&backend).await;
}

#[tokio::test]
async fn conformance_metadata() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: mongodb not available");
        return;
    };
    strev_testsuite::metadata_fidelity(&backend).await;
}

#[tokio::test]
async fn conformance_consumer_group_resume() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: mongodb not available");
        return;
    };
    strev_testsuite::consumer_group_resume(&backend).await;
}

#[tokio::test]
async fn router_with_mongodb() {
    let client = match mongo_client().await {
        Some(c) => c,
        None => {
            eprintln!("skipping: mongodb not available");
            return;
        }
    };

    let topic = Topic::new(format!("router.{}", uuid::Uuid::new_v4().simple()));
    let processed = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();
    let counter = processed.clone();
    let group = format!("grp-{}", uuid::Uuid::new_v4().simple());
    router.add_consumer(
        "mongo_handler",
        topic.clone(),
        MongoSubscriber::new(MongoSubscriberConfig::new(client.clone(), group)),
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

    let publisher = MongoPublisher::new(MongoPublisherConfig::new(client));
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
async fn conformance_throughput() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: backend not available");
        return;
    };
    strev_testsuite::throughput(&backend).await;
}
