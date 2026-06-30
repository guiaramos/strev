use async_trait::async_trait;
use mongodb::Client;
use strev::{Publisher, Subscriber};
use strev_mongodb::{
    MongoPublisher, MongoPublisherConfig, MongoQueueSubscriber, MongoQueueSubscriberConfig,
};
use strev_testsuite::Backend;

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

struct MongoQueueBackend {
    client: Client,
}

#[async_trait]
impl Backend for MongoQueueBackend {
    async fn publisher(&self) -> Box<dyn Publisher> {
        Box::new(MongoPublisher::new(MongoPublisherConfig::new(
            self.client.clone(),
        )))
    }

    async fn subscriber(&self, group: &str) -> Box<dyn Subscriber> {
        Box::new(MongoQueueSubscriber::new(MongoQueueSubscriberConfig::new(
            self.client.clone(),
            group,
        )))
    }
}

async fn backend() -> Option<MongoQueueBackend> {
    Some(MongoQueueBackend {
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
async fn conformance_nack_redelivery() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: mongodb not available");
        return;
    };
    strev_testsuite::nack_redelivery(&backend).await;
}

#[tokio::test]
async fn conformance_competing_consumers() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: mongodb not available");
        return;
    };
    strev_testsuite::competing_consumers(&backend).await;
}

#[tokio::test]
async fn reports_consumer_lag() {
    use bytes::Bytes;
    use strev::{ConsumerLag, Message, Publisher, Topic};
    use strev_mongodb::{MongoQueueSubscriber, MongoQueueSubscriberConfig};
    use uuid::Uuid;

    let Some(backend) = backend().await else {
        return;
    };
    let topic = Topic::new(format!("lag-{}", Uuid::new_v4()));

    let subscriber =
        MongoQueueSubscriber::new(MongoQueueSubscriberConfig::new(backend.client.clone(), "g"));
    let publisher = backend.publisher().await;
    let messages = (0..5)
        .map(|i| Message::new(Bytes::from(format!("m-{i}"))))
        .collect();
    publisher.publish(&topic, messages).await.unwrap();

    assert_eq!(subscriber.lag(&topic).await.unwrap(), 5);
}

#[tokio::test]
async fn conformance_throughput() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: backend not available");
        return;
    };
    strev_testsuite::throughput(&backend).await;
}
