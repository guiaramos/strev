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
