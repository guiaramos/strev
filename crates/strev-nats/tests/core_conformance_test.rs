use std::time::Duration;

use async_trait::async_trait;
use strev::{Publisher, Subscriber};
use strev_nats::{
    NatsCorePublisher, NatsCorePublisherConfig, NatsCoreSubscriber, NatsCoreSubscriberConfig,
};
use strev_testsuite::Backend;

async fn client() -> Option<async_nats::Client> {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    async_nats::connect(url).await.ok()
}

struct CoreNatsBackend {
    client: async_nats::Client,
}

#[async_trait]
impl Backend for CoreNatsBackend {
    async fn publisher(&self) -> Box<dyn Publisher> {
        Box::new(NatsCorePublisher::new(NatsCorePublisherConfig::new(
            self.client.clone(),
        )))
    }

    async fn subscriber(&self, group: &str) -> Box<dyn Subscriber> {
        Box::new(NatsCoreSubscriber::new(
            NatsCoreSubscriberConfig::new(self.client.clone()).queue_group(group),
        ))
    }

    fn warmup(&self) -> Duration {
        Duration::from_millis(500)
    }
}

async fn backend() -> Option<CoreNatsBackend> {
    Some(CoreNatsBackend {
        client: client().await?,
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
async fn conformance_competing_consumers() {
    let Some(backend) = backend().await else {
        eprintln!("skipping: nats not available");
        return;
    };
    strev_testsuite::competing_consumers(&backend).await;
}
