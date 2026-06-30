use async_trait::async_trait;
use strev::{Publisher, Subscriber};
use strev_channel::Channel;
use strev_testsuite::Backend;

struct ChannelBackend {
    channel: Channel,
}

#[async_trait]
impl Backend for ChannelBackend {
    async fn publisher(&self) -> Box<dyn Publisher> {
        Box::new(self.channel.clone())
    }

    async fn subscriber(&self, _group: &str) -> Box<dyn Subscriber> {
        Box::new(self.channel.clone())
    }
}

fn backend() -> ChannelBackend {
    ChannelBackend {
        channel: Channel::new(64),
    }
}

#[tokio::test]
async fn conformance_roundtrip() {
    strev_testsuite::roundtrip(&backend()).await;
}

#[tokio::test]
async fn conformance_ordering() {
    strev_testsuite::ordering(&backend()).await;
}

#[tokio::test]
async fn conformance_metadata() {
    strev_testsuite::metadata_fidelity(&backend()).await;
}

#[tokio::test]
async fn conformance_nack_redelivery() {
    strev_testsuite::nack_redelivery(&backend()).await;
}
