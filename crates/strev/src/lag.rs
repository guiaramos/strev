use async_trait::async_trait;

use crate::topic::Topic;

/// Error returned when querying [`ConsumerLag`].
pub type LagError = Box<dyn std::error::Error + Send + Sync>;

/// An opt-in capability for backends that can report consumer lag: the approximate number of
/// messages published to a topic that the consumer group has not yet consumed. Useful for
/// autoscaling and alerting. Only backends that can answer it cheaply implement this trait.
#[async_trait]
pub trait ConsumerLag: Send + Sync {
    async fn lag(&self, topic: &Topic) -> Result<u64, LagError>;
}
