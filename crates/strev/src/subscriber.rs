use async_trait::async_trait;

use crate::error::{CloseError, SubscribeError};
use crate::stream::MessageStream;
use crate::topic::Topic;

#[async_trait]
pub trait Subscriber: Send + Sync {
    async fn subscribe(&self, topic: &Topic) -> Result<MessageStream, SubscribeError>;

    async fn close(&mut self) -> Result<(), CloseError>;
}
