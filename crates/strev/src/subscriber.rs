use std::sync::Arc;

use async_trait::async_trait;

use crate::error::{CloseError, SubscribeError};
use crate::stream::MessageStream;
use crate::topic::Topic;

#[async_trait]
pub trait Subscriber: Send + Sync {
    async fn subscribe(&self, topic: &Topic) -> Result<MessageStream, SubscribeError>;

    async fn close(&mut self) -> Result<(), CloseError>;
}

#[async_trait]
impl Subscriber for Box<dyn Subscriber> {
    async fn subscribe(&self, topic: &Topic) -> Result<MessageStream, SubscribeError> {
        (**self).subscribe(topic).await
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        (**self).close().await
    }
}

#[async_trait]
impl Subscriber for Arc<dyn Subscriber> {
    async fn subscribe(&self, topic: &Topic) -> Result<MessageStream, SubscribeError> {
        (**self).subscribe(topic).await
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        Ok(())
    }
}
