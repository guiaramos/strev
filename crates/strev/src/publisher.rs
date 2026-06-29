use async_trait::async_trait;

use crate::error::{CloseError, PublishError};
use crate::message::{Message, Pending};
use crate::outcome::Outcome;
use crate::topic::Topic;

#[async_trait]
pub trait Publisher: Send + Sync {
    async fn publish(
        &self,
        topic: &Topic,
        messages: Vec<Message<Pending>>,
    ) -> Result<Vec<Outcome>, PublishError>;

    async fn close(&mut self) -> Result<(), CloseError>;
}

#[async_trait]
impl Publisher for Box<dyn Publisher> {
    async fn publish(
        &self,
        topic: &Topic,
        messages: Vec<Message<Pending>>,
    ) -> Result<Vec<Outcome>, PublishError> {
        (**self).publish(topic, messages).await
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        (**self).close().await
    }
}
