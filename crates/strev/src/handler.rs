use std::future::Future;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::HandlerError;
use crate::message::{Message, Pending};
use crate::metadata::Metadata;
use crate::outcome::Outcome;
use crate::topic::Topic;

pub struct HandlerResult {
    pub outcome: Outcome,
    pub produced: Vec<ProducedMessage>,
}

pub struct ProducedMessage {
    pub topic: Topic,
    pub payload: Bytes,
    pub metadata: Metadata,
}

#[async_trait]
pub trait Handler: Send + Sync {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError>;
}

#[async_trait]
impl<F, Fut> Handler for F
where
    F: Fn(Message<Pending>) -> Fut + Send + Sync,
    Fut: Future<Output = Result<HandlerResult, HandlerError>> + Send,
{
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        (self)(msg).await
    }
}
