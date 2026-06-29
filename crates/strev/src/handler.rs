use std::future::Future;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::HandlerError;
use crate::message::{Message, Pending};
use crate::metadata::Metadata;
use crate::outcome::Outcome;
use crate::topic::Topic;

#[derive(Debug)]
pub struct HandlerResult {
    outcome: Outcome,
    produced: Vec<ProducedMessage>,
}

impl HandlerResult {
    pub fn ack(msg: Message<Pending>) -> Self {
        Self {
            outcome: msg.ack(),
            produced: vec![],
        }
    }

    pub fn nack(msg: Message<Pending>) -> Self {
        Self {
            outcome: msg.nack(),
            produced: vec![],
        }
    }

    pub fn ack_with(msg: Message<Pending>, produced: Vec<ProducedMessage>) -> Self {
        Self {
            outcome: msg.ack(),
            produced,
        }
    }

    pub fn outcome(&self) -> Outcome {
        self.outcome
    }

    pub fn produced(&self) -> &[ProducedMessage] {
        &self.produced
    }

    pub fn into_produced(self) -> Vec<ProducedMessage> {
        self.produced
    }

    pub(crate) fn empty_ack() -> Self {
        Self {
            outcome: Outcome::acked(),
            produced: vec![],
        }
    }
}

#[derive(Debug)]
pub struct ProducedMessage {
    pub topic: Topic,
    pub payload: Bytes,
    pub metadata: Metadata,
}

pub fn passthrough(topic: Topic) -> impl Handler {
    move |msg: Message<Pending>| {
        let t = topic.clone();
        async move {
            let payload = msg.payload().clone();
            let metadata = msg.metadata().clone();
            Ok(HandlerResult::ack_with(
                msg,
                vec![ProducedMessage {
                    topic: t,
                    payload,
                    metadata,
                }],
            ))
        }
    }
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

#[async_trait]
impl Handler for Box<dyn Handler> {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        (**self).handle(msg).await
    }
}
