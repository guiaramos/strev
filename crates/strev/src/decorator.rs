use std::sync::Arc;

use async_trait::async_trait;
use tokio_stream::StreamExt;

use crate::error::{CloseError, PublishError, SubscribeError};
use crate::message::{Message, Pending};
use crate::outcome::Outcome;
use crate::publisher::Publisher;
use crate::stream::MessageStream;
use crate::subscriber::Subscriber;
use crate::topic::Topic;

type MessageTransformFn = Arc<dyn Fn(&mut Message<Pending>) + Send + Sync>;

pub trait PublisherDecorator: Send + Sync {
    fn decorate(&self, publisher: Box<dyn Publisher>) -> Box<dyn Publisher>;
}

pub trait SubscriberDecorator: Send + Sync {
    fn decorate(&self, subscriber: Box<dyn Subscriber>) -> Box<dyn Subscriber>;
}

pub struct MessageTransformPublisherDecorator {
    pub transform: MessageTransformFn,
}

impl PublisherDecorator for MessageTransformPublisherDecorator {
    fn decorate(&self, publisher: Box<dyn Publisher>) -> Box<dyn Publisher> {
        Box::new(TransformedPublisher {
            inner: publisher,
            transform: self.transform.clone(),
        })
    }
}

struct TransformedPublisher {
    inner: Box<dyn Publisher>,
    transform: MessageTransformFn,
}

#[async_trait]
impl Publisher for TransformedPublisher {
    async fn publish(
        &self,
        topic: &Topic,
        messages: Vec<Message<Pending>>,
    ) -> Result<Vec<Outcome>, PublishError> {
        let transformed = messages
            .into_iter()
            .map(|mut m| {
                (self.transform)(&mut m);
                m
            })
            .collect();
        self.inner.publish(topic, transformed).await
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        self.inner.close().await
    }
}

pub struct MessageTransformSubscriberDecorator {
    pub transform: MessageTransformFn,
}

impl SubscriberDecorator for MessageTransformSubscriberDecorator {
    fn decorate(&self, subscriber: Box<dyn Subscriber>) -> Box<dyn Subscriber> {
        Box::new(TransformedSubscriber {
            inner: subscriber,
            transform: self.transform.clone(),
        })
    }
}

struct TransformedSubscriber {
    inner: Box<dyn Subscriber>,
    transform: MessageTransformFn,
}

#[async_trait]
impl Subscriber for TransformedSubscriber {
    async fn subscribe(&self, topic: &Topic) -> Result<MessageStream, SubscribeError> {
        let mut inner_stream = self.inner.subscribe(topic).await?;
        let (tx, stream) = MessageStream::channel(256);
        let transform = self.transform.clone();

        tokio::spawn(async move {
            while let Some(mut msg) = inner_stream.next().await {
                transform(&mut msg);
                if tx.send(msg).await.is_err() {
                    break;
                }
            }
        });

        Ok(stream)
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        self.inner.close().await
    }
}
