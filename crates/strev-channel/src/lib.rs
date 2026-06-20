//! In-memory channel backend for strev.
//!
//! [`Channel`] implements both [`strev::Publisher`] and [`strev::Subscriber`] over
//! in-process Tokio channels. Useful for tests and single-process pipelines.
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::mpsc;

use strev::{
    CloseError, Message, MessageStream, Outcome, PublishError, Publisher, SubscribeError,
    Subscriber, Topic,
};

#[derive(Clone)]
pub struct Channel {
    inner: Arc<ChannelInner>,
}

struct ChannelInner {
    buffer_size: usize,
    topics: DashMap<Topic, Vec<mpsc::Sender<Message>>>,
}

impl Channel {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            inner: Arc::new(ChannelInner {
                buffer_size,
                topics: DashMap::new(),
            }),
        }
    }
}

#[async_trait]
impl Publisher for Channel {
    async fn publish(
        &self,
        topic: &Topic,
        messages: Vec<Message>,
    ) -> Result<Vec<Outcome>, PublishError> {
        let senders = self.inner.topics.get(topic);
        let senders = match senders {
            Some(s) => s,
            None => return Ok(messages.into_iter().map(|m| m.ack()).collect()),
        };

        let mut outcomes = Vec::with_capacity(messages.len());

        for msg in messages {
            let payload = msg.payload().clone();
            let metadata = msg.metadata().clone();

            for sender in senders.iter() {
                let copy = Message::with_metadata(payload.clone(), metadata.clone());
                let _ = sender.send(copy).await;
            }

            outcomes.push(msg.ack());
        }

        drop(senders);
        self.inner.topics.alter(topic, |_, mut v| {
            v.retain(|s| !s.is_closed());
            v
        });

        Ok(outcomes)
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        self.inner.topics.clear();
        Ok(())
    }
}

#[async_trait]
impl Subscriber for Channel {
    async fn subscribe(&self, topic: &Topic) -> Result<MessageStream, SubscribeError> {
        let (tx, stream) = MessageStream::channel(self.inner.buffer_size);
        self.inner.topics.entry(topic.clone()).or_default().push(tx);
        Ok(stream)
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        self.inner.topics.clear();
        Ok(())
    }
}
