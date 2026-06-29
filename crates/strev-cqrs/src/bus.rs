use std::sync::Arc;

use bytes::Bytes;
use strev::{Message, Publisher, Topic};

use crate::{Command, CqrsError, Event, NAME_KEY};

type TopicFn = Arc<dyn Fn(&str) -> Topic + Send + Sync>;

fn topic_per_name() -> TopicFn {
    Arc::new(|name| Topic::new(name))
}

/// Publishes typed commands. Each command type is consumed by exactly one handler.
pub struct CommandBus {
    publisher: Box<dyn Publisher>,
    topic: TopicFn,
}

impl CommandBus {
    pub fn new(publisher: Box<dyn Publisher>) -> Self {
        Self {
            publisher,
            topic: topic_per_name(),
        }
    }

    /// Override how a command name maps to a topic (default: the name itself).
    pub fn with_topic(mut self, topic: impl Fn(&str) -> Topic + Send + Sync + 'static) -> Self {
        self.topic = Arc::new(topic);
        self
    }

    pub async fn send<C: Command>(&self, command: C) -> Result<(), CqrsError> {
        let payload = serde_json::to_vec(&command).map_err(CqrsError::Serialize)?;
        let mut message = Message::new(Bytes::from(payload));
        message.metadata_mut().set(NAME_KEY, C::NAME);
        self.publisher
            .publish(&(self.topic)(C::NAME), vec![message])
            .await?;
        Ok(())
    }
}

/// Publishes typed events. Each event is delivered to every registered handler.
pub struct EventBus {
    publisher: Box<dyn Publisher>,
    topic: TopicFn,
}

impl EventBus {
    pub fn new(publisher: Box<dyn Publisher>) -> Self {
        Self {
            publisher,
            topic: topic_per_name(),
        }
    }

    /// Override how an event name maps to a topic (default: the name itself).
    pub fn with_topic(mut self, topic: impl Fn(&str) -> Topic + Send + Sync + 'static) -> Self {
        self.topic = Arc::new(topic);
        self
    }

    pub async fn publish<E: Event>(&self, event: E) -> Result<(), CqrsError> {
        let payload = serde_json::to_vec(&event).map_err(CqrsError::Serialize)?;
        let mut message = Message::new(Bytes::from(payload));
        message.metadata_mut().set(NAME_KEY, E::NAME);
        self.publisher
            .publish(&(self.topic)(E::NAME), vec![message])
            .await?;
        Ok(())
    }
}
