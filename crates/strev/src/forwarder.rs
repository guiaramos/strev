use std::sync::Arc;

use async_trait::async_trait;

use crate::error::{CloseError, HandlerError, PublishError};
use crate::handler::HandlerResult;
use crate::message::Message;
use crate::outcome::Outcome;
use crate::publisher::Publisher;
use crate::router::Router;
use crate::subscriber::Subscriber;
use crate::topic::Topic;

const FORWARD_DESTINATION: &str = "forward-destination";
const DEFAULT_FORWARDER_TOPIC: &str = "forwarder";

/// A publisher decorator that records the real destination topic in metadata and sends
/// every message to a single forwarder topic instead, for a [`Forwarder`] to relay.
///
/// The payload is forwarded untouched (zero-copy), so this stays efficient for large or
/// high-throughput streams: there is no envelope (de)serialization, only one reserved
/// metadata key.
pub struct ForwarderPublisher {
    inner: Box<dyn Publisher>,
    forwarder_topic: Topic,
}

impl ForwarderPublisher {
    pub fn new(inner: Box<dyn Publisher>) -> Self {
        Self {
            inner,
            forwarder_topic: Topic::new(DEFAULT_FORWARDER_TOPIC),
        }
    }

    /// Override the forwarder topic (must match the [`Forwarder`]'s).
    pub fn with_forwarder_topic(mut self, topic: Topic) -> Self {
        self.forwarder_topic = topic;
        self
    }
}

#[async_trait]
impl Publisher for ForwarderPublisher {
    async fn publish(
        &self,
        topic: &Topic,
        messages: Vec<Message>,
    ) -> Result<Vec<Outcome>, PublishError> {
        let mut wrapped = Vec::with_capacity(messages.len());
        for message in &messages {
            let mut metadata = message.metadata().clone();
            metadata.set(FORWARD_DESTINATION, topic.as_str());
            wrapped.push(Message::with_metadata(message.payload().clone(), metadata));
        }
        self.inner.publish(&self.forwarder_topic, wrapped).await?;
        Ok(messages.into_iter().map(Message::ack).collect())
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        self.inner.close().await
    }
}

/// Configuration for a [`Forwarder`].
pub struct ForwarderConfig {
    pub forwarder_topic: Topic,
    pub ack_on_missing_destination: bool,
}

impl ForwarderConfig {
    pub fn new() -> Self {
        Self {
            forwarder_topic: Topic::new(DEFAULT_FORWARDER_TOPIC),
            ack_on_missing_destination: false,
        }
    }

    pub fn forwarder_topic(mut self, topic: Topic) -> Self {
        self.forwarder_topic = topic;
        self
    }

    /// Ack (instead of erroring on) messages on the forwarder topic that carry no
    /// destination, e.g. ones not produced by a [`ForwarderPublisher`].
    pub fn ack_on_missing_destination(mut self, ack: bool) -> Self {
        self.ack_on_missing_destination = ack;
        self
    }
}

impl Default for ForwarderConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Relays enveloped messages from a forwarder topic to their real destination topic,
/// possibly on a different backend (e.g. a Postgres outbox forwarded to Kafka).
pub struct Forwarder;

impl Forwarder {
    /// Register the forwarding consumer on `router`: it reads each message's destination,
    /// strips the marker, and republishes the untouched payload via `publisher`.
    pub fn register(
        router: &mut Router,
        subscriber: impl Subscriber + 'static,
        publisher: Arc<dyn Publisher>,
        config: ForwarderConfig,
    ) {
        let ack_on_missing = config.ack_on_missing_destination;
        router.add_consumer(
            "forwarder",
            config.forwarder_topic,
            subscriber,
            move |message: Message| {
                let publisher = publisher.clone();
                async move {
                    let Some(destination) = message
                        .metadata()
                        .get(FORWARD_DESTINATION)
                        .map(str::to_string)
                    else {
                        return if ack_on_missing {
                            Ok(HandlerResult::ack(message))
                        } else {
                            Err(HandlerError::Processing(
                                "forwarded message is missing its destination".into(),
                            ))
                        };
                    };

                    let mut metadata = message.metadata().clone();
                    metadata.remove(FORWARD_DESTINATION);
                    let forwarded = Message::with_metadata(message.payload().clone(), metadata);

                    match publisher
                        .publish(&Topic::new(destination), vec![forwarded])
                        .await
                    {
                        Ok(_) => Ok(HandlerResult::ack(message)),
                        Err(error) => Err(HandlerError::Processing(Box::new(error))),
                    }
                }
            },
        );
    }
}
