use async_trait::async_trait;
use futures::StreamExt;
use strev::{
    CloseError, Message, MessageStream, Metadata, Outcome, PublishError, SubscribeError, Topic,
};

/// Configuration for [`NatsCorePublisher`].
pub struct NatsCorePublisherConfig {
    pub client: async_nats::Client,
}

impl NatsCorePublisherConfig {
    pub fn new(client: async_nats::Client) -> Self {
        Self { client }
    }
}

/// A core-NATS publisher: fire-and-forget, at-most-once, no persistence. Use it for
/// low-latency ephemeral messaging where lost messages are acceptable. For durable delivery
/// with redelivery, use [`NatsPublisher`](crate::NatsPublisher) (JetStream) instead.
pub struct NatsCorePublisher {
    client: async_nats::Client,
}

impl NatsCorePublisher {
    pub fn new(config: NatsCorePublisherConfig) -> Self {
        Self {
            client: config.client,
        }
    }
}

#[async_trait]
impl strev::Publisher for NatsCorePublisher {
    async fn publish(
        &self,
        topic: &Topic,
        messages: Vec<Message>,
    ) -> Result<Vec<Outcome>, PublishError> {
        let subject = topic.as_str().to_string();
        let mut outcomes = Vec::with_capacity(messages.len());

        for msg in messages {
            let mut headers = async_nats::HeaderMap::new();
            for (k, v) in msg.metadata().iter() {
                headers.insert(k, v);
            }
            headers.insert("strev-uuid", msg.uuid().to_string().as_str());

            match self
                .client
                .publish_with_headers(subject.clone(), headers, msg.payload().clone())
                .await
            {
                Ok(_) => outcomes.push(msg.ack()),
                Err(e) => {
                    let _ = msg.nack();
                    return Err(PublishError::Backend(Box::new(e)));
                }
            }
        }

        self.client
            .flush()
            .await
            .map_err(|e| PublishError::Backend(Box::new(e)))?;

        Ok(outcomes)
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        Ok(())
    }
}

/// Configuration for [`NatsCoreSubscriber`].
pub struct NatsCoreSubscriberConfig {
    pub client: async_nats::Client,
    pub queue_group: Option<String>,
    pub buffer_size: usize,
}

impl NatsCoreSubscriberConfig {
    pub fn new(client: async_nats::Client) -> Self {
        Self {
            client,
            queue_group: None,
            buffer_size: 64,
        }
    }

    /// Join a queue group so messages are load-balanced across subscribers in the group.
    pub fn queue_group(mut self, group: impl Into<String>) -> Self {
        self.queue_group = Some(group.into());
        self
    }
}

/// A core-NATS subscriber: at-most-once delivery with no acknowledgement or redelivery.
/// Messages published while no subscriber is attached are lost. Set a queue group for
/// load-balanced delivery across subscribers.
pub struct NatsCoreSubscriber {
    config: NatsCoreSubscriberConfig,
}

impl NatsCoreSubscriber {
    pub fn new(config: NatsCoreSubscriberConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl strev::Subscriber for NatsCoreSubscriber {
    async fn subscribe(&self, topic: &Topic) -> Result<MessageStream, SubscribeError> {
        let subject = topic.as_str().to_string();

        let mut subscription = match &self.config.queue_group {
            Some(group) => {
                self.config
                    .client
                    .queue_subscribe(subject, group.clone())
                    .await
            }
            None => self.config.client.subscribe(subject).await,
        }
        .map_err(|e| SubscribeError::Backend(Box::new(e)))?;

        let (tx, stream) = MessageStream::channel(self.config.buffer_size);

        tokio::spawn(async move {
            loop {
                let next = tokio::select! {
                    biased;
                    _ = tx.closed() => break,
                    next = subscription.next() => next,
                };

                let Some(nats_msg) = next else {
                    break;
                };

                let mut metadata = Metadata::new();
                if let Some(headers) = &nats_msg.headers {
                    for (key, values) in headers.iter() {
                        let key = key.to_string();
                        if key == "strev-uuid" {
                            continue;
                        }
                        if let Some(value) = values.iter().next() {
                            metadata.set(key, value.to_string());
                        }
                    }
                }

                let message = Message::with_metadata(nats_msg.payload, metadata);
                if tx.send(message).await.is_err() {
                    break;
                }
            }
        });

        Ok(stream)
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        Ok(())
    }
}
