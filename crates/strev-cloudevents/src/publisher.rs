use async_trait::async_trait;
use strev::{CloseError, Message, Outcome, PublishError, Publisher, PublisherDecorator, Topic};

use crate::codec::CloudEventCodec;

pub struct CloudEventsPublisherDecorator {
    codec: CloudEventCodec,
}

impl CloudEventsPublisherDecorator {
    pub fn new(codec: CloudEventCodec) -> Self {
        Self { codec }
    }
}

impl PublisherDecorator for CloudEventsPublisherDecorator {
    fn decorate(&self, publisher: Box<dyn Publisher>) -> Box<dyn Publisher> {
        Box::new(EncodingPublisher {
            inner: publisher,
            codec: self.codec.clone(),
        })
    }
}

struct EncodingPublisher {
    inner: Box<dyn Publisher>,
    codec: CloudEventCodec,
}

#[async_trait]
impl Publisher for EncodingPublisher {
    async fn publish(
        &self,
        topic: &Topic,
        messages: Vec<Message>,
    ) -> Result<Vec<Outcome>, PublishError> {
        let mut encoded = Vec::with_capacity(messages.len());
        for msg in messages {
            let envelope = self
                .codec
                .encode(&msg)
                .map_err(|e| PublishError::Backend(Box::new(e)))?;
            encoded.push(envelope);
        }
        self.inner.publish(topic, encoded).await
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        self.inner.close().await
    }
}
