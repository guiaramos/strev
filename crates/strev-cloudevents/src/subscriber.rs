use async_trait::async_trait;
use strev::{CloseError, MessageStream, SubscribeError, Subscriber, SubscriberDecorator, Topic};
use tokio_stream::StreamExt;

use crate::codec::CloudEventCodec;

pub struct CloudEventsSubscriberDecorator {
    codec: CloudEventCodec,
}

impl CloudEventsSubscriberDecorator {
    pub fn new(codec: CloudEventCodec) -> Self {
        Self { codec }
    }
}

impl SubscriberDecorator for CloudEventsSubscriberDecorator {
    fn decorate(&self, subscriber: Box<dyn Subscriber>) -> Box<dyn Subscriber> {
        Box::new(DecodingSubscriber {
            inner: subscriber,
            codec: self.codec.clone(),
        })
    }
}

struct DecodingSubscriber {
    inner: Box<dyn Subscriber>,
    codec: CloudEventCodec,
}

#[async_trait]
impl Subscriber for DecodingSubscriber {
    async fn subscribe(&self, topic: &Topic) -> Result<MessageStream, SubscribeError> {
        let mut inner_stream = self.inner.subscribe(topic).await?;
        let (tx, stream) = MessageStream::channel(256);
        let codec = self.codec.clone();

        tokio::spawn(async move {
            while let Some(msg) = inner_stream.next().await {
                match codec.decode(&msg) {
                    Ok(decoded) => {
                        if tx.send(decoded).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to decode cloudevent; dropping message");
                    }
                }
            }
        });

        Ok(stream)
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        self.inner.close().await
    }
}
