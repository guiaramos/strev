use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::consumer::PullConsumer;
use async_trait::async_trait;
use futures::StreamExt;
use strev::{CloseError, Message, MessageStream, Metadata, SubscribeError, Topic};

pub struct NatsSubscriberConfig {
    pub client: async_nats::Client,
    pub stream_name: String,
    pub consumer_prefix: String,
    pub buffer_size: usize,
    pub ack_wait: Duration,
}

impl NatsSubscriberConfig {
    pub fn new(client: async_nats::Client, stream_name: impl Into<String>) -> Self {
        Self {
            client,
            stream_name: stream_name.into(),
            consumer_prefix: "strev".to_string(),
            buffer_size: 64,
            ack_wait: Duration::from_secs(30),
        }
    }
}

pub struct NatsSubscriber {
    config: NatsSubscriberConfig,
}

impl NatsSubscriber {
    pub fn new(config: NatsSubscriberConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl strev::Subscriber for NatsSubscriber {
    async fn subscribe(&self, topic: &Topic) -> Result<MessageStream, SubscribeError> {
        let jetstream = jetstream::new(self.config.client.clone());

        let subjects = format!("{}.>", self.config.stream_name);
        let stream = jetstream
            .get_or_create_stream(jetstream::stream::Config {
                name: self.config.stream_name.clone(),
                subjects: vec![subjects],
                ..Default::default()
            })
            .await
            .map_err(|e| SubscribeError::Backend(Box::new(e)))?;

        let consumer_name = format!(
            "{}-{}",
            self.config.consumer_prefix,
            topic.as_str().replace('.', "-")
        );
        let filter_subject = topic.as_str().to_string();

        let consumer: PullConsumer = stream
            .get_or_create_consumer(
                &consumer_name,
                jetstream::consumer::pull::Config {
                    durable_name: Some(consumer_name.clone()),
                    filter_subject,
                    ack_wait: self.config.ack_wait,
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| SubscribeError::Backend(Box::new(e)))?;

        let (tx, out_stream) = MessageStream::channel(self.config.buffer_size);

        tokio::spawn(async move {
            let mut messages = match consumer.messages().await {
                Ok(m) => m,
                Err(_) => return,
            };

            loop {
                match messages.next().await {
                    Some(Ok(jetstream_msg)) => {
                        let payload = jetstream_msg.payload.clone();
                        let mut metadata = Metadata::new();

                        if let Some(headers) = &jetstream_msg.headers {
                            for (key, values) in headers.iter() {
                                let key_str = key.to_string();
                                if key_str == "strev-uuid" {
                                    continue;
                                }
                                if let Some(val) = values.iter().next() {
                                    metadata.set(key_str, val.to_string());
                                }
                            }
                        }

                        let msg = Message::with_metadata(payload, metadata);

                        if jetstream_msg.ack().await.is_err() {
                            continue;
                        }

                        if tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(_)) => {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    None => break,
                }
            }
        });

        Ok(out_stream)
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        Ok(())
    }
}
