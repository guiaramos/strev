use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use lapin::ExchangeKind;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicQosOptions, ExchangeDeclareOptions,
    QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable};
use strev::{CloseError, Message, MessageStream, Metadata, SubscribeError, Topic};

use crate::connect;

pub struct AmqpSubscriberConfig {
    pub uri: String,
    pub group: String,
    pub prefetch: u16,
    pub buffer_size: usize,
}

impl AmqpSubscriberConfig {
    pub fn new(uri: impl Into<String>, group: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            group: group.into(),
            prefetch: 1,
            buffer_size: 64,
        }
    }
}

pub struct AmqpSubscriber {
    config: AmqpSubscriberConfig,
}

impl AmqpSubscriber {
    pub fn new(config: AmqpSubscriberConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl strev::Subscriber for AmqpSubscriber {
    async fn subscribe(&self, topic: &Topic) -> Result<MessageStream, SubscribeError> {
        let connection = connect(&self.config.uri)
            .await
            .map_err(|e| SubscribeError::Backend(Box::new(e)))?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|e| SubscribeError::Backend(Box::new(e)))?;

        channel
            .exchange_declare(
                topic.as_str(),
                ExchangeKind::Fanout,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| SubscribeError::Backend(Box::new(e)))?;

        let queue_name = format!("{}.{}", topic.as_str(), self.config.group);
        channel
            .queue_declare(
                &queue_name,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| SubscribeError::Backend(Box::new(e)))?;
        channel
            .queue_bind(
                &queue_name,
                topic.as_str(),
                "",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| SubscribeError::Backend(Box::new(e)))?;
        channel
            .basic_qos(self.config.prefetch, BasicQosOptions::default())
            .await
            .map_err(|e| SubscribeError::Backend(Box::new(e)))?;

        let mut consumer = channel
            .basic_consume(
                &queue_name,
                "",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| SubscribeError::Backend(Box::new(e)))?;

        let (tx, stream) = MessageStream::channel(self.config.buffer_size);

        tokio::spawn(async move {
            let _connection = connection;
            let _channel = channel;
            loop {
                tokio::select! {
                    biased;
                    _ = tx.closed() => break,
                    next = consumer.next() => match next {
                        Some(Ok(delivery)) => {
                            let payload = Bytes::copy_from_slice(&delivery.data);
                            let mut metadata = Metadata::new();
                            if let Some(headers) = delivery.properties.headers() {
                                for (key, value) in headers.inner() {
                                    if key.as_str() == "strev-uuid" {
                                        continue;
                                    }
                                    if let AMQPValue::LongString(text) = value {
                                        metadata.set(key.as_str(), text.to_string());
                                    }
                                }
                            }

                            let msg = Message::with_metadata(payload, metadata);
                            if tx.send(msg).await.is_err() {
                                break;
                            }
                            let _ = delivery.ack(BasicAckOptions::default()).await;
                        }
                        Some(Err(_)) => {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                        None => break,
                    }
                }
            }
        });

        Ok(stream)
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        Ok(())
    }
}
