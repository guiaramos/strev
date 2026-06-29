use async_trait::async_trait;
use lapin::options::{BasicPublishOptions, ExchangeDeclareOptions};
use lapin::types::{AMQPValue, FieldTable};
use lapin::{BasicProperties, Channel, Connection, ExchangeKind};
use strev::{CloseError, Message, Outcome, PublishError, Topic};

use crate::connect;

pub struct AmqpPublisherConfig {
    pub uri: String,
}

impl AmqpPublisherConfig {
    pub fn new(uri: impl Into<String>) -> Self {
        Self { uri: uri.into() }
    }
}

pub struct AmqpPublisher {
    _connection: Connection,
    channel: Channel,
}

impl AmqpPublisher {
    pub async fn new(config: AmqpPublisherConfig) -> Result<Self, PublishError> {
        let connection = connect(&config.uri)
            .await
            .map_err(|e| PublishError::Backend(Box::new(e)))?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|e| PublishError::Backend(Box::new(e)))?;
        Ok(Self {
            _connection: connection,
            channel,
        })
    }
}

#[async_trait]
impl strev::Publisher for AmqpPublisher {
    async fn publish(
        &self,
        topic: &Topic,
        messages: Vec<Message>,
    ) -> Result<Vec<Outcome>, PublishError> {
        self.channel
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
            .map_err(|e| PublishError::Backend(Box::new(e)))?;

        let mut outcomes = Vec::with_capacity(messages.len());
        for msg in messages {
            let mut headers = FieldTable::default();
            for (key, value) in msg.metadata().iter() {
                headers.insert(key.into(), AMQPValue::LongString(value.into()));
            }
            headers.insert(
                "strev-uuid".into(),
                AMQPValue::LongString(msg.uuid().to_string().into()),
            );

            let properties = BasicProperties::default()
                .with_delivery_mode(2)
                .with_headers(headers);

            let result = self
                .channel
                .basic_publish(
                    topic.as_str(),
                    "",
                    BasicPublishOptions::default(),
                    msg.payload(),
                    properties,
                )
                .await;

            let outcome = match result {
                Ok(confirm) => confirm.await,
                Err(e) => {
                    let _ = msg.nack();
                    return Err(PublishError::Backend(Box::new(e)));
                }
            };

            match outcome {
                Ok(_) => outcomes.push(msg.ack()),
                Err(e) => {
                    let _ = msg.nack();
                    return Err(PublishError::Backend(Box::new(e)));
                }
            }
        }

        Ok(outcomes)
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        Ok(())
    }
}
