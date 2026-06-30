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

        if messages.is_empty() {
            return Ok(Vec::new());
        }

        // Publish every message first, collecting the confirm futures, then await them all,
        // so broker confirms pipeline instead of a round-trip per message.
        let mut confirms = Vec::with_capacity(messages.len());
        let mut failure: Option<Box<dyn std::error::Error + Send + Sync>> = None;

        for msg in &messages {
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

            match self
                .channel
                .basic_publish(
                    topic.as_str(),
                    "",
                    BasicPublishOptions::default(),
                    msg.payload(),
                    properties,
                )
                .await
            {
                Ok(confirm) => confirms.push(confirm),
                Err(e) => {
                    failure = Some(Box::new(e));
                    break;
                }
            }
        }

        if failure.is_none() {
            for confirm in confirms {
                if let Err(e) = confirm.await {
                    failure = Some(Box::new(e));
                    break;
                }
            }
        }

        match failure {
            None => Ok(messages.into_iter().map(Message::ack).collect()),
            Some(e) => {
                for msg in messages {
                    let _ = msg.nack();
                }
                Err(PublishError::Backend(e))
            }
        }
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        Ok(())
    }
}
