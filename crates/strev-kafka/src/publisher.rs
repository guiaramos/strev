use std::time::Duration;

use async_trait::async_trait;
use rdkafka::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use strev::{CloseError, Message, Outcome, PublishError, Topic};

/// Metadata key whose value, if present, is used as the Kafka record key, controlling
/// partitioning. Messages sharing a partition key land on the same partition and are
/// therefore delivered in order. Absent it, the message UUID is used (random distribution).
pub const PARTITION_KEY: &str = "partition-key";

pub struct KafkaPublisherConfig {
    pub brokers: String,
    pub message_timeout: Duration,
    pub options: Vec<(String, String)>,
}

impl KafkaPublisherConfig {
    pub fn new(brokers: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            message_timeout: Duration::from_secs(5),
            options: Vec::new(),
        }
    }

    pub fn option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.push((key.into(), value.into()));
        self
    }
}

pub struct KafkaPublisher {
    producer: FutureProducer,
}

impl KafkaPublisher {
    pub fn new(config: KafkaPublisherConfig) -> Result<Self, PublishError> {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", &config.brokers).set(
            "message.timeout.ms",
            config.message_timeout.as_millis().to_string(),
        );
        for (key, value) in &config.options {
            client_config.set(key, value);
        }

        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| PublishError::Backend(Box::new(e)))?;

        Ok(Self { producer })
    }
}

#[async_trait]
impl strev::Publisher for KafkaPublisher {
    async fn publish(
        &self,
        topic: &Topic,
        messages: Vec<Message>,
    ) -> Result<Vec<Outcome>, PublishError> {
        let topic_name = topic.as_str();
        let mut outcomes = Vec::with_capacity(messages.len());

        for msg in messages {
            let uuid = msg.uuid().to_string();
            let payload = msg.payload().clone();

            let mut headers = OwnedHeaders::new();
            for (k, v) in msg.metadata().iter() {
                headers = headers.insert(Header {
                    key: k,
                    value: Some(v),
                });
            }
            headers = headers.insert(Header {
                key: "strev-uuid",
                value: Some(&uuid),
            });

            let key = msg.metadata().get(PARTITION_KEY).unwrap_or(uuid.as_str());
            let record = FutureRecord::to(topic_name)
                .payload(payload.as_ref())
                .key(key)
                .headers(headers);

            match self.producer.send(record, Timeout::Never).await {
                Ok(_) => outcomes.push(msg.ack()),
                Err((e, _)) => {
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
