use async_nats::jetstream;
use async_trait::async_trait;
use strev::{CloseError, Message, Outcome, PublishError, Topic};

pub struct NatsPublisherConfig {
    pub client: async_nats::Client,
    pub stream_name: String,
}

impl NatsPublisherConfig {
    pub fn new(client: async_nats::Client, stream_name: impl Into<String>) -> Self {
        Self {
            client,
            stream_name: stream_name.into(),
        }
    }
}

pub struct NatsPublisher {
    jetstream: jetstream::Context,
}

impl NatsPublisher {
    pub async fn new(config: NatsPublisherConfig) -> Result<Self, PublishError> {
        let jetstream = jetstream::new(config.client.clone());

        let subjects = format!("{}.>", config.stream_name);
        jetstream
            .get_or_create_stream(jetstream::stream::Config {
                name: config.stream_name,
                subjects: vec![subjects],
                ..Default::default()
            })
            .await
            .map_err(|e| PublishError::Backend(Box::new(e)))?;

        Ok(Self { jetstream })
    }
}

#[async_trait]
impl strev::Publisher for NatsPublisher {
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

            let payload = msg.payload().clone();

            let result = self
                .jetstream
                .publish_with_headers(subject.clone(), headers, payload)
                .await;

            match result {
                Ok(ack_future) => match ack_future.await {
                    Ok(_) => outcomes.push(msg.ack()),
                    Err(e) => {
                        let _ = msg.nack();
                        return Err(PublishError::Backend(Box::new(e)));
                    }
                },
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
