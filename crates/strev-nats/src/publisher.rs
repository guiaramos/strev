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
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        let subject = topic.as_str().to_string();

        // Send every message first, collecting the publish-ack futures, then await them all,
        // so server acks pipeline instead of a round-trip per message.
        let mut acks = Vec::with_capacity(messages.len());
        let mut failure: Option<Box<dyn std::error::Error + Send + Sync>> = None;

        for msg in &messages {
            let mut headers = async_nats::HeaderMap::new();
            for (k, v) in msg.metadata().iter() {
                headers.insert(k, v);
            }
            headers.insert("strev-uuid", msg.uuid().to_string().as_str());

            match self
                .jetstream
                .publish_with_headers(subject.clone(), headers, msg.payload().clone())
                .await
            {
                Ok(ack_future) => acks.push(ack_future),
                Err(e) => {
                    failure = Some(Box::new(e));
                    break;
                }
            }
        }

        if failure.is_none() {
            for ack in acks {
                if let Err(e) = ack.await {
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
