use async_trait::async_trait;
use mongodb::Client;
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{Binary, Bson, Document, doc};
use strev::{CloseError, Message, Outcome, PublishError, Topic};

use crate::{DEFAULT_DATABASE, MESSAGES_COLLECTION};

pub struct MongoPublisherConfig {
    pub client: Client,
    pub database: String,
}

impl MongoPublisherConfig {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            database: DEFAULT_DATABASE.to_string(),
        }
    }

    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self
    }
}

pub struct MongoPublisher {
    collection: mongodb::Collection<Document>,
}

impl MongoPublisher {
    pub fn new(config: MongoPublisherConfig) -> Self {
        let collection = config
            .client
            .database(&config.database)
            .collection(MESSAGES_COLLECTION);
        Self { collection }
    }
}

#[async_trait]
impl strev::Publisher for MongoPublisher {
    async fn publish(
        &self,
        topic: &Topic,
        messages: Vec<Message>,
    ) -> Result<Vec<Outcome>, PublishError> {
        let mut outcomes = Vec::with_capacity(messages.len());

        for msg in messages {
            let mut metadata = Document::new();
            for (key, value) in msg.metadata().iter() {
                metadata.insert(key, value);
            }

            let document = doc! {
                "topic": topic.as_str(),
                "uuid": msg.uuid().to_string(),
                "payload": Bson::Binary(Binary {
                    subtype: BinarySubtype::Generic,
                    bytes: msg.payload().to_vec(),
                }),
                "metadata": metadata,
            };

            match self.collection.insert_one(document).await {
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
