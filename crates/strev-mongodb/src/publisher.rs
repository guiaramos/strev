use async_trait::async_trait;
use mongodb::Client;
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{Binary, Bson, DateTime, Document, doc};
use strev::{CloseError, Delay, DelayedPublisher, Message, Outcome, PublishError, Topic};

use crate::{DEFAULT_DATABASE, DELAYED_COLLECTION, MESSAGES_COLLECTION};

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
    delayed: mongodb::Collection<Document>,
}

impl MongoPublisher {
    pub fn new(config: MongoPublisherConfig) -> Self {
        let database = config.client.database(&config.database);
        let collection = database.collection(MESSAGES_COLLECTION);
        let delayed = database.collection(DELAYED_COLLECTION);
        Self {
            collection,
            delayed,
        }
    }
}

#[async_trait]
impl strev::Publisher for MongoPublisher {
    async fn publish(
        &self,
        topic: &Topic,
        messages: Vec<Message>,
    ) -> Result<Vec<Outcome>, PublishError> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        let documents: Vec<Document> = messages
            .iter()
            .map(|msg| message_document(topic, msg))
            .collect();

        settle(messages, self.collection.insert_many(documents).await.err())
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        Ok(())
    }
}

#[async_trait]
impl DelayedPublisher for MongoPublisher {
    async fn publish_after(
        &self,
        topic: &Topic,
        messages: Vec<Message>,
        delay: Delay,
    ) -> Result<Vec<Outcome>, PublishError> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        let deliver_after = DateTime::from_system_time(delay.not_before());
        let documents: Vec<Document> = messages
            .iter()
            .map(|msg| {
                let mut document = message_document(topic, msg);
                document.insert("deliver_after", deliver_after);
                document
            })
            .collect();

        settle(messages, self.delayed.insert_many(documents).await.err())
    }
}

/// Ack every message on success, or nack every message and return the error.
fn settle(
    messages: Vec<Message>,
    failure: Option<mongodb::error::Error>,
) -> Result<Vec<Outcome>, PublishError> {
    match failure {
        None => Ok(messages.into_iter().map(Message::ack).collect()),
        Some(e) => {
            for msg in messages {
                let _ = msg.nack();
            }
            Err(PublishError::Backend(Box::new(e)))
        }
    }
}

fn message_document(topic: &Topic, msg: &Message) -> Document {
    let mut metadata = Document::new();
    for (key, value) in msg.metadata().iter() {
        metadata.insert(key, value);
    }

    doc! {
        "topic": topic.as_str(),
        "uuid": msg.uuid().to_string(),
        "payload": Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: msg.payload().to_vec(),
        }),
        "metadata": metadata,
    }
}
