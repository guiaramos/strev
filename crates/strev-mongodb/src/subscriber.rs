use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use mongodb::Client;
use mongodb::bson::{Bson, Document, doc};
use mongodb::change_stream::event::ResumeToken;
use strev::{CloseError, Message, MessageStream, Metadata, SubscribeError, Topic};

use crate::{DEFAULT_DATABASE, MESSAGES_COLLECTION, RESUME_TOKENS_COLLECTION};

pub struct MongoSubscriberConfig {
    pub client: Client,
    pub database: String,
    pub consumer_group: String,
    pub buffer_size: usize,
}

impl MongoSubscriberConfig {
    pub fn new(client: Client, consumer_group: impl Into<String>) -> Self {
        Self {
            client,
            database: DEFAULT_DATABASE.to_string(),
            consumer_group: consumer_group.into(),
            buffer_size: 64,
        }
    }

    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self
    }
}

pub struct MongoSubscriber {
    config: MongoSubscriberConfig,
}

impl MongoSubscriber {
    pub fn new(config: MongoSubscriberConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl strev::Subscriber for MongoSubscriber {
    async fn subscribe(&self, topic: &Topic) -> Result<MessageStream, SubscribeError> {
        let database = self.config.client.database(&self.config.database);
        let messages: mongodb::Collection<Document> = database.collection(MESSAGES_COLLECTION);
        let tokens: mongodb::Collection<Document> = database.collection(RESUME_TOKENS_COLLECTION);
        let token_id = format!("{}:{}", self.config.consumer_group, topic.as_str());

        let stored_token: Option<ResumeToken> = tokens
            .find_one(doc! { "_id": &token_id })
            .await
            .ok()
            .flatten()
            .and_then(|doc| doc.get("token").cloned())
            .and_then(|bson| mongodb::bson::from_bson(bson).ok());

        let pipeline = vec![doc! {
            "$match": {
                "operationType": "insert",
                "fullDocument.topic": topic.as_str(),
            }
        }];

        let mut watch = messages.watch().pipeline(pipeline);
        if let Some(token) = stored_token {
            watch = watch.resume_after(token);
        }

        let mut change_stream = watch
            .await
            .map_err(|e| SubscribeError::Backend(Box::new(e)))?;

        let (sender, stream) = MessageStream::channel(self.config.buffer_size);

        tokio::spawn(async move {
            loop {
                let next = tokio::select! {
                    biased;
                    _ = sender.closed() => break,
                    next = change_stream.next() => next,
                };

                let Some(result) = next else {
                    break;
                };

                let event = match result {
                    Ok(event) => event,
                    Err(_) => break,
                };

                let Some(document) = event.full_document else {
                    continue;
                };

                if sender.send(document_to_message(document)).await.is_err() {
                    break;
                }

                if let Some(token) = change_stream.resume_token()
                    && let Ok(bson) = mongodb::bson::to_bson(&token)
                {
                    let _ = tokens
                        .update_one(
                            doc! { "_id": &token_id },
                            doc! { "$set": { "token": bson } },
                        )
                        .upsert(true)
                        .await;
                }
            }
        });

        Ok(stream)
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        Ok(())
    }
}

fn document_to_message(document: Document) -> Message {
    let payload = match document.get("payload") {
        Some(Bson::Binary(binary)) => Bytes::copy_from_slice(&binary.bytes),
        _ => Bytes::new(),
    };

    let mut metadata = Metadata::new();
    if let Some(Bson::Document(meta)) = document.get("metadata") {
        for (key, value) in meta {
            if let Bson::String(text) = value {
                metadata.set(key, text);
            }
        }
    }

    Message::with_metadata(payload, metadata)
}
