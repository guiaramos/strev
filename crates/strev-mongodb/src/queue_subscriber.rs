use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use mongodb::Client;
use mongodb::IndexModel;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::{DateTime, Document, doc};
use mongodb::options::ReturnDocument;
use strev::{
    AckReceiver, CloseError, ConsumerLag, Disposition, LagError, MessageStream, SubscribeError,
    Topic,
};

use crate::subscriber::document_to_message;
use crate::{DEFAULT_DATABASE, MESSAGES_COLLECTION};

/// Configuration for a [`MongoQueueSubscriber`].
pub struct MongoQueueSubscriberConfig {
    pub client: Client,
    pub database: String,
    pub consumer_group: String,
    pub poll_interval: Duration,
    pub batch_size: usize,
    pub buffer_size: usize,
    pub visibility_timeout: Duration,
}

impl MongoQueueSubscriberConfig {
    pub fn new(client: Client, consumer_group: impl Into<String>) -> Self {
        Self {
            client,
            database: DEFAULT_DATABASE.to_string(),
            consumer_group: consumer_group.into(),
            poll_interval: Duration::from_millis(200),
            batch_size: 100,
            buffer_size: 64,
            visibility_timeout: Duration::from_secs(30),
        }
    }

    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self
    }
}

/// A polling subscriber that leases messages per consumer group, so a nacked or timed-out
/// message is redelivered. Use this instead of [`MongoSubscriber`](crate::MongoSubscriber)
/// when you need redelivery rather than real-time change-stream delivery. Per-group consume
/// state is stored on each message document under a `consumed.<group>` key.
pub struct MongoQueueSubscriber {
    config: Arc<MongoQueueSubscriberConfig>,
}

impl MongoQueueSubscriber {
    pub fn new(config: MongoQueueSubscriberConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

/// Hex-encode the group name into a field key that is always safe as a BSON path component
/// (group names may contain `.` or `$`, which are illegal in field names).
fn group_key(group: &str) -> String {
    group.bytes().map(|b| format!("{b:02x}")).collect()
}

#[async_trait]
impl strev::Subscriber for MongoQueueSubscriber {
    async fn subscribe(&self, topic: &Topic) -> Result<MessageStream, SubscribeError> {
        let config = self.config.clone();
        let topic = topic.as_str().to_string();
        let collection: mongodb::Collection<Document> = config
            .client
            .database(&config.database)
            .collection(MESSAGES_COLLECTION);

        // Index the claim/lag access pattern: filter by topic, ordered by _id. Without this
        // the per-poll find/count is a full collection scan, unworkable at high volume.
        collection
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "topic": 1, "_id": 1 })
                    .build(),
            )
            .await
            .map_err(|e| SubscribeError::Backend(Box::new(e)))?;

        let (sender, stream) = MessageStream::channel(config.buffer_size);
        let key = group_key(&config.consumer_group);

        tokio::spawn(async move {
            loop {
                if sender.is_closed() {
                    break;
                }

                let mut delivered = 0;
                for _ in 0..config.batch_size {
                    let claimed =
                        match claim_one(&collection, &topic, &key, config.visibility_timeout).await
                        {
                            Ok(Some(doc)) => doc,
                            _ => break,
                        };

                    let Ok(id) = claimed.get_object_id("_id") else {
                        continue;
                    };
                    let (message, ack) = document_to_message(claimed).leased();

                    if sender.send(message).await.is_err() {
                        expire_lease(&collection, id, &key).await;
                        return;
                    }

                    tokio::spawn(resolve_ack(collection.clone(), id, key.clone(), ack));
                    delivered += 1;
                }

                if delivered == 0 {
                    tokio::time::sleep(config.poll_interval).await;
                }
            }
        });

        Ok(stream)
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        Ok(())
    }
}

#[async_trait]
impl ConsumerLag for MongoQueueSubscriber {
    async fn lag(&self, topic: &Topic) -> Result<u64, LagError> {
        let collection: mongodb::Collection<Document> = self
            .config
            .client
            .database(&self.config.database)
            .collection(MESSAGES_COLLECTION);
        let key = group_key(&self.config.consumer_group);

        let filter = doc! {
            "topic": topic.as_str(),
            "$or": [
                { format!("consumed.{key}"): { "$exists": false } },
                { format!("consumed.{key}.acked"): false },
            ],
        };

        Ok(collection.count_documents(filter).await?)
    }
}

async fn claim_one(
    collection: &mongodb::Collection<Document>,
    topic: &str,
    key: &str,
    visibility: Duration,
) -> Result<Option<Document>, mongodb::error::Error> {
    let now = DateTime::now();
    let deadline = DateTime::from_system_time(SystemTime::now() + visibility);
    let acked_field = format!("consumed.{key}.acked");
    let locked_field = format!("consumed.{key}.locked_until");

    let filter = doc! {
        "topic": topic,
        "$or": [
            { format!("consumed.{key}"): { "$exists": false } },
            { &acked_field: false, &locked_field: { "$lt": now } },
        ],
    };
    let update = doc! {
        "$set": { &acked_field: false, &locked_field: deadline },
    };

    collection
        .find_one_and_update(filter, update)
        .sort(doc! { "_id": 1 })
        .return_document(ReturnDocument::After)
        .await
}

async fn resolve_ack(
    collection: mongodb::Collection<Document>,
    id: ObjectId,
    key: String,
    ack: AckReceiver,
) {
    match ack.recv().await {
        Disposition::Ack => {
            let _ = collection
                .update_one(
                    doc! { "_id": id },
                    doc! { "$set": { format!("consumed.{key}.acked"): true } },
                )
                .await;
        }
        Disposition::Nack => {
            expire_lease(&collection, id, &key).await;
        }
    }
}

/// Expire the lease so the next poll re-claims the message (nack, shutdown, or timeout).
async fn expire_lease(collection: &mongodb::Collection<Document>, id: ObjectId, key: &str) {
    let past = DateTime::from_system_time(SystemTime::now() - Duration::from_secs(1));
    let _ = collection
        .update_one(
            doc! { "_id": id, format!("consumed.{key}.acked"): false },
            doc! { "$set": { format!("consumed.{key}.locked_until"): past } },
        )
        .await;
}
