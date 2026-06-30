use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use futures::TryStreamExt;
use mongodb::Client;
use mongodb::IndexModel;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::{DateTime, Document, doc};
use mongodb::options::ReturnDocument;
use strev::{CloseError, ConsumerLag, Disposition, LagError, MessageStream, SubscribeError, Topic};
use tokio::sync::mpsc;

use crate::subscriber::document_to_message;
use crate::{DEFAULT_DATABASE, MESSAGES_COLLECTION};

const CURSORS_COLLECTION: &str = "strev_cursors";
const ADVANCE_SCAN_LIMIT: i64 = 1000;
const MAX_ACK_BATCH: usize = 500;
const VERDICT_BUFFER: usize = 4096;

/// The smallest ObjectId, used as the initial cursor (deliver from the very beginning).
fn min_object_id() -> ObjectId {
    ObjectId::from_bytes([0u8; 12])
}

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

        let cursors: mongodb::Collection<Document> = config
            .client
            .database(&config.database)
            .collection(CURSORS_COLLECTION);

        let (sender, stream) = MessageStream::channel(config.buffer_size);
        let key = group_key(&config.consumer_group);

        let (verdict_tx, verdict_rx) = mpsc::channel::<(ObjectId, Disposition)>(VERDICT_BUFFER);
        tokio::spawn(ack_flusher(collection.clone(), key.clone(), verdict_rx));

        tokio::spawn(async move {
            loop {
                if sender.is_closed() {
                    break;
                }

                // Advance the cursor over the acked prefix so the claim starts past consumed
                // messages (index range scan on (topic, _id)) instead of rescanning them.
                let cursor = advance_cursor(&collection, &cursors, &key, &topic)
                    .await
                    .unwrap_or_else(|_| min_object_id());

                let mut delivered = 0;
                for _ in 0..config.batch_size {
                    let claimed = match claim_one(
                        &collection,
                        &topic,
                        &key,
                        cursor,
                        config.visibility_timeout,
                    )
                    .await
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

                    let verdict_tx = verdict_tx.clone();
                    tokio::spawn(async move {
                        let disposition = ack.recv().await;
                        let _ = verdict_tx.send((id, disposition)).await;
                    });
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
        let cursors: mongodb::Collection<Document> = self
            .config
            .client
            .database(&self.config.database)
            .collection(CURSORS_COLLECTION);
        let cursor = read_cursor(&cursors, &key, topic.as_str()).await?;

        // Everything at or below the cursor is acked, so count only the unconsumed window.
        let filter = doc! {
            "topic": topic.as_str(),
            "_id": { "$gt": cursor },
            "$or": [
                { format!("consumed.{key}"): { "$exists": false } },
                { format!("consumed.{key}.acked"): false },
            ],
        };

        Ok(collection.count_documents(filter).await?)
    }
}

fn cursor_id(key: &str, topic: &str) -> String {
    format!("{key}:{topic}")
}

async fn read_cursor(
    cursors: &mongodb::Collection<Document>,
    key: &str,
    topic: &str,
) -> Result<ObjectId, mongodb::error::Error> {
    let doc = cursors
        .find_one(doc! { "_id": cursor_id(key, topic) })
        .await?;
    Ok(doc
        .and_then(|d| d.get_object_id("cursor").ok())
        .unwrap_or_else(min_object_id))
}

/// Advance the per-group cursor over the contiguous acked prefix of the topic's messages, so
/// subsequent claims skip consumed documents. Returns the current cursor.
async fn advance_cursor(
    messages: &mongodb::Collection<Document>,
    cursors: &mongodb::Collection<Document>,
    key: &str,
    topic: &str,
) -> Result<ObjectId, mongodb::error::Error> {
    let current = read_cursor(cursors, key, topic).await?;

    let mut stream = messages
        .find(doc! { "topic": topic, "_id": { "$gt": current } })
        .sort(doc! { "_id": 1 })
        .limit(ADVANCE_SCAN_LIMIT)
        .await?;

    let mut new_cursor = current;
    while let Some(doc) = stream.try_next().await? {
        let Ok(id) = doc.get_object_id("_id") else {
            break;
        };
        let acked = doc
            .get_document("consumed")
            .ok()
            .and_then(|c| c.get_document(key).ok())
            .and_then(|g| g.get_bool("acked").ok())
            .unwrap_or(false);
        if acked {
            new_cursor = id;
        } else {
            break;
        }
    }

    if new_cursor != current {
        cursors
            .update_one(
                doc! { "_id": cursor_id(key, topic) },
                doc! { "$set": { "cursor": new_cursor } },
            )
            .upsert(true)
            .await?;
    }

    Ok(new_cursor)
}

async fn claim_one(
    collection: &mongodb::Collection<Document>,
    topic: &str,
    key: &str,
    cursor: ObjectId,
    visibility: Duration,
) -> Result<Option<Document>, mongodb::error::Error> {
    let now = DateTime::now();
    let deadline = DateTime::from_system_time(SystemTime::now() + visibility);
    let acked_field = format!("consumed.{key}.acked");
    let locked_field = format!("consumed.{key}.locked_until");

    let filter = doc! {
        "topic": topic,
        "_id": { "$gt": cursor },
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

/// Drain verdicts and settle them in batched updateMany calls: one for all acks and one for
/// all nack-expiries, instead of a round-trip per message. Bursts are coalesced via
/// `try_recv` up to [`MAX_ACK_BATCH`].
async fn ack_flusher(
    collection: mongodb::Collection<Document>,
    key: String,
    mut verdicts: mpsc::Receiver<(ObjectId, Disposition)>,
) {
    while let Some((id, disposition)) = verdicts.recv().await {
        let mut acks: Vec<ObjectId> = Vec::new();
        let mut nacks: Vec<ObjectId> = Vec::new();
        match disposition {
            Disposition::Ack => acks.push(id),
            Disposition::Nack => nacks.push(id),
        }

        while acks.len() + nacks.len() < MAX_ACK_BATCH {
            match verdicts.try_recv() {
                Ok((id, Disposition::Ack)) => acks.push(id),
                Ok((id, Disposition::Nack)) => nacks.push(id),
                Err(_) => break,
            }
        }

        if !acks.is_empty() {
            let _ = collection
                .update_many(
                    doc! { "_id": { "$in": acks } },
                    doc! { "$set": { format!("consumed.{key}.acked"): true } },
                )
                .await;
        }

        if !nacks.is_empty() {
            let past = DateTime::from_system_time(SystemTime::now() - Duration::from_secs(1));
            let _ = collection
                .update_many(
                    doc! { "_id": { "$in": nacks }, format!("consumed.{key}.acked"): false },
                    doc! { "$set": { format!("consumed.{key}.locked_until"): past } },
                )
                .await;
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
