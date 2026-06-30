use std::time::Duration;

use futures::TryStreamExt;
use mongodb::bson::{Document, doc};
use mongodb::{Client, Collection};
use tokio_util::sync::CancellationToken;

use crate::{CURSORS_COLLECTION, DEFAULT_DATABASE, MESSAGES_COLLECTION};

/// Configuration for [`MongoRetention`].
pub struct MongoRetentionConfig {
    pub client: Client,
    pub database: String,
    pub interval: Duration,
    pub batch_size: i64,
}

impl MongoRetentionConfig {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            database: DEFAULT_DATABASE.to_string(),
            interval: Duration::from_secs(60),
            batch_size: 10_000,
        }
    }

    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self
    }
}

/// Purges messages every [`MongoQueueSubscriber`](crate::MongoQueueSubscriber) group has
/// consumed: for each topic, deletes messages whose `_id` is at or below the minimum group
/// cursor, in batches. Keeps the messages collection bounded under high publish rates. Run
/// one instance.
pub struct MongoRetention {
    messages: Collection<Document>,
    cursors: Collection<Document>,
    interval: Duration,
    batch_size: i64,
}

impl MongoRetention {
    pub fn new(config: MongoRetentionConfig) -> Self {
        let database = config.client.database(&config.database);
        Self {
            messages: database.collection(MESSAGES_COLLECTION),
            cursors: database.collection(CURSORS_COLLECTION),
            interval: config.interval,
            batch_size: config.batch_size,
        }
    }

    pub async fn run(self, shutdown: CancellationToken) {
        loop {
            match self.purge_once().await {
                Ok(deleted) if deleted > 0 => continue,
                _ => {}
            }

            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(self.interval) => {}
            }
        }
    }

    async fn purge_once(&self) -> Result<u64, mongodb::error::Error> {
        // Minimum cursor per topic across all groups: everything at or below is consumed.
        let mut floors = self
            .cursors
            .aggregate(vec![
                doc! { "$group": { "_id": "$topic", "min": { "$min": "$cursor" } } },
            ])
            .await?;

        let mut deleted = 0;
        while let Some(group) = floors.try_next().await? {
            let (Ok(topic), Ok(min)) = (group.get_str("_id"), group.get_object_id("min")) else {
                continue;
            };

            let mut ids = self
                .messages
                .find(doc! { "topic": topic, "_id": { "$lte": min } })
                .projection(doc! { "_id": 1 })
                .limit(self.batch_size)
                .await?;

            let mut batch = Vec::new();
            while let Some(doc) = ids.try_next().await? {
                if let Ok(id) = doc.get_object_id("_id") {
                    batch.push(id);
                }
            }

            if !batch.is_empty() {
                let result = self
                    .messages
                    .delete_many(doc! { "_id": { "$in": batch } })
                    .await?;
                deleted += result.deleted_count;
            }
        }

        Ok(deleted)
    }
}
