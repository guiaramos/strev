use std::time::Duration;

use futures::TryStreamExt;
use mongodb::bson::{DateTime, Document, doc};
use mongodb::{Client, Collection, IndexModel};
use strev::PublishError;
use tokio_util::sync::CancellationToken;

use crate::{DEFAULT_DATABASE, DELAYED_COLLECTION, MESSAGES_COLLECTION};

/// Configuration for a [`MongoDelayPromoter`].
pub struct MongoDelayPromoterConfig {
    pub client: Client,
    pub database: String,
    pub poll_interval: Duration,
    pub batch_size: i64,
}

impl MongoDelayPromoterConfig {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            database: DEFAULT_DATABASE.to_string(),
            poll_interval: Duration::from_millis(200),
            batch_size: 100,
        }
    }

    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self
    }
}

/// Moves due messages staged by [`publish_after`](strev::DelayedPublisher::publish_after)
/// into the watched collection, where the change stream delivers them. Run one for
/// exactly-once promotion, or several for high availability (delivery is then
/// at-least-once; pair with the `Deduplicator` middleware).
pub struct MongoDelayPromoter {
    messages: Collection<Document>,
    delayed: Collection<Document>,
    poll_interval: Duration,
    batch_size: i64,
}

impl MongoDelayPromoter {
    pub async fn new(config: MongoDelayPromoterConfig) -> Result<Self, PublishError> {
        let database = config.client.database(&config.database);
        let messages = database.collection(MESSAGES_COLLECTION);
        let delayed: Collection<Document> = database.collection(DELAYED_COLLECTION);

        delayed
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "deliver_after": 1 })
                    .build(),
            )
            .await
            .map_err(|e| PublishError::Backend(Box::new(e)))?;

        Ok(Self {
            messages,
            delayed,
            poll_interval: config.poll_interval,
            batch_size: config.batch_size,
        })
    }

    pub async fn run(self, shutdown: CancellationToken) {
        loop {
            match self.promote_once().await {
                Ok(count) if count > 0 => continue,
                _ => {}
            }

            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(self.poll_interval) => {}
            }
        }
    }

    async fn promote_once(&self) -> Result<u64, mongodb::error::Error> {
        let mut cursor = self
            .delayed
            .find(doc! { "deliver_after": { "$lte": DateTime::now() } })
            .sort(doc! { "deliver_after": 1 })
            .limit(self.batch_size)
            .await?;

        let mut promoted = 0;
        while let Some(staged) = cursor.try_next().await? {
            let id = staged.get("_id").cloned();

            let mut document = staged;
            document.remove("_id");
            document.remove("deliver_after");
            self.messages.insert_one(document).await?;

            if let Some(id) = id {
                self.delayed.delete_one(doc! { "_id": id }).await?;
            }
            promoted += 1;
        }

        Ok(promoted)
    }
}
