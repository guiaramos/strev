use crate::Topic;

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("publisher closed")]
    Closed,
    #[error("topic not found: {0}")]
    TopicNotFound(Topic),
    #[error(transparent)]
    Backend(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, thiserror::Error)]
pub enum SubscribeError {
    #[error("subscriber closed")]
    Closed,
    #[error(transparent)]
    Backend(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    #[error(transparent)]
    Processing(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    #[error("subscribe failed on handler {handler}: {source}")]
    Subscribe {
        handler: String,
        source: SubscribeError,
    },
    #[error("publish failed on handler {handler}: {source}")]
    Publish {
        handler: String,
        source: PublishError,
    },
    #[error("already running")]
    AlreadyRunning,
}

#[derive(Debug, thiserror::Error)]
pub enum CloseError {
    #[error("already closed")]
    AlreadyClosed,
    #[error(transparent)]
    Backend(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, thiserror::Error)]
pub enum DeserializeError {
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
