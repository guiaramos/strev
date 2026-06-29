//! CQRS command/event buses and processors for strev.
//!
//! Commands and events are strongly-typed `serde` structs identified by a `NAME`
//! constant, so only valid, named messages can reach a bus. [`CommandBus`]/[`EventBus`]
//! publish them; [`CommandProcessor`]/[`EventProcessor`] subscribe and dispatch to typed
//! handlers through the strev [`strev::Router`].
//!
//! Design notes:
//! - Invalid states are kept unrepresentable: a command type maps to exactly one handler
//!   (a duplicate registration is a [`CqrsError::DuplicateCommandHandler`], not a silent
//!   override), and the message ack lifecycle is enforced by strev's typestate `Message`.
//! - [`Context`] is immutable and `Copy`, carrying only the message id, so dispatch adds
//!   no per-message allocation. Payloads ride on zero-copy [`bytes::Bytes`].
use serde::Serialize;
use serde::de::DeserializeOwned;

mod bus;
mod processor;

pub use bus::{CommandBus, EventBus};
pub use processor::{CommandProcessor, EventProcessor, SubscriberFactory};

pub(crate) const NAME_KEY: &str = "name";

/// A typed, named, serializable request handled by exactly one handler.
pub trait Command: Serialize + DeserializeOwned + Send + Sync + 'static {
    const NAME: &'static str;
}

/// A typed, named, serializable fact delivered to every interested handler.
pub trait Event: Serialize + DeserializeOwned + Send + Sync + 'static {
    const NAME: &'static str;
}

/// Immutable context handed to a handler alongside the decoded command or event.
#[derive(Debug, Clone, Copy)]
pub struct Context {
    message_id: uuid::Uuid,
}

impl Context {
    pub(crate) fn new(message_id: uuid::Uuid) -> Self {
        Self { message_id }
    }

    /// The originating message's unique id, useful for correlation and idempotency.
    pub fn message_id(&self) -> uuid::Uuid {
        self.message_id
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CqrsError {
    #[error("failed to serialize command/event: {0}")]
    Serialize(serde_json::Error),
    #[error(transparent)]
    Publish(#[from] strev::PublishError),
    #[error("a handler is already registered for command {0}")]
    DuplicateCommandHandler(&'static str),
}
