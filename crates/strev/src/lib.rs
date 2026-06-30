//! Event-driven messaging for Rust.
//!
//! strev provides a uniform publish/subscribe API across multiple transports, a
//! [`Router`] that wires subscribers to handlers, composable [`Middleware`], and
//! publisher/subscriber [`decorator`]s. Messages carry their acknowledgement state in
//! the type system so each message is acked or nacked exactly once.
//!
//! Backends live in companion crates: `strev-channel`, `strev-redis`, `strev-nats`, and
//! `strev-kafka`. CloudEvents enveloping lives in `strev-cloudevents`.
pub mod decorator;
mod delay;
mod error;
mod fanin;
mod forwarder;
mod handler;
mod message;
mod metadata;
pub mod middleware;
mod outcome;
mod publisher;
mod requestreply;
mod requeuer;
mod router;
mod stream;
mod subscriber;
mod topic;

pub use decorator::{
    MessageTransformPublisherDecorator, MessageTransformSubscriberDecorator, PublisherDecorator,
    SubscriberDecorator,
};
pub use delay::{Delay, DelayedPublisher};
pub use error::{
    CloseError, DeserializeError, HandlerError, PublishError, RouterError, SubscribeError,
};
pub use fanin::{FanIn, FanInConfig};
pub use forwarder::{Forwarder, ForwarderConfig, ForwarderPublisher};
pub use handler::{Handler, HandlerResult, ProducedMessage, passthrough};
pub use message::{AckReceiver, AckState, Acked, Disposition, Message, Nacked, Pending};
pub use metadata::Metadata;
pub use middleware::Middleware;
pub use outcome::Outcome;
pub use publisher::Publisher;
pub use requestreply::{RequestReply, RequestReplyError};
pub use requeuer::{Requeuer, RequeuerConfig};
pub use router::{HandlerBuilder, Router, RouterConfig, ShutdownSignal};
pub use stream::{MessageStream, bulk_read};
pub use subscriber::Subscriber;
pub use topic::Topic;
