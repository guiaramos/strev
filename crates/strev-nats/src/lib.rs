//! NATS JetStream backend for strev.
//!
//! Provides [`NatsPublisher`] and [`NatsSubscriber`] backed by JetStream with durable
//! pull consumers. Message metadata travels as NATS headers.
mod publisher;
mod subscriber;

pub use publisher::{NatsPublisher, NatsPublisherConfig};
pub use subscriber::{NatsSubscriber, NatsSubscriberConfig};
