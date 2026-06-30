//! NATS backend for strev.
//!
//! Provides [`NatsPublisher`] and [`NatsSubscriber`] backed by JetStream with durable pull
//! consumers and redelivery, plus [`NatsCorePublisher`] and [`NatsCoreSubscriber`] for
//! at-most-once core NATS (ephemeral, with queue-group load balancing). Metadata travels as
//! NATS headers.
mod core_nats;
mod publisher;
mod subscriber;

pub use core_nats::{
    NatsCorePublisher, NatsCorePublisherConfig, NatsCoreSubscriber, NatsCoreSubscriberConfig,
};
pub use publisher::{NatsPublisher, NatsPublisherConfig};
pub use subscriber::{NatsSubscriber, NatsSubscriberConfig};
