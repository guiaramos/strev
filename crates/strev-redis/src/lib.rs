//! Redis Streams backend for strev.
//!
//! Provides [`RedisPublisher`] and [`RedisSubscriber`] backed by Redis Streams with
//! consumer groups and a pluggable [`Marshaller`] for field serialization.
mod delay;
mod marshaller;
mod publisher;
mod subscriber;

pub use delay::{RedisDelayPromoter, RedisDelayPromoterConfig};
pub use marshaller::{DefaultMarshaller, Marshaller};
pub use publisher::{RedisPublisher, RedisPublisherConfig};
pub use subscriber::{RedisSubscriber, RedisSubscriberConfig};
