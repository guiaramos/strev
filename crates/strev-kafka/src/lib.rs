//! Apache Kafka backend for strev.
//!
//! Provides [`KafkaPublisher`] and [`KafkaSubscriber`] backed by rdkafka with consumer
//! groups and manual offset commits. Enable the `sasl-ssl` feature for TLS and SASL
//! against managed brokers.
mod publisher;
mod subscriber;

pub use publisher::{KafkaPublisher, KafkaPublisherConfig};
pub use subscriber::{KafkaSubscriber, KafkaSubscriberConfig};
