//! PostgreSQL backend for strev.
//!
//! Provides [`PostgresPublisher`] and [`PostgresSubscriber`] backed by a durable message
//! table with per-consumer-group offset tracking. Subscribers poll for new messages and
//! advance their offset transactionally, using `FOR UPDATE SKIP LOCKED` so competing
//! consumers in the same group never process the same message twice.
mod publisher;
mod schema;
mod subscriber;

pub use publisher::{PostgresPublisher, PostgresPublisherConfig};
pub use subscriber::{PostgresSubscriber, PostgresSubscriberConfig};
