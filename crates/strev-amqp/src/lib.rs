//! AMQP (RabbitMQ) backend for strev.
//!
//! Provides [`AmqpPublisher`] and [`AmqpSubscriber`]. Publishing declares a durable fanout
//! exchange named after the topic and publishes to it. Subscribing declares a durable
//! queue named `{topic}.{group}` bound to that exchange, so each consumer group gets its
//! own copy of the messages and consumers within a group compete on the shared queue.
use lapin::{Connection, ConnectionProperties};

mod publisher;
mod subscriber;

pub use publisher::{AmqpPublisher, AmqpPublisherConfig};
pub use subscriber::{AmqpSubscriber, AmqpSubscriberConfig};

pub(crate) async fn connect(uri: &str) -> Result<Connection, lapin::Error> {
    Connection::connect(
        uri,
        ConnectionProperties::default()
            .with_executor(tokio_executor_trait::Tokio::current())
            .with_reactor(tokio_reactor_trait::Tokio),
    )
    .await
}
