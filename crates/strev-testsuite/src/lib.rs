//! Reusable pub/sub conformance scenarios for strev backends.
//!
//! Implement [`Backend`] for a publisher/subscriber pair and run the scenario functions
//! from your integration tests. Each scenario uses a fresh topic so runs are isolated.
//! Scenarios panic on failure, so call them directly from `#[tokio::test]` functions.
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use strev::{Message, MessageStream, Publisher, Subscriber, Topic};
use tokio_stream::StreamExt;

/// A backend under test: produces publishers and group-scoped subscribers on demand.
#[async_trait]
pub trait Backend: Send + Sync {
    async fn publisher(&self) -> Box<dyn Publisher>;
    async fn subscriber(&self, group: &str) -> Box<dyn Subscriber>;

    /// Time to wait after subscribing before publishing, to let the subscription become
    /// active (e.g. Kafka consumer-group assignment). Override per backend as needed.
    fn warmup(&self) -> Duration {
        Duration::from_millis(500)
    }
}

fn unique_topic() -> Topic {
    Topic::new(format!("conformance.{}", uuid::Uuid::new_v4().simple()))
}

fn unique_group() -> String {
    format!("cg.{}", uuid::Uuid::new_v4().simple())
}

async fn next_message(stream: &mut MessageStream) -> Message {
    tokio::time::timeout(Duration::from_secs(15), stream.next())
        .await
        .expect("timed out waiting for message")
        .expect("stream ended unexpectedly")
}

/// A single published message is received with its payload intact.
pub async fn roundtrip(backend: &dyn Backend) {
    let topic = unique_topic();
    let subscriber = backend.subscriber(&unique_group()).await;
    let mut stream = subscriber.subscribe(&topic).await.expect("subscribe");
    tokio::time::sleep(backend.warmup()).await;

    let publisher = backend.publisher().await;
    publisher
        .publish(&topic, vec![Message::new(Bytes::from("payload"))])
        .await
        .expect("publish");

    let message = next_message(&mut stream).await;
    assert_eq!(message.payload().as_ref(), b"payload");
    let _ = message.ack();
}

/// A large batch published in one call is fully delivered and acked, exercising the
/// batched publish/delivery/ack paths end to end.
pub async fn throughput(backend: &dyn Backend) {
    let topic = unique_topic();
    let subscriber = backend.subscriber(&unique_group()).await;
    let mut stream = subscriber.subscribe(&topic).await.expect("subscribe");
    tokio::time::sleep(backend.warmup()).await;

    let count = 1000usize;
    let publisher = backend.publisher().await;
    let publish_topic = topic.clone();

    // Publish concurrently with consuming: in-process backends (channel) apply backpressure
    // on bounded buffers, so publishing the whole batch before consuming would deadlock.
    let producer = tokio::spawn(async move {
        let messages = (0..count)
            .map(|i| Message::new(Bytes::from(format!("m-{i}"))))
            .collect();
        publisher.publish(&publish_topic, messages).await
    });

    let mut received = 0usize;
    while received < count {
        match tokio::time::timeout(Duration::from_secs(30), stream.next()).await {
            Ok(Some(message)) => {
                received += 1;
                let _ = message.ack();
            }
            _ => break,
        }
    }

    let _ = producer.await;
    assert_eq!(
        received, count,
        "every published message should be delivered"
    );
}

/// Messages published to a topic are delivered in publication order.
pub async fn ordering(backend: &dyn Backend) {
    let topic = unique_topic();
    let subscriber = backend.subscriber(&unique_group()).await;
    let mut stream = subscriber.subscribe(&topic).await.expect("subscribe");
    tokio::time::sleep(backend.warmup()).await;

    let publisher = backend.publisher().await;
    let messages = (0..5)
        .map(|i| Message::new(Bytes::from(format!("msg-{i}"))))
        .collect();
    publisher.publish(&topic, messages).await.expect("publish");

    for i in 0..5 {
        let message = next_message(&mut stream).await;
        assert_eq!(message.payload().as_ref(), format!("msg-{i}").as_bytes());
        let _ = message.ack();
    }
}

/// Message metadata survives the round trip.
pub async fn metadata_fidelity(backend: &dyn Backend) {
    let topic = unique_topic();
    let subscriber = backend.subscriber(&unique_group()).await;
    let mut stream = subscriber.subscribe(&topic).await.expect("subscribe");
    tokio::time::sleep(backend.warmup()).await;

    let publisher = backend.publisher().await;
    let mut message = Message::new(Bytes::from("payload"));
    message.metadata_mut().set("source", "conformance");
    message.metadata_mut().set("version", "1.0");
    publisher
        .publish(&topic, vec![message])
        .await
        .expect("publish");

    let received = next_message(&mut stream).await;
    assert_eq!(received.metadata().get("source"), Some("conformance"));
    assert_eq!(received.metadata().get("version"), Some("1.0"));
    let _ = received.ack();
}

/// A nacked message is redelivered. Only applies to backends that support redelivery.
pub async fn nack_redelivery(backend: &dyn Backend) {
    let topic = unique_topic();
    let subscriber = backend.subscriber(&unique_group()).await;
    let mut stream = subscriber.subscribe(&topic).await.expect("subscribe");
    tokio::time::sleep(backend.warmup()).await;

    let publisher = backend.publisher().await;
    publisher
        .publish(&topic, vec![Message::new(Bytes::from("retry-me"))])
        .await
        .expect("publish");

    let first = next_message(&mut stream).await;
    assert_eq!(first.payload().as_ref(), b"retry-me");
    let _ = first.nack();

    let second = next_message(&mut stream).await;
    assert_eq!(second.payload().as_ref(), b"retry-me");
    let _ = second.ack();
}

/// With two subscribers in the same group, each message is delivered to exactly one of them
/// (consumer-group load balancing, no duplicates). Only applies to backends with group
/// semantics, not fan-out backends like the in-memory channel.
pub async fn competing_consumers(backend: &dyn Backend) {
    let topic = unique_topic();
    let group = unique_group();

    let sub_a = backend.subscriber(&group).await;
    let sub_b = backend.subscriber(&group).await;
    let a = sub_a.subscribe(&topic).await.expect("subscribe a");
    let b = sub_b.subscribe(&topic).await.expect("subscribe b");
    tokio::time::sleep(backend.warmup()).await;

    let publisher = backend.publisher().await;
    let count = 10usize;
    let messages = (0..count)
        .map(|i| Message::new(Bytes::from(format!("m-{i}"))))
        .collect();
    publisher.publish(&topic, messages).await.expect("publish");

    let mut merged = a.merge(b);
    let mut received = Vec::new();
    while received.len() < count {
        match tokio::time::timeout(Duration::from_secs(10), merged.next()).await {
            Ok(Some(message)) => {
                received.push(String::from_utf8_lossy(message.payload()).to_string());
                let _ = message.ack();
            }
            _ => break,
        }
    }

    received.sort();
    received.dedup();
    assert_eq!(
        received.len(),
        count,
        "every message must be delivered exactly once across the group"
    );
}

/// A new subscriber in the same group resumes after a restart: a message published while
/// no consumer was attached is still delivered once a consumer rejoins the group. Only
/// applies to durable backends.
pub async fn consumer_group_resume(backend: &dyn Backend) {
    let topic = unique_topic();
    let group = unique_group();

    let first = backend.subscriber(&group).await;
    let mut stream = first.subscribe(&topic).await.expect("subscribe");
    tokio::time::sleep(backend.warmup()).await;

    let publisher = backend.publisher().await;
    publisher
        .publish(&topic, vec![Message::new(Bytes::from("before-restart"))])
        .await
        .expect("publish");

    let message = next_message(&mut stream).await;
    assert_eq!(message.payload().as_ref(), b"before-restart");
    let _ = message.ack();

    drop(stream);
    drop(first);
    tokio::time::sleep(Duration::from_millis(500)).await;

    publisher
        .publish(&topic, vec![Message::new(Bytes::from("after-restart"))])
        .await
        .expect("publish");

    let second = backend.subscriber(&group).await;
    let mut resumed = second.subscribe(&topic).await.expect("resubscribe");
    tokio::time::sleep(backend.warmup()).await;

    let message = next_message(&mut resumed).await;
    assert_eq!(message.payload().as_ref(), b"after-restart");
    let _ = message.ack();
}
