use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use strev::{HandlerResult, Message, Publisher, Router, ShutdownSignal, Subscriber, Topic};
use strev_kafka::{KafkaPublisher, KafkaPublisherConfig, KafkaSubscriber, KafkaSubscriberConfig};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

fn kafka_brokers() -> String {
    std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into())
}

async fn kafka_available(brokers: &str) -> bool {
    let brokers = brokers.to_string();
    tokio::task::spawn_blocking(move || {
        let consumer: Result<BaseConsumer, _> = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create();
        match consumer {
            Ok(c) => c.fetch_metadata(None, Duration::from_secs(3)).is_ok(),
            Err(_) => false,
        }
    })
    .await
    .unwrap_or(false)
}

fn unique_topic() -> String {
    format!("strevtest-{}", uuid::Uuid::new_v4().simple())
}

fn unique_group() -> String {
    format!("strevgrp-{}", uuid::Uuid::new_v4().simple())
}

#[tokio::test]
async fn publish_and_subscribe() {
    let brokers = kafka_brokers();
    if !kafka_available(&brokers).await {
        eprintln!("skipping: kafka not available");
        return;
    }

    let topic = Topic::new(unique_topic());

    let sub_config = KafkaSubscriberConfig::new(&brokers, unique_group());
    let subscriber = KafkaSubscriber::new(sub_config);
    let mut msg_stream = subscriber.subscribe(&topic).await.unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;

    let publisher = KafkaPublisher::new(KafkaPublisherConfig::new(&brokers)).unwrap();
    Publisher::publish(
        &publisher,
        &topic,
        vec![Message::new(Bytes::from("hello kafka"))],
    )
    .await
    .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(15), msg_stream.next())
        .await
        .expect("timeout waiting for message")
        .expect("stream ended");

    assert_eq!(msg.payload().as_ref(), b"hello kafka");
    let _ = msg.ack();
}

#[tokio::test]
async fn publish_multiple_messages() {
    let brokers = kafka_brokers();
    if !kafka_available(&brokers).await {
        eprintln!("skipping: kafka not available");
        return;
    }

    let topic = Topic::new(unique_topic());

    let sub_config = KafkaSubscriberConfig::new(&brokers, unique_group());
    let subscriber = KafkaSubscriber::new(sub_config);
    let mut msg_stream = subscriber.subscribe(&topic).await.unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;

    let publisher = KafkaPublisher::new(KafkaPublisherConfig::new(&brokers)).unwrap();
    let msgs: Vec<Message> = (0..3)
        .map(|i| Message::new(Bytes::from(format!("msg-{i}"))))
        .collect();

    Publisher::publish(&publisher, &topic, msgs).await.unwrap();

    for i in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(15), msg_stream.next())
            .await
            .expect("timeout")
            .expect("stream ended");

        assert_eq!(msg.payload().as_ref(), format!("msg-{i}").as_bytes());
        let _ = msg.ack();
    }
}

#[tokio::test]
async fn metadata_roundtrip() {
    let brokers = kafka_brokers();
    if !kafka_available(&brokers).await {
        eprintln!("skipping: kafka not available");
        return;
    }

    let topic = Topic::new(unique_topic());

    let sub_config = KafkaSubscriberConfig::new(&brokers, unique_group());
    let subscriber = KafkaSubscriber::new(sub_config);
    let mut msg_stream = subscriber.subscribe(&topic).await.unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;

    let publisher = KafkaPublisher::new(KafkaPublisherConfig::new(&brokers)).unwrap();
    let mut msg = Message::new(Bytes::from("with-meta"));
    msg.metadata_mut().set("source", "test");
    msg.metadata_mut().set("version", "1.0");

    Publisher::publish(&publisher, &topic, vec![msg])
        .await
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(15), msg_stream.next())
        .await
        .expect("timeout")
        .expect("stream ended");

    assert_eq!(received.metadata().get("source"), Some("test"));
    assert_eq!(received.metadata().get("version"), Some("1.0"));
    let _ = received.ack();
}

#[tokio::test]
async fn router_with_kafka() {
    let brokers = kafka_brokers();
    if !kafka_available(&brokers).await {
        eprintln!("skipping: kafka not available");
        return;
    }

    let topic = Topic::new(unique_topic());
    let processed = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();

    let counter = processed.clone();
    let sub_config = KafkaSubscriberConfig::new(&brokers, unique_group());
    router.add_consumer(
        "kafka_handler",
        topic.clone(),
        KafkaSubscriber::new(sub_config),
        move |msg: Message| {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(HandlerResult::ack(msg))
            }
        },
    );

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });

    tokio::time::sleep(Duration::from_secs(3)).await;

    let publisher = KafkaPublisher::new(KafkaPublisherConfig::new(&brokers)).unwrap();
    for i in 0..3 {
        let msg = Message::new(Bytes::from(format!("router-msg-{i}")));
        Publisher::publish(&publisher, &topic, vec![msg])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    tokio::time::sleep(Duration::from_secs(8)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(processed.load(Ordering::SeqCst), 3);
}
