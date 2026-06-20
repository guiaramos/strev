use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use strev::{HandlerResult, Message, Publisher, Router, ShutdownSignal, Subscriber, Topic};
use strev_nats::{NatsPublisher, NatsPublisherConfig, NatsSubscriber, NatsSubscriberConfig};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

async fn nats_client() -> Option<async_nats::Client> {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    async_nats::connect(&url).await.ok()
}

fn unique_stream() -> String {
    format!("strevtest{}", uuid::Uuid::new_v4().simple())
}

#[tokio::test]
async fn publish_and_subscribe() {
    let client = match nats_client().await {
        Some(c) => c,
        None => {
            eprintln!("skipping: nats not available");
            return;
        }
    };

    let stream = unique_stream();
    let topic = Topic::new(format!("{stream}.orders"));

    let pub_config = NatsPublisherConfig::new(client.clone(), &stream);
    let publisher = NatsPublisher::new(pub_config).await.unwrap();

    let sub_config = NatsSubscriberConfig::new(client, &stream);
    let subscriber = NatsSubscriber::new(sub_config);

    let mut msg_stream = subscriber.subscribe(&topic).await.unwrap();

    Publisher::publish(
        &publisher,
        &topic,
        vec![Message::new(Bytes::from("hello nats"))],
    )
    .await
    .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(5), msg_stream.next())
        .await
        .expect("timeout waiting for message")
        .expect("stream ended");

    assert_eq!(msg.payload().as_ref(), b"hello nats");
    let _ = msg.ack();
}

#[tokio::test]
async fn publish_multiple_messages() {
    let client = match nats_client().await {
        Some(c) => c,
        None => {
            eprintln!("skipping: nats not available");
            return;
        }
    };

    let stream = unique_stream();
    let topic = Topic::new(format!("{stream}.events"));

    let pub_config = NatsPublisherConfig::new(client.clone(), &stream);
    let publisher = NatsPublisher::new(pub_config).await.unwrap();

    let sub_config = NatsSubscriberConfig::new(client, &stream);
    let subscriber = NatsSubscriber::new(sub_config);

    let mut msg_stream = subscriber.subscribe(&topic).await.unwrap();

    let msgs: Vec<Message> = (0..3)
        .map(|i| Message::new(Bytes::from(format!("msg-{i}"))))
        .collect();

    Publisher::publish(&publisher, &topic, msgs).await.unwrap();

    for i in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(5), msg_stream.next())
            .await
            .expect("timeout")
            .expect("stream ended");

        assert_eq!(msg.payload().as_ref(), format!("msg-{i}").as_bytes());
        let _ = msg.ack();
    }
}

#[tokio::test]
async fn metadata_roundtrip() {
    let client = match nats_client().await {
        Some(c) => c,
        None => {
            eprintln!("skipping: nats not available");
            return;
        }
    };

    let stream = unique_stream();
    let topic = Topic::new(format!("{stream}.meta"));

    let pub_config = NatsPublisherConfig::new(client.clone(), &stream);
    let publisher = NatsPublisher::new(pub_config).await.unwrap();

    let sub_config = NatsSubscriberConfig::new(client, &stream);
    let subscriber = NatsSubscriber::new(sub_config);

    let mut msg_stream = subscriber.subscribe(&topic).await.unwrap();

    let mut msg = Message::new(Bytes::from("with-meta"));
    msg.metadata_mut().set("source", "test");
    msg.metadata_mut().set("version", "1.0");

    Publisher::publish(&publisher, &topic, vec![msg])
        .await
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(5), msg_stream.next())
        .await
        .expect("timeout")
        .expect("stream ended");

    assert_eq!(received.metadata().get("source"), Some("test"));
    assert_eq!(received.metadata().get("version"), Some("1.0"));
    let _ = received.ack();
}

#[tokio::test]
async fn router_with_nats() {
    let client = match nats_client().await {
        Some(c) => c,
        None => {
            eprintln!("skipping: nats not available");
            return;
        }
    };

    let stream = unique_stream();
    let topic = Topic::new(format!("{stream}.router"));
    let processed = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();

    let counter = processed.clone();
    let sub_config = NatsSubscriberConfig::new(client.clone(), &stream);
    router.add_consumer(
        "nats_handler",
        topic.clone(),
        NatsSubscriber::new(sub_config),
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

    tokio::time::sleep(Duration::from_secs(1)).await;

    let pub_config = NatsPublisherConfig::new(client, &stream);
    let publisher = NatsPublisher::new(pub_config).await.unwrap();

    for i in 0..3 {
        let msg = Message::new(Bytes::from(format!("router-msg-{i}")));
        Publisher::publish(&publisher, &topic, vec![msg])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    tokio::time::sleep(Duration::from_secs(5)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(processed.load(Ordering::SeqCst), 3);
}
