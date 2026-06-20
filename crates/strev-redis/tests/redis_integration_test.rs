use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use strev::{HandlerResult, Message, Publisher, Router, ShutdownSignal, Subscriber, Topic};
use strev_redis::{RedisPublisher, RedisPublisherConfig, RedisSubscriber, RedisSubscriberConfig};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

async fn redis_client() -> Option<redis::Client> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".into());
    let client = redis::Client::open(url).ok()?;
    let mut conn = client.get_multiplexed_async_connection().await.ok()?;
    let _: String = redis::cmd("PING").query_async(&mut conn).await.ok()?;
    Some(client)
}

fn unique_topic() -> Topic {
    Topic::new(format!("strev-test-{}", uuid::Uuid::new_v4()))
}

#[tokio::test]
async fn publish_and_subscribe() {
    let client = match redis_client().await {
        Some(c) => c,
        None => {
            eprintln!("skipping: redis not available");
            return;
        }
    };

    let topic = unique_topic();

    let pub_config = RedisPublisherConfig::new(client.clone());
    let publisher = RedisPublisher::new(pub_config).await.unwrap();

    let sub_config = RedisSubscriberConfig::new(client, "test-group");
    let subscriber = RedisSubscriber::new(sub_config);

    let mut stream = subscriber.subscribe(&topic).await.unwrap();

    Publisher::publish(
        &publisher,
        &topic,
        vec![Message::new(Bytes::from("hello redis"))],
    )
    .await
    .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout waiting for message")
        .expect("stream ended");

    assert_eq!(msg.payload().as_ref(), b"hello redis");
    let _ = msg.ack();
}

#[tokio::test]
async fn publish_multiple_messages() {
    let client = match redis_client().await {
        Some(c) => c,
        None => {
            eprintln!("skipping: redis not available");
            return;
        }
    };

    let topic = unique_topic();

    let pub_config = RedisPublisherConfig::new(client.clone());
    let publisher = RedisPublisher::new(pub_config).await.unwrap();

    let sub_config = RedisSubscriberConfig::new(client, "test-group");
    let subscriber = RedisSubscriber::new(sub_config);

    let mut stream = subscriber.subscribe(&topic).await.unwrap();

    let msgs: Vec<Message> = (0..3)
        .map(|i| Message::new(Bytes::from(format!("msg-{i}"))))
        .collect();

    Publisher::publish(&publisher, &topic, msgs).await.unwrap();

    for i in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("timeout")
            .expect("stream ended");

        assert_eq!(msg.payload().as_ref(), format!("msg-{i}").as_bytes());
        let _ = msg.ack();
    }
}

#[tokio::test]
async fn metadata_roundtrip() {
    let client = match redis_client().await {
        Some(c) => c,
        None => {
            eprintln!("skipping: redis not available");
            return;
        }
    };

    let topic = unique_topic();

    let pub_config = RedisPublisherConfig::new(client.clone());
    let publisher = RedisPublisher::new(pub_config).await.unwrap();

    let sub_config = RedisSubscriberConfig::new(client, "test-group");
    let subscriber = RedisSubscriber::new(sub_config);

    let mut stream = subscriber.subscribe(&topic).await.unwrap();

    let mut msg = Message::new(Bytes::from("with-meta"));
    msg.metadata_mut().set("source", "test");
    msg.metadata_mut().set("version", "1.0");

    Publisher::publish(&publisher, &topic, vec![msg])
        .await
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout")
        .expect("stream ended");

    assert_eq!(received.metadata().get("source"), Some("test"));
    assert_eq!(received.metadata().get("version"), Some("1.0"));
    assert!(received.metadata().get("redis_stream_id").is_some());
    let _ = received.ack();
}

#[tokio::test]
async fn router_with_redis() {
    let client = match redis_client().await {
        Some(c) => c,
        None => {
            eprintln!("skipping: redis not available");
            return;
        }
    };

    let topic = unique_topic();
    let processed = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();

    let counter = processed.clone();
    let sub_config = RedisSubscriberConfig::new(client.clone(), "router-test");
    router.add_consumer(
        "redis_handler",
        topic.clone(),
        RedisSubscriber::new(sub_config),
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

    tokio::time::sleep(Duration::from_millis(200)).await;

    let pub_config = RedisPublisherConfig::new(client);
    let publisher = RedisPublisher::new(pub_config).await.unwrap();

    for i in 0..3 {
        let msg = Message::new(Bytes::from(format!("router-msg-{i}")));
        Publisher::publish(&publisher, &topic, vec![msg])
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_secs(3)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(processed.load(Ordering::SeqCst), 3);
}
