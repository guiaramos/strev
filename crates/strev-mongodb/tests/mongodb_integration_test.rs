use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use mongodb::Client;
use strev::{HandlerResult, Message, Publisher, Router, ShutdownSignal, Subscriber, Topic};
use strev_mongodb::{MongoPublisher, MongoPublisherConfig, MongoSubscriber, MongoSubscriberConfig};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

async fn mongo_client() -> Option<Client> {
    let uri = std::env::var("MONGODB_URI")
        .unwrap_or_else(|_| "mongodb://127.0.0.1:27017/?directConnection=true".into());
    let client = Client::with_uri_str(&uri).await.ok()?;
    client
        .database("strev")
        .run_command(mongodb::bson::doc! { "ping": 1 })
        .await
        .ok()?;
    Some(client)
}

fn unique_topic() -> String {
    format!("strevtest.{}", uuid::Uuid::new_v4().simple())
}

fn unique_group() -> String {
    format!("grp-{}", uuid::Uuid::new_v4().simple())
}

#[tokio::test]
async fn publish_and_subscribe() {
    let client = match mongo_client().await {
        Some(c) => c,
        None => {
            eprintln!("skipping: mongodb not available");
            return;
        }
    };

    let topic = Topic::new(unique_topic());

    let subscriber =
        MongoSubscriber::new(MongoSubscriberConfig::new(client.clone(), unique_group()));
    let mut msg_stream = subscriber.subscribe(&topic).await.unwrap();

    let publisher = MongoPublisher::new(MongoPublisherConfig::new(client));
    Publisher::publish(
        &publisher,
        &topic,
        vec![Message::new(Bytes::from("hello mongo"))],
    )
    .await
    .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(10), msg_stream.next())
        .await
        .expect("timeout waiting for message")
        .expect("stream ended");

    assert_eq!(msg.payload().as_ref(), b"hello mongo");
    let _ = msg.ack();
}

#[tokio::test]
async fn publish_multiple_messages() {
    let client = match mongo_client().await {
        Some(c) => c,
        None => {
            eprintln!("skipping: mongodb not available");
            return;
        }
    };

    let topic = Topic::new(unique_topic());

    let subscriber =
        MongoSubscriber::new(MongoSubscriberConfig::new(client.clone(), unique_group()));
    let mut msg_stream = subscriber.subscribe(&topic).await.unwrap();

    let publisher = MongoPublisher::new(MongoPublisherConfig::new(client));
    let msgs: Vec<Message> = (0..3)
        .map(|i| Message::new(Bytes::from(format!("msg-{i}"))))
        .collect();

    Publisher::publish(&publisher, &topic, msgs).await.unwrap();

    for i in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(10), msg_stream.next())
            .await
            .expect("timeout")
            .expect("stream ended");
        assert_eq!(msg.payload().as_ref(), format!("msg-{i}").as_bytes());
        let _ = msg.ack();
    }
}

#[tokio::test]
async fn metadata_roundtrip() {
    let client = match mongo_client().await {
        Some(c) => c,
        None => {
            eprintln!("skipping: mongodb not available");
            return;
        }
    };

    let topic = Topic::new(unique_topic());

    let subscriber =
        MongoSubscriber::new(MongoSubscriberConfig::new(client.clone(), unique_group()));
    let mut msg_stream = subscriber.subscribe(&topic).await.unwrap();

    let publisher = MongoPublisher::new(MongoPublisherConfig::new(client));
    let mut msg = Message::new(Bytes::from("with-meta"));
    msg.metadata_mut().set("source", "test");
    msg.metadata_mut().set("version", "1.0");

    Publisher::publish(&publisher, &topic, vec![msg])
        .await
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(10), msg_stream.next())
        .await
        .expect("timeout")
        .expect("stream ended");

    assert_eq!(received.metadata().get("source"), Some("test"));
    assert_eq!(received.metadata().get("version"), Some("1.0"));
    let _ = received.ack();
}

#[tokio::test]
async fn router_with_mongodb() {
    let client = match mongo_client().await {
        Some(c) => c,
        None => {
            eprintln!("skipping: mongodb not available");
            return;
        }
    };

    let topic = Topic::new(unique_topic());
    let processed = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();

    let counter = processed.clone();
    let sub_config = MongoSubscriberConfig::new(client.clone(), unique_group());
    router.add_consumer(
        "mongo_handler",
        topic.clone(),
        MongoSubscriber::new(sub_config),
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

    let publisher = MongoPublisher::new(MongoPublisherConfig::new(client));
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
