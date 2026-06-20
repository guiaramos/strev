use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use strev::{
    HandlerResult, Message, Publisher, Router, ShutdownSignal, Topic,
};
use strev_channel::Channel;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn router_processes_messages_end_to_end() {
    let channel = Channel::new(16);
    let topic_in = Topic::new("input");
    let count = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();
    let count_clone = count.clone();

    router.add_handler(
        "test_handler",
        topic_in.clone(),
        channel.clone(),
        channel.clone(),
        move |msg: Message| {
            let c = count_clone.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(HandlerResult::ack(msg))
            }
        },
    );

    let token = CancellationToken::new();
    let token_clone = token.clone();

    let router_handle = tokio::spawn(async move {
        router.run(ShutdownSignal::Token(token_clone)).await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let msg = Message::new(Bytes::from("test"));
    Publisher::publish(&channel, &topic_in, vec![msg]).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    token.cancel();

    router_handle.await.unwrap().unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn router_consumer_without_publisher() {
    let channel = Channel::new(16);
    let topic = Topic::new("events");
    let count = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();
    let count_clone = count.clone();

    router.add_consumer(
        "consumer",
        topic.clone(),
        channel.clone(),
        move |msg: Message| {
            let c = count_clone.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(HandlerResult::ack(msg))
            }
        },
    );

    let token = CancellationToken::new();
    let token_clone = token.clone();

    let router_handle = tokio::spawn(async move {
        router.run(ShutdownSignal::Token(token_clone)).await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..5 {
        let msg = Message::new(Bytes::from(format!("msg-{i}")));
        Publisher::publish(&channel, &topic, vec![msg]).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    token.cancel();

    router_handle.await.unwrap().unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn router_shutdown_via_cancellation_token() {
    let channel = Channel::new(16);
    let topic = Topic::new("test");

    let mut router = Router::new();
    router.add_consumer(
        "noop",
        topic,
        channel.clone(),
        |msg: Message| async move {
            Ok(HandlerResult::ack(msg))
        },
    );

    let token = CancellationToken::new();
    let token_clone = token.clone();

    let handle = tokio::spawn(async move {
        router.run(ShutdownSignal::Token(token_clone)).await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    token.cancel();

    let result = tokio::time::timeout(Duration::from_secs(2), handle).await;
    assert!(result.is_ok());
}
