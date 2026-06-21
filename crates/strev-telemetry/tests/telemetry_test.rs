use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use strev::{
    HandlerError, HandlerResult, Message, Middleware, Publisher, Router, ShutdownSignal, Topic,
};
use strev_channel::Channel;
use strev_telemetry::Telemetry;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn passes_ack_through() {
    let handler = |msg: Message| async move { Ok(HandlerResult::ack(msg)) };
    let wrapped = Telemetry::new().wrap(Box::new(handler));

    let result = wrapped
        .handle(Message::new(Bytes::from("payload")))
        .await
        .unwrap();

    assert!(result.outcome().is_acked());
}

#[tokio::test]
async fn passes_nack_through() {
    let handler = |msg: Message| async move { Ok(HandlerResult::nack(msg)) };
    let wrapped = Telemetry::new().wrap(Box::new(handler));

    let result = wrapped
        .handle(Message::new(Bytes::from("payload")))
        .await
        .unwrap();

    assert!(result.outcome().is_nacked());
}

#[tokio::test]
async fn propagates_error() {
    let handler = |_msg: Message| async move {
        Err(HandlerError::Processing(Box::new(std::io::Error::other(
            "boom",
        ))))
    };
    let wrapped = Telemetry::new().wrap(Box::new(handler));

    let result = wrapped.handle(Message::new(Bytes::from("payload"))).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn router_with_telemetry() {
    let channel = Channel::new(16);
    let topic = Topic::new("orders");
    let processed = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();
    router.add_middleware(Telemetry::new());

    let counter = processed.clone();
    router.add_consumer(
        "orders",
        topic.clone(),
        channel.clone(),
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

    tokio::time::sleep(Duration::from_millis(100)).await;
    for i in 0..3 {
        Publisher::publish(
            &channel,
            &topic,
            vec![Message::new(Bytes::from(format!("m-{i}")))],
        )
        .await
        .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(processed.load(Ordering::SeqCst), 3);
}
