use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use strev::{Message, Publisher, RequeuerConfig, Router, ShutdownSignal, Subscriber, Topic};
use strev_channel::Channel;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn requeues_to_resolved_topic_and_counts_retries() {
    let channel = Channel::new(64);
    let mut destination = Subscriber::subscribe(&channel, &Topic::new("orders"))
        .await
        .unwrap();

    let mut router = Router::new();
    RequeuerConfig::new("poison")
        .destination(|message: &Message| {
            Ok(Topic::new(
                message
                    .metadata()
                    .get("original-topic")
                    .unwrap_or("orders")
                    .to_string(),
            ))
        })
        .register(&mut router, channel.clone(), Arc::new(channel.clone()));

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut message = Message::new(Bytes::from("payload"));
    message.metadata_mut().set("original-topic", "orders");
    message.metadata_mut().set("requeue-retries", "2");
    Publisher::publish(&channel, &Topic::new("poison"), vec![message])
        .await
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(2), destination.next())
        .await
        .expect("timeout")
        .expect("stream ended");

    assert_eq!(received.payload().as_ref(), b"payload");
    assert_eq!(received.metadata().get("requeue-retries"), Some("3"));
    let _ = received.ack();

    token.cancel();
    handle.await.unwrap().unwrap();
}
