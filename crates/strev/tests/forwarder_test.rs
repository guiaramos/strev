use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use strev::{
    Forwarder, ForwarderConfig, ForwarderPublisher, Message, Publisher, Router, ShutdownSignal,
    Subscriber, Topic,
};
use strev_channel::Channel;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn forwards_payload_and_metadata_to_destination() {
    let channel = Channel::new(64);
    let mut destination = Subscriber::subscribe(&channel, &Topic::new("orders"))
        .await
        .unwrap();

    let mut router = Router::new();
    Forwarder::register(
        &mut router,
        channel.clone(),
        Arc::new(channel.clone()),
        ForwarderConfig::new(),
    );

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let forwarder = ForwarderPublisher::new(Box::new(channel.clone()));
    let mut message = Message::new(Bytes::from("payload"));
    message.metadata_mut().set("trace", "abc");
    Publisher::publish(&forwarder, &Topic::new("orders"), vec![message])
        .await
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(2), destination.next())
        .await
        .expect("timeout")
        .expect("stream ended");

    assert_eq!(received.payload().as_ref(), b"payload");
    assert_eq!(received.metadata().get("trace"), Some("abc"));
    assert_eq!(received.metadata().get("forward-destination"), None);
    let _ = received.ack();

    token.cancel();
    handle.await.unwrap().unwrap();
}
