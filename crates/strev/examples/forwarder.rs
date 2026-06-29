use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use strev::{
    Forwarder, ForwarderConfig, ForwarderPublisher, HandlerResult, Message, Publisher, Router,
    ShutdownSignal, Topic,
};
use strev_channel::Channel;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let channel = Channel::new(64);
    let topic = Topic::new("orders");
    let processed = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();

    // Relay messages from the forwarder topic to their real destination.
    Forwarder::register(
        &mut router,
        channel.clone(),
        Arc::new(channel.clone()),
        ForwarderConfig::new(),
    );

    // The real consumer on the destination topic.
    let counter = processed.clone();
    router.add_consumer(
        "orders",
        topic.clone(),
        channel.clone(),
        move |msg: Message| {
            let counter = counter.clone();
            async move {
                println!("delivered: {}", String::from_utf8_lossy(msg.payload()));
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(HandlerResult::ack(msg))
            }
        },
    );

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The app publishes "to orders" but the forwarder publisher redirects via the
    // forwarder topic; the Forwarder relays each message to "orders".
    let publisher = ForwarderPublisher::new(Box::new(channel.clone()));
    for i in 0..3 {
        let msg = Message::new(Bytes::from(format!("order-{i}")));
        Publisher::publish(&publisher, &topic, vec![msg])
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    println!("processed: {}", processed.load(Ordering::SeqCst));
}
