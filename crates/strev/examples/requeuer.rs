use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use strev::{HandlerResult, Message, Publisher, RequeuerConfig, Router, ShutdownSignal, Topic};
use strev_channel::Channel;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let channel = Channel::new(64);
    let processed = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();

    // Drain the dead-letter topic back to "orders", pacing each retry.
    RequeuerConfig::new("poison")
        .delay(Duration::from_millis(50))
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

    let counter = processed.clone();
    router.add_consumer(
        "orders",
        Topic::new("orders"),
        channel.clone(),
        move |msg: Message| {
            let counter = counter.clone();
            async move {
                let retries = msg.metadata().get("requeue-retries").unwrap_or("0");
                println!(
                    "reprocessed: {} (retry {retries})",
                    String::from_utf8_lossy(msg.payload())
                );
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
        let mut msg = Message::new(Bytes::from(format!("order-{i}")));
        msg.metadata_mut().set("original-topic", "orders");
        Publisher::publish(&channel, &Topic::new("poison"), vec![msg])
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    println!("processed: {}", processed.load(Ordering::SeqCst));
}
