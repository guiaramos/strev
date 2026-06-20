use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use strev::middleware::{Deduplicator, InMemoryDeduplicateRepository};
use strev::{
    HandlerError, HandlerResult, Message, Publisher, Router, ShutdownSignal, Topic,
};
use strev_channel::Channel;
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize, Debug)]
struct OrderEvent {
    order_id: String,
    action: String,
}

#[tokio::main]
async fn main() {
    let channel = Channel::new(64);
    let topic = Topic::new("orders");

    let processed = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();

    let count = processed.clone();
    router
        .add_consumer(
            "order_processor",
            topic.clone(),
            channel.clone(),
            move |msg: Message| {
                let count = count.clone();
                async move {
                    let (event, msg): (OrderEvent, _) = match msg.try_deserialize() {
                        Ok(v) => v,
                        Err((e, msg)) => {
                            let _ = msg.nack();
                            return Err(HandlerError::Processing(Box::new(e)));
                        }
                    };

                    count.fetch_add(1, Ordering::SeqCst);
                    println!("processing order {}: {}", event.order_id, event.action);
                    Ok(HandlerResult::ack(msg))
                }
            },
        )
        .with_middleware(Deduplicator {
            repository: Arc::new(InMemoryDeduplicateRepository::new(Duration::from_secs(60))),
            key_factory: Some(Arc::new(|msg: &Message| {
                String::from_utf8_lossy(msg.payload()).to_string()
            })),
        });

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let events = vec![
        OrderEvent { order_id: "ORD-001".into(), action: "created".into() },
        OrderEvent { order_id: "ORD-001".into(), action: "created".into() },
        OrderEvent { order_id: "ORD-002".into(), action: "created".into() },
        OrderEvent { order_id: "ORD-001".into(), action: "created".into() },
        OrderEvent { order_id: "ORD-003".into(), action: "created".into() },
    ];

    println!("publishing {} messages (with duplicates)...", events.len());
    for event in &events {
        let payload = serde_json::to_vec(event).unwrap();
        Publisher::publish(&channel, &topic, vec![Message::new(Bytes::from(payload))])
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    println!(
        "\npublished: {}, processed (deduplicated): {}",
        events.len(),
        processed.load(Ordering::SeqCst)
    );
}
