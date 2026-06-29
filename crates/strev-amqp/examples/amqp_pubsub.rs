use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use strev::{HandlerResult, Message, Publisher, Router, ShutdownSignal, Topic};
use strev_amqp::{AmqpPublisher, AmqpPublisherConfig, AmqpSubscriber, AmqpSubscriberConfig};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let uri = std::env::var("AMQP_URI")
        .unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:5672/%2f".into());

    let topic = Topic::new("strev.example.orders");
    let processed = Arc::new(AtomicU32::new(0));

    let subscriber = AmqpSubscriber::new(AmqpSubscriberConfig::new(&uri, "strev-example"));

    let mut router = Router::new();
    let counter = processed.clone();
    router.add_consumer(
        "order_processor",
        topic.clone(),
        subscriber,
        move |msg: Message| {
            let counter = counter.clone();
            async move {
                let payload = String::from_utf8_lossy(msg.payload()).to_string();
                println!("processing: {payload}");
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(HandlerResult::ack(msg))
            }
        },
    );

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });

    tokio::time::sleep(Duration::from_secs(1)).await;

    let publisher = AmqpPublisher::new(AmqpPublisherConfig::new(&uri))
        .await
        .unwrap();
    for i in 0..5 {
        let msg = Message::new(Bytes::from(format!("order-{i}")));
        Publisher::publish(&publisher, &topic, vec![msg])
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_secs(2)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    println!("processed: {}", processed.load(Ordering::SeqCst));
}
