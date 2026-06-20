use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use strev::{HandlerResult, Message, Publisher, Router, ShutdownSignal, Topic};
use strev_nats::{NatsPublisher, NatsPublisherConfig, NatsSubscriber, NatsSubscriberConfig};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let client = async_nats::connect(&nats_url)
        .await
        .expect("failed to connect to nats");

    let stream_name = "strev_example";
    let topic = Topic::new(format!("{stream_name}.orders"));
    let processed = Arc::new(AtomicU32::new(0));

    let sub_config = NatsSubscriberConfig::new(client.clone(), stream_name);
    let subscriber = NatsSubscriber::new(sub_config);

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

    tokio::time::sleep(Duration::from_millis(500)).await;

    let pub_config = NatsPublisherConfig::new(client, stream_name);
    let publisher = NatsPublisher::new(pub_config).await.unwrap();

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
