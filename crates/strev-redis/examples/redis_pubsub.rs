use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use strev::{HandlerResult, Message, Publisher, Router, ShutdownSignal, Topic};
use strev_redis::{RedisPublisher, RedisPublisherConfig, RedisSubscriber, RedisSubscriberConfig};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".into());
    let client = redis::Client::open(redis_url).expect("invalid redis url");

    let topic = Topic::new("strev.example.orders");
    let processed = Arc::new(AtomicU32::new(0));

    let sub_config = RedisSubscriberConfig::new(client.clone(), "example-group");
    let subscriber = RedisSubscriber::new(sub_config);

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

    tokio::time::sleep(Duration::from_millis(200)).await;

    let pub_config = RedisPublisherConfig::new(client);
    let publisher = RedisPublisher::new(pub_config).await.unwrap();

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
