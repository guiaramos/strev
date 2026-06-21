use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use sqlx::postgres::PgPoolOptions;
use strev::{HandlerResult, Message, Publisher, Router, ShutdownSignal, Topic};
use strev_postgres::{
    PostgresPublisher, PostgresPublisherConfig, PostgresSubscriber, PostgresSubscriberConfig,
};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@127.0.0.1:5432/postgres".into());
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .expect("failed to connect to postgres");

    let topic = Topic::new("strev_example_orders");
    let processed = Arc::new(AtomicU32::new(0));

    let subscriber =
        PostgresSubscriber::new(PostgresSubscriberConfig::new(pool.clone(), "strev-example"));

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

    let publisher = PostgresPublisher::new(PostgresPublisherConfig::new(pool))
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
