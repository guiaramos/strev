use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use strev::{FanIn, FanInConfig, HandlerResult, Message, Publisher, Router, ShutdownSignal, Topic};
use strev_channel::Channel;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let channel = Channel::new(64);

    let mut router = Router::new();
    FanIn::register(
        &mut router,
        Arc::new(channel.clone()),
        Arc::new(channel.clone()),
        FanInConfig::new(
            vec![Topic::new("orders"), Topic::new("payments")],
            Topic::new("all"),
        ),
    );

    router.add_consumer(
        "all",
        Topic::new("all"),
        channel.clone(),
        |msg: Message| async move {
            println!("merged: {}", String::from_utf8_lossy(msg.payload()));
            Ok(HandlerResult::ack(msg))
        },
    );

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    Publisher::publish(
        &channel,
        &Topic::new("orders"),
        vec![Message::new(Bytes::from("order-1"))],
    )
    .await
    .unwrap();
    Publisher::publish(
        &channel,
        &Topic::new("payments"),
        vec![Message::new(Bytes::from("payment-1"))],
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;
    token.cancel();
    handle.await.unwrap().unwrap();
}
