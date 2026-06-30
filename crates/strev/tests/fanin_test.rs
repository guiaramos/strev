use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use strev::{FanIn, FanInConfig, Message, Publisher, Router, ShutdownSignal, Subscriber, Topic};
use strev_channel::Channel;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn multiplexes_source_topics_onto_target() {
    let channel = Channel::new(64);
    let mut target = Subscriber::subscribe(&channel, &Topic::new("all"))
        .await
        .unwrap();

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

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    Publisher::publish(
        &channel,
        &Topic::new("orders"),
        vec![Message::new(Bytes::from("o"))],
    )
    .await
    .unwrap();
    Publisher::publish(
        &channel,
        &Topic::new("payments"),
        vec![Message::new(Bytes::from("p"))],
    )
    .await
    .unwrap();

    let mut received = Vec::new();
    for _ in 0..2 {
        let msg = tokio::time::timeout(Duration::from_secs(2), target.next())
            .await
            .expect("timeout")
            .expect("stream ended");
        received.push(String::from_utf8_lossy(msg.payload()).to_string());
        let _ = msg.ack();
    }
    received.sort();

    assert_eq!(received, vec!["o".to_string(), "p".to_string()]);

    token.cancel();
    handle.await.unwrap().unwrap();
}
