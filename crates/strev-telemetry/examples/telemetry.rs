use std::time::Duration;

use bytes::Bytes;
use metrics_util::debugging::DebuggingRecorder;
use strev::{HandlerResult, Message, Publisher, Router, ShutdownSignal, Topic};
use strev_channel::Channel;
use strev_telemetry::Telemetry;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install metrics recorder");

    let channel = Channel::new(16);
    let topic = Topic::new("orders");

    let mut router = Router::new();
    router.add_middleware(Telemetry::new());
    router.add_consumer(
        "orders",
        topic.clone(),
        channel.clone(),
        |msg: Message| async move {
            if msg.payload().as_ref() == b"order-2" {
                return Ok(HandlerResult::nack(msg));
            }
            Ok(HandlerResult::ack(msg))
        },
    );

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });

    tokio::time::sleep(Duration::from_millis(100)).await;
    for i in 0..5 {
        Publisher::publish(
            &channel,
            &topic,
            vec![Message::new(Bytes::from(format!("order-{i}")))],
        )
        .await
        .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(400)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    println!("--- captured metrics ---");
    for (key, _unit, _desc, value) in snapshotter.snapshot().into_vec() {
        println!("{:?} = {:?}", key.key().name(), value);
    }
}
