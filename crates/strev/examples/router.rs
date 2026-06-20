use std::num::NonZeroU32;
use std::time::Duration;

use bytes::Bytes;
use strev::middleware::{CorrelationId, Retry, Timeout};
use strev::{HandlerResult, Message, Publisher, Router, ShutdownSignal, Topic};
use strev_channel::Channel;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let channel = Channel::new(64);
    let topic = Topic::new("incoming_messages");

    let mut router = Router::new();

    router.add_middleware(CorrelationId);
    router.add_middleware(Retry {
        max_attempts: NonZeroU32::new(3).unwrap(),
        initial_delay: Duration::from_millis(100),
        multiplier: 2.0,
        max_delay: Duration::from_secs(1),
    });

    router
        .add_consumer(
            "print_messages",
            topic.clone(),
            channel.clone(),
            |msg: Message| async move {
                let correlation_id = msg
                    .metadata()
                    .get("correlation_id")
                    .unwrap_or("none")
                    .to_string();

                let text = String::from_utf8_lossy(msg.payload());
                println!("[{correlation_id}] received: {text}");

                Ok(HandlerResult::ack(msg))
            },
        )
        .with_middleware(Timeout {
            duration: Duration::from_secs(5),
        });

    let token = CancellationToken::new();
    let token_clone = token.clone();

    let router_handle =
        tokio::spawn(async move { router.run(ShutdownSignal::Token(token_clone)).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..5 {
        let msg = Message::new(Bytes::from(format!("Hello, world! #{i}")));
        Publisher::publish(&channel, &topic, vec![msg])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    token.cancel();
    router_handle.await.unwrap().unwrap();
    println!("router shut down gracefully");
}
