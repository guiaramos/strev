use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use strev::middleware::{
    CircuitBreaker, CorrelationId, Retry, Throttle, Timeout,
};
use strev::{
    HandlerError, HandlerResult, Message, Publisher, Router, ShutdownSignal, Topic,
};
use strev_channel::Channel;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let channel = Channel::new(64);
    let topic = Topic::new("orders");

    let attempt_count = Arc::new(AtomicU32::new(0));
    let success_count = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();

    router.add_middleware(CorrelationId);

    router.add_middleware(Timeout {
        duration: Duration::from_secs(5),
    });

    router.add_middleware(Throttle {
        max_per_second: NonZeroU32::new(50).unwrap(),
    });

    let attempts = attempt_count.clone();
    let successes = success_count.clone();
    router
        .add_consumer(
            "flaky_processor",
            topic.clone(),
            channel.clone(),
            move |msg: Message| {
                let attempts = attempts.clone();
                let successes = successes.clone();
                async move {
                    let n = attempts.fetch_add(1, Ordering::SeqCst);
                    let payload = String::from_utf8_lossy(msg.payload()).to_string();
                    let cid = msg
                        .metadata()
                        .get("correlation_id")
                        .unwrap_or("none")
                        .to_string();

                    if n % 3 == 0 {
                        println!("[{cid}] FAIL: {payload} (attempt {n})");
                        let _ = msg.nack();
                        return Err(HandlerError::Processing("transient failure".into()));
                    }

                    successes.fetch_add(1, Ordering::SeqCst);
                    println!("[{cid}] OK: {payload}");
                    Ok(HandlerResult::ack(msg))
                }
            },
        )
        .with_middleware(Retry {
            max_attempts: NonZeroU32::new(3).unwrap(),
            initial_delay: Duration::from_millis(10),
            multiplier: 2.0,
            max_delay: Duration::from_millis(100),
        })
        .with_middleware(CircuitBreaker {
            max_failures: NonZeroU32::new(5).unwrap(),
            reset_timeout: Duration::from_secs(5),
        });

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..8 {
        let msg = Message::new(Bytes::from(format!("order-{i}")));
        Publisher::publish(&channel, &topic, vec![msg]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    tokio::time::sleep(Duration::from_millis(1000)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    println!("\n--- Stats ---");
    println!("total attempts: {}", attempt_count.load(Ordering::SeqCst));
    println!("successes: {}", success_count.load(Ordering::SeqCst));
}
