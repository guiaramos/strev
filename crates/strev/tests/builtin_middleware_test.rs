use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use strev::middleware::{CorrelationId, Retry, Throttle, Timeout};
use strev::{Handler, HandlerError, HandlerResult, Message, Middleware, Outcome};

async fn ack_handler(msg: Message) -> Result<HandlerResult, HandlerError> {
    Ok(HandlerResult {
        outcome: msg.ack(),
        produced: vec![],
    })
}

#[tokio::test]
async fn retry_retries_on_error() {
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts_clone = attempts.clone();

    let failing_handler = move |msg: Message| {
        let attempts = attempts_clone.clone();
        async move {
            let n = attempts.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                let _ = msg.nack();
                Err(HandlerError::Processing("transient".into()))
            } else {
                Ok(HandlerResult {
                    outcome: msg.ack(),
                    produced: vec![],
                })
            }
        }
    };

    let handler: Box<dyn Handler> = Box::new(failing_handler);

    let retry = Retry {
        max_attempts: 5,
        initial_delay: Duration::from_millis(1),
        multiplier: 1.0,
        max_delay: Duration::from_millis(10),
    };

    let wrapped = retry.wrap(handler);
    let msg = Message::new(Bytes::from("test"));
    let result = wrapped.handle(msg).await.unwrap();
    assert_eq!(result.outcome, Outcome::Acked);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn retry_exhausts_max_attempts() {
    let handler: Box<dyn Handler> = Box::new(|msg: Message| async move {
        let _ = msg.nack();
        Err(HandlerError::Processing("permanent".into()))
    });

    let retry = Retry {
        max_attempts: 3,
        initial_delay: Duration::from_millis(1),
        multiplier: 1.0,
        max_delay: Duration::from_millis(10),
    };

    let wrapped = retry.wrap(handler);
    let msg = Message::new(Bytes::from("test"));
    let result = wrapped.handle(msg).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn timeout_cancels_slow_handler() {
    let handler: Box<dyn Handler> = Box::new(|msg: Message| async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        Ok(HandlerResult {
            outcome: msg.ack(),
            produced: vec![],
        })
    });

    let timeout = Timeout {
        duration: Duration::from_millis(50),
    };

    let wrapped = timeout.wrap(handler);
    let msg = Message::new(Bytes::from("test"));
    let result = wrapped.handle(msg).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn timeout_passes_fast_handler() {
    let handler: Box<dyn Handler> = Box::new(ack_handler as fn(Message) -> _);

    let timeout = Timeout {
        duration: Duration::from_secs(5),
    };

    let wrapped = timeout.wrap(handler);
    let msg = Message::new(Bytes::from("test"));
    let result = wrapped.handle(msg).await.unwrap();
    assert_eq!(result.outcome, Outcome::Acked);
}

#[tokio::test]
async fn correlation_id_propagates() {
    let handler: Box<dyn Handler> = Box::new(|msg: Message| async move {
        assert!(msg.metadata().get("correlation_id").is_some());
        Ok(HandlerResult {
            outcome: msg.ack(),
            produced: vec![],
        })
    });

    let wrapped = CorrelationId.wrap(handler);

    let mut msg = Message::new(Bytes::from("test"));
    msg.metadata_mut().set("correlation_id", "abc-123");
    let result = wrapped.handle(msg).await.unwrap();
    assert_eq!(result.outcome, Outcome::Acked);
}

#[tokio::test]
async fn correlation_id_generates_when_missing() {
    let handler: Box<dyn Handler> = Box::new(|msg: Message| async move {
        let cid = msg.metadata().get("correlation_id");
        assert!(cid.is_some());
        assert!(!cid.unwrap().is_empty());
        Ok(HandlerResult {
            outcome: msg.ack(),
            produced: vec![],
        })
    });

    let wrapped = CorrelationId.wrap(handler);
    let msg = Message::new(Bytes::from("test"));
    wrapped.handle(msg).await.unwrap();
}

#[tokio::test]
async fn throttle_limits_rate() {
    let handler: Box<dyn Handler> = Box::new(ack_handler as fn(Message) -> _);

    let throttle = Throttle { max_per_second: 100 };
    let wrapped = throttle.wrap(handler);

    let start = Instant::now();
    for _ in 0..3 {
        let msg = Message::new(Bytes::from("test"));
        wrapped.handle(msg).await.unwrap();
    }
    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_millis(20));
}
