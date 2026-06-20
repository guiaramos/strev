use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use strev::middleware::{
    CircuitBreaker, Deduplicator, DelayOnError, Duplicator, IgnoreErrors,
    InMemoryDeduplicateRepository, InstantAck, RandomFail,
};
use strev::{Handler, HandlerError, HandlerResult, Message, Middleware};

async fn ack_handler(msg: Message) -> Result<HandlerResult, HandlerError> {
    Ok(HandlerResult::ack(msg))
}

#[tokio::test]
async fn instant_ack_acks_before_handler() {
    let handler_called = Arc::new(AtomicU32::new(0));
    let called = handler_called.clone();

    let handler: Box<dyn Handler> = Box::new(move |msg: Message| {
        let called = called.clone();
        async move {
            called.fetch_add(1, Ordering::SeqCst);
            Ok(HandlerResult::ack(msg))
        }
    });

    let wrapped = InstantAck.wrap(handler);
    let msg = Message::new(Bytes::from("test"));
    let result = wrapped.handle(msg).await.unwrap();

    assert!(result.outcome().is_acked());
    assert_eq!(handler_called.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn duplicator_calls_handler_twice() {
    let call_count = Arc::new(AtomicU32::new(0));
    let count = call_count.clone();

    let handler: Box<dyn Handler> = Box::new(move |msg: Message| {
        let count = count.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(HandlerResult::ack(msg))
        }
    });

    let wrapped = Duplicator.wrap(handler);
    let msg = Message::new(Bytes::from("test"));
    wrapped.handle(msg).await.unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn delay_on_error_delays_on_failure() {
    let handler: Box<dyn Handler> = Box::new(|msg: Message| async move {
        let _ = msg.nack();
        Err(HandlerError::Processing("fail".into()))
    });

    let mw = DelayOnError {
        initial_interval: Duration::from_millis(50),
        max_interval: Duration::from_secs(1),
        multiplier: 2.0,
    };

    let wrapped = mw.wrap(handler);
    let msg = Message::new(Bytes::from("test"));

    let start = std::time::Instant::now();
    let result = wrapped.handle(msg).await;
    let elapsed = start.elapsed();

    assert!(result.is_err());
    assert!(elapsed >= Duration::from_millis(40));
}

#[tokio::test]
async fn delay_on_error_no_delay_on_success() {
    let handler: Box<dyn Handler> = Box::new(ack_handler as fn(Message) -> _);

    let mw = DelayOnError {
        initial_interval: Duration::from_secs(1),
        max_interval: Duration::from_secs(5),
        multiplier: 2.0,
    };

    let wrapped = mw.wrap(handler);
    let msg = Message::new(Bytes::from("test"));

    let start = std::time::Instant::now();
    let result = wrapped.handle(msg).await;
    let elapsed = start.elapsed();

    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(100));
}

#[tokio::test]
async fn ignore_errors_swallows_matching_error() {
    let handler: Box<dyn Handler> = Box::new(|msg: Message| async move {
        let _ = msg.nack();
        Err(HandlerError::Processing("transient".into()))
    });

    let mw = IgnoreErrors {
        should_ignore: Arc::new(|e| e.to_string().contains("transient")),
    };

    let wrapped = mw.wrap(handler);
    let msg = Message::new(Bytes::from("test"));
    let result = wrapped.handle(msg).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn ignore_errors_propagates_non_matching() {
    let handler: Box<dyn Handler> = Box::new(|msg: Message| async move {
        let _ = msg.nack();
        Err(HandlerError::Processing("permanent".into()))
    });

    let mw = IgnoreErrors {
        should_ignore: Arc::new(|e| e.to_string().contains("transient")),
    };

    let wrapped = mw.wrap(handler);
    let msg = Message::new(Bytes::from("test"));
    let result = wrapped.handle(msg).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn circuit_breaker_opens_after_max_failures() {
    let handler: Box<dyn Handler> = Box::new(|msg: Message| async move {
        let _ = msg.nack();
        Err(HandlerError::Processing("fail".into()))
    });

    let cb = CircuitBreaker {
        max_failures: NonZeroU32::new(2).unwrap(),
        reset_timeout: Duration::from_secs(60),
    };

    let wrapped = cb.wrap(handler);

    let msg1 = Message::new(Bytes::from("1"));
    assert!(wrapped.handle(msg1).await.is_err());

    let msg2 = Message::new(Bytes::from("2"));
    assert!(wrapped.handle(msg2).await.is_err());

    let msg3 = Message::new(Bytes::from("3"));
    let err = wrapped.handle(msg3).await.unwrap_err();
    assert!(err.to_string().contains("circuit breaker open"));
}

#[tokio::test]
async fn circuit_breaker_resets_on_success() {
    let attempt = Arc::new(AtomicU32::new(0));
    let a = attempt.clone();

    let handler: Box<dyn Handler> = Box::new(move |msg: Message| {
        let a = a.clone();
        async move {
            let n = a.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                let _ = msg.nack();
                Err(HandlerError::Processing("fail".into()))
            } else {
                Ok(HandlerResult::ack(msg))
            }
        }
    });

    let cb = CircuitBreaker {
        max_failures: NonZeroU32::new(3).unwrap(),
        reset_timeout: Duration::from_secs(60),
    };

    let wrapped = cb.wrap(handler);

    let msg1 = Message::new(Bytes::from("1"));
    assert!(wrapped.handle(msg1).await.is_err());

    let msg2 = Message::new(Bytes::from("2"));
    assert!(wrapped.handle(msg2).await.is_ok());

    let msg3 = Message::new(Bytes::from("3"));
    assert!(wrapped.handle(msg3).await.is_ok());
}

#[tokio::test]
async fn deduplicator_drops_duplicate_messages() {
    let call_count = Arc::new(AtomicU32::new(0));
    let count = call_count.clone();

    let handler: Box<dyn Handler> = Box::new(move |msg: Message| {
        let count = count.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(HandlerResult::ack(msg))
        }
    });

    let repo = Arc::new(InMemoryDeduplicateRepository::new(Duration::from_secs(60)));
    let dedup = Deduplicator {
        repository: repo,
        key_factory: Some(Arc::new(|msg: &Message| {
            String::from_utf8_lossy(msg.payload()).to_string()
        })),
    };

    let wrapped = dedup.wrap(handler);

    let msg1 = Message::new(Bytes::from("unique-payload"));
    wrapped.handle(msg1).await.unwrap();

    let msg2 = Message::new(Bytes::from("unique-payload"));
    wrapped.handle(msg2).await.unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn deduplicator_allows_different_messages() {
    let call_count = Arc::new(AtomicU32::new(0));
    let count = call_count.clone();

    let handler: Box<dyn Handler> = Box::new(move |msg: Message| {
        let count = count.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(HandlerResult::ack(msg))
        }
    });

    let repo = Arc::new(InMemoryDeduplicateRepository::new(Duration::from_secs(60)));
    let dedup = Deduplicator {
        repository: repo,
        key_factory: Some(Arc::new(|msg: &Message| {
            String::from_utf8_lossy(msg.payload()).to_string()
        })),
    };

    let wrapped = dedup.wrap(handler);

    let msg1 = Message::new(Bytes::from("a"));
    wrapped.handle(msg1).await.unwrap();

    let msg2 = Message::new(Bytes::from("b"));
    wrapped.handle(msg2).await.unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn random_fail_low_probability_passes() {
    let handler: Box<dyn Handler> = Box::new(ack_handler as fn(Message) -> _);

    let mw = RandomFail { probability: 0.0 };
    let wrapped = mw.wrap(handler);

    let msg = Message::new(Bytes::from("test"));
    assert!(wrapped.handle(msg).await.is_ok());
}

#[tokio::test]
async fn random_fail_high_probability_fails() {
    let handler: Box<dyn Handler> = Box::new(ack_handler as fn(Message) -> _);

    let mw = RandomFail { probability: 1.0 };
    let wrapped = mw.wrap(handler);

    let msg = Message::new(Bytes::from("test"));
    assert!(wrapped.handle(msg).await.is_err());
}
