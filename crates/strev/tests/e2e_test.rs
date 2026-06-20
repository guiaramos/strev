use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use strev::middleware::{CorrelationId, Retry, Timeout};
use strev::{
    HandlerError, HandlerResult, Message, Metadata, ProducedMessage, Publisher, Router,
    ShutdownSignal, Subscriber, Topic,
};
use strev_channel::Channel;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Order {
    id: u32,
    item: String,
    quantity: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct ConfirmedOrder {
    order_id: u32,
    status: String,
}

#[tokio::test]
async fn pipeline_order_placed_to_confirmed_to_notification() {
    let channel = Channel::new(64);
    let orders_placed = Topic::new("orders.placed");
    let orders_confirmed = Topic::new("orders.confirmed");
    let confirmed_orders = Arc::new(Mutex::new(Vec::<ConfirmedOrder>::new()));
    let notified_orders = Arc::new(Mutex::new(Vec::<u32>::new()));

    let mut router = Router::new();

    let confirmed_clone = confirmed_orders.clone();
    router.add_handler(
        "process_order",
        orders_placed.clone(),
        channel.clone(),
        orders_confirmed.clone(),
        channel.clone(),
        move |msg: Message| {
            let confirmed = confirmed_clone.clone();
            async move {
                let order: Order = match msg.deserialize() {
                    Ok(o) => o,
                    Err(e) => {
                        let _ = msg.nack();
                        return Err(HandlerError::Processing(Box::new(e)));
                    }
                };

                let confirmation = ConfirmedOrder {
                    order_id: order.id,
                    status: "confirmed".into(),
                };

                confirmed.lock().await.push(confirmation.clone());

                let payload = serde_json::to_vec(&confirmation).unwrap();

                Ok(HandlerResult {
                    outcome: msg.ack(),
                    produced: vec![ProducedMessage {
                        topic: Topic::new("orders.confirmed"),
                        payload: Bytes::from(payload),
                        metadata: Metadata::new(),
                    }],
                })
            }
        },
    );

    let notified_clone = notified_orders.clone();
    router.add_consumer(
        "send_notification",
        orders_confirmed.clone(),
        channel.clone(),
        move |msg: Message| {
            let notified = notified_clone.clone();
            async move {
                let confirmed: ConfirmedOrder = match msg.deserialize() {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = msg.nack();
                        return Err(HandlerError::Processing(Box::new(e)));
                    }
                };

                notified.lock().await.push(confirmed.order_id);

                Ok(HandlerResult {
                    outcome: msg.ack(),
                    produced: vec![],
                })
            }
        },
    );

    let token = CancellationToken::new();
    let token_clone = token.clone();

    let router_handle = tokio::spawn(async move {
        router.run(ShutdownSignal::Token(token_clone)).await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let orders = vec![
        Order { id: 1, item: "widget".into(), quantity: 5 },
        Order { id: 2, item: "gadget".into(), quantity: 3 },
        Order { id: 3, item: "doohickey".into(), quantity: 1 },
    ];

    for order in &orders {
        let payload = serde_json::to_vec(order).unwrap();
        let msg = Message::new(Bytes::from(payload));
        Publisher::publish(&channel, &orders_placed, vec![msg])
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    token.cancel();
    router_handle.await.unwrap().unwrap();

    let confirmed = confirmed_orders.lock().await;
    assert_eq!(confirmed.len(), 3);
    assert!(confirmed.iter().all(|c| c.status == "confirmed"));
    assert_eq!(
        confirmed.iter().map(|c| c.order_id).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );

    let notified = notified_orders.lock().await;
    assert_eq!(notified.len(), 3);
    let mut sorted = notified.clone();
    sorted.sort();
    assert_eq!(sorted, vec![1, 2, 3]);
}

#[tokio::test]
async fn middleware_chain_applies_to_handler() {
    let channel = Channel::new(64);
    let topic_in = Topic::new("input");
    let topic_out = Topic::new("output");

    let processed = Arc::new(Mutex::new(Vec::<(String, bool)>::new()));

    let mut router = Router::new();

    router.add_middleware(CorrelationId);
    router.add_middleware(Timeout {
        duration: Duration::from_secs(5),
    });

    let processed_clone = processed.clone();
    router
        .add_handler(
            "check_middleware",
            topic_in.clone(),
            channel.clone(),
            topic_out.clone(),
            channel.clone(),
            move |msg: Message| {
                let processed = processed_clone.clone();
                async move {
                    let has_correlation = msg.metadata().get("correlation_id").is_some();
                    let payload = String::from_utf8_lossy(msg.payload()).to_string();
                    processed.lock().await.push((payload, has_correlation));

                    Ok(HandlerResult {
                        outcome: msg.ack(),
                        produced: vec![],
                    })
                }
            },
        )
        .with_middleware(Retry {
            max_attempts: 3,
            initial_delay: Duration::from_millis(10),
            multiplier: 2.0,
            max_delay: Duration::from_secs(1),
        });

    let token = CancellationToken::new();
    let token_clone = token.clone();

    let router_handle = tokio::spawn(async move {
        router.run(ShutdownSignal::Token(token_clone)).await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let msg = Message::new(Bytes::from("hello"));
    Publisher::publish(&channel, &topic_in, vec![msg])
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    token.cancel();
    router_handle.await.unwrap().unwrap();

    let results = processed.lock().await;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, "hello");
    assert!(results[0].1);
}

#[tokio::test]
async fn handler_nack_does_not_produce_messages() {
    let channel = Channel::new(64);
    let topic_in = Topic::new("input");
    let topic_out = Topic::new("output");

    let nack_count = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();

    let nack_clone = nack_count.clone();
    router.add_handler(
        "nack_handler",
        topic_in.clone(),
        channel.clone(),
        topic_out.clone(),
        channel.clone(),
        move |msg: Message| {
            let count = nack_clone.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(HandlerResult {
                    outcome: msg.nack(),
                    produced: vec![],
                })
            }
        },
    );

    let mut output_stream = Subscriber::subscribe(&channel, &topic_out).await.unwrap();

    let token = CancellationToken::new();
    let token_clone = token.clone();

    let router_handle = tokio::spawn(async move {
        router.run(ShutdownSignal::Token(token_clone)).await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let msg = Message::new(Bytes::from("reject_me"));
    Publisher::publish(&channel, &topic_in, vec![msg])
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    token.cancel();
    router_handle.await.unwrap().unwrap();

    assert_eq!(nack_count.load(Ordering::SeqCst), 1);

    let next = tokio::time::timeout(Duration::from_millis(100), output_stream.next()).await;
    assert!(next.is_err() || next.unwrap().is_none());
}

#[tokio::test]
async fn retry_middleware_recovers_transient_failure() {
    let channel = Channel::new(64);
    let topic = Topic::new("flaky");

    let attempt_count = Arc::new(AtomicU32::new(0));
    let success_payloads = Arc::new(Mutex::new(Vec::<String>::new()));

    let mut router = Router::new();

    let attempts = attempt_count.clone();
    let payloads = success_payloads.clone();
    router
        .add_consumer(
            "flaky_handler",
            topic.clone(),
            channel.clone(),
            move |msg: Message| {
                let attempts = attempts.clone();
                let payloads = payloads.clone();
                async move {
                    let n = attempts.fetch_add(1, Ordering::SeqCst);
                    if n < 2 {
                        let _ = msg.nack();
                        return Err(HandlerError::Processing("transient".into()));
                    }
                    let payload = String::from_utf8_lossy(msg.payload()).to_string();
                    payloads.lock().await.push(payload);
                    Ok(HandlerResult {
                        outcome: msg.ack(),
                        produced: vec![],
                    })
                }
            },
        )
        .with_middleware(Retry {
            max_attempts: 5,
            initial_delay: Duration::from_millis(1),
            multiplier: 1.0,
            max_delay: Duration::from_millis(10),
        });

    let token = CancellationToken::new();
    let token_clone = token.clone();

    let router_handle = tokio::spawn(async move {
        router.run(ShutdownSignal::Token(token_clone)).await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let msg = Message::new(Bytes::from("eventually_works"));
    Publisher::publish(&channel, &topic, vec![msg])
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;
    token.cancel();
    router_handle.await.unwrap().unwrap();

    assert_eq!(attempt_count.load(Ordering::SeqCst), 3);
    let payloads = success_payloads.lock().await;
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0], "eventually_works");
}

#[tokio::test]
async fn multiple_consumers_on_same_topic_both_receive() {
    let channel = Channel::new(64);
    let topic = Topic::new("events");

    let consumer_a = Arc::new(AtomicU32::new(0));
    let consumer_b = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();

    let a = consumer_a.clone();
    router.add_consumer("consumer_a", topic.clone(), channel.clone(), move |msg: Message| {
        let a = a.clone();
        async move {
            a.fetch_add(1, Ordering::SeqCst);
            Ok(HandlerResult {
                outcome: msg.ack(),
                produced: vec![],
            })
        }
    });

    let b = consumer_b.clone();
    router.add_consumer("consumer_b", topic.clone(), channel.clone(), move |msg: Message| {
        let b = b.clone();
        async move {
            b.fetch_add(1, Ordering::SeqCst);
            Ok(HandlerResult {
                outcome: msg.ack(),
                produced: vec![],
            })
        }
    });

    let token = CancellationToken::new();
    let token_clone = token.clone();

    let router_handle = tokio::spawn(async move {
        router.run(ShutdownSignal::Token(token_clone)).await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..3 {
        let msg = Message::new(Bytes::from(format!("event-{i}")));
        Publisher::publish(&channel, &topic, vec![msg]).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    token.cancel();
    router_handle.await.unwrap().unwrap();

    assert_eq!(consumer_a.load(Ordering::SeqCst), 3);
    assert_eq!(consumer_b.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn handler_error_does_not_crash_router() {
    let channel = Channel::new(64);
    let topic = Topic::new("mixed");

    let processed = Arc::new(Mutex::new(Vec::<String>::new()));

    let mut router = Router::new();

    let p = processed.clone();
    router.add_consumer("mixed_handler", topic.clone(), channel.clone(), move |msg: Message| {
        let p = p.clone();
        async move {
            let payload = String::from_utf8_lossy(msg.payload()).to_string();
            if payload == "bad" {
                let _ = msg.nack();
                return Err(HandlerError::Processing("bad message".into()));
            }
            p.lock().await.push(payload);
            Ok(HandlerResult {
                outcome: msg.ack(),
                produced: vec![],
            })
        }
    });

    let token = CancellationToken::new();
    let token_clone = token.clone();

    let router_handle = tokio::spawn(async move {
        router.run(ShutdownSignal::Token(token_clone)).await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    for payload in ["good_1", "bad", "good_2"] {
        let msg = Message::new(Bytes::from(payload));
        Publisher::publish(&channel, &topic, vec![msg]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    token.cancel();
    router_handle.await.unwrap().unwrap();

    let results = processed.lock().await;
    assert_eq!(results.len(), 2);
    assert!(results.contains(&"good_1".to_string()));
    assert!(results.contains(&"good_2".to_string()));
}
