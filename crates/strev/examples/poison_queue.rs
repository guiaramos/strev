use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use strev::middleware::PoisonQueue;
use strev::{HandlerError, HandlerResult, Message, Publisher, Router, ShutdownSignal, Topic};
use strev_channel::Channel;
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize, Debug)]
struct Payment {
    id: u32,
    amount: f64,
}

#[tokio::main]
async fn main() {
    let channel = Channel::new(64);
    let payments_topic = Topic::new("payments");
    let dead_letter_topic = Topic::new("payments.dead_letter");

    let mut router = Router::new();

    router.add_middleware(PoisonQueue {
        topic: dead_letter_topic.clone(),
        publisher: Arc::new(channel.clone()),
    });

    router.add_consumer(
        "process_payment",
        payments_topic.clone(),
        channel.clone(),
        |msg: Message| async move {
            let (payment, msg): (Payment, _) = match msg.try_deserialize() {
                Ok(v) => v,
                Err((e, msg)) => {
                    let _ = msg.nack();
                    return Err(HandlerError::Processing(Box::new(e)));
                }
            };

            if payment.amount > 10000.0 {
                let _ = msg.nack();
                return Err(HandlerError::Processing(
                    format!(
                        "payment {} exceeds limit: ${:.2}",
                        payment.id, payment.amount
                    )
                    .into(),
                ));
            }

            println!("processed payment #{}: ${:.2}", payment.id, payment.amount);
            Ok(HandlerResult::ack(msg))
        },
    );

    router.add_consumer(
        "dead_letter_logger",
        dead_letter_topic,
        channel.clone(),
        |msg: Message| async move {
            let error = msg
                .metadata()
                .get("poison_error")
                .unwrap_or("unknown")
                .to_string();
            let payload = String::from_utf8_lossy(msg.payload());
            println!("DEAD LETTER: error={error}, payload={payload}");
            Ok(HandlerResult::ack(msg))
        },
    );

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let payments = vec![
        Payment {
            id: 1,
            amount: 50.0,
        },
        Payment {
            id: 2,
            amount: 99999.0,
        },
        Payment {
            id: 3,
            amount: 25.0,
        },
        Payment {
            id: 4,
            amount: 50000.0,
        },
    ];

    for payment in &payments {
        let payload = serde_json::to_vec(payment).unwrap();
        Publisher::publish(
            &channel,
            &payments_topic,
            vec![Message::new(Bytes::from(payload))],
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    token.cancel();
    handle.await.unwrap().unwrap();
}
