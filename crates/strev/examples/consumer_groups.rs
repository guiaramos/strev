use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use strev::{
    HandlerError, HandlerResult, Message, Publisher, Router, ShutdownSignal, Topic,
};
use strev_channel::Channel;
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize, Debug)]
struct UserSignedUp {
    user_id: u32,
    email: String,
    name: String,
}

#[tokio::main]
async fn main() {
    let channel = Channel::new(64);
    let signups_topic = Topic::new("user.signed_up");

    let emails_sent = Arc::new(AtomicU32::new(0));
    let crm_updated = Arc::new(AtomicU32::new(0));
    let analytics_tracked = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();

    let counter = emails_sent.clone();
    router.add_consumer(
        "send_welcome_email",
        signups_topic.clone(),
        channel.clone(),
        move |msg: Message| {
            let counter = counter.clone();
            async move {
                let (user, msg): (UserSignedUp, _) = match msg.try_deserialize() {
                    Ok(v) => v,
                    Err((e, msg)) => {
                        let _ = msg.nack();
                        return Err(HandlerError::Processing(Box::new(e)));
                    }
                };
                counter.fetch_add(1, Ordering::SeqCst);
                println!("email: welcome {} <{}>", user.name, user.email);
                Ok(HandlerResult::ack(msg))
            }
        },
    );

    let counter = crm_updated.clone();
    router.add_consumer(
        "update_crm",
        signups_topic.clone(),
        channel.clone(),
        move |msg: Message| {
            let counter = counter.clone();
            async move {
                let (user, msg): (UserSignedUp, _) = match msg.try_deserialize() {
                    Ok(v) => v,
                    Err((e, msg)) => {
                        let _ = msg.nack();
                        return Err(HandlerError::Processing(Box::new(e)));
                    }
                };
                counter.fetch_add(1, Ordering::SeqCst);
                println!("crm: added contact {} (id={})", user.name, user.user_id);
                Ok(HandlerResult::ack(msg))
            }
        },
    );

    let counter = analytics_tracked.clone();
    router.add_consumer(
        "track_analytics",
        signups_topic.clone(),
        channel.clone(),
        move |msg: Message| {
            let counter = counter.clone();
            async move {
                let (user, msg): (UserSignedUp, _) = match msg.try_deserialize() {
                    Ok(v) => v,
                    Err((e, msg)) => {
                        let _ = msg.nack();
                        return Err(HandlerError::Processing(Box::new(e)));
                    }
                };
                counter.fetch_add(1, Ordering::SeqCst);
                println!("analytics: signup event for user_id={}", user.user_id);
                Ok(HandlerResult::ack(msg))
            }
        },
    );

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let users = vec![
        UserSignedUp { user_id: 1, email: "alice@example.com".into(), name: "Alice".into() },
        UserSignedUp { user_id: 2, email: "bob@example.com".into(), name: "Bob".into() },
        UserSignedUp { user_id: 3, email: "carol@example.com".into(), name: "Carol".into() },
    ];

    for user in &users {
        let payload = serde_json::to_vec(user).unwrap();
        Publisher::publish(&channel, &signups_topic, vec![Message::new(Bytes::from(payload))])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    println!("\n--- Summary ---");
    println!("emails sent: {}", emails_sent.load(Ordering::SeqCst));
    println!("crm updated: {}", crm_updated.load(Ordering::SeqCst));
    println!("analytics tracked: {}", analytics_tracked.load(Ordering::SeqCst));
}
