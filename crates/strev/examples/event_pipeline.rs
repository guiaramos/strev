use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use strev::{
    HandlerError, HandlerResult, Message, Metadata, ProducedMessage, Publisher, Router,
    ShutdownSignal, Topic,
};
use strev_channel::Channel;
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize, Debug)]
struct PostAdded {
    author: String,
    title: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct PostsCountUpdated {
    new_count: u64,
}

#[tokio::main]
async fn main() {
    let channel = Channel::new(64);

    let posts_topic = Topic::new("posts_published");
    let count_topic = Topic::new("posts_count");

    let counter = Arc::new(AtomicU64::new(0));
    let mut router = Router::new();

    let counter_clone = counter.clone();
    let count_topic_clone = count_topic.clone();
    router.add_handler(
        "posts_counter",
        posts_topic.clone(),
        channel.clone(),
        channel.clone(),
        move |msg: Message| {
            let counter = counter_clone.clone();
            let topic = count_topic_clone.clone();
            async move {
                let (post, msg): (PostAdded, _) = match msg.try_deserialize() {
                    Ok(v) => v,
                    Err((e, msg)) => {
                        let _ = msg.nack();
                        return Err(HandlerError::Processing(Box::new(e)));
                    }
                };

                let new_count = counter.fetch_add(1, Ordering::SeqCst) + 1;
                println!("post #{new_count}: \"{title}\" by {author}", title = post.title, author = post.author);

                let payload = serde_json::to_vec(&PostsCountUpdated { new_count }).unwrap();
                Ok(HandlerResult::ack_with(
                    msg,
                    vec![ProducedMessage {
                        topic,
                        payload: Bytes::from(payload),
                        metadata: Metadata::new(),
                    }],
                ))
            }
        },
    );

    router.add_consumer(
        "feed_generator",
        posts_topic.clone(),
        channel.clone(),
        |msg: Message| async move {
            let (post, msg): (PostAdded, _) = match msg.try_deserialize() {
                Ok(v) => v,
                Err((e, msg)) => {
                    let _ = msg.nack();
                    return Err(HandlerError::Processing(Box::new(e)));
                }
            };

            println!("feed: \"{title}\" by {author}", title = post.title, author = post.author);
            Ok(HandlerResult::ack(msg))
        },
    );

    router.add_consumer(
        "count_printer",
        count_topic,
        channel.clone(),
        |msg: Message| async move {
            let (update, msg): (PostsCountUpdated, _) = match msg.try_deserialize() {
                Ok(v) => v,
                Err((e, msg)) => {
                    let _ = msg.nack();
                    return Err(HandlerError::Processing(Box::new(e)));
                }
            };

            println!("total posts: {}", update.new_count);
            Ok(HandlerResult::ack(msg))
        },
    );

    let token = CancellationToken::new();
    let token_clone = token.clone();

    let router_handle =
        tokio::spawn(async move { router.run(ShutdownSignal::Token(token_clone)).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let posts = vec![
        PostAdded { author: "alice".into(), title: "Getting Started with Rust".into() },
        PostAdded { author: "bob".into(), title: "Async Patterns in Tokio".into() },
        PostAdded { author: "carol".into(), title: "Event-Driven Architecture".into() },
    ];

    for post in &posts {
        let payload = serde_json::to_vec(post).unwrap();
        let msg = Message::new(Bytes::from(payload));
        Publisher::publish(&channel, &posts_topic, vec![msg])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    token.cancel();
    router_handle.await.unwrap().unwrap();

    println!("pipeline shut down, total posts processed: {}", counter.load(Ordering::SeqCst));
}
