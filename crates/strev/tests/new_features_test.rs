use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use strev::middleware::{correlation_id, set_correlation_id};
use strev::{
    bulk_read, passthrough, HandlerResult, Message, Publisher, Router, ShutdownSignal, Subscriber,
    Topic,
};
use strev_channel::Channel;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

#[test]
fn message_copy_creates_new_uuid() {
    let msg = Message::new(Bytes::from("hello"));
    let copy = msg.copy();
    assert_ne!(msg.uuid(), copy.uuid());
    assert_eq!(msg.payload(), copy.payload());
    let _ = msg.ack();
    let _ = copy.ack();
}

#[test]
fn message_copy_preserves_metadata() {
    let mut msg = Message::new(Bytes::from("hello"));
    msg.metadata_mut().set("key", "value");
    let copy = msg.copy();
    assert_eq!(copy.metadata().get("key"), Some("value"));
    let _ = msg.ack();
    let _ = copy.ack();
}

#[test]
fn set_and_get_correlation_id() {
    let mut msg = Message::new(Bytes::from("test"));
    assert!(correlation_id(&msg).is_none());
    set_correlation_id(&mut msg, "abc-123");
    assert_eq!(correlation_id(&msg), Some("abc-123"));
    let _ = msg.ack();
}

#[tokio::test]
async fn passthrough_handler_produces_message() {
    let topic = Topic::new("output");
    let handler = passthrough(topic.clone());

    use strev::Handler;
    let msg = Message::new(Bytes::from("forwarded"));
    let result = handler.handle(msg).await.unwrap();
    assert!(result.outcome().is_acked());
    assert_eq!(result.produced().len(), 1);
    assert_eq!(result.produced()[0].topic, topic);
    assert_eq!(result.produced()[0].payload.as_ref(), b"forwarded");
}

#[tokio::test]
async fn bulk_read_collects_messages() {
    let channel = Channel::new(16);
    let topic = Topic::new("bulk");

    let mut stream = Subscriber::subscribe(&channel, &topic).await.unwrap();

    for i in 0..5 {
        let msg = Message::new(Bytes::from(format!("msg-{i}")));
        Publisher::publish(&channel, &topic, vec![msg]).await.unwrap();
    }

    let messages = bulk_read(&mut stream, 10, Duration::from_millis(200)).await;
    assert_eq!(messages.len(), 5);
    for msg in messages {
        let _ = msg.ack();
    }
}

#[tokio::test]
async fn bulk_read_respects_limit() {
    let channel = Channel::new(16);
    let topic = Topic::new("bulk");

    let mut stream = Subscriber::subscribe(&channel, &topic).await.unwrap();

    for i in 0..10 {
        let msg = Message::new(Bytes::from(format!("msg-{i}")));
        Publisher::publish(&channel, &topic, vec![msg]).await.unwrap();
    }

    let messages = bulk_read(&mut stream, 3, Duration::from_millis(200)).await;
    assert_eq!(messages.len(), 3);
    for msg in messages {
        let _ = msg.ack();
    }
}

#[tokio::test]
async fn bulk_read_respects_timeout() {
    let channel = Channel::new(16);
    let topic = Topic::new("bulk");

    let mut stream = Subscriber::subscribe(&channel, &topic).await.unwrap();

    let msg = Message::new(Bytes::from("only-one"));
    Publisher::publish(&channel, &topic, vec![msg]).await.unwrap();

    let start = std::time::Instant::now();
    let messages = bulk_read(&mut stream, 10, Duration::from_millis(100)).await;
    let elapsed = start.elapsed();

    assert_eq!(messages.len(), 1);
    assert!(elapsed >= Duration::from_millis(80));
    for msg in messages {
        let _ = msg.ack();
    }
}

#[test]
fn router_config_default_close_timeout() {
    use strev::RouterConfig;
    let config = RouterConfig::default();
    assert_eq!(config.close_timeout, Duration::from_secs(30));
}

#[test]
fn router_handler_names() {
    let channel = Channel::new(16);
    let mut router = Router::new();

    router.add_consumer("handler_a", Topic::new("a"), channel.clone(), |msg: Message| async move {
        Ok(HandlerResult::ack(msg))
    });

    router.add_consumer("handler_b", Topic::new("b"), channel.clone(), |msg: Message| async move {
        Ok(HandlerResult::ack(msg))
    });

    let names = router.handler_names();
    assert_eq!(names, vec!["handler_a", "handler_b"]);
}

#[test]
#[should_panic(expected = "duplicate handler name")]
fn router_rejects_duplicate_handler_names() {
    let channel = Channel::new(16);
    let mut router = Router::new();

    router.add_consumer("same_name", Topic::new("a"), channel.clone(), |msg: Message| async move {
        Ok(HandlerResult::ack(msg))
    });

    router.add_consumer("same_name", Topic::new("b"), channel.clone(), |msg: Message| async move {
        Ok(HandlerResult::ack(msg))
    });
}

#[tokio::test]
async fn router_with_publisher_decorator() {
    let channel = Channel::new(64);
    let topic_in = Topic::new("input");
    let topic_out = Topic::new("output");

    let mut router = Router::new();

    router.add_publisher_decorator(strev::MessageTransformPublisherDecorator {
        transform: Arc::new(|msg: &mut Message| {
            msg.metadata_mut().set("decorated", "true");
        }),
    });

    router.add_handler(
        "forward",
        topic_in.clone(),
        channel.clone(),
        channel.clone(),
        passthrough(topic_out.clone()),
    );

    let mut output = Subscriber::subscribe(&channel, &topic_out).await.unwrap();

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let msg = Message::new(Bytes::from("test"));
    Publisher::publish(&channel, &topic_in, vec![msg]).await.unwrap();

    let received = tokio::time::timeout(Duration::from_millis(500), output.next())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(received.metadata().get("decorated"), Some("true"));
    let _ = received.ack();

    token.cancel();
    handle.await.unwrap().unwrap();
}
