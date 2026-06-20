use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use strev::{HandlerResult, Message, PublisherDecorator, Router, ShutdownSignal, Topic};
use strev_channel::Channel;
use strev_cloudevents::{
    CloudEventCodec, CloudEventsPublisherDecorator, CloudEventsSubscriberDecorator,
};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let channel = Channel::new(16);
    let topic = Topic::new("orders");
    let processed = Arc::new(AtomicU32::new(0));

    let codec =
        CloudEventCodec::new("https://strev.example/orders").event_type("com.strev.order.created");

    let mut router = Router::new();
    router.add_subscriber_decorator(CloudEventsSubscriberDecorator::new(codec.clone()));
    router.add_publisher_decorator(CloudEventsPublisherDecorator::new(codec.clone()));

    let counter = processed.clone();
    router.add_consumer(
        "order_processor",
        topic.clone(),
        channel.clone(),
        move |msg: Message| {
            let counter = counter.clone();
            async move {
                println!(
                    "received id={} type={} payload={}",
                    msg.metadata().get("ce-id").unwrap_or("?"),
                    msg.metadata().get("ce-type").unwrap_or("?"),
                    String::from_utf8_lossy(msg.payload())
                );
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(HandlerResult::ack(msg))
            }
        },
    );

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let publisher = CloudEventsPublisherDecorator::new(codec).decorate(Box::new(channel.clone()));
    for i in 0..3 {
        let domain = Message::new(Bytes::from(format!("{{\"order_id\":{i}}}")));
        publisher.publish(&topic, vec![domain]).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    println!("processed: {}", processed.load(Ordering::SeqCst));
}
