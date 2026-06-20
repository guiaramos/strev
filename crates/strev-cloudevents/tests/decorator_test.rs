use std::time::Duration;

use bytes::Bytes;
use strev::{Message, PublisherDecorator, SubscriberDecorator, Topic};
use strev_channel::Channel;
use strev_cloudevents::{
    CloudEventCodec, CloudEventsPublisherDecorator, CloudEventsSubscriberDecorator,
};
use tokio_stream::StreamExt;

#[tokio::test]
async fn decorators_envelope_and_unwrap_through_channel() {
    let channel = Channel::new(16);
    let topic = Topic::new("orders");
    let codec =
        CloudEventCodec::new("https://strev.example/orders").event_type("com.strev.order.created");

    let decoded_sub =
        CloudEventsSubscriberDecorator::new(codec.clone()).decorate(Box::new(channel.clone()));
    let encoding_pub =
        CloudEventsPublisherDecorator::new(codec).decorate(Box::new(channel.clone()));

    let mut stream = decoded_sub.subscribe(&topic).await.unwrap();

    let domain = Message::new(Bytes::from_static(b"{\"order_id\":7}"));
    encoding_pub.publish(&topic, vec![domain]).await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("timeout")
        .expect("stream ended");

    assert_eq!(received.payload().as_ref(), b"{\"order_id\":7}");
    assert_eq!(
        received.metadata().get("ce-type"),
        Some("com.strev.order.created")
    );
    assert_eq!(
        received.metadata().get("ce-source"),
        Some("https://strev.example/orders")
    );
    assert!(received.metadata().get("ce-id").is_some());
}
