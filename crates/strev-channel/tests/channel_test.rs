use bytes::Bytes;
use strev::{Message, Publisher, Subscriber, Topic};
use strev_channel::Channel;
use tokio_stream::StreamExt;

#[tokio::test]
async fn publish_and_subscribe_single_message() {
    let channel = Channel::new(16);
    let topic = Topic::new("test");

    let mut stream = Subscriber::subscribe(&channel, &topic).await.unwrap();

    let msg = Message::new(Bytes::from("hello"));
    let outcomes = Publisher::publish(&channel, &topic, vec![msg]).await.unwrap();
    assert!(outcomes.iter().all(|o| o.is_acked()));

    let received = stream.next().await.unwrap();
    assert_eq!(received.payload().as_ref(), b"hello");
    let _ = received.ack();
}

#[tokio::test]
async fn publish_multiple_messages() {
    let channel = Channel::new(16);
    let topic = Topic::new("test");

    let mut stream = Subscriber::subscribe(&channel, &topic).await.unwrap();

    let messages = vec![
        Message::new(Bytes::from("a")),
        Message::new(Bytes::from("b")),
        Message::new(Bytes::from("c")),
    ];
    let outcomes = Publisher::publish(&channel, &topic, messages).await.unwrap();
    assert_eq!(outcomes.len(), 3);

    for expected in [b"a", b"b", b"c"] {
        let msg = stream.next().await.unwrap();
        assert_eq!(msg.payload().as_ref(), expected);
        let _ = msg.ack();
    }
}

#[tokio::test]
async fn multiple_subscribers_receive_copies() {
    let channel = Channel::new(16);
    let topic = Topic::new("test");

    let mut stream_a = Subscriber::subscribe(&channel, &topic).await.unwrap();
    let mut stream_b = Subscriber::subscribe(&channel, &topic).await.unwrap();

    let msg = Message::new(Bytes::from("fanout"));
    Publisher::publish(&channel, &topic, vec![msg]).await.unwrap();

    let a = stream_a.next().await.unwrap();
    let b = stream_b.next().await.unwrap();
    assert_eq!(a.payload().as_ref(), b"fanout");
    assert_eq!(b.payload().as_ref(), b"fanout");
    let _ = a.ack();
    let _ = b.ack();
}

#[tokio::test]
async fn subscribe_to_nonexistent_topic_gets_empty_stream() {
    let channel = Channel::new(16);
    let topic = Topic::new("empty");

    let mut stream = Subscriber::subscribe(&channel, &topic).await.unwrap();

    drop(channel);

    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn channel_clone_shares_state() {
    let channel = Channel::new(16);
    let topic = Topic::new("shared");

    let channel2 = channel.clone();
    let mut stream = Subscriber::subscribe(&channel, &topic).await.unwrap();

    let msg = Message::new(Bytes::from("from_clone"));
    Publisher::publish(&channel2, &topic, vec![msg]).await.unwrap();

    let received = stream.next().await.unwrap();
    assert_eq!(received.payload().as_ref(), b"from_clone");
    let _ = received.ack();
}
