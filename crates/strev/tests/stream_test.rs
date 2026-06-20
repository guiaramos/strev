use bytes::Bytes;
use strev::{Message, MessageStream};
use tokio_stream::StreamExt;

#[tokio::test]
async fn message_stream_receives_messages() {
    let (tx, stream) = MessageStream::channel(16);
    let msg = Message::new(Bytes::from("hello"));
    tx.send(msg).await.unwrap();
    drop(tx);

    let mut stream = stream;
    let received = stream.next().await.unwrap();
    assert_eq!(received.payload().as_ref(), b"hello");
    let _ = received.ack();
}

#[tokio::test]
async fn message_stream_returns_none_when_closed() {
    let (tx, stream) = MessageStream::channel(16);
    drop(tx);

    let mut stream = stream;
    assert!(stream.next().await.is_none());
}
