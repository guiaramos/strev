use std::time::{Duration, Instant};

use bytes::Bytes;
use strev::{Delay, DelayedPublisher, Message, Subscriber, Topic};
use strev_channel::Channel;
use tokio_stream::StreamExt;

#[tokio::test]
async fn withholds_message_until_delay_elapses() {
    let channel = Channel::new(64);
    let mut stream = Subscriber::subscribe(&channel, &Topic::new("orders"))
        .await
        .unwrap();

    let started = Instant::now();
    channel
        .publish_after(
            &Topic::new("orders"),
            vec![Message::new(Bytes::from("payload"))],
            Delay::after(Duration::from_millis(200)),
        )
        .await
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("timeout")
        .expect("stream ended");

    assert!(started.elapsed() >= Duration::from_millis(200));
    assert_eq!(received.payload().as_ref(), b"payload");
    let _ = received.ack();
}
