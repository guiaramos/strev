use std::time::Duration;

use bytes::Bytes;
use strev::{Message, Publisher, Subscriber, Topic};
use strev_channel::Channel;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() {
    let channel = Channel::new(64);
    let topic = Topic::new("greetings");

    let mut stream = Subscriber::subscribe(&channel, &topic).await.unwrap();

    let consumer = tokio::spawn(async move {
        while let Some(msg) = stream.next().await {
            let text = String::from_utf8_lossy(msg.payload());
            println!("received: {text}");
            let _ = msg.ack();
        }
    });

    for i in 0..5 {
        let msg = Message::new(Bytes::from(format!("Hello #{i}")));
        Publisher::publish(&channel, &topic, vec![msg])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(channel);
    let _ = consumer.await;
}
