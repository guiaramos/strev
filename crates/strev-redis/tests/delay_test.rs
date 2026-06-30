use std::time::{Duration, Instant};

use bytes::Bytes;
use strev::{Delay, DelayedPublisher, Message, Subscriber, Topic};
use strev_redis::{
    RedisDelayPromoter, RedisDelayPromoterConfig, RedisPublisher, RedisPublisherConfig,
    RedisSubscriber, RedisSubscriberConfig,
};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

async fn redis_client() -> Option<redis::Client> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());
    let client = redis::Client::open(url).ok()?;
    client.get_multiplexed_async_connection().await.ok()?;
    Some(client)
}

#[tokio::test]
async fn promotes_delayed_message_after_due() {
    let Some(client) = redis_client().await else {
        return;
    };
    let topic = Topic::new(format!("delay-{}", Uuid::new_v4()));

    let subscriber =
        RedisSubscriber::new(RedisSubscriberConfig::new(client.clone(), "delay-group"));
    let mut stream = subscriber.subscribe(&topic).await.unwrap();

    let publisher = RedisPublisher::new(RedisPublisherConfig::new(client.clone()))
        .await
        .unwrap();

    let started = Instant::now();
    publisher
        .publish_after(
            &topic,
            vec![Message::new(Bytes::from("payload"))],
            Delay::after(Duration::from_millis(300)),
        )
        .await
        .unwrap();

    let token = CancellationToken::new();
    let promoter = RedisDelayPromoter::new(RedisDelayPromoterConfig::new(client.clone()))
        .await
        .unwrap();
    let tc = token.clone();
    let handle = tokio::spawn(async move { promoter.run(tc).await });

    let received = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("timeout")
        .expect("stream ended");

    assert!(started.elapsed() >= Duration::from_millis(300));
    assert_eq!(received.payload().as_ref(), b"payload");
    let _ = received.ack();

    token.cancel();
    handle.await.unwrap();
}
