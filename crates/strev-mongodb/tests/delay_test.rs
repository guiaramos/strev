use std::time::{Duration, Instant};

use bytes::Bytes;
use mongodb::Client;
use strev::{Delay, DelayedPublisher, Message, Subscriber, Topic};
use strev_mongodb::{
    MongoDelayPromoter, MongoDelayPromoterConfig, MongoPublisher, MongoPublisherConfig,
    MongoSubscriber, MongoSubscriberConfig,
};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

async fn mongo_client() -> Option<Client> {
    let uri = std::env::var("MONGODB_URI")
        .unwrap_or_else(|_| "mongodb://127.0.0.1:27017/?directConnection=true".into());
    let client = Client::with_uri_str(&uri).await.ok()?;
    client
        .database("strev")
        .run_command(mongodb::bson::doc! { "ping": 1 })
        .await
        .ok()?;
    Some(client)
}

#[tokio::test]
async fn promotes_delayed_message_after_due() {
    let Some(client) = mongo_client().await else {
        eprintln!("skipping: mongodb not available");
        return;
    };
    let topic = Topic::new(format!("delay-{}", Uuid::new_v4()));

    let subscriber =
        MongoSubscriber::new(MongoSubscriberConfig::new(client.clone(), "delay-group"));
    let mut stream = subscriber.subscribe(&topic).await.unwrap();

    let publisher = MongoPublisher::new(MongoPublisherConfig::new(client.clone()));

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
    let promoter = MongoDelayPromoter::new(MongoDelayPromoterConfig::new(client.clone()))
        .await
        .unwrap();
    let tc = token.clone();
    let handle = tokio::spawn(async move { promoter.run(tc).await });

    let received = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout")
        .expect("stream ended");

    assert!(started.elapsed() >= Duration::from_millis(300));
    assert_eq!(received.payload().as_ref(), b"payload");
    let _ = received.ack();

    token.cancel();
    handle.await.unwrap();
}
