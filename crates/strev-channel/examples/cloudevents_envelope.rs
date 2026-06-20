use std::time::Duration;

use bytes::Bytes;
use cloudevents::{AttributesReader, Data, Event, EventBuilder, EventBuilderV10};
use serde_json::json;
use strev::{HandlerResult, Message, Publisher, Router, ShutdownSignal, Topic};
use strev_channel::Channel;
use tokio_util::sync::CancellationToken;

const CLOUDEVENTS_JSON: &str = "application/cloudevents+json";

fn into_message(event: &Event) -> Message {
    let payload = serde_json::to_vec(event).expect("serialize cloudevent");
    let mut msg = Message::new(Bytes::from(payload));
    msg.metadata_mut().set("content-type", CLOUDEVENTS_JSON);
    msg
}

fn from_message(msg: &Message) -> Result<Event, serde_json::Error> {
    serde_json::from_slice(msg.payload())
}

#[tokio::main]
async fn main() {
    let channel = Channel::new(16);
    let topic = Topic::new("orders");

    let mut router = Router::new();
    router.add_consumer(
        "order_processor",
        topic.clone(),
        channel.clone(),
        move |msg: Message| async move {
            let event = from_message(&msg).expect("decode cloudevent");
            println!(
                "received id={} type={} source={}",
                event.id(),
                event.ty(),
                event.source()
            );
            if let Some(Data::Json(value)) = event.data() {
                println!("  data: {value}");
            }
            Ok(HandlerResult::ack(msg))
        },
    );

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..3 {
        let event = EventBuilderV10::new()
            .id(uuid::Uuid::new_v4().to_string())
            .ty("com.strev.order.created")
            .source("https://strev.example/orders")
            .data(
                "application/json",
                json!({ "order_id": i, "amount": 19.99 }),
            )
            .build()
            .expect("build cloudevent");

        Publisher::publish(&channel, &topic, vec![into_message(&event)])
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    token.cancel();
    handle.await.unwrap().unwrap();
}
