use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use strev::{Message, RequestReply, Router, ShutdownSignal, Topic};
use strev_channel::Channel;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let channel = Channel::new(64);

    let mut router = Router::new();
    RequestReply::respond(
        &mut router,
        "uppercase",
        Topic::new("rpc"),
        channel.clone(),
        Arc::new(channel.clone()),
        |request: Message| async move {
            let reply = String::from_utf8_lossy(request.payload()).to_uppercase();
            Ok(Bytes::from(reply.into_bytes()))
        },
    );

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = RequestReply::new(Arc::new(channel.clone()), &channel, Topic::new("replies"))
        .await
        .unwrap();

    let reply = client
        .request(
            &Topic::new("rpc"),
            Message::new(Bytes::from("ping")),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    println!("reply: {}", String::from_utf8_lossy(reply.payload()));

    token.cancel();
    handle.await.unwrap().unwrap();
}
