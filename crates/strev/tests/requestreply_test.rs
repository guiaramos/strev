use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use strev::{Message, RequestReply, Router, ShutdownSignal, Topic};
use strev_channel::Channel;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn request_gets_correlated_reply() {
    let channel = Channel::new(64);

    let mut router = Router::new();
    RequestReply::respond(
        &mut router,
        "reverser",
        Topic::new("rpc"),
        channel.clone(),
        Arc::new(channel.clone()),
        |request: Message| async move {
            let mut bytes = request.payload().to_vec();
            bytes.reverse();
            Ok(Bytes::from(bytes))
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
            Message::new(Bytes::from("hello")),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(reply.payload().as_ref(), b"olleh");

    let timed_out = client
        .request(
            &Topic::new("nowhere"),
            Message::new(Bytes::from("x")),
            Duration::from_millis(200),
        )
        .await;
    assert!(matches!(timed_out, Err(strev::RequestReplyError::Timeout)));

    token.cancel();
    handle.await.unwrap().unwrap();
}
