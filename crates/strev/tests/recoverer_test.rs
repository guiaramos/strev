use bytes::Bytes;
use strev::middleware::{Middleware, Recoverer};
use strev::{HandlerError, HandlerResult, Message};

async fn panicking(_msg: Message) -> Result<HandlerResult, HandlerError> {
    panic!("boom");
}

#[tokio::test]
async fn recovers_from_panicking_handler() {
    let wrapped = Recoverer::new().wrap(Box::new(panicking));
    let result = wrapped.handle(Message::new(Bytes::from("payload"))).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn passes_successful_result_through() {
    let handler = |msg: Message| async move { Ok(HandlerResult::ack(msg)) };
    let wrapped = Recoverer::new().wrap(Box::new(handler));
    let result = wrapped
        .handle(Message::new(Bytes::from("payload")))
        .await
        .unwrap();
    assert!(result.outcome().is_acked());
}
