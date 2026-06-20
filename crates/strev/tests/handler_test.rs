use bytes::Bytes;
use strev::{Handler, HandlerError, HandlerResult, Message, Metadata, Outcome, ProducedMessage, Topic};

async fn ack_handler(msg: Message) -> Result<HandlerResult, HandlerError> {
    Ok(HandlerResult {
        outcome: msg.ack(),
        produced: vec![],
    })
}

async fn produce_handler(msg: Message) -> Result<HandlerResult, HandlerError> {
    Ok(HandlerResult {
        outcome: msg.ack(),
        produced: vec![ProducedMessage {
            topic: Topic::new("output"),
            payload: Bytes::from("produced"),
            metadata: Metadata::new(),
        }],
    })
}

#[tokio::test]
async fn fn_handler_acks() {
    let msg = Message::new(Bytes::from("hello"));
    let result = ack_handler.handle(msg).await.unwrap();
    assert_eq!(result.outcome, Outcome::Acked);
    assert!(result.produced.is_empty());
}

#[tokio::test]
async fn fn_handler_produces_messages() {
    let msg = Message::new(Bytes::from("hello"));
    let result = produce_handler.handle(msg).await.unwrap();
    assert_eq!(result.outcome, Outcome::Acked);
    assert_eq!(result.produced.len(), 1);
    assert_eq!(result.produced[0].topic, Topic::new("output"));
}

#[tokio::test]
async fn produced_message_carries_metadata() {
    let mut meta = Metadata::new();
    meta.set("trace", "123");
    let pm = ProducedMessage {
        topic: Topic::new("out"),
        payload: Bytes::from("data"),
        metadata: meta,
    };
    assert_eq!(pm.metadata.get("trace"), Some("123"));
}
