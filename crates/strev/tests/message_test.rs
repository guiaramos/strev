use bytes::Bytes;
use serde::{Deserialize, Serialize};
use strev::{Message, Metadata};

#[test]
fn message_new_has_uuid() {
    let msg = Message::new(Bytes::from("hello"));
    assert!(!msg.uuid().is_nil());
}

#[test]
fn message_payload_roundtrip() {
    let msg = Message::new(Bytes::from("hello"));
    assert_eq!(msg.payload().as_ref(), b"hello");
    let _ = msg.ack();
}

#[test]
fn message_ack_returns_acked_outcome() {
    let msg = Message::new(Bytes::from("hello"));
    let outcome = msg.ack();
    assert!(outcome.is_acked());
}

#[test]
fn message_nack_returns_nacked_outcome() {
    let msg = Message::new(Bytes::from("hello"));
    let outcome = msg.nack();
    assert!(outcome.is_nacked());
}

#[test]
fn message_metadata_mutate() {
    let mut msg = Message::new(Bytes::from("hello"));
    msg.metadata_mut().set("key", "value");
    assert_eq!(msg.metadata().get("key"), Some("value"));
    let _ = msg.ack();
}

#[test]
fn message_with_metadata() {
    let mut meta = Metadata::new();
    meta.set("trace_id", "abc123");
    let msg = Message::with_metadata(Bytes::from("hello"), meta);
    assert_eq!(msg.metadata().get("trace_id"), Some("abc123"));
    let _ = msg.ack();
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct TestEvent {
    name: String,
    count: u32,
}

#[test]
fn message_deserialize_json() {
    let event = TestEvent { name: "test".into(), count: 42 };
    let payload = serde_json::to_vec(&event).unwrap();
    let msg = Message::new(Bytes::from(payload));
    let decoded: TestEvent = msg.deserialize().unwrap();
    assert_eq!(decoded, event);
    let _ = msg.ack();
}

#[test]
fn message_deserialize_invalid_json_fails() {
    let msg = Message::new(Bytes::from("not json"));
    let result = msg.deserialize::<TestEvent>();
    assert!(result.is_err());
    let _ = msg.nack();
}
