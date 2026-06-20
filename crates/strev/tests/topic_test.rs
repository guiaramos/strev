use bytes::Bytes;
use strev::{Message, Metadata, Topic};

#[test]
fn topic_from_str() {
    let topic = Topic::new("orders.placed");
    assert_eq!(topic.as_str(), "orders.placed");
}

#[test]
fn topic_equality() {
    let a = Topic::new("orders");
    let b = Topic::new("orders");
    assert_eq!(a, b);
}

#[test]
fn topic_clone() {
    let a = Topic::new("orders");
    let b = a.clone();
    assert_eq!(a, b);
}

#[test]
fn topic_display() {
    let topic = Topic::new("orders.placed");
    assert_eq!(format!("{topic}"), "orders.placed");
}

#[test]
fn metadata_insert_and_get() {
    let mut meta = Metadata::new();
    meta.set("key", "value");
    assert_eq!(meta.get("key"), Some("value"));
}

#[test]
fn metadata_missing_key_returns_none() {
    let meta = Metadata::new();
    assert_eq!(meta.get("missing"), None);
}

#[test]
fn outcome_ack_via_message() {
    let msg = Message::new(Bytes::from("test"));
    let outcome = msg.ack();
    assert!(outcome.is_acked());
    assert!(!outcome.is_nacked());
}

#[test]
fn outcome_nack_via_message() {
    let msg = Message::new(Bytes::from("test"));
    let outcome = msg.nack();
    assert!(outcome.is_nacked());
    assert!(!outcome.is_acked());
}

#[test]
#[should_panic(expected = "topic name must not be empty")]
fn topic_rejects_empty_name() {
    Topic::new("");
}
