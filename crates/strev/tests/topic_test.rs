use strev::{Topic, Metadata, Outcome};

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
fn outcome_variants() {
    let acked = Outcome::Acked;
    let nacked = Outcome::Nacked;
    assert!(matches!(acked, Outcome::Acked));
    assert!(matches!(nacked, Outcome::Nacked));
}
