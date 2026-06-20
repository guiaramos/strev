use bytes::Bytes;
use strev::Message;
use strev_cloudevents::CloudEventCodec;

fn codec() -> CloudEventCodec {
    CloudEventCodec::new("https://strev.example/orders").event_type("com.strev.order.created")
}

#[test]
fn encode_then_decode_roundtrips_payload() {
    let codec = codec();
    let original = Message::new(Bytes::from_static(b"{\"order_id\":1}"));

    let envelope = codec.encode(&original).unwrap();
    assert_eq!(
        envelope.metadata().get("content-type"),
        Some("application/cloudevents+json")
    );

    let decoded = codec.decode(&envelope).unwrap();
    assert_eq!(decoded.payload().as_ref(), b"{\"order_id\":1}");
    assert_eq!(
        decoded.metadata().get("ce-type"),
        Some("com.strev.order.created")
    );
    assert_eq!(
        decoded.metadata().get("ce-source"),
        Some("https://strev.example/orders")
    );
    assert_eq!(decoded.metadata().get("ce-specversion"), Some("1.0"));
    assert!(decoded.metadata().get("ce-id").is_some());
}

#[test]
fn metadata_overrides_codec_defaults() {
    let codec = codec();
    let mut msg = Message::new(Bytes::from_static(b"payload"));
    msg.metadata_mut().set("ce-id", "fixed-id");
    msg.metadata_mut().set("ce-type", "com.strev.custom");
    msg.metadata_mut().set("ce-source", "https://other.example");
    msg.metadata_mut().set("ce-subject", "orders/1");

    let envelope = codec.encode(&msg).unwrap();
    let decoded = codec.decode(&envelope).unwrap();

    assert_eq!(decoded.metadata().get("ce-id"), Some("fixed-id"));
    assert_eq!(decoded.metadata().get("ce-type"), Some("com.strev.custom"));
    assert_eq!(
        decoded.metadata().get("ce-source"),
        Some("https://other.example")
    );
    assert_eq!(decoded.metadata().get("ce-subject"), Some("orders/1"));
}

#[test]
fn missing_type_is_an_error() {
    let codec = CloudEventCodec::new("https://strev.example/orders");
    let msg = Message::new(Bytes::from_static(b"payload"));
    assert!(codec.encode(&msg).is_err());
}

#[test]
fn non_json_payload_uses_binary_data() {
    let codec = codec().data_content_type("application/octet-stream");
    let original = Message::new(Bytes::from_static(&[0x00, 0x01, 0x02, 0xff]));

    let envelope = codec.encode(&original).unwrap();
    let decoded = codec.decode(&envelope).unwrap();

    assert_eq!(decoded.payload().as_ref(), &[0x00, 0x01, 0x02, 0xff]);
    assert_eq!(
        decoded.metadata().get("ce-datacontenttype"),
        Some("application/octet-stream")
    );
}
