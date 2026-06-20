use strev::{CloseError, HandlerError, PublishError, RouterError, SubscribeError, Topic};

#[test]
fn publish_error_closed_display() {
    let err = PublishError::Closed;
    assert_eq!(err.to_string(), "publisher closed");
}

#[test]
fn publish_error_topic_not_found_display() {
    let err = PublishError::TopicNotFound(Topic::new("missing"));
    assert_eq!(err.to_string(), "topic not found: missing");
}

#[test]
fn publish_error_backend_wraps_source() {
    let source = std::io::Error::other("connection lost");
    let err = PublishError::Backend(Box::new(source));
    assert_eq!(err.to_string(), "connection lost");
}

#[test]
fn subscribe_error_closed_display() {
    let err = SubscribeError::Closed;
    assert_eq!(err.to_string(), "subscriber closed");
}

#[test]
fn handler_error_wraps_source() {
    let source = std::io::Error::other("parse failed");
    let err = HandlerError::Processing(Box::new(source));
    assert_eq!(err.to_string(), "parse failed");
}

#[test]
fn router_error_subscribe_includes_handler_name() {
    let err = RouterError::Subscribe {
        handler: "my_handler".into(),
        source: SubscribeError::Closed,
    };
    assert!(err.to_string().contains("my_handler"));
}

#[test]
fn router_error_already_running() {
    let err = RouterError::AlreadyRunning;
    assert_eq!(err.to_string(), "already running");
}

#[test]
fn close_error_already_closed() {
    let err = CloseError::AlreadyClosed;
    assert_eq!(err.to_string(), "already closed");
}
