# strev Core Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement the strev core library and strev-channel in-memory backend as defined in the design doc.

**Architecture:** Bottom-up implementation. Core types first (Message, Topic, Metadata, errors), then traits (Publisher, Subscriber, Handler, Middleware), then the Router, then the in-memory Channel backend, then built-in middleware. Each layer depends only on layers below it.

**Tech Stack:** Rust (edition 2024), Tokio, async-trait, bytes, uuid, serde, thiserror, dashmap, tokio-stream, tracing

---

### Task 1: Message core types (Topic, Metadata, Outcome)

**Files:**
- Create: `crates/strev/src/topic.rs`
- Create: `crates/strev/src/metadata.rs`
- Create: `crates/strev/src/outcome.rs`
- Modify: `crates/strev/src/lib.rs`

**Step 1: Write failing tests**

Create `crates/strev/tests/topic_test.rs`:

```rust
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
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p strev --test topic_test`
Expected: FAIL with compilation errors (types don't exist yet)

**Step 3: Implement Topic**

Create `crates/strev/src/topic.rs`:

```rust
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Topic(String);

impl Topic {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
```

**Step 4: Implement Metadata**

Create `crates/strev/src/metadata.rs`:

```rust
use std::collections::HashMap;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Metadata(HashMap<String, String>);

impl Metadata {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|v| v.as_str())
    }

    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.0.insert(key.into(), value.into());
    }

    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.0.remove(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}
```

**Step 5: Implement Outcome**

Create `crates/strev/src/outcome.rs`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Acked,
    Nacked,
}
```

**Step 6: Wire up lib.rs**

Update `crates/strev/src/lib.rs` to declare modules and re-export:

```rust
mod topic;
mod metadata;
mod outcome;

pub use topic::Topic;
pub use metadata::Metadata;
pub use outcome::Outcome;
```

**Step 7: Run tests to verify they pass**

Run: `cargo test -p strev --test topic_test`
Expected: 7 tests PASS

**Step 8: Commit**

```bash
git add crates/strev/
git commit -m "feat(strev): add Topic, Metadata, and Outcome types"
```

---

### Task 2: Error types

**Files:**
- Create: `crates/strev/src/error.rs`
- Modify: `crates/strev/src/lib.rs`

**Step 1: Write failing tests**

Create `crates/strev/tests/error_test.rs`:

```rust
use strev::{PublishError, SubscribeError, HandlerError, RouterError, CloseError, Topic};

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
    let source = std::io::Error::new(std::io::ErrorKind::Other, "connection lost");
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
    let source = std::io::Error::new(std::io::ErrorKind::Other, "parse failed");
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
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p strev --test error_test`
Expected: FAIL

**Step 3: Implement error types**

Create `crates/strev/src/error.rs`:

```rust
use crate::Topic;

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("publisher closed")]
    Closed,
    #[error("topic not found: {0}")]
    TopicNotFound(Topic),
    #[error(transparent)]
    Backend(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, thiserror::Error)]
pub enum SubscribeError {
    #[error("subscriber closed")]
    Closed,
    #[error(transparent)]
    Backend(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    #[error(transparent)]
    Processing(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    #[error("subscribe failed on handler {handler}: {source}")]
    Subscribe {
        handler: String,
        source: SubscribeError,
    },
    #[error("publish failed on handler {handler}: {source}")]
    Publish {
        handler: String,
        source: PublishError,
    },
    #[error("already running")]
    AlreadyRunning,
}

#[derive(Debug, thiserror::Error)]
pub enum CloseError {
    #[error("already closed")]
    AlreadyClosed,
    #[error(transparent)]
    Backend(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, thiserror::Error)]
pub enum DeserializeError {
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
```

**Step 4: Update lib.rs**

Add to `crates/strev/src/lib.rs`:

```rust
mod error;

pub use error::{
    CloseError, DeserializeError, HandlerError, PublishError, RouterError, SubscribeError,
};
```

**Step 5: Run tests to verify they pass**

Run: `cargo test -p strev --test error_test`
Expected: 8 tests PASS

**Step 6: Commit**

```bash
git add crates/strev/
git commit -m "feat(strev): add error types"
```

---

### Task 3: Message with typestate ack

**Files:**
- Create: `crates/strev/src/message.rs`
- Modify: `crates/strev/src/lib.rs`

**Step 1: Write failing tests**

Create `crates/strev/tests/message_test.rs`:

```rust
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use strev::{Message, Metadata, Outcome, Pending};

#[test]
fn message_new_has_uuid() {
    let msg = Message::new(Bytes::from("hello"));
    assert!(!msg.uuid().is_nil());
}

#[test]
fn message_payload_roundtrip() {
    let msg = Message::new(Bytes::from("hello"));
    assert_eq!(msg.payload().as_ref(), b"hello");
}

#[test]
fn message_ack_returns_acked_outcome() {
    let msg = Message::new(Bytes::from("hello"));
    let outcome = msg.ack();
    assert_eq!(outcome, Outcome::Acked);
}

#[test]
fn message_nack_returns_nacked_outcome() {
    let msg = Message::new(Bytes::from("hello"));
    let outcome = msg.nack();
    assert_eq!(outcome, Outcome::Nacked);
}

#[test]
fn message_metadata_mutate() {
    let mut msg = Message::new(Bytes::from("hello"));
    msg.metadata_mut().set("key", "value");
    assert_eq!(msg.metadata().get("key"), Some("value"));
}

#[test]
fn message_with_metadata() {
    let mut meta = Metadata::new();
    meta.set("trace_id", "abc123");
    let msg = Message::with_metadata(Bytes::from("hello"), meta);
    assert_eq!(msg.metadata().get("trace_id"), Some("abc123"));
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
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p strev --test message_test`
Expected: FAIL

**Step 3: Implement Message**

Create `crates/strev/src/message.rs`:

```rust
use std::marker::PhantomData;

use bytes::Bytes;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::error::DeserializeError;
use crate::metadata::Metadata;
use crate::outcome::Outcome;

pub trait AckState: sealed::Sealed {}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Pending {}
    impl Sealed for super::Acked {}
    impl Sealed for super::Nacked {}
}

pub struct Pending;
pub struct Acked;
pub struct Nacked;

impl AckState for Pending {}
impl AckState for Acked {}
impl AckState for Nacked {}

#[must_use = "message must be acked or nacked"]
pub struct Message<S: AckState = Pending> {
    uuid: Uuid,
    metadata: Metadata,
    payload: Bytes,
    _state: PhantomData<S>,
}

impl Message<Pending> {
    pub fn new(payload: Bytes) -> Self {
        Self {
            uuid: Uuid::new_v4(),
            metadata: Metadata::new(),
            payload,
            _state: PhantomData,
        }
    }

    pub fn with_metadata(payload: Bytes, metadata: Metadata) -> Self {
        Self {
            uuid: Uuid::new_v4(),
            metadata,
            payload,
            _state: PhantomData,
        }
    }

    pub fn ack(self) -> Outcome {
        Outcome::Acked
    }

    pub fn nack(self) -> Outcome {
        Outcome::Nacked
    }

    pub fn uuid(&self) -> &Uuid {
        &self.uuid
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub fn deserialize<T: DeserializeOwned>(&self) -> Result<T, DeserializeError> {
        Ok(serde_json::from_slice(&self.payload)?)
    }
}
```

**Step 4: Update lib.rs**

Add to `crates/strev/src/lib.rs`:

```rust
mod message;

pub use message::{AckState, Acked, Message, Nacked, Pending};
```

**Step 5: Run tests to verify they pass**

Run: `cargo test -p strev --test message_test`
Expected: 8 tests PASS

**Step 6: Commit**

```bash
git add crates/strev/
git commit -m "feat(strev): add Message with typestate ack"
```

---

### Task 4: Publisher, Subscriber traits and MessageStream

**Files:**
- Create: `crates/strev/src/publisher.rs`
- Create: `crates/strev/src/subscriber.rs`
- Create: `crates/strev/src/stream.rs`
- Modify: `crates/strev/src/lib.rs`

**Step 1: Write failing tests**

Create `crates/strev/tests/stream_test.rs`:

```rust
use bytes::Bytes;
use strev::{Message, MessageStream};
use tokio_stream::StreamExt;

#[tokio::test]
async fn message_stream_receives_messages() {
    let (tx, stream) = MessageStream::channel(16);
    let msg = Message::new(Bytes::from("hello"));
    tx.send(msg).await.unwrap();
    drop(tx);

    let mut stream = stream;
    let received = stream.next().await.unwrap();
    assert_eq!(received.payload().as_ref(), b"hello");
    let _ = received.ack();
}

#[tokio::test]
async fn message_stream_returns_none_when_closed() {
    let (tx, stream) = MessageStream::channel(16);
    drop(tx);

    let mut stream = stream;
    assert!(stream.next().await.is_none());
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p strev --test stream_test`
Expected: FAIL

**Step 3: Implement MessageStream**

Create `crates/strev/src/stream.rs`:

```rust
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;

use crate::message::{Message, Pending};

pub struct MessageStream {
    inner: ReceiverStream<Message<Pending>>,
}

impl MessageStream {
    pub fn channel(buffer: usize) -> (mpsc::Sender<Message<Pending>>, Self) {
        let (tx, rx) = mpsc::channel(buffer);
        (tx, Self { inner: ReceiverStream::new(rx) })
    }
}

impl Stream for MessageStream {
    type Item = Message<Pending>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
```

**Step 4: Implement Publisher trait**

Create `crates/strev/src/publisher.rs`:

```rust
use async_trait::async_trait;

use crate::error::{CloseError, PublishError};
use crate::message::{Message, Pending};
use crate::outcome::Outcome;
use crate::topic::Topic;

#[async_trait]
pub trait Publisher: Send + Sync {
    async fn publish(
        &self,
        topic: &Topic,
        messages: Vec<Message<Pending>>,
    ) -> Result<Vec<Outcome>, PublishError>;

    async fn close(&mut self) -> Result<(), CloseError>;
}
```

**Step 5: Implement Subscriber trait**

Create `crates/strev/src/subscriber.rs`:

```rust
use async_trait::async_trait;

use crate::error::{CloseError, SubscribeError};
use crate::stream::MessageStream;
use crate::topic::Topic;

#[async_trait]
pub trait Subscriber: Send + Sync {
    async fn subscribe(&self, topic: &Topic) -> Result<MessageStream, SubscribeError>;

    async fn close(&mut self) -> Result<(), CloseError>;
}
```

**Step 6: Update lib.rs**

Add to `crates/strev/src/lib.rs`:

```rust
mod publisher;
mod subscriber;
mod stream;

pub use publisher::Publisher;
pub use subscriber::Subscriber;
pub use stream::MessageStream;
```

**Step 7: Run tests to verify they pass**

Run: `cargo test -p strev --test stream_test`
Expected: 2 tests PASS

**Step 8: Commit**

```bash
git add crates/strev/
git commit -m "feat(strev): add Publisher, Subscriber traits and MessageStream"
```

---

### Task 5: Handler trait, HandlerResult, ProducedMessage

**Files:**
- Create: `crates/strev/src/handler.rs`
- Modify: `crates/strev/src/lib.rs`

**Step 1: Write failing tests**

Create `crates/strev/tests/handler_test.rs`:

```rust
use bytes::Bytes;
use strev::{Handler, HandlerResult, Message, Outcome, ProducedMessage, Topic, Metadata, HandlerError};

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
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p strev --test handler_test`
Expected: FAIL

**Step 3: Implement Handler**

Create `crates/strev/src/handler.rs`:

```rust
use std::future::Future;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::HandlerError;
use crate::metadata::Metadata;
use crate::message::{Message, Pending};
use crate::outcome::Outcome;
use crate::topic::Topic;

pub struct HandlerResult {
    pub outcome: Outcome,
    pub produced: Vec<ProducedMessage>,
}

pub struct ProducedMessage {
    pub topic: Topic,
    pub payload: Bytes,
    pub metadata: Metadata,
}

#[async_trait]
pub trait Handler: Send + Sync {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError>;
}

#[async_trait]
impl<F, Fut> Handler for F
where
    F: Fn(Message<Pending>) -> Fut + Send + Sync,
    Fut: Future<Output = Result<HandlerResult, HandlerError>> + Send,
{
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        (self)(msg).await
    }
}
```

**Step 4: Update lib.rs**

Add to `crates/strev/src/lib.rs`:

```rust
mod handler;

pub use handler::{Handler, HandlerResult, ProducedMessage};
```

**Step 5: Run tests to verify they pass**

Run: `cargo test -p strev --test handler_test`
Expected: 3 tests PASS

**Step 6: Commit**

```bash
git add crates/strev/
git commit -m "feat(strev): add Handler trait with blanket Fn impl"
```

---

### Task 6: Middleware trait

**Files:**
- Create: `crates/strev/src/middleware.rs`
- Modify: `crates/strev/src/lib.rs`

**Step 1: Write failing tests**

Create `crates/strev/tests/middleware_test.rs`:

```rust
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use strev::{Handler, HandlerResult, Message, Middleware, Outcome, HandlerError};

struct CountingMiddleware {
    count: Arc<AtomicU32>,
}

impl Middleware for CountingMiddleware {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        let count = self.count.clone();
        Box::new(WrappedHandler { count, next })
    }
}

struct WrappedHandler {
    count: Arc<AtomicU32>,
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for WrappedHandler {
    async fn handle(&self, msg: Message) -> Result<HandlerResult, HandlerError> {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.next.handle(msg).await
    }
}

async fn noop_handler(msg: Message) -> Result<HandlerResult, HandlerError> {
    Ok(HandlerResult {
        outcome: msg.ack(),
        produced: vec![],
    })
}

#[tokio::test]
async fn middleware_wraps_handler() {
    let count = Arc::new(AtomicU32::new(0));
    let mw = CountingMiddleware { count: count.clone() };

    let handler: Box<dyn Handler> = Box::new(noop_handler as fn(Message) -> _);
    let wrapped = mw.wrap(handler);

    let msg = Message::new(Bytes::from("test"));
    let result = wrapped.handle(msg).await.unwrap();
    assert_eq!(result.outcome, Outcome::Acked);
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn middleware_chain_executes_in_order() {
    let log = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

    let mw_a = {
        let log = log.clone();
        ClosureMiddleware(Arc::new(move |next: Box<dyn Handler>| -> Box<dyn Handler> {
            let log = log.clone();
            Box::new(LogHandler { label: "A".into(), log, next })
        }))
    };

    let mw_b = {
        let log = log.clone();
        ClosureMiddleware(Arc::new(move |next: Box<dyn Handler>| -> Box<dyn Handler> {
            let log = log.clone();
            Box::new(LogHandler { label: "B".into(), log, next })
        }))
    };

    let handler: Box<dyn Handler> = Box::new(noop_handler as fn(Message) -> _);
    let wrapped = mw_a.wrap(mw_b.wrap(handler));

    let msg = Message::new(Bytes::from("test"));
    wrapped.handle(msg).await.unwrap();

    let entries = log.lock().unwrap();
    assert_eq!(&*entries, &["A", "B"]);
}

struct ClosureMiddleware(Arc<dyn Fn(Box<dyn Handler>) -> Box<dyn Handler> + Send + Sync>);

impl Middleware for ClosureMiddleware {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        (self.0)(next)
    }
}

struct LogHandler {
    label: String,
    log: Arc<std::sync::Mutex<Vec<String>>>,
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for LogHandler {
    async fn handle(&self, msg: Message) -> Result<HandlerResult, HandlerError> {
        self.log.lock().unwrap().push(self.label.clone());
        self.next.handle(msg).await
    }
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p strev --test middleware_test`
Expected: FAIL

**Step 3: Implement Middleware trait**

Create `crates/strev/src/middleware.rs`:

```rust
use crate::handler::Handler;

pub trait Middleware: Send + Sync {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler>;
}
```

**Step 4: Update lib.rs**

Add to `crates/strev/src/lib.rs`:

```rust
mod middleware;

pub use middleware::Middleware;
```

**Step 5: Run tests to verify they pass**

Run: `cargo test -p strev --test middleware_test`
Expected: 2 tests PASS

**Step 6: Commit**

```bash
git add crates/strev/
git commit -m "feat(strev): add Middleware trait"
```

---

### Task 7: Router

**Files:**
- Create: `crates/strev/src/router.rs`
- Modify: `crates/strev/src/lib.rs`

**Step 1: Write failing tests**

Create `crates/strev/tests/router_test.rs`:

```rust
use strev::{Router, ShutdownSignal};

#[test]
fn router_new_creates_empty_router() {
    let router = Router::new();
    assert!(router.is_empty());
}

#[test]
fn router_add_middleware_returns_self() {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use strev::{Handler, HandlerResult, Message, Middleware, HandlerError};

    struct NoopMiddleware;
    impl Middleware for NoopMiddleware {
        fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
            next
        }
    }

    let mut router = Router::new();
    router.add_middleware(NoopMiddleware);
}
```

Note: Full integration tests for the router's `run` method will be added in Task 9 after the Channel backend is implemented, since `run` needs a concrete Publisher and Subscriber.

**Step 2: Run tests to verify they fail**

Run: `cargo test -p strev --test router_test`
Expected: FAIL

**Step 3: Implement Router**

Create `crates/strev/src/router.rs`:

```rust
use tokio::select;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::error::{HandlerError, PublishError, RouterError};
use crate::handler::{Handler, HandlerResult, ProducedMessage};
use crate::message::{Message, Pending};
use crate::middleware::Middleware;
use crate::outcome::Outcome;
use crate::publisher::Publisher;
use crate::subscriber::Subscriber;
use crate::topic::Topic;

pub enum ShutdownSignal {
    Token(CancellationToken),
    CtrlC,
}

pub struct Router {
    handlers: Vec<HandlerRegistration>,
    middlewares: Vec<Box<dyn Middleware>>,
}

struct HandlerRegistration {
    name: String,
    subscribe_topic: Topic,
    publish_topic: Option<Topic>,
    handler: Box<dyn Handler>,
    subscriber: Box<dyn Subscriber>,
    publisher: Option<Box<dyn Publisher>>,
    middlewares: Vec<Box<dyn Middleware>>,
}

pub struct HandlerBuilder<'r> {
    router: &'r mut Router,
    index: usize,
}

impl<'r> HandlerBuilder<'r> {
    pub fn with_middleware(self, middleware: impl Middleware + 'static) -> Self {
        self.router.handlers[self.index]
            .middlewares
            .push(Box::new(middleware));
        self
    }
}

impl Router {
    pub fn new() -> Self {
        Self {
            handlers: Vec::new(),
            middlewares: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    pub fn add_middleware(&mut self, middleware: impl Middleware + 'static) -> &mut Self {
        self.middlewares.push(Box::new(middleware));
        self
    }

    pub fn add_handler(
        &mut self,
        name: impl Into<String>,
        subscribe_topic: Topic,
        subscriber: impl Subscriber + 'static,
        publish_topic: Topic,
        publisher: impl Publisher + 'static,
        handler: impl Handler + 'static,
    ) -> HandlerBuilder<'_> {
        let index = self.handlers.len();
        self.handlers.push(HandlerRegistration {
            name: name.into(),
            subscribe_topic,
            publish_topic: Some(publish_topic),
            handler: Box::new(handler),
            subscriber: Box::new(subscriber),
            publisher: Some(Box::new(publisher)),
            middlewares: Vec::new(),
        });
        HandlerBuilder { router: self, index }
    }

    pub fn add_consumer(
        &mut self,
        name: impl Into<String>,
        subscribe_topic: Topic,
        subscriber: impl Subscriber + 'static,
        handler: impl Handler + 'static,
    ) -> HandlerBuilder<'_> {
        let index = self.handlers.len();
        self.handlers.push(HandlerRegistration {
            name: name.into(),
            subscribe_topic,
            publish_topic: None,
            handler: Box::new(handler),
            subscriber: Box::new(subscriber),
            publisher: None,
            middlewares: Vec::new(),
        });
        HandlerBuilder { router: self, index }
    }

    pub async fn run(self, shutdown: ShutdownSignal) -> Result<(), RouterError> {
        let token = match shutdown {
            ShutdownSignal::Token(t) => t,
            ShutdownSignal::CtrlC => {
                let t = CancellationToken::new();
                let t2 = t.clone();
                tokio::spawn(async move {
                    let _ = tokio::signal::ctrl_c().await;
                    t2.cancel();
                });
                t
            }
        };

        let mut tasks = Vec::new();

        for reg in self.handlers {
            let mut stream = reg
                .subscriber
                .subscribe(&reg.subscribe_topic)
                .await
                .map_err(|source| RouterError::Subscribe {
                    handler: reg.name.clone(),
                    source,
                })?;

            let handler = self.build_handler_chain(
                reg.handler,
                &self.middlewares,
                reg.middlewares,
            );

            let name = reg.name;
            let publish_topic = reg.publish_topic;
            let publisher = reg.publisher;
            let cancel = token.clone();

            tasks.push(tokio::spawn(async move {
                loop {
                    select! {
                        _ = cancel.cancelled() => break,
                        maybe_msg = stream.next() => {
                            match maybe_msg {
                                Some(msg) => {
                                    Self::process_message(
                                        &name,
                                        &*handler,
                                        msg,
                                        publish_topic.as_ref(),
                                        publisher.as_deref(),
                                    ).await;
                                }
                                None => break,
                            }
                        }
                    }
                }
            }));
        }

        for task in tasks {
            let _ = task.await;
        }

        Ok(())
    }

    async fn process_message(
        handler_name: &str,
        handler: &dyn Handler,
        msg: Message<Pending>,
        publish_topic: Option<&Topic>,
        publisher: Option<&dyn Publisher>,
    ) {
        match handler.handle(msg).await {
            Ok(result) => {
                if !result.produced.is_empty() {
                    if let (Some(topic), Some(pub_)) = (publish_topic, publisher) {
                        let messages = result
                            .produced
                            .into_iter()
                            .map(|pm| Message::with_metadata(pm.payload, pm.metadata))
                            .collect();

                        if let Err(e) = pub_.publish(topic, messages).await {
                            error!(handler = handler_name, error = %e, "failed to publish produced messages");
                        }
                    }
                }
            }
            Err(e) => {
                error!(handler = handler_name, error = %e, "handler error");
            }
        }
    }
}

impl Router {
    fn build_handler_chain(
        &self,
        handler: Box<dyn Handler>,
        router_middlewares: &[Box<dyn Middleware>],
        handler_middlewares: Vec<Box<dyn Middleware>>,
    ) -> Box<dyn Handler> {
        let mut h = handler;
        for mw in handler_middlewares.into_iter().rev() {
            h = mw.wrap(h);
        }
        for mw in router_middlewares.iter().rev() {
            h = mw.wrap(h);
        }
        h
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}
```

Note: The `run` method takes `self` by value (not `&mut self`) because it consumes the handler registrations to move them into spawned tasks. This prevents using a router after it has started running.

**Step 4: Add `tokio-util` dependency**

The Router uses `CancellationToken` from `tokio-util`. Add to `crates/strev/Cargo.toml` under `[dependencies]`:

```toml
tokio-util = "0.7"
```

**Step 5: Update lib.rs**

Add to `crates/strev/src/lib.rs`:

```rust
mod router;

pub use router::{HandlerBuilder, Router, ShutdownSignal};
```

**Step 6: Run tests to verify they pass**

Run: `cargo test -p strev --test router_test`
Expected: 2 tests PASS

**Step 7: Commit**

```bash
git add crates/strev/
git commit -m "feat(strev): add Router with middleware chain and graceful shutdown"
```

---

### Task 8: In-memory Channel backend (strev-channel)

**Files:**
- Modify: `crates/strev-channel/src/lib.rs`

**Step 1: Write failing tests**

Create `crates/strev-channel/tests/channel_test.rs`:

```rust
use bytes::Bytes;
use strev::{Message, Outcome, Publisher, Subscriber, Topic};
use strev_channel::Channel;
use tokio_stream::StreamExt;

#[tokio::test]
async fn publish_and_subscribe_single_message() {
    let channel = Channel::new(16);
    let topic = Topic::new("test");

    let mut stream = Subscriber::subscribe(&channel, &topic).await.unwrap();

    let msg = Message::new(Bytes::from("hello"));
    let outcomes = Publisher::publish(&channel, &topic, vec![msg]).await.unwrap();
    assert_eq!(outcomes, vec![Outcome::Acked]);

    let received = stream.next().await.unwrap();
    assert_eq!(received.payload().as_ref(), b"hello");
    let _ = received.ack();
}

#[tokio::test]
async fn publish_multiple_messages() {
    let channel = Channel::new(16);
    let topic = Topic::new("test");

    let mut stream = Subscriber::subscribe(&channel, &topic).await.unwrap();

    let messages = vec![
        Message::new(Bytes::from("a")),
        Message::new(Bytes::from("b")),
        Message::new(Bytes::from("c")),
    ];
    let outcomes = Publisher::publish(&channel, &topic, messages).await.unwrap();
    assert_eq!(outcomes.len(), 3);

    for expected in [b"a", b"b", b"c"] {
        let msg = stream.next().await.unwrap();
        assert_eq!(msg.payload().as_ref(), expected);
        let _ = msg.ack();
    }
}

#[tokio::test]
async fn multiple_subscribers_receive_copies() {
    let channel = Channel::new(16);
    let topic = Topic::new("test");

    let mut stream_a = Subscriber::subscribe(&channel, &topic).await.unwrap();
    let mut stream_b = Subscriber::subscribe(&channel, &topic).await.unwrap();

    let msg = Message::new(Bytes::from("fanout"));
    Publisher::publish(&channel, &topic, vec![msg]).await.unwrap();

    let a = stream_a.next().await.unwrap();
    let b = stream_b.next().await.unwrap();
    assert_eq!(a.payload().as_ref(), b"fanout");
    assert_eq!(b.payload().as_ref(), b"fanout");
    let _ = a.ack();
    let _ = b.ack();
}

#[tokio::test]
async fn subscribe_to_nonexistent_topic_gets_empty_stream() {
    let channel = Channel::new(16);
    let topic = Topic::new("empty");

    let mut stream = Subscriber::subscribe(&channel, &topic).await.unwrap();

    drop(channel);

    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn channel_clone_shares_state() {
    let channel = Channel::new(16);
    let topic = Topic::new("shared");

    let channel2 = channel.clone();
    let mut stream = Subscriber::subscribe(&channel, &topic).await.unwrap();

    let msg = Message::new(Bytes::from("from_clone"));
    Publisher::publish(&channel2, &topic, vec![msg]).await.unwrap();

    let received = stream.next().await.unwrap();
    assert_eq!(received.payload().as_ref(), b"from_clone");
    let _ = received.ack();
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p strev-channel --test channel_test`
Expected: FAIL

**Step 3: Implement Channel**

Replace `crates/strev-channel/src/lib.rs`:

```rust
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::mpsc;

use strev::error::{CloseError, PublishError, SubscribeError};
use strev::message::{Message, Pending};
use strev::outcome::Outcome;
use strev::stream::MessageStream;
use strev::topic::Topic;
use strev::{Publisher, Subscriber};

#[derive(Clone)]
pub struct Channel {
    inner: Arc<ChannelInner>,
}

struct ChannelInner {
    buffer_size: usize,
    topics: DashMap<Topic, Vec<mpsc::Sender<Message<Pending>>>>,
}

impl Channel {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            inner: Arc::new(ChannelInner {
                buffer_size,
                topics: DashMap::new(),
            }),
        }
    }
}

#[async_trait]
impl Publisher for Channel {
    async fn publish(
        &self,
        topic: &Topic,
        messages: Vec<Message<Pending>>,
    ) -> Result<Vec<Outcome>, PublishError> {
        let senders = self.inner.topics.get(topic);
        let senders = match senders {
            Some(s) => s,
            None => return Ok(messages.into_iter().map(|m| m.ack()).collect()),
        };

        let mut outcomes = Vec::with_capacity(messages.len());

        for msg in messages {
            let payload = msg.payload().clone();
            let metadata = msg.metadata().clone();

            for sender in senders.iter() {
                let copy = Message::with_metadata(payload.clone(), metadata.clone());
                let _ = sender.send(copy).await;
            }

            outcomes.push(msg.ack());
        }

        Ok(outcomes)
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        self.inner.topics.clear();
        Ok(())
    }
}

#[async_trait]
impl Subscriber for Channel {
    async fn subscribe(&self, topic: &Topic) -> Result<MessageStream, SubscribeError> {
        let (tx, stream) = MessageStream::channel(self.inner.buffer_size);
        self.inner
            .topics
            .entry(topic.clone())
            .or_default()
            .push(tx);
        Ok(stream)
    }

    async fn close(&mut self) -> Result<(), CloseError> {
        self.inner.topics.clear();
        Ok(())
    }
}
```

Note: This requires `strev` to make certain modules public. The following items need `pub` visibility from `strev`:
- `strev::error` module (or re-export all error types)
- `strev::message` module (or re-export Message, Pending)
- `strev::outcome` module (or re-export Outcome)
- `strev::stream` module (or re-export MessageStream)
- `strev::topic` module (or re-export Topic)
- `Metadata::clone` (add `Clone` derive)

Update `crates/strev/src/lib.rs` to use `pub mod` for modules that external crates need, or keep the re-exports and have strev-channel use the re-exported names. The cleaner approach: strev-channel imports only from `strev::{Publisher, Subscriber, Message, ...}` via the public re-exports. Adjust the Channel implementation imports to use `strev::` paths only.

**Step 4: Run tests to verify they pass**

Run: `cargo test -p strev-channel --test channel_test`
Expected: 5 tests PASS

**Step 5: Commit**

```bash
git add crates/
git commit -m "feat(strev-channel): add in-memory Channel backend"
```

---

### Task 9: Router integration tests with Channel

**Files:**
- Create: `crates/strev/tests/router_integration_test.rs`

**Step 1: Add strev-channel as a dev-dependency**

Add to `crates/strev/Cargo.toml`:

```toml
[dev-dependencies]
strev-channel = { path = "../strev-channel" }
tokio = { version = "1", features = ["full", "test-util"] }
bytes = "1"
```

**Step 2: Write integration tests**

Create `crates/strev/tests/router_integration_test.rs`:

```rust
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use strev::{
    HandlerResult, Message, Outcome, Publisher, Router, ShutdownSignal, Topic, HandlerError,
};
use strev_channel::Channel;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn router_processes_messages_end_to_end() {
    let channel = Channel::new(16);
    let topic_in = Topic::new("input");
    let topic_out = Topic::new("output");
    let count = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();
    let count_clone = count.clone();

    router.add_handler(
        "test_handler",
        topic_in.clone(),
        channel.clone(),
        topic_out.clone(),
        channel.clone(),
        move |msg: Message| async move {
            count_clone.fetch_add(1, Ordering::SeqCst);
            Ok(HandlerResult {
                outcome: msg.ack(),
                produced: vec![],
            })
        },
    );

    let token = CancellationToken::new();
    let token_clone = token.clone();

    let router_handle = tokio::spawn(async move {
        router.run(ShutdownSignal::Token(token_clone)).await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let msg = Message::new(Bytes::from("test"));
    Publisher::publish(&channel, &topic_in, vec![msg]).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    token.cancel();

    router_handle.await.unwrap().unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn router_consumer_without_publisher() {
    let channel = Channel::new(16);
    let topic = Topic::new("events");
    let count = Arc::new(AtomicU32::new(0));

    let mut router = Router::new();
    let count_clone = count.clone();

    router.add_consumer(
        "consumer",
        topic.clone(),
        channel.clone(),
        move |msg: Message| async move {
            count_clone.fetch_add(1, Ordering::SeqCst);
            Ok(HandlerResult {
                outcome: msg.ack(),
                produced: vec![],
            })
        },
    );

    let token = CancellationToken::new();
    let token_clone = token.clone();

    let router_handle = tokio::spawn(async move {
        router.run(ShutdownSignal::Token(token_clone)).await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..5 {
        let msg = Message::new(Bytes::from(format!("msg-{i}")));
        Publisher::publish(&channel, &topic, vec![msg]).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    token.cancel();

    router_handle.await.unwrap().unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn router_shutdown_via_cancellation_token() {
    let channel = Channel::new(16);
    let topic = Topic::new("test");

    let mut router = Router::new();
    router.add_consumer(
        "noop",
        topic,
        channel.clone(),
        |msg: Message| async move {
            Ok(HandlerResult {
                outcome: msg.ack(),
                produced: vec![],
            })
        },
    );

    let token = CancellationToken::new();
    let token_clone = token.clone();

    let handle = tokio::spawn(async move {
        router.run(ShutdownSignal::Token(token_clone)).await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    token.cancel();

    let result = tokio::time::timeout(Duration::from_secs(2), handle).await;
    assert!(result.is_ok());
}
```

**Step 3: Run integration tests**

Run: `cargo test -p strev --test router_integration_test`
Expected: 3 tests PASS

**Step 4: Commit**

```bash
git add crates/strev/
git commit -m "test(strev): add router integration tests"
```

---

### Task 10: Built-in middleware (Retry, Timeout, CorrelationId, Throttle, PoisonQueue)

**Files:**
- Create: `crates/strev/src/middleware/retry.rs`
- Create: `crates/strev/src/middleware/timeout.rs`
- Create: `crates/strev/src/middleware/correlation_id.rs`
- Create: `crates/strev/src/middleware/throttle.rs`
- Create: `crates/strev/src/middleware/poison_queue.rs`
- Convert: `crates/strev/src/middleware.rs` into `crates/strev/src/middleware/mod.rs`
- Modify: `crates/strev/src/lib.rs`

**Step 1: Write failing tests**

Create `crates/strev/tests/builtin_middleware_test.rs`:

```rust
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use strev::middleware::{CorrelationId, Retry, Throttle, Timeout};
use strev::{Handler, HandlerError, HandlerResult, Message, Metadata, Middleware, Outcome};

async fn ack_handler(msg: Message) -> Result<HandlerResult, HandlerError> {
    Ok(HandlerResult {
        outcome: msg.ack(),
        produced: vec![],
    })
}

#[tokio::test]
async fn retry_retries_on_error() {
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts_clone = attempts.clone();

    let handler: Box<dyn Handler> = Box::new(move |msg: Message| {
        let attempts = attempts_clone.clone();
        async move {
            let n = attempts.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                Err(HandlerError::Processing("transient".into()))
            } else {
                Ok(HandlerResult {
                    outcome: msg.ack(),
                    produced: vec![],
                })
            }
        }
    });

    let retry = Retry {
        max_attempts: 5,
        initial_delay: Duration::from_millis(1),
        multiplier: 1.0,
        max_delay: Duration::from_millis(10),
    };

    let wrapped = retry.wrap(handler);
    let msg = Message::new(Bytes::from("test"));
    let result = wrapped.handle(msg).await.unwrap();
    assert_eq!(result.outcome, Outcome::Acked);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn retry_exhausts_max_attempts() {
    let handler: Box<dyn Handler> = Box::new(|msg: Message| async move {
        let _ = msg.nack();
        Err(HandlerError::Processing("permanent".into()))
    });

    let retry = Retry {
        max_attempts: 3,
        initial_delay: Duration::from_millis(1),
        multiplier: 1.0,
        max_delay: Duration::from_millis(10),
    };

    let wrapped = retry.wrap(handler);
    let msg = Message::new(Bytes::from("test"));
    let result = wrapped.handle(msg).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn timeout_cancels_slow_handler() {
    let handler: Box<dyn Handler> = Box::new(|msg: Message| async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        Ok(HandlerResult {
            outcome: msg.ack(),
            produced: vec![],
        })
    });

    let timeout = Timeout {
        duration: Duration::from_millis(50),
    };

    let wrapped = timeout.wrap(handler);
    let msg = Message::new(Bytes::from("test"));
    let result = wrapped.handle(msg).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn timeout_passes_fast_handler() {
    let handler: Box<dyn Handler> = Box::new(ack_handler as fn(Message) -> _);

    let timeout = Timeout {
        duration: Duration::from_secs(5),
    };

    let wrapped = timeout.wrap(handler);
    let msg = Message::new(Bytes::from("test"));
    let result = wrapped.handle(msg).await.unwrap();
    assert_eq!(result.outcome, Outcome::Acked);
}

#[tokio::test]
async fn correlation_id_propagates() {
    let handler: Box<dyn Handler> = Box::new(|msg: Message| async move {
        assert!(msg.metadata().get("correlation_id").is_some());
        Ok(HandlerResult {
            outcome: msg.ack(),
            produced: vec![],
        })
    });

    let wrapped = CorrelationId.wrap(handler);

    let mut msg = Message::new(Bytes::from("test"));
    msg.metadata_mut().set("correlation_id", "abc-123");
    let result = wrapped.handle(msg).await.unwrap();
    assert_eq!(result.outcome, Outcome::Acked);
}

#[tokio::test]
async fn correlation_id_generates_when_missing() {
    let handler: Box<dyn Handler> = Box::new(|msg: Message| async move {
        let cid = msg.metadata().get("correlation_id");
        assert!(cid.is_some());
        assert!(!cid.unwrap().is_empty());
        Ok(HandlerResult {
            outcome: msg.ack(),
            produced: vec![],
        })
    });

    let wrapped = CorrelationId.wrap(handler);
    let msg = Message::new(Bytes::from("test"));
    wrapped.handle(msg).await.unwrap();
}

#[tokio::test]
async fn throttle_limits_rate() {
    let handler: Box<dyn Handler> = Box::new(ack_handler as fn(Message) -> _);

    let throttle = Throttle { max_per_second: 100 };
    let wrapped = throttle.wrap(handler);

    let start = Instant::now();
    for _ in 0..3 {
        let msg = Message::new(Bytes::from("test"));
        wrapped.handle(msg).await.unwrap();
    }
    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_millis(20));
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p strev --test builtin_middleware_test`
Expected: FAIL

**Step 3: Convert middleware.rs to module directory**

Move `crates/strev/src/middleware.rs` to `crates/strev/src/middleware/mod.rs` and add submodules.

`crates/strev/src/middleware/mod.rs`:
```rust
mod correlation_id;
mod poison_queue;
mod retry;
mod throttle;
mod timeout;

pub use correlation_id::CorrelationId;
pub use poison_queue::PoisonQueue;
pub use retry::Retry;
pub use throttle::Throttle;
pub use timeout::Timeout;

use crate::handler::Handler;

pub trait Middleware: Send + Sync {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler>;
}
```

**Step 4: Implement Retry**

`crates/strev/src/middleware/retry.rs`:
```rust
use std::time::Duration;

use bytes::Bytes;

use crate::handler::{Handler, HandlerResult};
use crate::error::HandlerError;
use crate::message::{Message, Pending};
use crate::middleware::Middleware;

pub struct Retry {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub multiplier: f64,
    pub max_delay: Duration,
}

impl Middleware for Retry {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        Box::new(RetryHandler {
            max_attempts: self.max_attempts,
            initial_delay: self.initial_delay,
            multiplier: self.multiplier,
            max_delay: self.max_delay,
            next,
        })
    }
}

struct RetryHandler {
    max_attempts: u32,
    initial_delay: Duration,
    multiplier: f64,
    max_delay: Duration,
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for RetryHandler {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        let payload = msg.payload().clone();
        let metadata = msg.metadata().clone();
        let mut last_err = None;
        let mut delay = self.initial_delay;

        for attempt in 0..self.max_attempts {
            let attempt_msg = if attempt == 0 {
                msg
            } else {
                tokio::time::sleep(delay).await;
                delay = Duration::from_secs_f64(
                    (delay.as_secs_f64() * self.multiplier).min(self.max_delay.as_secs_f64()),
                );
                Message::with_metadata(payload.clone(), metadata.clone())
            };

            match self.next.handle(attempt_msg).await {
                Ok(result) => return Ok(result),
                Err(e) => last_err = Some(e),
            }

            if attempt == 0 {
                // msg was moved into self.next.handle on first attempt,
                // subsequent attempts create copies above
            }
        }

        Err(last_err.unwrap())
    }
}
```

Note: The Retry middleware has a subtlety with typestate: after the first attempt consumes `msg`, retries must create new `Message<Pending>` from the cloned payload/metadata. This is correct because each retry is a fresh processing attempt.

However, this implementation has a flaw: `msg` is moved in the `if attempt == 0` branch but referenced in the `else` branch via `payload` and `metadata` which were cloned before the loop. Let me fix the implementation to clone payload/metadata first, then pass the original msg on attempt 0.

**Step 5: Implement Timeout**

`crates/strev/src/middleware/timeout.rs`:
```rust
use std::time::Duration;

use crate::error::HandlerError;
use crate::handler::{Handler, HandlerResult};
use crate::message::{Message, Pending};
use crate::middleware::Middleware;

pub struct Timeout {
    pub duration: Duration,
}

impl Middleware for Timeout {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        Box::new(TimeoutHandler {
            duration: self.duration,
            next,
        })
    }
}

struct TimeoutHandler {
    duration: Duration,
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for TimeoutHandler {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        match tokio::time::timeout(self.duration, self.next.handle(msg)).await {
            Ok(result) => result,
            Err(_) => Err(HandlerError::Processing("handler timed out".into())),
        }
    }
}
```

**Step 6: Implement CorrelationId**

`crates/strev/src/middleware/correlation_id.rs`:
```rust
use uuid::Uuid;

use crate::error::HandlerError;
use crate::handler::{Handler, HandlerResult};
use crate::message::{Message, Pending};
use crate::middleware::Middleware;

pub struct CorrelationId;

impl Middleware for CorrelationId {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        Box::new(CorrelationIdHandler { next })
    }
}

struct CorrelationIdHandler {
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for CorrelationIdHandler {
    async fn handle(&self, mut msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        if msg.metadata().get("correlation_id").is_none() {
            msg.metadata_mut()
                .set("correlation_id", Uuid::new_v4().to_string());
        }
        self.next.handle(msg).await
    }
}
```

**Step 7: Implement Throttle**

`crates/strev/src/middleware/throttle.rs`:
```rust
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::time;

use crate::error::HandlerError;
use crate::handler::{Handler, HandlerResult};
use crate::message::{Message, Pending};
use crate::middleware::Middleware;

pub struct Throttle {
    pub max_per_second: u32,
}

impl Middleware for Throttle {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        let interval = Duration::from_secs_f64(1.0 / self.max_per_second as f64);
        Box::new(ThrottleHandler { interval, next })
    }
}

struct ThrottleHandler {
    interval: Duration,
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for ThrottleHandler {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        time::sleep(self.interval).await;
        self.next.handle(msg).await
    }
}
```

**Step 8: Implement PoisonQueue**

`crates/strev/src/middleware/poison_queue.rs`:
```rust
use std::sync::Arc;

use crate::error::HandlerError;
use crate::handler::{Handler, HandlerResult};
use crate::message::{Message, Pending};
use crate::middleware::Middleware;
use crate::publisher::Publisher;
use crate::topic::Topic;

pub struct PoisonQueue {
    pub topic: Topic,
    pub publisher: Arc<dyn Publisher>,
}

impl Middleware for PoisonQueue {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        Box::new(PoisonQueueHandler {
            topic: self.topic.clone(),
            publisher: self.publisher.clone(),
            next,
        })
    }
}

struct PoisonQueueHandler {
    topic: Topic,
    publisher: Arc<dyn Publisher>,
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for PoisonQueueHandler {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        let payload = msg.payload().clone();
        let metadata = msg.metadata().clone();

        match self.next.handle(msg).await {
            Ok(result) => Ok(result),
            Err(e) => {
                let mut poison_meta = metadata;
                poison_meta.set("poison_error", e.to_string());
                let poison_msg = Message::with_metadata(payload, poison_meta);
                let _ = self.publisher.publish(&self.topic, vec![poison_msg]).await;
                Err(e)
            }
        }
    }
}
```

**Step 9: Update lib.rs middleware re-exports**

Update `crates/strev/src/lib.rs`:

```rust
pub mod middleware;

pub use middleware::Middleware;
```

**Step 10: Run tests to verify they pass**

Run: `cargo test -p strev --test builtin_middleware_test`
Expected: 7 tests PASS

**Step 11: Commit**

```bash
git add crates/strev/
git commit -m "feat(strev): add built-in middleware (Retry, Timeout, CorrelationId, Throttle, PoisonQueue)"
```

---

### Task 11: Final integration, public API cleanup, and cargo doc

**Files:**
- Modify: `crates/strev/src/lib.rs`

**Step 1: Verify full public API exports**

Ensure `crates/strev/src/lib.rs` exports everything cleanly:

```rust
mod error;
mod handler;
mod message;
pub mod middleware;
mod outcome;
mod publisher;
mod router;
mod stream;
mod subscriber;
mod topic;

pub use error::{CloseError, DeserializeError, HandlerError, PublishError, RouterError, SubscribeError};
pub use handler::{Handler, HandlerResult, ProducedMessage};
pub use message::{AckState, Acked, Message, Nacked, Pending};
pub use middleware::Middleware;
pub use outcome::Outcome;
pub use publisher::Publisher;
pub use router::{HandlerBuilder, Router, ShutdownSignal};
pub use stream::MessageStream;
pub use subscriber::Subscriber;
pub use topic::Topic;
pub use metadata::Metadata;

mod metadata;
```

**Step 2: Run full test suite**

Run: `cargo test --workspace`
Expected: All tests PASS across both crates

**Step 3: Run cargo clippy**

Run: `cargo clippy --workspace -- -D warnings`
Expected: No warnings

**Step 4: Run cargo doc**

Run: `cargo doc --workspace --no-deps`
Expected: Docs generate without errors

**Step 5: Commit**

```bash
git add crates/
git commit -m "feat(strev): finalize public API"
```

---

### Summary

| Task | Component | Tests |
|------|-----------|-------|
| 1 | Topic, Metadata, Outcome | 7 |
| 2 | Error types | 8 |
| 3 | Message with typestate | 8 |
| 4 | Publisher, Subscriber, MessageStream | 2 |
| 5 | Handler, HandlerResult | 3 |
| 6 | Middleware trait | 2 |
| 7 | Router | 2 |
| 8 | Channel backend | 5 |
| 9 | Router integration | 3 |
| 10 | Built-in middleware | 7 |
| 11 | API cleanup | 0 (verification only) |
| **Total** | | **47** |
