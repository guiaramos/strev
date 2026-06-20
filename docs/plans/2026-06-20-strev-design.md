# strev: Design Document

A Rust library for building event-driven applications with pub/sub messaging, inspired by [watermill](https://github.com/ThreeDotsLabs/watermill) but designed from the ground up for Rust's type system.

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Async runtime | Tokio | De facto standard, largest ecosystem |
| Initial scope | Core only | Traits, router, middleware, in-memory backend |
| Ack model | Pure typestate | Compile-time enforcement, no internal channels |
| Error handling | `Result<Outcome>` | Errors separate from ack state |
| Topic type | Newtype `Topic(String)` | Invalid states unrepresentable |

## Crate Structure

- `strev` — core library (traits, message, router, middleware)
- `strev-channel` — in-memory channel backend (ships with core initially)
- Future: `strev-nats`, `strev-kafka`, `strev-redis` as separate crates

## Core Types

### Message & Ack Typestate

```rust
pub struct Pending;
pub struct Acked;
pub struct Nacked;

pub trait AckState {}
impl AckState for Pending {}
impl AckState for Acked {}
impl AckState for Nacked {}

pub struct Message<S: AckState = Pending> {
    uuid: Uuid,
    metadata: Metadata,
    payload: Bytes,
    _state: PhantomData<S>,
}

#[must_use = "message must be acked or nacked"]
impl Message<Pending> {
    pub fn ack(self) -> Outcome { Outcome::Acked }
    pub fn nack(self) -> Outcome { Outcome::Nacked }
    pub fn uuid(&self) -> &Uuid { &self.uuid }
    pub fn metadata(&self) -> &Metadata { &self.metadata }
    pub fn metadata_mut(&mut self) -> &mut Metadata { &mut self.metadata }
    pub fn payload(&self) -> &Bytes { &self.payload }
    pub fn deserialize<T: DeserializeOwned>(&self) -> Result<T, DeserializeError> { ... }
}

pub enum Outcome {
    Acked,
    Nacked,
}
```

`Message<Pending>` is `#[must_use]`, so dropping it without calling `ack()` or `nack()` produces a compiler warning. Since `ack(self)` consumes the message by value, double-ack is impossible. Both invalid states are caught before runtime.

### Topic & Metadata

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Topic(String);

impl Topic {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

pub struct Metadata(HashMap<String, String>);
```

## Publisher & Subscriber Traits

```rust
#[async_trait]
pub trait Publisher: Send + Sync {
    async fn publish(
        &self,
        topic: &Topic,
        messages: Vec<Message<Pending>>,
    ) -> Result<Vec<Outcome>, PublishError>;

    async fn close(&mut self) -> Result<(), CloseError>;
}

#[async_trait]
pub trait Subscriber: Send + Sync {
    async fn subscribe(
        &self,
        topic: &Topic,
    ) -> Result<MessageStream, SubscribeError>;

    async fn close(&mut self) -> Result<(), CloseError>;
}

pub struct MessageStream {
    inner: ReceiverStream<Message<Pending>>,
}

impl Stream for MessageStream {
    type Item = Message<Pending>;
}
```

- `publish` returns `Vec<Outcome>` for per-message results without channels.
- `subscribe` returns a typed `MessageStream` composable with `StreamExt` (`.map()`, `.filter()`, `.buffer_unordered()`).
- Error types are distinct per operation.

## Handler & Middleware

```rust
#[async_trait]
pub trait Handler: Send + Sync {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError>;
}

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
impl<F, Fut> Handler for F
where
    F: Fn(Message<Pending>) -> Fut + Send + Sync,
    Fut: Future<Output = Result<HandlerResult, HandlerError>> + Send,
{
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        (self)(msg).await
    }
}

pub trait Middleware: Send + Sync {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler>;
}

impl<F> Middleware for F
where
    F: Fn(Box<dyn Handler>) -> Box<dyn Handler> + Send + Sync,
{
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        (self)(next)
    }
}
```

- `HandlerResult` bundles ack outcome with produced messages.
- Blanket impl for `Fn` allows plain async functions as handlers.
- `ProducedMessage` is not a `Message<Pending>` since produced messages are fresh outputs that don't need ack resolution.

## Router

```rust
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

impl Router {
    pub fn new() -> Self { ... }

    pub fn add_middleware(&mut self, middleware: impl Middleware + 'static) -> &mut Self { ... }

    pub fn add_handler(
        &mut self,
        name: impl Into<String>,
        subscribe_topic: Topic,
        subscriber: impl Subscriber + 'static,
        publish_topic: Topic,
        publisher: impl Publisher + 'static,
        handler: impl Handler + 'static,
    ) -> HandlerBuilder<'_> { ... }

    pub fn add_consumer(
        &mut self,
        name: impl Into<String>,
        subscribe_topic: Topic,
        subscriber: impl Subscriber + 'static,
        handler: impl Handler + 'static,
    ) -> HandlerBuilder<'_> { ... }

    pub async fn run(&mut self, shutdown: ShutdownSignal) -> Result<(), RouterError> { ... }
}

pub struct HandlerBuilder<'r> {
    router: &'r mut Router,
    index: usize,
}

impl<'r> HandlerBuilder<'r> {
    pub fn with_middleware(self, middleware: impl Middleware + 'static) -> Self { ... }
}

pub enum ShutdownSignal {
    Token(CancellationToken),
    CtrlC,
}
```

- `add_handler` for consume-and-produce. `add_consumer` for sink-only.
- `HandlerBuilder` enables per-handler middleware without cluttering the registration signature.
- `ShutdownSignal` makes graceful shutdown explicit. No implicit global state.
- Router-level middlewares apply to all handlers. Handler-level middlewares apply only to that specific handler, executing inside the router-level chain.

## Error Types

```rust
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
    Subscribe { handler: String, source: SubscribeError },
    #[error("publish failed on handler {handler}: {source}")]
    Publish { handler: String, source: PublishError },
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
```

Every error type carries a `Backend` variant with `Box<dyn Error>` so backend crates can wrap their own errors without strev depending on their types. `RouterError` includes the handler name for debugging context.

## In-Memory Backend (strev-channel)

```rust
pub struct Channel {
    buffer_size: usize,
    topics: DashMap<Topic, Vec<Sender<Message<Pending>>>>,
}

impl Channel {
    pub fn new(buffer_size: usize) -> Self { ... }
    pub fn persistent() -> Self { ... }
}

#[async_trait]
impl Publisher for Channel { ... }

#[async_trait]
impl Subscriber for Channel { ... }
```

`Channel` implements both `Publisher` and `Subscriber`, making it trivial to wire up for testing. `persistent()` retains messages for late subscribers in integration testing scenarios.

## Built-in Middleware

| Middleware | Purpose |
|-----------|---------|
| `Retry` | Exponential backoff with configurable max attempts, initial delay, multiplier, max delay |
| `Timeout` | Cancel handler execution after a duration |
| `CorrelationId` | Propagate correlation IDs through message chains |
| `PoisonQueue` | Route exhausted-retry messages to a separate topic |
| `Throttle` | Rate-limit message processing |

```rust
pub struct Retry {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub multiplier: f64,
    pub max_delay: Duration,
}

pub struct Timeout {
    pub duration: Duration,
}

pub struct CorrelationId;

pub struct PoisonQueue {
    pub topic: Topic,
    pub publisher: Box<dyn Publisher>,
}

pub struct Throttle {
    pub max_per_second: u32,
}
```

## Usage Example

```rust
use strev::{Router, Topic, Message, Pending, HandlerResult, Outcome, ShutdownSignal};
use strev::middleware::{Retry, Timeout, CorrelationId};
use strev_channel::Channel;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let channel = Channel::new(256);

    let mut router = Router::new();

    router.add_middleware(CorrelationId);
    router.add_middleware(Timeout { duration: Duration::from_secs(30) });

    router
        .add_handler(
            "process_orders",
            Topic::new("orders.placed"),
            channel.clone(),
            Topic::new("orders.confirmed"),
            channel.clone(),
            |msg: Message<Pending>| async move {
                let order: Order = msg.deserialize()?;
                let confirmed = process_order(order).await?;
                Ok(HandlerResult {
                    outcome: msg.ack(),
                    produced: vec![confirmed.into_message()?],
                })
            },
        )
        .with_middleware(Retry {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            multiplier: 2.0,
            max_delay: Duration::from_secs(10),
        });

    router
        .add_consumer(
            "send_notifications",
            Topic::new("orders.confirmed"),
            channel.clone(),
            |msg: Message<Pending>| async move {
                let order: Order = msg.deserialize()?;
                notify_customer(order).await?;
                Ok(HandlerResult {
                    outcome: msg.ack(),
                    produced: vec![],
                })
            },
        );

    router.run(ShutdownSignal::CtrlC).await?;
    Ok(())
}
```

## Dependencies

| Crate | Purpose |
|-------|---------|
| `tokio` | Async runtime, channels, cancellation |
| `async-trait` | Async trait support |
| `bytes` | Zero-copy byte buffers for payloads |
| `uuid` | Message identifiers |
| `serde` / `serde_json` | Serialization/deserialization |
| `thiserror` | Error type derivation |
| `dashmap` | Concurrent hash map (strev-channel) |
| `tokio-stream` | Stream adapters for MessageStream |
| `tracing` | Structured logging/instrumentation |
