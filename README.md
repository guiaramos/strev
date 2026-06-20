# strev

[![CI](https://github.com/guiaramos/strev/actions/workflows/ci.yml/badge.svg)](https://github.com/guiaramos/strev/actions/workflows/ci.yml)

An event-driven messaging library for Rust. strev gives you a single, uniform way to
publish and consume messages across in-memory channels, Redis Streams, NATS JetStream,
and Apache Kafka, with a router, composable middleware, and pluggable serialization.

It is built for event-driven applications: event sourcing, async read models, sagas,
and message-based integration between services.

## Highlights

- **Uniform API** across every transport. Swap backends without touching handler code.
- **Invalid states unrepresentable.** A `Message` carries its ack/nack lifecycle in the
  type system, so a message can be acknowledged exactly once.
- **Router** that wires subscribers to handlers, fans out to multiple consumers, and
  shuts down gracefully.
- **Composable middleware** for retries, timeouts, throttling, deduplication, and more.
- **Decorators** for cross-cutting wire-format concerns such as CloudEvents enveloping.

## Installation

The crates are not yet published to crates.io. Add what you need as a git dependency:

```toml
[dependencies]
strev = { git = "https://github.com/guiaramos/strev" }
strev-channel = { git = "https://github.com/guiaramos/strev" }   # in-memory
strev-redis = { git = "https://github.com/guiaramos/strev" }     # Redis Streams
strev-nats = { git = "https://github.com/guiaramos/strev" }      # NATS JetStream
strev-kafka = { git = "https://github.com/guiaramos/strev" }     # Apache Kafka
```

## Quickstart

Publish and consume through the in-memory channel using the router:

```rust
use std::time::Duration;

use bytes::Bytes;
use strev::{HandlerResult, Message, Publisher, Router, ShutdownSignal, Topic};
use strev_channel::Channel;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let channel = Channel::new(64);
    let topic = Topic::new("orders");

    let mut router = Router::new();
    router.add_consumer(
        "orders",
        topic.clone(),
        channel.clone(),
        |msg: Message| async move {
            println!("received: {}", String::from_utf8_lossy(msg.payload()));
            Ok(HandlerResult::ack(msg))
        },
    );

    let token = CancellationToken::new();
    let tc = token.clone();
    let handle = tokio::spawn(async move { router.run(ShutdownSignal::Token(tc)).await });

    Publisher::publish(&channel, &topic, vec![Message::new(Bytes::from("order-1"))])
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    token.cancel();
    handle.await.unwrap().unwrap();
}
```

Swapping in a real backend only changes how you construct the publisher and subscriber.
For example, with Redis:

```rust
use strev_redis::{RedisPublisher, RedisPublisherConfig, RedisSubscriber, RedisSubscriberConfig};

let client = redis::Client::open("redis://127.0.0.1:6379/")?;
let publisher = RedisPublisher::new(RedisPublisherConfig::new(client.clone())).await?;
let subscriber = RedisSubscriber::new(RedisSubscriberConfig::new(client, "orders-group"));
```

## Core concepts

**Message.** A payload plus metadata and a UUID. Its acknowledgement state is a type
parameter, so the compiler enforces that you ack or nack each message once:

```rust
let msg = Message::new(Bytes::from("payload"));
let outcome = msg.ack(); // consumes the message; it cannot be acked again
```

**Handler.** A handler receives a message and decides what happens next: acknowledge it,
negatively acknowledge it, or acknowledge and produce new messages. Any
`async fn(Message) -> Result<HandlerResult, HandlerError>` is a handler:

```rust
|msg: Message| async move {
    let produced = vec![/* ProducedMessage */];
    Ok(HandlerResult::ack_with(msg, produced))
}
```

**Publisher and Subscriber.** Every backend implements two traits:

```rust
#[async_trait]
pub trait Publisher: Send + Sync {
    async fn publish(&self, topic: &Topic, messages: Vec<Message>) -> Result<Vec<Outcome>, PublishError>;
    async fn close(&mut self) -> Result<(), CloseError>;
}

#[async_trait]
pub trait Subscriber: Send + Sync {
    async fn subscribe(&self, topic: &Topic) -> Result<MessageStream, SubscribeError>;
    async fn close(&mut self) -> Result<(), CloseError>;
}
```

**Router.** Registers handlers against subscribers, applies middleware and decorators,
and runs every consumer concurrently until a shutdown signal fires. Use `add_consumer`
for a sink, or `add_handler` when a handler also publishes to another topic.

## Backends

| Transport      | Crate              | Notes                                              |
|----------------|--------------------|----------------------------------------------------|
| In-memory      | `strev-channel`    | single process, ideal for tests and local dev      |
| Redis Streams  | `strev-redis`      | consumer groups, pluggable marshaller              |
| NATS JetStream | `strev-nats`       | durable pull consumers, headers as metadata        |
| Apache Kafka   | `strev-kafka`      | consumer groups, manual offset commits             |

`strev-kafka` exposes a `sasl-ssl` feature that enables TLS and SASL for managed brokers,
and a config passthrough for arbitrary client properties:

```rust
KafkaPublisherConfig::new("broker:9092")
    .option("security.protocol", "SASL_SSL")
    .option("sasl.mechanisms", "PLAIN")
    .option("sasl.username", "<key>")
    .option("sasl.password", "<secret>");
```

## Middleware

Register middleware on the router with `add_middleware`; it wraps every handler in
order. Built-in middleware:

`Retry`, `Timeout`, `Throttle`, `CircuitBreaker`, `Deduplicator`, `CorrelationId`,
`PoisonQueue`, `DelayOnError`, `Duplicator`, `IgnoreErrors`, `InstantAck`, `RandomFail`.

## Decorators and CloudEvents

Decorators transform messages at the transport boundary on both the publish and
subscribe side. The `strev-cloudevents` crate uses this to envelope and unwrap messages
as CloudEvents, mapping event attributes to `ce-*` metadata:

```rust
let codec = CloudEventCodec::new("https://example.com/orders")
    .event_type("com.example.order.created");

router.add_subscriber_decorator(CloudEventsSubscriberDecorator::new(codec.clone()));
router.add_publisher_decorator(CloudEventsPublisherDecorator::new(codec));
```

## Examples

Runnable examples live under each crate's `examples/` directory:

- `strev`: `basic_pubsub`, `router`, `consumer_groups`, `middleware_chain`, `deduplication`, `poison_queue`, `event_pipeline`
- `strev-redis`: `redis_pubsub`
- `strev-nats`: `nats_pubsub`
- `strev-kafka`: `kafka_pubsub`
- `strev-cloudevents`: `router_cloudevents`

Run one with, for example:

```bash
cargo run -p strev --example basic_pubsub
```

## Development

Unit tests need no services:

```bash
make test-unit
```

Integration tests run against pinned Docker services:

```bash
make services       # start redis, nats, kafka
make test-all       # unit + integration
make services-down  # tear down
```

Format and lint:

```bash
make check          # fmt check + clippy + unit tests
```

## License

[MIT](LICENSE)
