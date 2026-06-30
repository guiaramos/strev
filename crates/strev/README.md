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
strev-postgres = { git = "https://github.com/guiaramos/strev" }  # PostgreSQL
strev-mongodb = { git = "https://github.com/guiaramos/strev" }   # MongoDB
strev-amqp = { git = "https://github.com/guiaramos/strev" }      # AMQP / RabbitMQ
strev-telemetry = { git = "https://github.com/guiaramos/strev" } # tracing + metrics
strev-cqrs = { git = "https://github.com/guiaramos/strev" }      # CQRS buses + processors
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
| PostgreSQL     | `strev-postgres`   | durable table, per-group offsets, pure Rust (sqlx) |
| MongoDB        | `strev-mongodb`    | change streams, resume tokens (needs replica set)  |
| AMQP (RabbitMQ)| `strev-amqp`       | durable fanout exchange, a durable queue per group |

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
`PoisonQueue`, `DelayOnError`, `Duplicator`, `IgnoreErrors`, `InstantAck`, `RandomFail`,
`Recoverer` (catches a panicking handler and converts it to an error so the consumer survives).

The `strev-telemetry` crate adds a `Telemetry` middleware that emits a `tracing` span per
message plus `metrics` facade measurements (handler-duration histogram, acked/nacked/
errored counters), so you can wire strev into any tracing/metrics exporter you already use.

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

## Forwarder (outbox / bridge)

`ForwarderPublisher` redirects published messages to a single forwarder topic, recording
their real destination in a reserved metadata key (payload untouched, zero-copy - no
envelope serialization). A `Forwarder` consumes that topic and relays each message to its
destination, possibly on a different backend - enabling the transactional outbox pattern
and cross-backend bridging.

```rust
Forwarder::register(&mut router, subscriber_in, Arc::new(publisher_out), ForwarderConfig::new());
let publisher = ForwarderPublisher::new(Box::new(outbox_publisher)); // app publishes as usual
```

A `Requeuer` complements this for operational requeues: it drains one topic (e.g. a
dead-letter or poison topic) back to a destination chosen by a resolver, optionally after a
delay, recording attempts in the `requeue-retries` metadata key so the resolver can cap them.

```rust
RequeuerConfig::new("poison")
    .delay(Duration::from_secs(1))
    .destination(|m: &Message| Ok(Topic::new(m.metadata().get("original-topic").unwrap_or("orders"))))
    .register(&mut router, subscriber, Arc::new(publisher));
```

## Delayed delivery

Withholding a message until a future instant is an opt-in backend capability, not a core
guarantee. Only backends that can actually enforce it implement the `DelayedPublisher`
trait, so `publish_after` on a backend that cannot delay is a compile error rather than a
silent no-op. The pattern is the same everywhere: `publish_after` stages the message, and a
promoter moves it into the live topic once it is due, leaving the normal subscriber path
untouched.

```rust
publisher.publish_after(&Topic::new("orders"), messages, Delay::after(Duration::from_secs(30))).await?;

// run a promoter to move due messages into their live topics
let promoter = RedisDelayPromoter::new(RedisDelayPromoterConfig::new(client)).await?;
tokio::spawn(async move { promoter.run(shutdown).await });
```

| Backend | Staging | Promoter |
|---------|---------|----------|
| `strev-channel`  | in-process timer task         | none (delivers itself when due) |
| `strev-redis`    | sorted set scored by due-time | `RedisDelayPromoter` |
| `strev-postgres` | `deliver_after` column        | `PostgresDelayPromoter` (single atomic claim-and-insert) |
| `strev-mongodb`  | `deliver_after` collection    | `MongoDelayPromoter` (moves due docs into the watched collection) |

Run one promoter for exactly-once promotion, or several for high availability (delivery is
then at-least-once; pair with the `Deduplicator` middleware).

## CQRS

The `strev-cqrs` crate adds typed command/event buses and processors on top of the
router. Commands and events are `serde` types identified by a `NAME`; a command is
handled by exactly one handler, an event fans out to every handler.

```rust
#[derive(Serialize, Deserialize)]
struct PlaceOrder { order_id: u64 }
impl Command for PlaceOrder { const NAME: &'static str = "PlaceOrder"; }

command_bus.send(PlaceOrder { order_id: 1 }).await?;

command_processor.add_handler("place-order", |cmd: PlaceOrder, ctx: Context| async move {
    // ctx.message_id() for correlation
    Ok(())
})?;
command_processor.register(&mut router);
```

## Examples

Runnable examples live under each crate's `examples/` directory:

- `strev`: `basic_pubsub`, `router`, `consumer_groups`, `middleware_chain`, `deduplication`, `poison_queue`, `event_pipeline`, `forwarder`, `requeuer`
- `strev-redis`: `redis_pubsub`
- `strev-nats`: `nats_pubsub`
- `strev-kafka`: `kafka_pubsub`
- `strev-postgres`: `postgres_pubsub`
- `strev-mongodb`: `mongodb_pubsub`
- `strev-amqp`: `amqp_pubsub`
- `strev-cloudevents`: `router_cloudevents`
- `strev-telemetry`: `telemetry`
- `strev-cqrs`: `cqrs`

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

### Git hooks

Commits follow [Conventional Commits](https://www.conventionalcommits.org), which the
release automation relies on. Install the hook manager and commit linter, then enable
the hooks:

```bash
brew install lefthook committed
lefthook install
```

The `pre-commit` hook runs `cargo fmt --check` and clippy; the `commit-msg` hook checks
the message format. CI enforces both regardless of local hooks.

## License

[MIT](LICENSE)
