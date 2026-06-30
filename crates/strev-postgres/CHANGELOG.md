# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.8.0](https://github.com/guiaramos/strev/compare/v0.7.0...v0.8.0) - 2026-06-30

### Added

- *(strev-postgres)* add PostgresRetention to purge consumed messages
- add ConsumerLag capability with postgres and redis support
- *(strev-nats)* add core NATS at-most-once publisher and subscriber
- *(strev-kafka)* honor a partition-key metadata for ordering
- *(strev-postgres)* add publish_tx for the transactional outbox pattern
- *(strev)* add FanIn and RequestReply components
- *(strev-mongodb)* add MongoQueueSubscriber with nack redelivery
- *(strev-kafka)* redeliver nacked messages by seeking the partition
- *(strev-postgres)* redeliver nacked messages via a per-row lease
- *(strev)* redeliver nacked messages via an ack-feedback lease
- *(strev-mongodb)* honor delayed delivery via DelayedPublisher
- add opt-in delayed delivery via DelayedPublisher
- *(strev)* add Requeuer to move messages between topics
- *(strev)* add Forwarder for the outbox/bridge pattern

### Fixed

- *(strev-postgres)* advance the watermark over the topic id sequence

### Other

- add a bulk throughput conformance scenario for all backends
- batch acknowledgements in the postgres, redis, and mongo subscribers
- batch publishes and move postgres advance off the ack path
- *(strev-channel)* add criterion throughput benchmarks
- add competing-consumers and lease-timeout conformance coverage

## [0.6.0](https://github.com/guiaramos/strev/releases/tag/v0.6.0) - 2026-06-22

### Added

- *(strev-telemetry)* add tracing and metrics middleware
- *(strev-mongodb)* add MongoDB backend with change streams
- *(strev-postgres)* add PostgreSQL pubsub backend

### Other

- release v0.6.0
- release v0.5.0
- release v0.4.0 ([#10](https://github.com/guiaramos/strev/pull/10))
- release v0.4.0
- *(strev-testsuite)* add cross-backend pub/sub conformance suite
- release v0.3.0 ([#8](https://github.com/guiaramos/strev/pull/8))
- release v0.3.0
- release v0.2.0 ([#6](https://github.com/guiaramos/strev/pull/6))
- release v0.2.0
- enforce conventional commits with committed and lefthook
- add README, LICENSE, and crate-level documentation

## [0.4.0](https://github.com/guiaramos/strev/releases/tag/v0.4.0) - 2026-06-22

### Added

- *(strev-postgres)* add PostgreSQL pubsub backend

### Other

- release v0.4.0
- *(strev-testsuite)* add cross-backend pub/sub conformance suite
- release v0.3.0 ([#8](https://github.com/guiaramos/strev/pull/8))
- release v0.3.0
- release v0.2.0 ([#6](https://github.com/guiaramos/strev/pull/6))
- release v0.2.0

## [0.3.0](https://github.com/guiaramos/strev/releases/tag/v0.3.0) - 2026-06-21

### Added

- *(strev-postgres)* add PostgreSQL pubsub backend

### Other

- release v0.3.0
- release v0.2.0 ([#6](https://github.com/guiaramos/strev/pull/6))
- release v0.2.0

## [0.2.0](https://github.com/guiaramos/strev/releases/tag/v0.2.0) - 2026-06-21

### Added

- *(strev-postgres)* add PostgreSQL pubsub backend

### Other

- release v0.2.0
