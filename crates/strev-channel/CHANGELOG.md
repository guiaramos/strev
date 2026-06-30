# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.8.0](https://github.com/guiaramos/strev/compare/v0.7.0...v0.8.0) - 2026-06-30

### Added

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

### Other

- add a bulk throughput conformance scenario for all backends
- *(strev-channel)* add criterion throughput benchmarks

## [0.6.0](https://github.com/guiaramos/strev/compare/v0.5.0...v0.6.0) - 2026-06-22

### Other

- release v0.6.0

## [0.4.0](https://github.com/guiaramos/strev/releases/tag/v0.4.0) - 2026-06-22

### Added

- *(strev-cloudevents)* add reusable CloudEvents decorators
- *(strev-channel)* add in-memory Channel backend

### Fixed

- *(strev)* use per-message topics in router and add ergonomic APIs

### Other

- release v0.4.0
- *(strev-testsuite)* add cross-backend pub/sub conformance suite
- release v0.3.0 ([#8](https://github.com/guiaramos/strev/pull/8))
- release v0.3.0
- release v0.2.0 ([#6](https://github.com/guiaramos/strev/pull/6))
- release v0.2.0
- release v0.1.0
- adopt unified workspace versioning via package inheritance
- add README, LICENSE, and crate-level documentation
- *(strev-channel)* add CloudEvents envelope example
- add CI workflow, docker-compose, Makefile, and fix clippy/fmt
- *(strev)* seal Outcome, enforce typestate, validate invariants
- init workspace with strev and strev-channel crates

## [0.3.0](https://github.com/guiaramos/strev/releases/tag/v0.3.0) - 2026-06-21

### Added

- *(strev-cloudevents)* add reusable CloudEvents decorators
- *(strev-channel)* add in-memory Channel backend

### Fixed

- *(strev)* use per-message topics in router and add ergonomic APIs

### Other

- release v0.3.0
- release v0.2.0 ([#6](https://github.com/guiaramos/strev/pull/6))
- release v0.2.0
- release v0.1.0
- adopt unified workspace versioning via package inheritance
- add README, LICENSE, and crate-level documentation
- *(strev-channel)* add CloudEvents envelope example
- add CI workflow, docker-compose, Makefile, and fix clippy/fmt
- *(strev)* seal Outcome, enforce typestate, validate invariants
- init workspace with strev and strev-channel crates

## [0.2.0](https://github.com/guiaramos/strev/releases/tag/v0.2.0) - 2026-06-21

### Added

- *(strev-cloudevents)* add reusable CloudEvents decorators
- *(strev-channel)* add in-memory Channel backend

### Fixed

- *(strev)* use per-message topics in router and add ergonomic APIs

### Other

- release v0.2.0
- release v0.1.0
- adopt unified workspace versioning via package inheritance
- add README, LICENSE, and crate-level documentation
- *(strev-channel)* add CloudEvents envelope example
- add CI workflow, docker-compose, Makefile, and fix clippy/fmt
- *(strev)* seal Outcome, enforce typestate, validate invariants
- init workspace with strev and strev-channel crates

## [0.1.0](https://github.com/guiaramos/strev/releases/tag/v0.1.0) - 2026-06-21

### Added

- *(strev-cloudevents)* add reusable CloudEvents decorators
- *(strev-channel)* add in-memory Channel backend

### Fixed

- *(strev)* use per-message topics in router and add ergonomic APIs

### Other

- adopt unified workspace versioning via package inheritance
- add README, LICENSE, and crate-level documentation
- *(strev-channel)* add CloudEvents envelope example
- add CI workflow, docker-compose, Makefile, and fix clippy/fmt
- *(strev)* seal Outcome, enforce typestate, validate invariants
- init workspace with strev and strev-channel crates
