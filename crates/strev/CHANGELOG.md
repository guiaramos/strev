# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.0](https://github.com/guiaramos/strev/compare/v0.5.0...v0.6.0) - 2026-06-22

### Other

- release v0.6.0

## [0.4.0](https://github.com/guiaramos/strev/releases/tag/v0.4.0) - 2026-06-22

### Added

- *(strev)* add real-world examples for middleware patterns
- *(strev)* port remaining watermill features
- *(strev)* add examples mirroring watermill patterns
- *(strev)* add built-in middleware (Retry, Timeout, CorrelationId, Throttle, PoisonQueue)
- *(strev)* add Router with middleware chain and graceful shutdown
- *(strev)* add Middleware trait
- *(strev)* add Handler trait with blanket Fn impl
- *(strev)* add Publisher, Subscriber traits and MessageStream
- *(strev)* add Message with typestate ack
- *(strev)* add error types
- *(strev)* add Topic, Metadata, and Outcome types

### Fixed

- *(strev)* use per-message topics in router and add ergonomic APIs

### Other

- release v0.4.0
- release v0.3.0 ([#8](https://github.com/guiaramos/strev/pull/8))
- release v0.3.0
- release v0.2.0 ([#6](https://github.com/guiaramos/strev/pull/6))
- release v0.2.0
- release v0.1.0
- adopt unified workspace versioning via package inheritance
- add README, LICENSE, and crate-level documentation
- add CI workflow, docker-compose, Makefile, and fix clippy/fmt
- *(strev)* seal Outcome, enforce typestate, validate invariants
- *(strev)* add e2e tests for full message pipeline
- *(strev)* fix clippy warning in router
- *(strev)* add router integration tests
- init workspace with strev and strev-channel crates

## [0.3.0](https://github.com/guiaramos/strev/releases/tag/v0.3.0) - 2026-06-21

### Added

- *(strev)* add real-world examples for middleware patterns
- *(strev)* port remaining watermill features
- *(strev)* add examples mirroring watermill patterns
- *(strev)* add built-in middleware (Retry, Timeout, CorrelationId, Throttle, PoisonQueue)
- *(strev)* add Router with middleware chain and graceful shutdown
- *(strev)* add Middleware trait
- *(strev)* add Handler trait with blanket Fn impl
- *(strev)* add Publisher, Subscriber traits and MessageStream
- *(strev)* add Message with typestate ack
- *(strev)* add error types
- *(strev)* add Topic, Metadata, and Outcome types

### Fixed

- *(strev)* use per-message topics in router and add ergonomic APIs

### Other

- release v0.3.0
- release v0.2.0 ([#6](https://github.com/guiaramos/strev/pull/6))
- release v0.2.0
- release v0.1.0
- adopt unified workspace versioning via package inheritance
- add README, LICENSE, and crate-level documentation
- add CI workflow, docker-compose, Makefile, and fix clippy/fmt
- *(strev)* seal Outcome, enforce typestate, validate invariants
- *(strev)* add e2e tests for full message pipeline
- *(strev)* fix clippy warning in router
- *(strev)* add router integration tests
- init workspace with strev and strev-channel crates

## [0.2.0](https://github.com/guiaramos/strev/releases/tag/v0.2.0) - 2026-06-21

### Added

- *(strev)* add real-world examples for middleware patterns
- *(strev)* port remaining watermill features
- *(strev)* add examples mirroring watermill patterns
- *(strev)* add built-in middleware (Retry, Timeout, CorrelationId, Throttle, PoisonQueue)
- *(strev)* add Router with middleware chain and graceful shutdown
- *(strev)* add Middleware trait
- *(strev)* add Handler trait with blanket Fn impl
- *(strev)* add Publisher, Subscriber traits and MessageStream
- *(strev)* add Message with typestate ack
- *(strev)* add error types
- *(strev)* add Topic, Metadata, and Outcome types

### Fixed

- *(strev)* use per-message topics in router and add ergonomic APIs

### Other

- release v0.2.0
- release v0.1.0
- adopt unified workspace versioning via package inheritance
- add README, LICENSE, and crate-level documentation
- add CI workflow, docker-compose, Makefile, and fix clippy/fmt
- *(strev)* seal Outcome, enforce typestate, validate invariants
- *(strev)* add e2e tests for full message pipeline
- *(strev)* fix clippy warning in router
- *(strev)* add router integration tests
- init workspace with strev and strev-channel crates

## [0.1.0](https://github.com/guiaramos/strev/releases/tag/v0.1.0) - 2026-06-21

### Added

- *(strev)* add real-world examples for middleware patterns
- *(strev)* port remaining watermill features
- *(strev)* add examples mirroring watermill patterns
- *(strev)* add built-in middleware (Retry, Timeout, CorrelationId, Throttle, PoisonQueue)
- *(strev)* add Router with middleware chain and graceful shutdown
- *(strev)* add Middleware trait
- *(strev)* add Handler trait with blanket Fn impl
- *(strev)* add Publisher, Subscriber traits and MessageStream
- *(strev)* add Message with typestate ack
- *(strev)* add error types
- *(strev)* add Topic, Metadata, and Outcome types

### Fixed

- *(strev)* use per-message topics in router and add ergonomic APIs

### Other

- adopt unified workspace versioning via package inheritance
- add README, LICENSE, and crate-level documentation
- add CI workflow, docker-compose, Makefile, and fix clippy/fmt
- *(strev)* seal Outcome, enforce typestate, validate invariants
- *(strev)* add e2e tests for full message pipeline
- *(strev)* fix clippy warning in router
- *(strev)* add router integration tests
- init workspace with strev and strev-channel crates
