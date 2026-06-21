# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
