.PHONY: test test-unit test-integration test-all fmt lint check clean services services-down

test: test-unit

test-unit:
	cargo test -p strev -p strev-channel -p strev-cloudevents

test-integration: services
	REDIS_URL="redis://127.0.0.1:6379/" cargo test -p strev-redis -- --nocapture
	NATS_URL="nats://127.0.0.1:4222" cargo test -p strev-nats -- --nocapture
	KAFKA_BROKERS="localhost:9092" cargo test -p strev-kafka -- --nocapture
	$(MAKE) services-down

test-all: services
	cargo test -p strev -p strev-channel -p strev-cloudevents
	REDIS_URL="redis://127.0.0.1:6379/" cargo test -p strev-redis -- --nocapture
	NATS_URL="nats://127.0.0.1:4222" cargo test -p strev-nats -- --nocapture
	KAFKA_BROKERS="localhost:9092" cargo test -p strev-kafka -- --nocapture
	$(MAKE) services-down

services:
	docker compose up -d --wait

services-down:
	docker compose down -v

fmt:
	cargo fmt --all

lint:
	cargo clippy --workspace --all-targets -- -D warnings

check:
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test -p strev -p strev-channel -p strev-cloudevents

clean:
	cargo clean
	docker compose down -v 2>/dev/null || true
