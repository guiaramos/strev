.PHONY: test test-unit test-integration test-all fmt lint check clean services services-down

test: test-unit

test-unit:
	cargo test -p strev -p strev-channel -p strev-cloudevents -p strev-telemetry

test-integration: services
	REDIS_URL="redis://127.0.0.1:6379/" cargo test -p strev-redis -- --nocapture
	NATS_URL="nats://127.0.0.1:4222" cargo test -p strev-nats -- --nocapture
	KAFKA_BROKERS="localhost:9092" cargo test -p strev-kafka -- --nocapture
	DATABASE_URL="postgres://postgres:postgres@127.0.0.1:5432/postgres" cargo test -p strev-postgres -- --nocapture
	MONGODB_URI="mongodb://127.0.0.1:27017/?directConnection=true" cargo test -p strev-mongodb -- --nocapture
	AMQP_URI="amqp://guest:guest@127.0.0.1:5672/%2f" cargo test -p strev-amqp -- --nocapture
	$(MAKE) services-down

test-all: services
	cargo test -p strev -p strev-channel -p strev-cloudevents -p strev-telemetry
	REDIS_URL="redis://127.0.0.1:6379/" cargo test -p strev-redis -- --nocapture
	NATS_URL="nats://127.0.0.1:4222" cargo test -p strev-nats -- --nocapture
	KAFKA_BROKERS="localhost:9092" cargo test -p strev-kafka -- --nocapture
	DATABASE_URL="postgres://postgres:postgres@127.0.0.1:5432/postgres" cargo test -p strev-postgres -- --nocapture
	MONGODB_URI="mongodb://127.0.0.1:27017/?directConnection=true" cargo test -p strev-mongodb -- --nocapture
	AMQP_URI="amqp://guest:guest@127.0.0.1:5672/%2f" cargo test -p strev-amqp -- --nocapture
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
	cargo test -p strev -p strev-channel -p strev-cloudevents -p strev-telemetry

clean:
	cargo clean
	docker compose down -v 2>/dev/null || true
