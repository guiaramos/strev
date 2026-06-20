use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::error::HandlerError;
use crate::handler::{Handler, HandlerResult};
use crate::message::{Message, Pending};
use crate::middleware::Middleware;

pub struct CircuitBreaker {
    pub max_failures: NonZeroU32,
    pub reset_timeout: Duration,
}

impl Middleware for CircuitBreaker {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        Box::new(CircuitBreakerHandler {
            state: Arc::new(CircuitBreakerState {
                failures: AtomicU32::new(0),
                last_failure_ms: AtomicU64::new(0),
                max_failures: self.max_failures.get(),
                reset_timeout_ms: self.reset_timeout.as_millis() as u64,
            }),
            next,
        })
    }
}

struct CircuitBreakerState {
    failures: AtomicU32,
    last_failure_ms: AtomicU64,
    max_failures: u32,
    reset_timeout_ms: u64,
}

impl CircuitBreakerState {
    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn is_open(&self) -> bool {
        let failures = self.failures.load(Ordering::SeqCst);
        if failures < self.max_failures {
            return false;
        }
        let elapsed = Self::now_ms() - self.last_failure_ms.load(Ordering::SeqCst);
        elapsed < self.reset_timeout_ms
    }

    fn record_success(&self) {
        self.failures.store(0, Ordering::SeqCst);
    }

    fn record_failure(&self) {
        self.failures.fetch_add(1, Ordering::SeqCst);
        self.last_failure_ms.store(Self::now_ms(), Ordering::SeqCst);
    }
}

struct CircuitBreakerHandler {
    state: Arc<CircuitBreakerState>,
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for CircuitBreakerHandler {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        if self.state.is_open() {
            let _ = msg.nack();
            return Err(HandlerError::Processing("circuit breaker open".into()));
        }

        match self.next.handle(msg).await {
            Ok(result) => {
                self.state.record_success();
                Ok(result)
            }
            Err(e) => {
                self.state.record_failure();
                Err(e)
            }
        }
    }
}
