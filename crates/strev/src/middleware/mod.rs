mod circuit_breaker;
mod correlation_id;
mod deduplicator;
mod delay_on_error;
mod duplicator;
mod ignore_errors;
mod instant_ack;
mod poison_queue;
mod random_fail;
mod retry;
mod throttle;
mod timeout;

pub use circuit_breaker::CircuitBreaker;
pub use correlation_id::{correlation_id, set_correlation_id, CorrelationId};
pub use deduplicator::{DeduplicateRepository, Deduplicator, InMemoryDeduplicateRepository};
pub use delay_on_error::DelayOnError;
pub use duplicator::Duplicator;
pub use ignore_errors::IgnoreErrors;
pub use instant_ack::InstantAck;
pub use poison_queue::PoisonQueue;
pub use random_fail::RandomFail;
pub use retry::Retry;
pub use throttle::Throttle;
pub use timeout::Timeout;

use crate::handler::Handler;

pub trait Middleware: Send + Sync {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler>;
}
