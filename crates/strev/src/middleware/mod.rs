mod correlation_id;
mod poison_queue;
mod retry;
mod throttle;
mod timeout;

pub use correlation_id::CorrelationId;
pub use poison_queue::PoisonQueue;
pub use retry::Retry;
pub use throttle::Throttle;
pub use timeout::Timeout;

use crate::handler::Handler;

pub trait Middleware: Send + Sync {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler>;
}
