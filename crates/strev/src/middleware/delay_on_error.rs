use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use crate::error::HandlerError;
use crate::handler::{Handler, HandlerResult};
use crate::message::{Message, Pending};
use crate::middleware::Middleware;

pub struct DelayOnError {
    pub initial_interval: Duration,
    pub max_interval: Duration,
    pub multiplier: f64,
}

impl Middleware for DelayOnError {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        Box::new(DelayOnErrorHandler {
            initial_interval: self.initial_interval,
            max_interval: self.max_interval,
            multiplier: self.multiplier,
            consecutive_errors: AtomicU32::new(0),
            next,
        })
    }
}

struct DelayOnErrorHandler {
    initial_interval: Duration,
    max_interval: Duration,
    multiplier: f64,
    consecutive_errors: AtomicU32,
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for DelayOnErrorHandler {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        match self.next.handle(msg).await {
            Ok(result) => {
                self.consecutive_errors.store(0, Ordering::SeqCst);
                Ok(result)
            }
            Err(e) => {
                let n = self.consecutive_errors.fetch_add(1, Ordering::SeqCst);
                let delay = Duration::from_secs_f64(
                    (self.initial_interval.as_secs_f64() * self.multiplier.powi(n as i32))
                        .min(self.max_interval.as_secs_f64()),
                );
                tokio::time::sleep(delay).await;
                Err(e)
            }
        }
    }
}
