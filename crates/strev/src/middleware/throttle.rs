use std::num::NonZeroU32;
use std::time::Duration;

use crate::error::HandlerError;
use crate::handler::{Handler, HandlerResult};
use crate::message::{Message, Pending};
use crate::middleware::Middleware;

pub struct Throttle {
    pub max_per_second: NonZeroU32,
}

impl Middleware for Throttle {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        let interval = Duration::from_secs_f64(1.0 / self.max_per_second.get() as f64);
        Box::new(ThrottleHandler { interval, next })
    }
}

struct ThrottleHandler {
    interval: Duration,
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for ThrottleHandler {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        tokio::time::sleep(self.interval).await;
        self.next.handle(msg).await
    }
}
