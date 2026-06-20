use std::time::Duration;

use crate::error::HandlerError;
use crate::handler::{Handler, HandlerResult};
use crate::message::{Message, Pending};
use crate::middleware::Middleware;

pub struct Timeout {
    pub duration: Duration,
}

impl Middleware for Timeout {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        Box::new(TimeoutHandler {
            duration: self.duration,
            next,
        })
    }
}

struct TimeoutHandler {
    duration: Duration,
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for TimeoutHandler {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        match tokio::time::timeout(self.duration, self.next.handle(msg)).await {
            Ok(result) => result,
            Err(_) => Err(HandlerError::Processing("handler timed out".into())),
        }
    }
}
