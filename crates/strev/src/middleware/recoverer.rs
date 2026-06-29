use std::panic::AssertUnwindSafe;

use futures::FutureExt;

use crate::error::HandlerError;
use crate::handler::{Handler, HandlerResult};
use crate::message::{Message, Pending};
use crate::middleware::Middleware;

pub struct Recoverer;

impl Recoverer {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Recoverer {
    fn default() -> Self {
        Self::new()
    }
}

impl Middleware for Recoverer {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        Box::new(RecovererHandler { next })
    }
}

struct RecovererHandler {
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for RecovererHandler {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        match AssertUnwindSafe(self.next.handle(msg)).catch_unwind().await {
            Ok(result) => result,
            Err(panic) => {
                let detail = panic
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "unknown panic".to_string());
                Err(HandlerError::Processing(
                    format!("handler panicked: {detail}").into(),
                ))
            }
        }
    }
}
