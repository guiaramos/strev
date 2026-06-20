use std::sync::Arc;

use crate::error::HandlerError;
use crate::handler::{Handler, HandlerResult};
use crate::message::{Message, Pending};
use crate::middleware::Middleware;

pub struct IgnoreErrors {
    pub should_ignore: Arc<dyn Fn(&HandlerError) -> bool + Send + Sync>,
}

impl Middleware for IgnoreErrors {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        Box::new(IgnoreErrorsHandler {
            should_ignore: self.should_ignore.clone(),
            next,
        })
    }
}

struct IgnoreErrorsHandler {
    should_ignore: Arc<dyn Fn(&HandlerError) -> bool + Send + Sync>,
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for IgnoreErrorsHandler {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        match self.next.handle(msg).await {
            Ok(result) => Ok(result),
            Err(e) if (self.should_ignore)(&e) => Ok(HandlerResult::empty_ack()),
            Err(e) => Err(e),
        }
    }
}
