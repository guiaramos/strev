use crate::error::HandlerError;
use crate::handler::{Handler, HandlerResult};
use crate::message::{Message, Pending};
use crate::middleware::Middleware;

pub struct Duplicator;

impl Middleware for Duplicator {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        Box::new(DuplicatorHandler { next })
    }
}

struct DuplicatorHandler {
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for DuplicatorHandler {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        let copy = msg.copy();
        let _ = self.next.handle(copy).await?;
        self.next.handle(msg).await
    }
}
