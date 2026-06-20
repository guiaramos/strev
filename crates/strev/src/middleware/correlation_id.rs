use uuid::Uuid;

use crate::error::HandlerError;
use crate::handler::{Handler, HandlerResult};
use crate::message::{Message, Pending};
use crate::middleware::Middleware;

pub struct CorrelationId;

impl Middleware for CorrelationId {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        Box::new(CorrelationIdHandler { next })
    }
}

struct CorrelationIdHandler {
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for CorrelationIdHandler {
    async fn handle(&self, mut msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        if msg.metadata().get("correlation_id").is_none() {
            msg.metadata_mut()
                .set("correlation_id", Uuid::new_v4().to_string());
        }
        self.next.handle(msg).await
    }
}
