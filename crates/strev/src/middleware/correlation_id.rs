use uuid::Uuid;

use crate::error::HandlerError;
use crate::handler::{Handler, HandlerResult};
use crate::message::{Message, Pending};
use crate::middleware::Middleware;

const CORRELATION_ID_KEY: &str = "correlation_id";

pub fn set_correlation_id(msg: &mut Message<Pending>, id: impl Into<String>) {
    msg.metadata_mut().set(CORRELATION_ID_KEY, id);
}

pub fn correlation_id(msg: &Message<Pending>) -> Option<&str> {
    msg.metadata().get(CORRELATION_ID_KEY)
}

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
        if msg.metadata().get(CORRELATION_ID_KEY).is_none() {
            msg.metadata_mut()
                .set(CORRELATION_ID_KEY, Uuid::new_v4().to_string());
        }
        self.next.handle(msg).await
    }
}
