use crate::error::HandlerError;
use crate::handler::{Handler, HandlerResult};
use crate::message::{Message, Pending};
use crate::middleware::Middleware;

pub struct InstantAck;

impl Middleware for InstantAck {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        Box::new(InstantAckHandler { next })
    }
}

struct InstantAckHandler {
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for InstantAckHandler {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        let payload = msg.payload().clone();
        let metadata = msg.metadata().clone();
        let _ = msg.ack();
        let new_msg = Message::with_metadata(payload, metadata);
        self.next.handle(new_msg).await
    }
}
