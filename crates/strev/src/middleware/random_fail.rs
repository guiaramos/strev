use crate::error::HandlerError;
use crate::handler::{Handler, HandlerResult};
use crate::message::{Message, Pending};
use crate::middleware::Middleware;

pub struct RandomFail {
    pub probability: f32,
}

impl Middleware for RandomFail {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        Box::new(RandomFailHandler {
            probability: self.probability,
            next,
        })
    }
}

struct RandomFailHandler {
    probability: f32,
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for RandomFailHandler {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        let hash = msg.uuid().as_bytes()[0] as f32 / 255.0;
        if hash < self.probability {
            let _ = msg.nack();
            return Err(HandlerError::Processing("random failure".into()));
        }
        self.next.handle(msg).await
    }
}
