use std::sync::Arc;

use crate::error::HandlerError;
use crate::handler::{Handler, HandlerResult};
use crate::message::{Message, Pending};
use crate::middleware::Middleware;
use crate::publisher::Publisher;
use crate::topic::Topic;

pub struct PoisonQueue {
    pub topic: Topic,
    pub publisher: Arc<dyn Publisher>,
}

impl Middleware for PoisonQueue {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        Box::new(PoisonQueueHandler {
            topic: self.topic.clone(),
            publisher: self.publisher.clone(),
            next,
        })
    }
}

struct PoisonQueueHandler {
    topic: Topic,
    publisher: Arc<dyn Publisher>,
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for PoisonQueueHandler {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        let payload = msg.payload().clone();
        let metadata = msg.metadata().clone();

        match self.next.handle(msg).await {
            Ok(result) => Ok(result),
            Err(e) => {
                let mut poison_meta = metadata;
                poison_meta.set("poison_error", e.to_string());
                let poison_msg = Message::with_metadata(payload, poison_meta);
                let _ = self.publisher.publish(&self.topic, vec![poison_msg]).await;
                Err(e)
            }
        }
    }
}
