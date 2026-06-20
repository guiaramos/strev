use std::time::Duration;

use crate::error::HandlerError;
use crate::handler::{Handler, HandlerResult};
use crate::message::{Message, Pending};
use crate::middleware::Middleware;

pub struct Retry {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub multiplier: f64,
    pub max_delay: Duration,
}

impl Middleware for Retry {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        Box::new(RetryHandler {
            max_attempts: self.max_attempts,
            initial_delay: self.initial_delay,
            multiplier: self.multiplier,
            max_delay: self.max_delay,
            next,
        })
    }
}

struct RetryHandler {
    max_attempts: u32,
    initial_delay: Duration,
    multiplier: f64,
    max_delay: Duration,
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for RetryHandler {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        let payload = msg.payload().clone();
        let metadata = msg.metadata().clone();

        match self.next.handle(msg).await {
            Ok(result) => return Ok(result),
            Err(e) => {
                let mut last_err = e;
                let mut delay = self.initial_delay;

                for _ in 1..self.max_attempts {
                    tokio::time::sleep(delay).await;
                    delay = Duration::from_secs_f64(
                        (delay.as_secs_f64() * self.multiplier).min(self.max_delay.as_secs_f64()),
                    );

                    let retry_msg = Message::with_metadata(payload.clone(), metadata.clone());
                    match self.next.handle(retry_msg).await {
                        Ok(result) => return Ok(result),
                        Err(e) => last_err = e,
                    }
                }

                Err(last_err)
            }
        }
    }
}
