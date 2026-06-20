use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::error::HandlerError;
use crate::handler::{Handler, HandlerResult};
use crate::message::{Message, Pending};
use crate::middleware::Middleware;

type KeyFactoryFn = Arc<dyn Fn(&Message<Pending>) -> String + Send + Sync>;

#[async_trait]
pub trait DeduplicateRepository: Send + Sync {
    async fn is_duplicate(&self, key: &str) -> bool;
}

pub struct InMemoryDeduplicateRepository {
    seen: std::sync::Mutex<HashMap<String, Instant>>,
    window: Duration,
}

impl InMemoryDeduplicateRepository {
    pub fn new(window: Duration) -> Self {
        Self {
            seen: std::sync::Mutex::new(HashMap::new()),
            window,
        }
    }
}

#[async_trait]
impl DeduplicateRepository for InMemoryDeduplicateRepository {
    async fn is_duplicate(&self, key: &str) -> bool {
        let mut map = self.seen.lock().unwrap();
        let now = Instant::now();
        map.retain(|_, ts| now.duration_since(*ts) < self.window);

        if map.contains_key(key) {
            true
        } else {
            map.insert(key.to_string(), now);
            false
        }
    }
}

pub struct Deduplicator {
    pub repository: Arc<dyn DeduplicateRepository>,
    pub key_factory: Option<KeyFactoryFn>,
}

impl Middleware for Deduplicator {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        Box::new(DeduplicatorHandler {
            repository: self.repository.clone(),
            key_factory: self.key_factory.clone(),
            next,
        })
    }
}

struct DeduplicatorHandler {
    repository: Arc<dyn DeduplicateRepository>,
    key_factory: Option<KeyFactoryFn>,
    next: Box<dyn Handler>,
}

#[async_trait]
impl Handler for DeduplicatorHandler {
    async fn handle(&self, msg: Message<Pending>) -> Result<HandlerResult, HandlerError> {
        let key = match &self.key_factory {
            Some(factory) => factory(&msg),
            None => msg.uuid().to_string(),
        };

        if self.repository.is_duplicate(&key).await {
            return Ok(HandlerResult::ack(msg));
        }

        self.next.handle(msg).await
    }
}
