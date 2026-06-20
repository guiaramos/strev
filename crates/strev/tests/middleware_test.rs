use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use strev::{Handler, HandlerError, HandlerResult, Message, Middleware};

struct CountingMiddleware {
    count: Arc<AtomicU32>,
}

impl Middleware for CountingMiddleware {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        let count = self.count.clone();
        Box::new(WrappedHandler { count, next })
    }
}

struct WrappedHandler {
    count: Arc<AtomicU32>,
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for WrappedHandler {
    async fn handle(&self, msg: Message) -> Result<HandlerResult, HandlerError> {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.next.handle(msg).await
    }
}

async fn noop_handler(msg: Message) -> Result<HandlerResult, HandlerError> {
    Ok(HandlerResult::ack(msg))
}

#[tokio::test]
async fn middleware_wraps_handler() {
    let count = Arc::new(AtomicU32::new(0));
    let mw = CountingMiddleware { count: count.clone() };

    let handler: Box<dyn Handler> = Box::new(noop_handler as fn(Message) -> _);
    let wrapped = mw.wrap(handler);

    let msg = Message::new(Bytes::from("test"));
    let result = wrapped.handle(msg).await.unwrap();
    assert!(result.outcome().is_acked());
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn middleware_chain_executes_in_order() {
    let log = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

    let mw_a = {
        let log = log.clone();
        ClosureMiddleware(Arc::new(move |next: Box<dyn Handler>| -> Box<dyn Handler> {
            let log = log.clone();
            Box::new(LogHandler { label: "A".into(), log, next })
        }))
    };

    let mw_b = {
        let log = log.clone();
        ClosureMiddleware(Arc::new(move |next: Box<dyn Handler>| -> Box<dyn Handler> {
            let log = log.clone();
            Box::new(LogHandler { label: "B".into(), log, next })
        }))
    };

    let handler: Box<dyn Handler> = Box::new(noop_handler as fn(Message) -> _);
    let wrapped = mw_a.wrap(mw_b.wrap(handler));

    let msg = Message::new(Bytes::from("test"));
    wrapped.handle(msg).await.unwrap();

    let entries = log.lock().unwrap();
    assert_eq!(&*entries, &["A", "B"]);
}

struct ClosureMiddleware(Arc<dyn Fn(Box<dyn Handler>) -> Box<dyn Handler> + Send + Sync>);

impl Middleware for ClosureMiddleware {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        (self.0)(next)
    }
}

struct LogHandler {
    label: String,
    log: Arc<std::sync::Mutex<Vec<String>>>,
    next: Box<dyn Handler>,
}

#[async_trait::async_trait]
impl Handler for LogHandler {
    async fn handle(&self, msg: Message) -> Result<HandlerResult, HandlerError> {
        self.log.lock().unwrap().push(self.label.clone());
        self.next.handle(msg).await
    }
}
