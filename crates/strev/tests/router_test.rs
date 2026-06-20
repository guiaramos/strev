use strev::{Handler, Middleware, Router};

struct NoopMiddleware;

impl Middleware for NoopMiddleware {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler> {
        next
    }
}

#[test]
fn router_new_creates_empty_router() {
    let router = Router::new();
    assert!(router.is_empty());
}

#[test]
fn router_add_middleware_returns_self() {
    let mut router = Router::new();
    router.add_middleware(NoopMiddleware);
}
