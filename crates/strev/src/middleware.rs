use crate::handler::Handler;

pub trait Middleware: Send + Sync {
    fn wrap(&self, next: Box<dyn Handler>) -> Box<dyn Handler>;
}
