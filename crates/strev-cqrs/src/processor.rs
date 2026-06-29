use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use strev::{Handler, HandlerError, HandlerResult, Message, Router, Subscriber, Topic};

use crate::{Command, Context, CqrsError, Event, NAME_KEY};

/// Builds a group-scoped subscriber for a handler. The argument is the handler name,
/// which is used as the consumer group so each handler consumes independently (giving
/// events their fan-out).
pub type SubscriberFactory = Arc<dyn Fn(&str) -> Box<dyn Subscriber> + Send + Sync>;

type TopicFn = Arc<dyn Fn(&str) -> Topic + Send + Sync>;

fn topic_per_name() -> TopicFn {
    Arc::new(|name| Topic::new(name))
}

struct Registration {
    handler_name: String,
    topic: Topic,
    handler: Box<dyn Handler>,
}

fn register_all(
    factory: &SubscriberFactory,
    registrations: Vec<Registration>,
    router: &mut Router,
) {
    for registration in registrations {
        let subscriber = factory(&registration.handler_name);
        router.add_consumer(
            registration.handler_name,
            registration.topic,
            subscriber,
            registration.handler,
        );
    }
}

struct TypedHandler<T, F> {
    name: &'static str,
    handler: F,
    ack_on_error: bool,
    _marker: PhantomData<fn() -> T>,
}

#[async_trait]
impl<T, F, Fut> Handler for TypedHandler<T, F>
where
    T: DeserializeOwned + Send + 'static,
    F: Fn(T, Context) -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), HandlerError>> + Send,
{
    async fn handle(&self, message: Message) -> Result<HandlerResult, HandlerError> {
        if message.metadata().get(NAME_KEY) != Some(self.name) {
            return Ok(HandlerResult::ack(message));
        }
        let decoded: T = match serde_json::from_slice(message.payload()) {
            Ok(value) => value,
            Err(_) => return Ok(HandlerResult::ack(message)),
        };
        let context = Context::new(*message.uuid());
        match (self.handler)(decoded, context).await {
            Ok(()) => Ok(HandlerResult::ack(message)),
            Err(_) if self.ack_on_error => Ok(HandlerResult::ack(message)),
            Err(error) => Err(error),
        }
    }
}

/// Dispatches commands to their single registered handler.
pub struct CommandProcessor {
    factory: SubscriberFactory,
    topic: TopicFn,
    ack_on_error: bool,
    registrations: Vec<Registration>,
    registered: HashSet<&'static str>,
}

impl CommandProcessor {
    pub fn new(factory: SubscriberFactory) -> Self {
        Self {
            factory,
            topic: topic_per_name(),
            ack_on_error: false,
            registrations: Vec::new(),
            registered: HashSet::new(),
        }
    }

    pub fn with_topic(mut self, topic: impl Fn(&str) -> Topic + Send + Sync + 'static) -> Self {
        self.topic = Arc::new(topic);
        self
    }

    /// Ack instead of erroring when a handler returns an error.
    pub fn ack_on_error(mut self, ack_on_error: bool) -> Self {
        self.ack_on_error = ack_on_error;
        self
    }

    pub fn add_handler<C, F, Fut>(
        &mut self,
        handler_name: impl Into<String>,
        handler: F,
    ) -> Result<&mut Self, CqrsError>
    where
        C: Command,
        F: Fn(C, Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
    {
        if !self.registered.insert(C::NAME) {
            return Err(CqrsError::DuplicateCommandHandler(C::NAME));
        }
        self.registrations.push(Registration {
            handler_name: handler_name.into(),
            topic: (self.topic)(C::NAME),
            handler: Box::new(TypedHandler {
                name: C::NAME,
                handler,
                ack_on_error: self.ack_on_error,
                _marker: PhantomData,
            }),
        });
        Ok(self)
    }

    pub fn register(self, router: &mut Router) {
        register_all(&self.factory, self.registrations, router);
    }
}

/// Dispatches events to every registered handler (fan-out).
pub struct EventProcessor {
    factory: SubscriberFactory,
    topic: TopicFn,
    ack_on_error: bool,
    registrations: Vec<Registration>,
}

impl EventProcessor {
    pub fn new(factory: SubscriberFactory) -> Self {
        Self {
            factory,
            topic: topic_per_name(),
            ack_on_error: false,
            registrations: Vec::new(),
        }
    }

    pub fn with_topic(mut self, topic: impl Fn(&str) -> Topic + Send + Sync + 'static) -> Self {
        self.topic = Arc::new(topic);
        self
    }

    /// Ack instead of erroring when a handler returns an error.
    pub fn ack_on_error(mut self, ack_on_error: bool) -> Self {
        self.ack_on_error = ack_on_error;
        self
    }

    pub fn add_handler<E, F, Fut>(
        &mut self,
        handler_name: impl Into<String>,
        handler: F,
    ) -> &mut Self
    where
        E: Event,
        F: Fn(E, Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
    {
        self.registrations.push(Registration {
            handler_name: handler_name.into(),
            topic: (self.topic)(E::NAME),
            handler: Box::new(TypedHandler {
                name: E::NAME,
                handler,
                ack_on_error: self.ack_on_error,
                _marker: PhantomData,
            }),
        });
        self
    }

    pub fn register(self, router: &mut Router) {
        register_all(&self.factory, self.registrations, router);
    }
}
