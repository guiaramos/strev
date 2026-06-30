use std::time::Duration;

use tokio::select;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::decorator::{PublisherDecorator, SubscriberDecorator};
use crate::error::RouterError;
use crate::handler::Handler;
use crate::message::{Disposition, Message, Pending};
use crate::middleware::Middleware;
use crate::publisher::Publisher;
use crate::subscriber::Subscriber;
use crate::topic::Topic;

pub enum ShutdownSignal {
    Token(CancellationToken),
    CtrlC,
}

pub struct RouterConfig {
    pub close_timeout: Duration,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            close_timeout: Duration::from_secs(30),
        }
    }
}

pub struct Router {
    config: RouterConfig,
    handlers: Vec<HandlerRegistration>,
    middlewares: Vec<Box<dyn Middleware>>,
    publisher_decorators: Vec<Box<dyn PublisherDecorator>>,
    subscriber_decorators: Vec<Box<dyn SubscriberDecorator>>,
}

struct HandlerRegistration {
    name: String,
    subscribe_topic: Topic,
    handler: Box<dyn Handler>,
    subscriber: Box<dyn Subscriber>,
    publisher: Option<Box<dyn Publisher>>,
    middlewares: Vec<Box<dyn Middleware>>,
}

pub struct HandlerBuilder<'r> {
    router: &'r mut Router,
    index: usize,
}

impl<'r> HandlerBuilder<'r> {
    pub fn with_middleware(self, middleware: impl Middleware + 'static) -> Self {
        self.router.handlers[self.index]
            .middlewares
            .push(Box::new(middleware));
        self
    }
}

impl Router {
    pub fn new() -> Self {
        Self::with_config(RouterConfig::default())
    }

    pub fn with_config(config: RouterConfig) -> Self {
        Self {
            config,
            handlers: Vec::new(),
            middlewares: Vec::new(),
            publisher_decorators: Vec::new(),
            subscriber_decorators: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    pub fn handler_names(&self) -> Vec<&str> {
        self.handlers.iter().map(|h| h.name.as_str()).collect()
    }

    pub fn add_middleware(&mut self, middleware: impl Middleware + 'static) -> &mut Self {
        self.middlewares.push(Box::new(middleware));
        self
    }

    pub fn add_publisher_decorator(
        &mut self,
        decorator: impl PublisherDecorator + 'static,
    ) -> &mut Self {
        self.publisher_decorators.push(Box::new(decorator));
        self
    }

    pub fn add_subscriber_decorator(
        &mut self,
        decorator: impl SubscriberDecorator + 'static,
    ) -> &mut Self {
        self.subscriber_decorators.push(Box::new(decorator));
        self
    }

    fn assert_unique_name(&self, name: &str) {
        assert!(
            !self.handlers.iter().any(|h| h.name == name),
            "duplicate handler name: {name}"
        );
    }

    pub fn add_handler(
        &mut self,
        name: impl Into<String>,
        subscribe_topic: Topic,
        subscriber: impl Subscriber + 'static,
        publisher: impl Publisher + 'static,
        handler: impl Handler + 'static,
    ) -> HandlerBuilder<'_> {
        let name = name.into();
        self.assert_unique_name(&name);
        let index = self.handlers.len();
        self.handlers.push(HandlerRegistration {
            name,
            subscribe_topic,
            handler: Box::new(handler),
            subscriber: Box::new(subscriber),
            publisher: Some(Box::new(publisher)),
            middlewares: Vec::new(),
        });
        HandlerBuilder {
            router: self,
            index,
        }
    }

    pub fn add_consumer(
        &mut self,
        name: impl Into<String>,
        subscribe_topic: Topic,
        subscriber: impl Subscriber + 'static,
        handler: impl Handler + 'static,
    ) -> HandlerBuilder<'_> {
        let name = name.into();
        self.assert_unique_name(&name);
        let index = self.handlers.len();
        self.handlers.push(HandlerRegistration {
            name,
            subscribe_topic,
            handler: Box::new(handler),
            subscriber: Box::new(subscriber),
            publisher: None,
            middlewares: Vec::new(),
        });
        HandlerBuilder {
            router: self,
            index,
        }
    }

    pub async fn run(self, shutdown: ShutdownSignal) -> Result<(), RouterError> {
        let token = match shutdown {
            ShutdownSignal::Token(t) => t,
            ShutdownSignal::CtrlC => {
                let t = CancellationToken::new();
                let t2 = t.clone();
                tokio::spawn(async move {
                    let _ = tokio::signal::ctrl_c().await;
                    t2.cancel();
                });
                t
            }
        };

        let Self {
            config,
            handlers,
            middlewares,
            publisher_decorators,
            subscriber_decorators,
        } = self;
        let mut tasks = Vec::new();

        for mut reg in handlers {
            for dec in &subscriber_decorators {
                reg.subscriber = dec.decorate(reg.subscriber);
            }
            if let Some(pub_) = reg.publisher.take() {
                let mut decorated = pub_;
                for dec in &publisher_decorators {
                    decorated = dec.decorate(decorated);
                }
                reg.publisher = Some(decorated);
            }

            let mut stream = reg
                .subscriber
                .subscribe(&reg.subscribe_topic)
                .await
                .map_err(|source| RouterError::Subscribe {
                    handler: reg.name.clone(),
                    source,
                })?;

            let handler = Self::build_handler_chain(reg.handler, &middlewares, reg.middlewares);

            let name = reg.name;
            let publisher = reg.publisher;
            let cancel = token.clone();

            tasks.push(tokio::spawn(async move {
                loop {
                    select! {
                        _ = cancel.cancelled() => break,
                        maybe_msg = StreamExt::next(&mut stream) => {
                            match maybe_msg {
                                Some(msg) => {
                                    Self::process_message(
                                        &name,
                                        &*handler,
                                        msg,
                                        publisher.as_deref(),
                                    ).await;
                                }
                                None => break,
                            }
                        }
                    }
                }
            }));
        }

        let close_timeout = config.close_timeout;
        token.cancelled().await;

        let shutdown_result =
            tokio::time::timeout(close_timeout, futures::future::join_all(tasks)).await;

        if shutdown_result.is_err() {
            error!("router close timeout exceeded");
        }

        Ok(())
    }

    fn build_handler_chain(
        handler: Box<dyn Handler>,
        router_middlewares: &[Box<dyn Middleware>],
        handler_middlewares: Vec<Box<dyn Middleware>>,
    ) -> Box<dyn Handler> {
        let mut h = handler;
        for mw in handler_middlewares.into_iter().rev() {
            h = mw.wrap(h);
        }
        for mw in router_middlewares.iter().rev() {
            h = mw.wrap(h);
        }
        h
    }

    async fn process_message(
        handler_name: &str,
        handler: &dyn Handler,
        mut msg: Message<Pending>,
        publisher: Option<&dyn Publisher>,
    ) {
        let responder = msg.take_ack();

        match handler.handle(msg).await {
            Ok(result) => {
                if let Some(tx) = responder {
                    let disposition = if result.outcome().is_acked() {
                        Disposition::Ack
                    } else {
                        Disposition::Nack
                    };
                    let _ = tx.send(disposition);
                }

                if !result.produced().is_empty()
                    && let Some(pub_) = publisher
                {
                    let mut by_topic: std::collections::HashMap<&Topic, Vec<Message<Pending>>> =
                        std::collections::HashMap::new();

                    for pm in result.produced() {
                        by_topic
                            .entry(&pm.topic)
                            .or_default()
                            .push(Message::with_metadata(
                                pm.payload.clone(),
                                pm.metadata.clone(),
                            ));
                    }

                    for (topic, messages) in by_topic {
                        if let Err(e) = pub_.publish(topic, messages).await {
                            error!(handler = handler_name, topic = %topic, error = %e, "failed to publish produced messages");
                        }
                    }
                }
            }
            Err(e) => {
                if let Some(tx) = responder {
                    let _ = tx.send(Disposition::Nack);
                }
                error!(handler = handler_name, error = %e, "handler error");
            }
        }
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}
