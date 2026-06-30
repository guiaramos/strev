use std::sync::Arc;
use std::time::Duration;

use crate::error::HandlerError;
use crate::handler::HandlerResult;
use crate::message::Message;
use crate::publisher::Publisher;
use crate::router::Router;
use crate::subscriber::Subscriber;
use crate::topic::Topic;

const REQUEUE_RETRIES: &str = "requeue-retries";

type ResolverError = Box<dyn std::error::Error + Send + Sync>;
type DestinationResolver = Arc<dyn Fn(&Message) -> Result<Topic, ResolverError> + Send + Sync>;

/// Builds a [`Requeuer`]. A destination resolver is mandatory, so the only way to obtain a
/// `Requeuer` is to call [`RequeuerConfig::destination`]; an unconfigured requeuer is
/// unrepresentable.
pub struct RequeuerConfig {
    subscribe_topic: Topic,
    delay: Option<Duration>,
}

impl RequeuerConfig {
    pub fn new(subscribe_topic: impl Into<String>) -> Self {
        Self {
            subscribe_topic: Topic::new(subscribe_topic),
            delay: None,
        }
    }

    /// Wait this long before republishing each message, e.g. to pace retries off a
    /// dead-letter topic. Keep it small: it blocks the requeue consumer for its duration.
    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Set the resolver that picks each message's destination topic, finishing the
    /// configuration and producing a registrable [`Requeuer`]. The resolver may read the
    /// message (e.g. its metadata) or return a constant.
    pub fn destination<F>(self, resolver: F) -> Requeuer
    where
        F: Fn(&Message) -> Result<Topic, ResolverError> + Send + Sync + 'static,
    {
        Requeuer {
            subscribe_topic: self.subscribe_topic,
            delay: self.delay,
            destination: Arc::new(resolver),
        }
    }
}

/// Moves messages from one topic to another, recording how many times each has been
/// requeued in the `requeue-retries` metadata key (callers cap retries by inspecting it in
/// their resolver). The payload is republished untouched (zero-copy).
pub struct Requeuer {
    subscribe_topic: Topic,
    delay: Option<Duration>,
    destination: DestinationResolver,
}

impl Requeuer {
    /// Register the requeue consumer on `router`: for each message it resolves the
    /// destination, increments the retry count, and republishes via `publisher`.
    pub fn register(
        self,
        router: &mut Router,
        subscriber: impl Subscriber + 'static,
        publisher: Arc<dyn Publisher>,
    ) {
        let Requeuer {
            subscribe_topic,
            delay,
            destination,
        } = self;
        router.add_consumer(
            "requeuer",
            subscribe_topic,
            subscriber,
            move |message: Message| {
                let publisher = publisher.clone();
                let destination = destination.clone();
                async move {
                    if let Some(delay) = delay {
                        tokio::time::sleep(delay).await;
                    }

                    let topic = (*destination)(&message).map_err(HandlerError::Processing)?;

                    let retries = message
                        .metadata()
                        .get(REQUEUE_RETRIES)
                        .and_then(|value| value.parse::<u32>().ok())
                        .unwrap_or(0)
                        + 1;

                    let mut metadata = message.metadata().clone();
                    metadata.set(REQUEUE_RETRIES, retries.to_string());
                    let requeued = Message::with_metadata(message.payload().clone(), metadata);

                    match publisher.publish(&topic, vec![requeued]).await {
                        Ok(_) => Ok(HandlerResult::ack(message)),
                        Err(error) => Err(HandlerError::Processing(Box::new(error))),
                    }
                }
            },
        );
    }
}
