use std::sync::Arc;

use crate::error::HandlerError;
use crate::handler::HandlerResult;
use crate::message::Message;
use crate::publisher::Publisher;
use crate::router::Router;
use crate::subscriber::Subscriber;
use crate::topic::Topic;

/// Configuration for [`FanIn`].
pub struct FanInConfig {
    pub source_topics: Vec<Topic>,
    pub target_topic: Topic,
}

impl FanInConfig {
    pub fn new(source_topics: Vec<Topic>, target_topic: Topic) -> Self {
        Self {
            source_topics,
            target_topic,
        }
    }
}

/// Multiplexes several source topics onto one target topic: each message received on a
/// source topic is republished, untouched, to the target topic. A source equal to the
/// target is skipped to avoid an obvious loop.
pub struct FanIn;

impl FanIn {
    pub fn register(
        router: &mut Router,
        subscriber: Arc<dyn Subscriber>,
        publisher: Arc<dyn Publisher>,
        config: FanInConfig,
    ) {
        for source in config.source_topics {
            if source == config.target_topic {
                continue;
            }

            let name = format!("fan-in-{}", source.as_str());
            let publisher = publisher.clone();
            let target = config.target_topic.clone();

            router.add_consumer(name, source, subscriber.clone(), move |message: Message| {
                let publisher = publisher.clone();
                let target = target.clone();
                async move {
                    let forwarded = Message::with_metadata(
                        message.payload().clone(),
                        message.metadata().clone(),
                    );
                    match publisher.publish(&target, vec![forwarded]).await {
                        Ok(_) => Ok(HandlerResult::ack(message)),
                        Err(error) => Err(HandlerError::Processing(Box::new(error))),
                    }
                }
            });
        }
    }
}
