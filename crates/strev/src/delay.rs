use std::time::{Duration, SystemTime};

use async_trait::async_trait;

use crate::error::PublishError;
use crate::message::Message;
use crate::outcome::Outcome;
use crate::publisher::Publisher;
use crate::topic::Topic;

/// The earliest instant at which a delayed message may be delivered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Delay {
    not_before: SystemTime,
}

impl Delay {
    /// Deliver no earlier than `duration` from now.
    pub fn after(duration: Duration) -> Self {
        Self {
            not_before: SystemTime::now() + duration,
        }
    }

    /// Deliver no earlier than `instant`.
    pub fn until(instant: SystemTime) -> Self {
        Self {
            not_before: instant,
        }
    }

    /// The earliest instant at which delivery may occur.
    pub fn not_before(self) -> SystemTime {
        self.not_before
    }

    /// Time remaining until the message is due, or zero if it already is.
    pub fn remaining(self) -> Duration {
        self.not_before
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO)
    }
}

/// A [`Publisher`] that can withhold messages until a [`Delay`] elapses.
///
/// Delayed delivery is an opt-in capability: only backends that can actually enforce it
/// implement this trait. Calling [`publish_after`](DelayedPublisher::publish_after) on a
/// backend that cannot delay is therefore a compile error, not a silent no-op.
#[async_trait]
pub trait DelayedPublisher: Publisher {
    /// Publish messages that must not be delivered before `delay` elapses. The returned
    /// outcomes acknowledge that the backend accepted the messages for later delivery.
    async fn publish_after(
        &self,
        topic: &Topic,
        messages: Vec<Message>,
        delay: Delay,
    ) -> Result<Vec<Outcome>, PublishError>;
}
