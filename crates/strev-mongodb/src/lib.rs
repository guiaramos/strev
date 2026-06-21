//! MongoDB backend for strev.
//!
//! Provides [`MongoPublisher`] and [`MongoSubscriber`]. Publishing inserts messages into a
//! collection; subscribing opens a change stream filtered by topic and persists a resume
//! token per consumer group, so a subscriber resumes exactly where it left off after a
//! restart. Change streams require the server to run as a replica set.
mod publisher;
mod subscriber;

pub use publisher::{MongoPublisher, MongoPublisherConfig};
pub use subscriber::{MongoSubscriber, MongoSubscriberConfig};

const MESSAGES_COLLECTION: &str = "strev_messages";
const RESUME_TOKENS_COLLECTION: &str = "strev_resume_tokens";
const DEFAULT_DATABASE: &str = "strev";
