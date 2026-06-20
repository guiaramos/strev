//! CloudEvents enveloping for strev.
//!
//! [`CloudEventsPublisherDecorator`] and [`CloudEventsSubscriberDecorator`] envelope and
//! unwrap messages as CloudEvents at the transport boundary, using a [`CloudEventCodec`]
//! that maps CloudEvent attributes to `ce-*` metadata. Register them once on the router
//! and they apply across every backend.
mod codec;
mod publisher;
mod subscriber;

pub use codec::{CloudEventCodec, CodecError};
pub use publisher::CloudEventsPublisherDecorator;
pub use subscriber::CloudEventsSubscriberDecorator;
