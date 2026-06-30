use std::marker::PhantomData;

use bytes::Bytes;
use serde::de::DeserializeOwned;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::error::DeserializeError;
use crate::metadata::Metadata;
use crate::outcome::Outcome;

/// A consumer's verdict on a delivered message, signalled back to the subscriber that
/// leased it. A message dropped without an explicit verdict resolves to [`Disposition::Nack`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    Ack,
    Nack,
}

/// Resolves to the consumer's [`Disposition`] once a leased message is acked, nacked, or
/// dropped. Backends await this to decide whether to commit the ack or redeliver.
pub struct AckReceiver {
    inner: oneshot::Receiver<Disposition>,
}

impl AckReceiver {
    pub async fn recv(self) -> Disposition {
        self.inner.await.unwrap_or(Disposition::Nack)
    }
}

pub trait AckState: sealed::Sealed {}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Pending {}
    impl Sealed for super::Acked {}
    impl Sealed for super::Nacked {}
}

pub struct Pending;
pub struct Acked;
pub struct Nacked;

impl AckState for Pending {}
impl AckState for Acked {}
impl AckState for Nacked {}

#[must_use = "message must be acked or nacked"]
pub struct Message<S: AckState = Pending> {
    uuid: Uuid,
    metadata: Metadata,
    payload: Bytes,
    ack: Option<oneshot::Sender<Disposition>>,
    _state: PhantomData<S>,
}

impl Message<Pending> {
    pub fn new(payload: Bytes) -> Self {
        Self {
            uuid: Uuid::new_v4(),
            metadata: Metadata::new(),
            payload,
            ack: None,
            _state: PhantomData,
        }
    }

    pub fn with_metadata(payload: Bytes, metadata: Metadata) -> Self {
        Self {
            uuid: Uuid::new_v4(),
            metadata,
            payload,
            ack: None,
            _state: PhantomData,
        }
    }

    /// Attach an acknowledgement channel, turning this into a leased message. The returned
    /// [`AckReceiver`] resolves when the consumer acks or nacks it (or to
    /// [`Disposition::Nack`] if the message is dropped without a verdict). Backends use this
    /// to defer the transport ack until the handler has run.
    pub fn leased(mut self) -> (Self, AckReceiver) {
        let (tx, rx) = oneshot::channel();
        self.ack = Some(tx);
        (self, AckReceiver { inner: rx })
    }

    /// Remove the acknowledgement channel so the caller can resolve it itself. The router
    /// uses this to take ownership of the verdict before running the middleware chain, so a
    /// handler's own `ack`/`nack` cannot signal the transport prematurely (e.g. mid-retry).
    pub(crate) fn take_ack(&mut self) -> Option<oneshot::Sender<Disposition>> {
        self.ack.take()
    }

    pub fn ack(mut self) -> Outcome {
        if let Some(tx) = self.ack.take() {
            let _ = tx.send(Disposition::Ack);
        }
        Outcome::acked()
    }

    pub fn nack(mut self) -> Outcome {
        if let Some(tx) = self.ack.take() {
            let _ = tx.send(Disposition::Nack);
        }
        Outcome::nacked()
    }

    pub fn uuid(&self) -> &Uuid {
        &self.uuid
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub fn deserialize<T: DeserializeOwned>(&self) -> Result<T, DeserializeError> {
        Ok(serde_json::from_slice(&self.payload)?)
    }

    pub fn copy(&self) -> Self {
        Self {
            uuid: Uuid::new_v4(),
            metadata: self.metadata.clone(),
            payload: self.payload.clone(),
            ack: None,
            _state: PhantomData,
        }
    }

    pub fn try_deserialize<T: DeserializeOwned>(
        self,
    ) -> Result<(T, Self), (DeserializeError, Self)> {
        match serde_json::from_slice(&self.payload) {
            Ok(value) => Ok((value, self)),
            Err(e) => Err((DeserializeError::Json(e), self)),
        }
    }
}
