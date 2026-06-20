use std::marker::PhantomData;

use bytes::Bytes;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::error::DeserializeError;
use crate::metadata::Metadata;
use crate::outcome::Outcome;

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
    _state: PhantomData<S>,
}

impl Message<Pending> {
    pub fn new(payload: Bytes) -> Self {
        Self {
            uuid: Uuid::new_v4(),
            metadata: Metadata::new(),
            payload,
            _state: PhantomData,
        }
    }

    pub fn with_metadata(payload: Bytes, metadata: Metadata) -> Self {
        Self {
            uuid: Uuid::new_v4(),
            metadata,
            payload,
            _state: PhantomData,
        }
    }

    pub fn ack(self) -> Outcome {
        Outcome::Acked
    }

    pub fn nack(self) -> Outcome {
        Outcome::Nacked
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

    pub fn try_deserialize<T: DeserializeOwned>(self) -> Result<(T, Self), (DeserializeError, Self)> {
        match serde_json::from_slice(&self.payload) {
            Ok(value) => Ok((value, self)),
            Err(e) => Err((DeserializeError::Json(e), self)),
        }
    }
}
