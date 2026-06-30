use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use uuid::Uuid;

use crate::error::{HandlerError, PublishError, SubscribeError};
use crate::handler::HandlerResult;
use crate::message::Message;
use crate::publisher::Publisher;
use crate::router::Router;
use crate::subscriber::Subscriber;
use crate::topic::Topic;

const CORRELATION_ID: &str = "correlation-id";
const REPLY_TO: &str = "reply-to";

/// Errors returned by [`RequestReply::request`].
#[derive(Debug, thiserror::Error)]
pub enum RequestReplyError {
    #[error("request timed out waiting for a reply")]
    Timeout,
    #[error("publishing the request failed: {0}")]
    Publish(#[from] PublishError),
    #[error("the reply listener closed before a reply arrived")]
    Closed,
}

type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<Message>>>>;

/// A request-reply client over pub/sub. Each request is tagged with a correlation id and a
/// reply-to topic; a single background listener on the reply topic routes replies back to the
/// waiting caller. Pair it with [`RequestReply::respond`] on the responder side.
pub struct RequestReply {
    publisher: Arc<dyn Publisher>,
    reply_topic: Topic,
    pending: Pending,
    listener: JoinHandle<()>,
}

impl Drop for RequestReply {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

impl RequestReply {
    /// Subscribe to `reply_topic` and start routing replies. The publisher is used to send
    /// requests; the subscriber is consumed only to open the reply subscription.
    pub async fn new(
        publisher: Arc<dyn Publisher>,
        subscriber: &dyn Subscriber,
        reply_topic: Topic,
    ) -> Result<Self, SubscribeError> {
        let mut stream = subscriber.subscribe(&reply_topic).await?;
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));

        let pending_listener = pending.clone();
        let listener = tokio::spawn(async move {
            while let Some(reply) = stream.next().await {
                if let Some(id) = reply.metadata().get(CORRELATION_ID).map(str::to_string) {
                    let waiter = pending_listener.lock().unwrap().remove(&id);
                    if let Some(tx) = waiter {
                        let _ = tx.send(reply.copy());
                    }
                }
                let _ = reply.ack();
            }
        });

        Ok(Self {
            publisher,
            reply_topic,
            pending,
            listener,
        })
    }

    /// Publish `message` to `topic` and await its reply, up to `timeout`.
    pub async fn request(
        &self,
        topic: &Topic,
        mut message: Message,
        timeout: Duration,
    ) -> Result<Message, RequestReplyError> {
        let correlation_id = Uuid::new_v4().to_string();
        message.metadata_mut().set(CORRELATION_ID, &correlation_id);
        message
            .metadata_mut()
            .set(REPLY_TO, self.reply_topic.as_str());

        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .unwrap()
            .insert(correlation_id.clone(), tx);

        if let Err(error) = self.publisher.publish(topic, vec![message]).await {
            self.pending.lock().unwrap().remove(&correlation_id);
            return Err(RequestReplyError::Publish(error));
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(_)) => Err(RequestReplyError::Closed),
            Err(_) => {
                self.pending.lock().unwrap().remove(&correlation_id);
                Err(RequestReplyError::Timeout)
            }
        }
    }

    /// Register a responder on `router`: it consumes requests on `request_topic`, runs
    /// `responder` to produce a reply payload, and publishes the reply to the request's
    /// reply-to topic carrying the same correlation id.
    pub fn respond<F, Fut>(
        router: &mut Router,
        name: impl Into<String>,
        request_topic: Topic,
        subscriber: impl Subscriber + 'static,
        publisher: Arc<dyn Publisher>,
        responder: F,
    ) where
        F: Fn(Message) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Bytes, HandlerError>> + Send,
    {
        let responder = Arc::new(responder);

        router.add_consumer(name, request_topic, subscriber, move |request: Message| {
            let publisher = publisher.clone();
            let responder = responder.clone();
            async move {
                let correlation_id = request.metadata().get(CORRELATION_ID).map(str::to_string);
                let reply_to = request.metadata().get(REPLY_TO).map(str::to_string);

                let payload = (*responder)(request.copy()).await?;

                if let (Some(correlation_id), Some(reply_to)) = (correlation_id, reply_to) {
                    let mut reply = Message::new(payload);
                    reply.metadata_mut().set(CORRELATION_ID, correlation_id);
                    if let Err(error) = publisher.publish(&Topic::new(reply_to), vec![reply]).await
                    {
                        return Err(HandlerError::Processing(Box::new(error)));
                    }
                }

                Ok(HandlerResult::ack(request))
            }
        });
    }
}
