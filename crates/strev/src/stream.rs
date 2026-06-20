use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};

use crate::message::{Message, Pending};

pub struct MessageStream {
    inner: ReceiverStream<Message<Pending>>,
}

impl MessageStream {
    pub fn channel(buffer: usize) -> (mpsc::Sender<Message<Pending>>, Self) {
        let (tx, rx) = mpsc::channel(buffer);
        (
            tx,
            Self {
                inner: ReceiverStream::new(rx),
            },
        )
    }
}

impl Stream for MessageStream {
    type Item = Message<Pending>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

pub async fn bulk_read(
    stream: &mut MessageStream,
    limit: usize,
    timeout: Duration,
) -> Vec<Message<Pending>> {
    let mut messages = Vec::with_capacity(limit);
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);

    loop {
        if messages.len() >= limit {
            break;
        }
        tokio::select! {
            _ = &mut deadline => break,
            msg = stream.next() => {
                match msg {
                    Some(m) => messages.push(m),
                    None => break,
                }
            }
        }
    }

    messages
}
