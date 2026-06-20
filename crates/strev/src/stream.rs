use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;

use crate::message::{Message, Pending};

pub struct MessageStream {
    inner: ReceiverStream<Message<Pending>>,
}

impl MessageStream {
    pub fn channel(buffer: usize) -> (mpsc::Sender<Message<Pending>>, Self) {
        let (tx, rx) = mpsc::channel(buffer);
        (tx, Self { inner: ReceiverStream::new(rx) })
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
