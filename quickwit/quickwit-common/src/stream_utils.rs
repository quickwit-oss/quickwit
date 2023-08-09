// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::any::TypeId;
use std::fmt;
use std::pin::Pin;

use futures::{stream, Stream, TryStreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tracing::warn;

pub type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send + Unpin + 'static>>;

/// A stream impl for code-generated services with streaming endpoints.
pub struct ServiceStream<T> {
    inner: BoxStream<T>,
}

impl<T> fmt::Debug for ServiceStream<T>
where T: 'static
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ServiceStream<{:?}>", TypeId::of::<T>())
    }
}

impl<T> Unpin for ServiceStream<T> {}

impl<T> ServiceStream<T>
where T: Send + 'static
{
    pub fn new_bounded(capacity: usize) -> (mpsc::Sender<T>, Self) {
        let (sender, receiver) = mpsc::channel(capacity);
        (sender, receiver.into())
    }

    pub fn new_unbounded() -> (mpsc::UnboundedSender<T>, Self) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (sender, receiver.into())
    }
}

impl<T, E> ServiceStream<Result<T, E>>
where
    T: Send + 'static,
    E: Send + 'static,
{
    pub fn map_err<F, U>(self, f: F) -> ServiceStream<Result<T, U>>
    where
        F: FnMut(E) -> U + Send + 'static,
        U: Send + 'static,
    {
        ServiceStream {
            inner: Box::pin(self.inner.map_err(f)),
        }
    }
}

impl<T> Stream for ServiceStream<T> {
    type Item = T;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl<T> From<mpsc::Receiver<T>> for ServiceStream<T>
where T: Send + 'static
{
    fn from(receiver: mpsc::Receiver<T>) -> Self {
        Self {
            inner: Box::pin(ReceiverStream::new(receiver)),
        }
    }
}

impl<T> From<mpsc::UnboundedReceiver<T>> for ServiceStream<T>
where T: Send + 'static
{
    fn from(receiver: mpsc::UnboundedReceiver<T>) -> Self {
        Self {
            inner: Box::pin(UnboundedReceiverStream::new(receiver)),
        }
    }
}

/// Adapts a server-side tonic::Streaming into a ServiceStream of `Result<T, tonic::Status>`. Once
/// an error is encountered, the stream will be closed and subsequent calls to `poll_next` will
/// return `None`.
impl<T> From<tonic::Streaming<T>> for ServiceStream<Result<T, tonic::Status>>
where T: Send + 'static
{
    fn from(streaming: tonic::Streaming<T>) -> Self {
        Self {
            inner: Box::pin(streaming),
        }
    }
}

/// Adapts a client-side tonic::Streaming into a ServiceStream of `T`. Once an error is encountered,
/// the stream will be closed and subsequent calls to `poll_next` will return `None`.
impl<T> From<tonic::Streaming<T>> for ServiceStream<T>
where T: Send + 'static
{
    fn from(streaming: tonic::Streaming<T>) -> Self {
        let message_stream = stream::unfold(streaming, |mut streaming| {
            Box::pin(async {
                match streaming.message().await {
                    Ok(Some(message)) => Some((message, streaming)),
                    Ok(None) => None,
                    Err(error) => {
                        warn!(error=?error, "gRPC transport error.");
                        None
                    }
                }
            })
        });
        Self {
            inner: Box::pin(message_stream),
        }
    }
}
