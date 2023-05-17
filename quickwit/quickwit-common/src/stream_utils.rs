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

use std::pin::Pin;

use futures::{Stream, TryStreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};

pub type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send + Unpin + 'static>>;

/// A stream impl for code-generated services with streaming endpoints.
pub struct ServiceStream<T, E> {
    inner: BoxStream<Result<T, E>>,
}

impl<T, E> Unpin for ServiceStream<T, E> {}

impl<T, E> ServiceStream<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
{
    pub fn new_bounded(capacity: usize) -> (mpsc::Sender<Result<T, E>>, Self) {
        let (sender, receiver) = mpsc::channel(capacity);
        (sender, receiver.into())
    }

    pub fn new_unbounded() -> (mpsc::UnboundedSender<Result<T, E>>, Self) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (sender, receiver.into())
    }

    pub fn map_err<F, U>(self, f: F) -> ServiceStream<T, U>
    where
        F: FnMut(E) -> U + Send + 'static,
        U: Send + 'static,
    {
        ServiceStream {
            inner: Box::pin(self.inner.map_err(f)),
        }
    }
}

impl<T, E> Stream for ServiceStream<T, E> {
    type Item = Result<T, E>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl<T, E> From<mpsc::Receiver<Result<T, E>>> for ServiceStream<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
{
    fn from(receiver: mpsc::Receiver<Result<T, E>>) -> Self {
        Self {
            inner: Box::pin(ReceiverStream::new(receiver)),
        }
    }
}

impl<T, E> From<mpsc::UnboundedReceiver<Result<T, E>>> for ServiceStream<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
{
    fn from(receiver: mpsc::UnboundedReceiver<Result<T, E>>) -> Self {
        Self {
            inner: Box::pin(UnboundedReceiverStream::new(receiver)),
        }
    }
}

impl<T> From<tonic::Streaming<T>> for ServiceStream<T, tonic::Status>
where T: Send + 'static
{
    fn from(streaming: tonic::Streaming<T>) -> Self {
        Self {
            inner: Box::pin(streaming),
        }
    }
}
