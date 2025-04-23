// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::TypeId;
use std::fmt;
use std::pin::Pin;

use bytesize::ByteSize;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use prometheus::IntGauge;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream, WatchStream};
use tracing::warn;

use crate::metrics::GaugeGuard;
use crate::tower::RpcName;

pub type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send + Unpin + 'static>>;

/// A stream impl for code-generated services with streaming endpoints.
pub struct ServiceStream<T> {
    inner: BoxStream<T>,
}

impl<T> ServiceStream<T>
where T: Send + 'static
{
    pub fn new(inner: BoxStream<T>) -> Self {
        Self { inner }
    }

    pub fn empty() -> Self {
        Self {
            inner: Box::pin(stream::empty()),
        }
    }

    pub fn map<F, U>(self, f: F) -> ServiceStream<U>
    where
        F: FnMut(T) -> U + Send + 'static,
        U: Send + 'static,
    {
        ServiceStream {
            inner: Box::pin(self.inner.map(f)),
        }
    }
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

    pub fn new_bounded_with_gauge(
        capacity: usize,
        gauge: &'static IntGauge,
    ) -> (TrackedSender<T>, Self) {
        let (sender, receiver) = mpsc::channel(capacity);
        let tracked_sender = TrackedSender { sender, gauge };
        let receiver_stream =
            ReceiverStream::new(receiver).map(|value: InFlightValue<T>| value.into_inner());
        let service_stream = Self {
            inner: Box::pin(receiver_stream),
        };
        (tracked_sender, service_stream)
    }

    pub fn new_unbounded() -> (mpsc::UnboundedSender<T>, Self) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (sender, receiver.into())
    }

    pub fn new_unbounded_with_gauge(gauge: &'static IntGauge) -> (TrackedUnboundedSender<T>, Self) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let tracked_sender = TrackedUnboundedSender { sender, gauge };
        let receiver_stream = UnboundedReceiverStream::new(receiver)
            .map(|value: InFlightValue<T>| value.into_inner());
        let service_stream = Self {
            inner: Box::pin(receiver_stream),
        };
        (tracked_sender, service_stream)
    }
}

impl<T> ServiceStream<T>
where T: Clone + Send + Sync + 'static
{
    pub fn new_watch(init: T) -> (watch::Sender<T>, Self) {
        let (sender, receiver) = watch::channel(init);
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

impl<T> From<watch::Receiver<T>> for ServiceStream<T>
where T: Clone + Send + Sync + 'static
{
    fn from(receiver: watch::Receiver<T>) -> Self {
        Self {
            inner: Box::pin(WatchStream::new(receiver)),
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
                        warn!(error=?error, "gRPC transport error");
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

#[cfg(any(test, feature = "testsuite"))]
impl<T> From<Vec<T>> for ServiceStream<T>
where T: Send + 'static
{
    fn from(values: Vec<T>) -> Self {
        Self {
            inner: Box::pin(stream::iter(values)),
        }
    }
}

impl<T> RpcName for ServiceStream<T>
where T: RpcName
{
    fn rpc_name() -> &'static str {
        T::rpc_name()
    }
}

pub struct InFlightValue<T>(T, #[allow(dead_code)] GaugeGuard<'static>);

impl<T> fmt::Debug for InFlightValue<T>
where T: fmt::Debug
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl<T> InFlightValue<T> {
    pub fn new(value: T, value_size: ByteSize, gauge: &'static IntGauge) -> Self {
        let mut gauge_guard = GaugeGuard::from_gauge(gauge);
        gauge_guard.add(value_size.as_u64() as i64);

        Self(value, gauge_guard)
    }

    pub fn into_inner(self) -> T {
        self.0
    }
}

pub struct TrackedSender<T> {
    sender: mpsc::Sender<InFlightValue<T>>,
    gauge: &'static IntGauge,
}

impl<T> TrackedSender<T> {
    pub async fn send(
        &self,
        value: T,
        value_size: ByteSize,
    ) -> Result<(), mpsc::error::SendError<T>> {
        self.sender
            .send(InFlightValue::new(value, value_size, self.gauge))
            .await
            .map_err(|send_error| mpsc::error::SendError(send_error.0.0))
    }
}

pub struct TrackedUnboundedSender<T> {
    sender: mpsc::UnboundedSender<InFlightValue<T>>,
    gauge: &'static IntGauge,
}

impl<T> TrackedUnboundedSender<T> {
    pub fn send(&self, value: T, value_size: ByteSize) -> Result<(), mpsc::error::SendError<T>> {
        self.sender
            .send(InFlightValue::new(value, value_size, self.gauge))
            .map_err(|send_error| mpsc::error::SendError(send_error.0.0))
    }
}

#[cfg(test)]
mod tests {
    use once_cell::sync::Lazy;

    use super::*;
    use crate::metrics::new_gauge;

    #[tokio::test]
    async fn test_service_stream_map() {
        let mapped_values = ServiceStream::from(vec![0, 1, 2, 3])
            .map(|x| x * 2)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(mapped_values, vec![0, 2, 4, 6]);
    }

    #[tokio::test]
    async fn test_tracked_service_stream_bounded() {
        static TEST_GAUGE: Lazy<IntGauge> =
            Lazy::new(|| new_gauge("common", "help", "test_tracked_service_stream_bounded", &[]));

        let (service_stream_tx, mut service_stream) =
            ServiceStream::new_bounded_with_gauge(3, &TEST_GAUGE);

        service_stream_tx.send(1, ByteSize(42)).await.unwrap();
        assert_eq!(TEST_GAUGE.get(), 42);

        service_stream_tx.send(2, ByteSize(1337)).await.unwrap();
        assert_eq!(TEST_GAUGE.get(), 1379);

        let value = service_stream.next().await.unwrap();
        assert_eq!(value, 1);
        assert_eq!(TEST_GAUGE.get(), 1337);
    }

    #[tokio::test]
    async fn test_tracked_service_stream_unbounded() {
        static TEST_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
            new_gauge(
                "common",
                "help",
                "test_tracked_service_stream_unbounded",
                &[],
            )
        });

        let (service_stream_tx, mut service_stream) =
            ServiceStream::new_unbounded_with_gauge(&TEST_GAUGE);

        service_stream_tx.send(1, ByteSize(42)).unwrap();
        assert_eq!(TEST_GAUGE.get(), 42);

        service_stream_tx.send(2, ByteSize(1337)).unwrap();
        assert_eq!(TEST_GAUGE.get(), 1379);

        let value = service_stream.next().await.unwrap();
        assert_eq!(value, 1);
        assert_eq!(TEST_GAUGE.get(), 1337);
    }
}
