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

use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt;
use std::hash::Hash;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::stream::once;
use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::channel::ClientTlsConfig;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::balance::p2c::Balance;
use tower::buffer::Buffer;
use tower::discover::Change as TowerChange;
use tower::load::{CompleteOnResponse, PendingRequestsDiscover};
use tower::{BoxError, Service, ServiceExt};

use super::{BoxFuture, Change};
use crate::BoxStream;

// Transforms a boxed stream of `Change<K, Channel>` into a stream of `Result<TowerChange<K,
// Channel>, Infallible>>` while keeping track of the number of connections.
struct ChangeStreamAdapter<K> {
    changes: BoxStream<Change<K, Channel>>,
    connection_keys_tx: watch::Sender<HashSet<K>>,
    keys: HashSet<K>,
}

// A blanket `Discover` implementation exists for any `Stream<Item = Result<Change<K, V>, E>>`
impl<K> Stream for ChangeStreamAdapter<K>
where K: Hash + Eq + Clone
{
    type Item = Result<TowerChange<K, Channel>, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut *self.changes).poll_next(cx) {
            Poll::Pending | Poll::Ready(None) => Poll::Pending,
            Poll::Ready(Some(change)) => match change {
                Change::Insert(key, channel) => {
                    if self.keys.insert(key.clone()) {
                        self.connection_keys_tx.send_modify(|connection_keys| {
                            connection_keys.insert(key.clone());
                        });
                    }
                    Poll::Ready(Some(Ok(TowerChange::Insert(key, channel))))
                }
                Change::Remove(key) => {
                    if self.keys.remove(&key) {
                        self.connection_keys_tx.send_modify(|connection_keys| {
                            connection_keys.remove(&key);
                        });
                    }
                    Poll::Ready(Some(Ok(TowerChange::Remove(key))))
                }
            },
        }
    }
}

impl<K> Unpin for ChangeStreamAdapter<K> where K: Hash + Eq + Clone {}

type HttpRequest = http::Request<tonic::body::Body>;
type HttpResponse = http::Response<tonic::body::Body>;
type ChangeStream<K> = UnboundedReceiverStream<Result<TowerChange<K, Channel>, Infallible>>;
type Discover<K> = PendingRequestsDiscover<ChangeStream<K>, CompleteOnResponse>;
type ChannelImpl<K> =
    Buffer<HttpRequest, <Balance<Discover<K>, HttpRequest> as Service<HttpRequest>>::Future>;

#[derive(Clone)]
pub struct BalanceChannel<K: Hash + Eq + Clone + Send> {
    inner: ChannelImpl<K>,
    connection_keys_rx: watch::Receiver<HashSet<K>>,
}

impl<K> BalanceChannel<K>
where K: Hash + Eq + Send + Sync + Clone + 'static
{
    pub fn new() -> (Self, mpsc::UnboundedSender<Change<K, Channel>>) {
        let (change_tx, change_rx) = mpsc::unbounded_channel();
        let changes = UnboundedReceiverStream::new(change_rx);
        let channel = Self::from_stream(changes);
        (channel, change_tx)
    }

    pub fn from_channel(key: K, channel: Channel) -> Self {
        Self::from_stream(once(Box::pin(async { Change::Insert(key, channel) })))
    }

    pub fn from_stream<S>(changes: S) -> Self
    where S: Stream<Item = Change<K, Channel>> + Send + Unpin + 'static {
        let (connection_keys_tx, connection_keys_rx) = watch::channel(HashSet::new());
        let change_stream = unlazy_stream(ChangeStreamAdapter::<K> {
            changes: Box::pin(changes),
            connection_keys_tx,
            keys: HashSet::new(),
        });
        let completion = CompleteOnResponse::default();
        let pending_requests_discover = PendingRequestsDiscover::new(change_stream, completion);
        let balance_svc = Balance::new(pending_requests_discover);
        let buffer_svc = Buffer::new(balance_svc, 512);

        BalanceChannel {
            inner: buffer_svc,
            connection_keys_rx,
        }
    }

    pub fn num_connections(&self) -> usize {
        self.connection_keys_rx.borrow().len()
    }

    pub fn connection_keys_watcher(&self) -> watch::Receiver<HashSet<K>> {
        self.connection_keys_rx.clone()
    }

    pub async fn wait_for(
        &self,
        timeout_after: Duration,
        predicate: impl Fn(&HashSet<K>) -> bool,
    ) -> bool {
        tokio::time::timeout(
            timeout_after,
            self.connection_keys_watcher().wait_for(predicate),
        )
        .await
        .is_ok()
    }
}

/// `tower::buffer::Buffer` and `tower::balance::Balance` lazily polls their inner services. As a
/// result, the underlying discover stream is only polled when requests are made to the
/// `BalanceChannel`. When the channel is idle, the pool of connections is not updated and
/// `num_connections` can be inaccurate. Since this number is used to determine whether a service is
/// ready or not, we must poll the stream eagerly to always supply an up-to-date value.
fn unlazy_stream<S, T>(mut inner_stream: S) -> UnboundedReceiverStream<T>
where
    T: Send + 'static,
    S: Stream<Item = T> + Send + Unpin + 'static,
{
    let (outer_stream_tx, outer_stream_rx) = mpsc::unbounded_channel();
    let future = async move {
        while let Some(item) = inner_stream.next().await {
            if outer_stream_tx.send(item).is_err() {
                break;
            }
        }
    };
    tokio::spawn(future);
    UnboundedReceiverStream::new(outer_stream_rx)
}

impl<K> Service<HttpRequest> for BalanceChannel<K>
where K: Hash + Eq + Clone + Send
{
    type Response = HttpResponse;
    type Error = BoxError;
    type Future = BoxFuture<HttpResponse, BoxError>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: HttpRequest) -> Self::Future {
        Box::pin(self.inner.call(request))
    }
}

impl<K> fmt::Debug for BalanceChannel<K>
where K: Hash + Eq + Clone + Send + Sync + 'static
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BalanceChannel")
            .field("num_connections", &self.num_connections())
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeepAliveConfig {
    pub interval: Duration,
    pub timeout: Duration,
}

#[derive(Clone, Default)]
pub struct ClientGrpcConfig {
    pub keep_alive_opt: Option<KeepAliveConfig>,
    pub tls_config_opt: Option<ClientTlsConfig>,
}

/// Creates a channel from a socket address.
///
/// The function is marked as `async` because it requires an executor (`connect_lazy`).
pub async fn make_channel(
    socket_addr: SocketAddr,
    client_grpc_config: ClientGrpcConfig,
) -> Channel {
    let ClientGrpcConfig {
        keep_alive_opt,
        tls_config_opt,
    } = client_grpc_config;
    let scheme = if tls_config_opt.is_some() {
        "https"
    } else {
        "http"
    };
    let uri = Uri::builder()
        .scheme(scheme)
        .authority(socket_addr.to_string())
        .path_and_query("/")
        .build()
        .expect("provided arguments should be valid");
    let mut endpoint = Endpoint::from(uri).connect_timeout(Duration::from_secs(5));
    if let Some(tls_config) = tls_config_opt {
        endpoint = endpoint.tls_config(tls_config).expect("sadness TODO");
    }
    if let Some(keep_alive) = keep_alive_opt {
        endpoint = endpoint
            .keep_alive_while_idle(true)
            .http2_keep_alive_interval(keep_alive.interval)
            .keep_alive_timeout(keep_alive.timeout);
    }
    endpoint.connect_lazy()
}

/// Forces a channel to initiate the underlying HTTP connection. Calling this function only makes
/// sense for channels connected lazily.
///
/// The function is marked as `async` because it requires a tokio runtime.
pub async fn warmup_channel(channel: Channel) {
    tokio::spawn(channel.ready_oneshot());
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use tonic::transport::Endpoint;
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn test_channel_discover() {
        let (change_tx, change_rx) = mpsc::unbounded_channel();
        let (connection_keys_tx, connection_keys_rx) = watch::channel(HashSet::new());

        let mut channel_discover = ChangeStreamAdapter::<&str> {
            changes: Box::pin(UnboundedReceiverStream::new(change_rx)),
            connection_keys_tx,
            keys: HashSet::new(),
        };
        assert!(connection_keys_rx.borrow().is_empty());

        let channel = Endpoint::from_static("http://[::1]:1212").connect_lazy();
        change_tx.send(Change::Insert("foo", channel)).unwrap();

        let change = channel_discover.next().await.unwrap().unwrap();
        assert!(matches!(change, TowerChange::Insert("foo", _)));
        assert_eq!(*connection_keys_rx.borrow(), HashSet::from_iter(["foo"]));

        let channel = Endpoint::from_static("http://[::1]:1337").connect_lazy();
        change_tx.send(Change::Insert("foo", channel)).unwrap();

        let change = channel_discover.next().await.unwrap().unwrap();
        assert!(matches!(change, TowerChange::Insert("foo", _)));
        assert_eq!(*connection_keys_rx.borrow(), HashSet::from_iter(["foo"]));

        change_tx.send(Change::Remove("bar")).unwrap();
        let change = channel_discover.next().await.unwrap().unwrap();

        assert!(matches!(change, TowerChange::Remove("bar")));
        assert_eq!(*connection_keys_rx.borrow(), HashSet::from_iter(["foo"]));

        change_tx.send(Change::Remove("foo")).unwrap();
        let change = channel_discover.next().await.unwrap().unwrap();

        assert!(matches!(change, TowerChange::Remove("foo")));
        assert!(connection_keys_rx.borrow().is_empty());
    }

    #[tokio::test]
    async fn test_balance_channel() {
        let (mut balance_channel, change_tx) = BalanceChannel::<&str>::new();
        let mut num_connections_watcher = balance_channel.connection_keys_watcher();
        assert_eq!(balance_channel.num_connections(), 0);

        let channel = Endpoint::from_static("http://[::1]:1212").connect_lazy();
        change_tx.send(Change::Insert("foo", channel)).unwrap();
        num_connections_watcher.changed().await.unwrap();
        assert_eq!(balance_channel.num_connections(), 1);

        change_tx.send(Change::Remove("foo")).unwrap();
        num_connections_watcher.changed().await.unwrap();
        assert_eq!(balance_channel.num_connections(), 0);

        // `ready()` is lying... See `unlazy_stream()` comment.
        balance_channel.ready().await.unwrap();

        // The rest of the test lives in the `quickwit-codegen-example` crate.
        // TODO: Move the test here.
    }
}
