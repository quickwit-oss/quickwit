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

//! HTTP/1.1-only HTTP client for the AWS SDK.
//!
//! The default connector of `aws-smithy-http-client` offers `h2` via ALPN. When the
//! server accepts it (Google Cloud Storage does), all the requests of the process are
//! multiplexed over a single TCP connection, capped by the server's
//! `max_concurrent_streams` (100 for GCS). For searchers downloading split data at
//! high concurrency, this caps throughput well below what the NIC allows.
//!
//! This client only offers HTTP/1.1 and relies on a pool of connections instead, which
//! is what Quickwit did before 0.9. It keeps the caching DNS resolver and the
//! `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` support of the default client.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use aws_smithy_runtime_api::client::dns::ResolveDns;
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpClient,
    SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::body::SdkBody;
use futures::future::BoxFuture;
use http::Uri;
use http::uri::Scheme;
use hyper::rt::{Read, ReadBufCursor, Write};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::dns::Name;
use hyper_util::client::legacy::connect::proxy::Tunnel;
use hyper_util::client::legacy::connect::{Connected, Connection, HttpConnector as TcpConnector};
use hyper_util::client::proxy::matcher::Matcher;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use tokio::net::TcpStream;
use tower::Service;

use crate::dns::CachingDnsResolver;

type BoxError = Box<dyn StdError + Send + Sync>;

/// Returns an AWS SDK HTTP client that only speaks HTTP/1.1.
pub(crate) fn build_http1_client() -> SharedHttpClient {
    SharedHttpClient::new(Http1Client::default())
}

type TimeoutsKey = (Option<Duration>, Option<Duration>);

#[derive(Debug, Default)]
struct Http1Client {
    connectors: Mutex<HashMap<TimeoutsKey, SharedHttpConnector>>,
}

impl HttpClient for Http1Client {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        _components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        let key = (settings.connect_timeout(), settings.read_timeout());
        let mut connectors = self.connectors.lock().unwrap();
        connectors
            .entry(key)
            .or_insert_with(|| SharedHttpConnector::new(Http1Connector::new(key.0, key.1)))
            .clone()
    }
}

struct Http1Connector {
    client: Client<ConnectTimeout<HttpsConnector<ProxyAwareConnector>>, SdkBody>,
    proxy_matcher: Arc<Matcher>,
    read_timeout: Option<Duration>,
}

impl std::fmt::Debug for Http1Connector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Http1Connector")
            .field("read_timeout", &self.read_timeout)
            .finish()
    }
}

impl Http1Connector {
    fn new(connect_timeout: Option<Duration>, read_timeout: Option<Duration>) -> Self {
        let mut tcp_connector = TcpConnector::new_with_resolver(DnsService::default());
        tcp_connector.enforce_http(false);
        tcp_connector.set_nodelay(true);
        let proxy_matcher = Arc::new(Matcher::from_env());
        let proxy_aware_connector = ProxyAwareConnector {
            tcp_connector,
            proxy_matcher: proxy_matcher.clone(),
        };
        let https_connector = HttpsConnectorBuilder::new()
            .with_provider_and_native_roots(rustls::crypto::aws_lc_rs::default_provider())
            .expect("failed to load native root certificates")
            .https_or_http()
            .enable_http1()
            .wrap_connector(proxy_aware_connector);
        let connector = ConnectTimeout {
            inner: https_connector,
            timeout_opt: connect_timeout,
        };
        let mut client_builder = Client::builder(TokioExecutor::new());
        // Without a pool timer, hyper-util does not evict idle connections on its own.
        client_builder.pool_timer(TokioTimer::new());
        let client = client_builder.build(connector);
        Http1Connector {
            client,
            proxy_matcher,
            read_timeout,
        }
    }

    /// Plain HTTP requests going through a forward proxy carry the proxy credentials
    /// themselves. HTTPS requests get them in the `CONNECT` request of the tunnel.
    fn add_proxy_auth_header(&self, request: &mut http::Request<SdkBody>) {
        if request.uri().scheme() != Some(&Scheme::HTTP)
            || request
                .headers()
                .contains_key(http::header::PROXY_AUTHORIZATION)
        {
            return;
        }
        if let Some(intercept) = self.proxy_matcher.intercept(request.uri())
            && let Some(auth_header) = intercept.basic_auth()
        {
            request
                .headers_mut()
                .insert(http::header::PROXY_AUTHORIZATION, auth_header.clone());
        }
    }
}

impl HttpConnector for Http1Connector {
    fn call(&self, smithy_request: HttpRequest) -> HttpConnectorFuture {
        let mut http_request = match smithy_request.try_into_http1x() {
            Ok(http_request) => http_request,
            Err(error) => {
                return HttpConnectorFuture::ready(Err(ConnectorError::user(error.into())));
            }
        };
        self.add_proxy_auth_header(&mut http_request);
        let response_future = self.client.request(http_request);
        let read_timeout_opt = self.read_timeout;
        HttpConnectorFuture::new(async move {
            let response_result = if let Some(read_timeout) = read_timeout_opt {
                tokio::time::timeout(read_timeout, response_future)
                    .await
                    .map_err(|elapsed| ConnectorError::timeout(elapsed.into()))?
            } else {
                response_future.await
            };
            let response = response_result.map_err(to_connector_error)?;
            HttpResponse::try_from(response.map(SdkBody::from_body_1_x))
                .map_err(|error| ConnectorError::other(error.into(), None))
        })
    }
}

fn to_connector_error(error: hyper_util::client::legacy::Error) -> ConnectorError {
    if find_source::<ConnectTimeoutError>(&error).is_some() {
        return ConnectorError::timeout(error.into());
    }
    if error.is_connect() || find_source::<std::io::Error>(&error).is_some() {
        return ConnectorError::io(error.into());
    }
    if let Some(hyper_error) = find_source::<hyper::Error>(&error)
        && (hyper_error.is_closed()
            || hyper_error.is_incomplete_message()
            || hyper_error.is_canceled())
    {
        return ConnectorError::io(error.into());
    }
    ConnectorError::other(error.into(), None)
}

/// Returns the first error of type `T` in the source chain of `error`, `error` included.
fn find_source<'a, T: StdError + 'static>(error: &'a (dyn StdError + 'static)) -> Option<&'a T> {
    let mut current_opt: Option<&(dyn StdError + 'static)> = Some(error);
    while let Some(current) = current_opt {
        if let Some(found) = current.downcast_ref::<T>() {
            return Some(found);
        }
        current_opt = current.source();
    }
    None
}

#[derive(Debug, thiserror::Error)]
#[error("connection establishment timed out after {0:?}")]
struct ConnectTimeoutError(Duration);

/// Applies the SDK connect timeout to the whole connection establishment: TCP connect,
/// proxy `CONNECT` tunnel and TLS handshake, like the default client of the AWS SDK.
#[derive(Clone)]
struct ConnectTimeout<C> {
    inner: C,
    timeout_opt: Option<Duration>,
}

impl<C> Service<Uri> for ConnectTimeout<C>
where
    C: Service<Uri> + Send + 'static,
    C::Response: Send + 'static,
    C::Future: Send + 'static,
    C::Error: Into<BoxError>,
{
    type Response = C::Response;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, dst: Uri) -> Self::Future {
        let connect_future = self.inner.call(dst);
        let Some(timeout) = self.timeout_opt else {
            return Box::pin(async move { connect_future.await.map_err(Into::into) });
        };
        Box::pin(async move {
            match tokio::time::timeout(timeout, connect_future).await {
                Ok(connect_result) => connect_result.map_err(Into::into),
                Err(_elapsed) => Err(Box::new(ConnectTimeoutError(timeout)) as BoxError),
            }
        })
    }
}

/// Adapts [`CachingDnsResolver`] to the resolver interface of hyper's connector.
#[derive(Clone, Default)]
struct DnsService {
    resolver: CachingDnsResolver,
}

impl Service<Name> for DnsService {
    type Response = std::vec::IntoIter<SocketAddr>;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, name: Name) -> Self::Future {
        let resolver = self.resolver.clone();
        Box::pin(async move {
            let ip_addrs = resolver.resolve_dns(name.as_str()).await?;
            let socket_addrs: Vec<SocketAddr> = ip_addrs
                .into_iter()
                .map(|ip_addr| SocketAddr::new(ip_addr, 0))
                .collect();
            Ok(socket_addrs.into_iter())
        })
    }
}

/// TCP connector that routes connections through the proxy configured in the
/// environment: a `CONNECT` tunnel for HTTPS destinations, a plain connection to the
/// proxy for HTTP destinations.
#[derive(Clone)]
struct ProxyAwareConnector {
    tcp_connector: TcpConnector<DnsService>,
    proxy_matcher: Arc<Matcher>,
}

impl Service<Uri> for ProxyAwareConnector {
    type Response = ProxyAwareStream;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.tcp_connector.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, dst: Uri) -> Self::Future {
        let mut tcp_connector = self.tcp_connector.clone();
        let Some(intercept) = self.proxy_matcher.intercept(&dst) else {
            return Box::pin(async move {
                let stream = tcp_connector.call(dst).await?;
                Ok(ProxyAwareStream::new(stream, false))
            });
        };
        let proxy_uri = intercept.uri().clone();
        if dst.scheme() == Some(&Scheme::HTTPS) {
            let mut tunnel = Tunnel::new(proxy_uri, tcp_connector);
            if let Some(auth_header) = intercept.basic_auth() {
                tunnel = tunnel.with_auth(auth_header.clone());
            }
            Box::pin(async move {
                let stream = tunnel.call(dst).await?;
                Ok(ProxyAwareStream::new(stream, false))
            })
        } else {
            Box::pin(async move {
                let stream = tcp_connector.call(proxy_uri).await?;
                Ok(ProxyAwareStream::new(stream, true))
            })
        }
    }
}

/// TCP stream that tells hyper whether it is connected to a forward proxy, so that
/// requests are sent in absolute form.
struct ProxyAwareStream {
    inner: TokioIo<TcpStream>,
    is_forward_proxy: bool,
}

impl ProxyAwareStream {
    fn new(inner: TokioIo<TcpStream>, is_forward_proxy: bool) -> Self {
        ProxyAwareStream {
            inner,
            is_forward_proxy,
        }
    }
}

impl Connection for ProxyAwareStream {
    fn connected(&self) -> Connected {
        self.inner.connected().proxy(self.is_forward_proxy)
    }
}

impl Read for ProxyAwareStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl Write for ProxyAwareStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use aws_smithy_runtime_api::client::http::HttpConnector;
    use http_body_util::BodyExt;
    use hyper::body::Incoming;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use tokio::net::TcpListener;

    use super::*;

    async fn spawn_http1_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let service = service_fn(|request: http::Request<Incoming>| async move {
                        let body = format!("{:?} {}", request.version(), request.uri());
                        Ok::<_, Infallible>(http::Response::new(body))
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn test_http1_connector_sends_http1_requests() {
        let addr = spawn_http1_server().await;
        let connector = Http1Connector::new(Some(Duration::from_secs(1)), None);
        let request = http::Request::builder()
            .uri(format!("http://{addr}/bucket/key"))
            .body(SdkBody::empty())
            .unwrap();
        let response = connector
            .call(HttpRequest::try_from(request).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"HTTP/1.1 /bucket/key");
    }

    #[tokio::test]
    async fn test_http1_connector_connect_timeout_covers_tls_handshake() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut silent_streams = Vec::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                silent_streams.push(stream);
            }
        });
        let connector = Http1Connector::new(Some(Duration::from_millis(200)), None);
        let request = http::Request::builder()
            .uri(format!("https://{addr}/"))
            .body(SdkBody::empty())
            .unwrap();
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            connector.call(HttpRequest::try_from(request).unwrap()),
        )
        .await
        .expect("the connect timeout should fire before the test timeout")
        .unwrap_err();
        assert!(error.is_timeout(), "{error:?}");
    }

    #[tokio::test]
    async fn test_http1_connector_connect_error_is_io() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let connector = Http1Connector::new(Some(Duration::from_secs(1)), None);
        let request = http::Request::builder()
            .uri(format!("http://{addr}/"))
            .body(SdkBody::empty())
            .unwrap();
        let error = connector
            .call(HttpRequest::try_from(request).unwrap())
            .await
            .unwrap_err();
        assert!(error.is_io(), "{error:?}");
    }
}
