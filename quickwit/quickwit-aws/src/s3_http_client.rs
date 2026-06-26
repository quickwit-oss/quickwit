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

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use aws_smithy_runtime_api::client::http::{
    HttpConnector, HttpConnectorFuture, SharedHttpClient, SharedHttpConnector, http_client_fn,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_types::body::SdkBody;
use hickory_resolver::TokioAsyncResolver;
use hyper_util::client::legacy::connect::{HttpConnector as HyperHttpConnector, dns};
use hyper_util::rt::{TokioExecutor, TokioTimer};
use quickwit_metrics::{LazyCounter, lazy_counter};
use tower::Service;

/// HTTP/1.1 read buffer size for S3 responses: 2^19 = 512 KiB.
///
/// The hyper default is 8 KiB. A larger buffer reduces syscall overhead when
/// reading large objects from S3.
const S3_HTTP1_READ_BUF_SIZE: usize = 1 << 19;

static S3_TCP_CONNECTIONS_TOTAL: LazyCounter = lazy_counter!(
    name: "s3_tcp_connections_total",
    description: "number of new TCP connections established by the S3 HTTP client",
    subsystem: "storage",
);

/// Builds an HTTP client for S3 with:
/// - hickory DNS resolver with built-in TTL-based response caching
/// - 512 KiB HTTP/1.1 read buffer
/// - TCP connection counter metric (`storage_s3_tcp_connections_total`)
pub fn build_s3_http_client() -> SharedHttpClient {
    let hickory_resolver = TokioAsyncResolver::tokio_from_system_conf()
        .expect("failed to build hickory DNS resolver for S3 HTTP client");
    let dns_resolver = HickoryDnsResolver {
        inner: Arc::new(hickory_resolver),
    };

    let mut http_connector = HyperHttpConnector::new_with_resolver(dns_resolver);
    // Allow HTTPS URIs — TLS is handled by the outer HttpsConnector layer.
    http_connector.enforce_http(false);

    let counting_connector = CountingConnector {
        inner: http_connector,
    };

    let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .expect("failed to load native TLS roots for S3 HTTP client")
        .https_or_http()
        .enable_http1()
        .wrap_connector(counting_connector);

    let mut builder = hyper_util::client::legacy::Builder::new(TokioExecutor::new());
    builder.pool_timer(TokioTimer::new());
    builder.http1_read_buf_exact_size(S3_HTTP1_READ_BUF_SIZE);
    let hyper_client = builder.build(https_connector);
    let connector = S3Connector {
        client: hyper_client,
    };
    http_client_fn(move |_, _| SharedHttpConnector::new(connector.clone()))
}

/// Hickory-based async DNS resolver with built-in TTL caching.
///
/// Replaces hyper's default `GaiResolver` (blocking system call) with an
/// async resolver that respects DNS TTLs and caches responses.
#[derive(Clone)]
struct HickoryDnsResolver {
    inner: Arc<TokioAsyncResolver>,
}

impl Service<dns::Name> for HickoryDnsResolver {
    type Response = std::vec::IntoIter<SocketAddr>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, name: dns::Name) -> Self::Future {
        let resolver = self.inner.clone();
        Box::pin(async move {
            let lookup = resolver
                .lookup_ip(name.as_str())
                .await
                .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)?;
            let addrs: Vec<SocketAddr> = lookup.iter().map(|ip| SocketAddr::new(ip, 0)).collect();
            Ok(addrs.into_iter())
        })
    }
}

/// Wraps an inner connector and increments `S3_TCP_CONNECTIONS_TOTAL` each
/// time a new TCP connection is established.
#[derive(Clone, Debug)]
struct CountingConnector<C> {
    inner: C,
}

impl<C, T> Service<T> for CountingConnector<C>
where C: Service<T>
{
    type Response = C::Response;
    type Error = C::Error;
    type Future = C::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, target: T) -> Self::Future {
        S3_TCP_CONNECTIONS_TOTAL.inc();
        self.inner.call(target)
    }
}

#[derive(Clone, Debug)]
struct S3Connector {
    client: hyper_util::client::legacy::Client<
        hyper_rustls::HttpsConnector<CountingConnector<HyperHttpConnector<HickoryDnsResolver>>>,
        SdkBody,
    >,
}

impl HttpConnector for S3Connector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let request = match request.try_into_http1x() {
            Ok(req) => req,
            Err(err) => {
                return HttpConnectorFuture::ready(Err(ConnectorError::user(err.into())));
            }
        };
        let fut = self.client.request(request);
        HttpConnectorFuture::new(async move {
            let response = fut
                .await
                .map_err(|err| ConnectorError::other(err.into(), None))?
                .map(SdkBody::from_body_1_x);
            HttpResponse::try_from(response).map_err(|err| ConnectorError::other(err.into(), None))
        })
    }
}
