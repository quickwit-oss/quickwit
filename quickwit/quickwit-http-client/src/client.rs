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

use std::sync::Arc;
use std::time::Duration;

use http_body::Body;
use tokio_rustls::TlsConnector;

use crate::body::{BufferHint, ResponseBody};
use crate::connection::{ConnStream, connect};
use crate::dns::DnsResolver;
use crate::endpoint::Endpoint;
use crate::error::HttpError;
use crate::exchange::exchange;
use crate::pool::ConnectionPool;
use crate::request::WriteState;

/// Default connect timeout.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Default per-write timeout for the request side.
pub const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(10);

struct HttpClientInner {
    pool: ConnectionPool,
    dns: Arc<dyn DnsResolver>,
    tls_connector: Option<TlsConnector>,
    tls_config: Option<Arc<rustls::ClientConfig>>,
    connect_timeout: Duration,
    read_timeout: Duration,
    write_timeout: Duration,
    buffer_hint: BufferHint,
}

/// A streaming HTTP/1.1 client.
#[derive(Clone)]
pub struct HttpClient {
    inner: Arc<HttpClientInner>,
}

impl std::fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpClient")
            .field("connect_timeout", &self.inner.connect_timeout)
            .field("read_timeout", &self.inner.read_timeout)
            .field("write_timeout", &self.inner.write_timeout)
            .field("buffer_hint", &self.inner.buffer_hint)
            .finish_non_exhaustive()
    }
}

impl HttpClient {
    /// Returns a handle to the shared connection pool.
    pub fn pool(&self) -> ConnectionPool {
        self.inner.pool.clone()
    }

    /// Returns the TLS client config, if one was configured.
    pub fn tls_config(&self) -> Option<Arc<rustls::ClientConfig>> {
        self.inner.tls_config.clone()
    }

    /// Returns the default `BufferHint` configured on this HttpClient.
    pub fn buffer_hint(&self) -> BufferHint {
        self.inner.buffer_hint
    }

    /// Returns the DNS resolver configured on this `HttpClient`.
    pub fn dns_resolver(&self) -> Arc<dyn DnsResolver> {
        self.inner.dns.clone()
    }

    /// Performs one request/response exchange and returns the streaming
    /// response.
    pub async fn execute<B>(
        &self,
        mut request: http::Request<B>,
    ) -> Result<http::Response<ResponseBody<ConnStream>>, HttpError>
    where
        B: Body + Unpin,
        B::Error: Into<HttpError>,
    {
        let endpoint = Endpoint::from_uri(request.uri())?;
        derive_host(&mut request);
        let method = request.method().clone();
        let buffer_hint = request
            .extensions()
            .get::<BufferHint>()
            .copied()
            .unwrap_or(self.inner.buffer_hint);
        let pool_hook = Some((self.inner.pool.clone(), endpoint.clone()));

        let (conn, was_reused) = match self.inner.pool.acquire(&endpoint) {
            Some(conn) => (conn, true),
            None => (self.connect(&endpoint).await?, false),
        };

        let mut write_state = WriteState::default();
        match exchange(
            conn,
            &mut request,
            buffer_hint,
            pool_hook.clone(),
            &mut write_state,
            self.inner.read_timeout,
            self.inner.write_timeout,
        )
        .await
        {
            Ok(response) => Ok(response),
            Err(error) => {
                // If on a pooled connection and we failed early enought, it might
                // just mean the connection was dead: retry the query on a fresh
                // connection (but only if doing so is safe)
                if was_reused && retry_is_safe(&method, &write_state) {
                    let conn = self.connect(&endpoint).await?;
                    let mut retry_state = WriteState::default();
                    exchange(
                        conn,
                        &mut request,
                        buffer_hint,
                        pool_hook,
                        &mut retry_state,
                        self.inner.read_timeout,
                        self.inner.write_timeout,
                    )
                    .await
                } else {
                    Err(error)
                }
            }
        }
    }

    async fn connect(&self, endpoint: &Endpoint) -> Result<ConnStream, HttpError> {
        connect(
            self.inner.dns.as_ref(),
            endpoint,
            self.inner.tls_connector.as_ref(),
            self.inner.connect_timeout,
        )
        .await
    }
}

/// Inserts a Host header if none is present.
fn derive_host<B>(request: &mut http::Request<B>) {
    use http::header::HOST;
    if request.headers().contains_key(HOST) {
        return;
    }
    let Some(authority) = request.uri().authority() else {
        return;
    };
    if let Ok(value) = authority.as_str().parse::<http::HeaderValue>() {
        request.headers_mut().insert(HOST, value);
    }
}

/// Whether a dead-connection retry is safe after `exchange` failed, given how
/// far `write_request` got.
///
/// If body was touched, we cannot replay (we'd me missing part of the body).
/// If not all head is sent, we can replay (the server didn't receive a full request).
/// If all head was sent, it depends on the method
fn retry_is_safe(method: &http::Method, state: &WriteState) -> bool {
    !state.body_touched && (!state.head_sent || is_idempotent(method))
}

/// Methods for which a full replay is safe even after the first attempt was
/// completely sent (the server may already have processed it).
fn is_idempotent(method: &http::Method) -> bool {
    matches!(
        *method,
        http::Method::GET | http::Method::HEAD | http::Method::OPTIONS
    )
}

/// Builder for [`HttpClient`].
pub struct HttpClientBuilder {
    dns: Arc<dyn DnsResolver>,
    tls_config: Option<Arc<rustls::ClientConfig>>,
    connect_timeout: Duration,
    read_timeout: Duration,
    write_timeout: Duration,
    max_idle_per_host: usize,
    idle_timeout: Duration,
    buffer_hint: BufferHint,
    // When `Some`, `build` reuses this pool instead of creating a new one
    shared_pool: Option<ConnectionPool>,
}

impl HttpClientBuilder {
    /// Build a default [`HttpClientBuilder`]
    pub fn new() -> Self {
        Self {
            dns: Arc::new(crate::dns::DefaultDnsResolver),
            tls_config: None,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            read_timeout: crate::body::DEFAULT_READ_TIMEOUT,
            write_timeout: DEFAULT_WRITE_TIMEOUT,
            max_idle_per_host: crate::pool::DEFAULT_MAX_IDLE_PER_HOST,
            idle_timeout: crate::pool::DEFAULT_IDLE_TIMEOUT,
            buffer_hint: BufferHint::DEFAULT,
            shared_pool: None,
        }
    }

    /// Overrides the DNS resolver.
    pub fn dns_resolver(mut self, dns: Arc<dyn DnsResolver>) -> Self {
        self.dns = dns;
        self
    }

    /// Overrides the TLS client config used for HTTPS endpoints.
    ///
    /// The caller is responsible for setting ALPN to HTTP/1.1.
    pub fn tls_config(mut self, config: Arc<rustls::ClientConfig>) -> Self {
        self.tls_config = Some(config);
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = timeout;
        self
    }

    pub fn write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    /// Per-host idle connection cap. `0` disables pooling.
    pub fn max_idle_per_host(mut self, max: usize) -> Self {
        self.max_idle_per_host = max;
        self
    }

    /// Idle connection timeout; idle connections older than this are dropped.
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Default [`BufferHint`] for requests without one in their extensions.
    pub fn buffer_hint(mut self, hint: BufferHint) -> Self {
        self.buffer_hint = hint;
        self
    }

    /// Reuses `pool` instead of creating a new one.
    pub fn shared_pool(mut self, pool: ConnectionPool) -> Self {
        self.shared_pool = Some(pool);
        self
    }

    /// Builds the client.
    pub fn build(self) -> Result<HttpClient, HttpError> {
        let tls_config = match self.tls_config {
            Some(config) => config,
            None => crate::tls::default_client_config()?,
        };
        let tls_connector = Some(TlsConnector::from(tls_config.clone()));
        let pool = self
            .shared_pool
            .unwrap_or_else(|| ConnectionPool::new(self.max_idle_per_host, self.idle_timeout));
        Ok(HttpClient {
            inner: Arc::new(HttpClientInner {
                pool,
                dns: self.dns,
                tls_connector,
                tls_config: Some(tls_config),
                connect_timeout: self.connect_timeout,
                read_timeout: self.read_timeout,
                write_timeout: self.write_timeout,
                buffer_hint: self.buffer_hint,
            }),
        })
    }

    /// Returns the configured connect timeout.
    pub fn configured_connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Returns the configured read timeout.
    pub fn configured_read_timeout(&self) -> Duration {
        self.read_timeout
    }

    /// Returns the configured write timeout.
    pub fn configured_write_timeout(&self) -> Duration {
        self.write_timeout
    }
}

impl Default for HttpClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
