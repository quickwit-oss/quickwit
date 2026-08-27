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

use aws_smithy_runtime_api::client::http::{
    HttpClient as SdkHttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings,
    SharedHttpClient, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::http::{Request as SdkRequest, Response as SdkResponse, StatusCode};
use aws_smithy_types::body::SdkBody;
use http_body_util::BodyExt;
use tokio_util::task::AbortOnDropHandle;

use crate::body::{BufferHint, ResponseBody};
use crate::client::{HttpClient, HttpClientBuilder};
use crate::connection::ConnStream;
use crate::error::HttpError;

#[derive(Clone)]
pub struct SingleBufferHttp1HttpClient {
    // A template client whose pool is shared across per-call connectors.
    template: HttpClient,
    default_connect_timeout: Duration,
    default_read_timeout: Duration,
    default_write_timeout: Duration,
}

impl std::fmt::Debug for SingleBufferHttp1HttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SingleBufferHttp1HttpClient")
            .field("default_connect_timeout", &self.default_connect_timeout)
            .field("default_read_timeout", &self.default_read_timeout)
            .field("default_write_timeout", &self.default_write_timeout)
            .finish_non_exhaustive()
    }
}

/// Builder for [`SingleBufferHttp1HttpClient`].
pub struct SingleBufferHttp1HttpClientBuilder {
    inner: HttpClientBuilder,
}

impl SingleBufferHttp1HttpClientBuilder {
    pub fn connect_timeout(mut self, d: Duration) -> Self {
        self.inner = self.inner.connect_timeout(d);
        self
    }
    pub fn read_timeout(mut self, d: Duration) -> Self {
        self.inner = self.inner.read_timeout(d);
        self
    }
    pub fn write_timeout(mut self, d: Duration) -> Self {
        self.inner = self.inner.write_timeout(d);
        self
    }
    pub fn max_idle_per_host(mut self, n: usize) -> Self {
        self.inner = self.inner.max_idle_per_host(n);
        self
    }
    pub fn idle_timeout(mut self, d: Duration) -> Self {
        self.inner = self.inner.idle_timeout(d);
        self
    }
    /// Overrides the DNS resolver.
    pub fn dns_resolver(mut self, resolver: Arc<dyn crate::dns::DnsResolver>) -> Self {
        self.inner = self.inner.dns_resolver(resolver);
        self
    }
    /// Overrides the TLS client config.
    pub fn tls_config(mut self, config: Arc<rustls::ClientConfig>) -> Self {
        self.inner = self.inner.tls_config(config);
        self
    }
    /// Overrides the default [`BufferHint`] for request without one in their extensions.
    pub fn buffer_hint(mut self, hint: BufferHint) -> Self {
        self.inner = self.inner.buffer_hint(hint);
        self
    }
    pub fn build(self) -> Result<SingleBufferHttp1HttpClient, HttpError> {
        let default_connect_timeout = self.inner.configured_connect_timeout();
        let default_read_timeout = self.inner.configured_read_timeout();
        let default_write_timeout = self.inner.configured_write_timeout();
        let template = self.inner.build()?;
        Ok(SingleBufferHttp1HttpClient {
            default_connect_timeout,
            default_read_timeout,
            default_write_timeout,
            template,
        })
    }
}

impl SingleBufferHttp1HttpClient {
    /// Creates a new SDK HTTP client with the OS native root store and the
    /// aws-lc-rs crypto provider.
    pub fn new() -> Result<Self, HttpError> {
        Self::builder().build()
    }

    /// Returns a builder allowing the to configure the client.
    pub fn builder() -> SingleBufferHttp1HttpClientBuilder {
        SingleBufferHttp1HttpClientBuilder {
            inner: HttpClientBuilder::new(),
        }
    }

    /// Builds a per-call [`HttpClient`] honoring `settings`, sharing this
    /// selector's pool.
    fn client_for_settings(&self, settings: &HttpConnectorSettings) -> HttpClient {
        let connect_timeout = settings
            .connect_timeout()
            .unwrap_or(self.default_connect_timeout);
        let read_timeout = settings.read_timeout().unwrap_or(self.default_read_timeout);
        HttpClientBuilder::new()
            .connect_timeout(connect_timeout)
            .read_timeout(read_timeout)
            .write_timeout(self.default_write_timeout)
            .shared_pool(self.template.pool())
            .build()
            .expect("tls config loads from native roots")
    }
}

impl Default for SingleBufferHttp1HttpClient {
    fn default() -> Self {
        Self::new().expect("native root TLS config loads")
    }
}

impl SdkHttpClient for SingleBufferHttp1HttpClient {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        _components: &aws_smithy_runtime_api::client::runtime_components::RuntimeComponents,
    ) -> SharedHttpConnector {
        let client = self.client_for_settings(settings);
        SharedHttpConnector::new(SingleBufferHttp1Connector { client })
    }
}

#[derive(Clone, Debug)]
pub struct SingleBufferHttp1Connector {
    client: HttpClient,
}

impl SingleBufferHttp1Connector {
    pub fn new(client: HttpClient) -> Self {
        Self { client }
    }
}

impl HttpConnector for SingleBufferHttp1Connector {
    fn call(&self, request: SdkRequest) -> HttpConnectorFuture {
        let client = self.client.clone();
        HttpConnectorFuture::new(async move {
            // Run the client in its own task so the task handling it has a much
            // shorter stack/state machine depth, and the sdk only get waked up
            // when a buffer is received
            let driver = AbortOnDropHandle::new(tokio::spawn(async move {
                let http_request: http::Request<SdkBody> = request
                    .try_into_http1x()
                    .map_err(|err| ConnectorError::other(err.into(), None))?;
                let response = client
                    .execute(http_request)
                    .await
                    .map_err(to_connector_error)?;
                convert_response(response).await
            }));
            driver
                .await
                .map_err(|err| ConnectorError::other(err.into(), None))?
        })
    }
}

/// Converts the core client's streaming `http::Response<ResponseBody>` into
/// the SDK's `Response<SdkBody>`.
async fn convert_response(
    response: http::Response<ResponseBody<ConnStream>>,
) -> Result<SdkResponse<SdkBody>, ConnectorError> {
    let (parts, body) = response.into_parts();
    let status = StatusCode::try_from(parts.status.as_u16())
        .map_err(|err| ConnectorError::other(err.into(), None))?;
    let bytes = body.collect().await.map_err(to_connector_error)?.to_bytes();
    let mut sdk_response = SdkResponse::new(status, SdkBody::from(bytes));
    // we borrow to get a (&Name, &Value) iterator, otherwise we get a (Option<Name>, Value)
    // iterator (multiple headers can have the same name)
    for (name, value) in &parts.headers {
        sdk_response
            .headers_mut()
            .try_insert(name.as_str().to_string(), value.to_str().unwrap_or("").to_string())
            .map_err(|err| ConnectorError::other(err.into(), None))?;
    }
    Ok(sdk_response)
}

/// Maps a core [`HttpError`] to the SDK's [`ConnectorError`].
fn to_connector_error(err: HttpError) -> ConnectorError {
    let is_io = err.is_io()
        || matches!(
            err,
            HttpError::UnexpectedEof { .. } | HttpError::InvalidLength(_)
        );
    if err.is_timeout() {
        ConnectorError::timeout(err.into())
    } else if is_io {
        ConnectorError::io(err.into())
    } else {
        ConnectorError::other(err.into(), None)
    }
}

/// Convenience: wrap a [`SingleBufferHttp1HttpClient`] into the SDK's
/// [`SharedHttpClient`] for `s3_config.set_http_client(...)`.
pub fn shared_http_client(client: SingleBufferHttp1HttpClient) -> SharedHttpClient {
    SharedHttpClient::new(client)
}

#[cfg(test)]
mod tests;
