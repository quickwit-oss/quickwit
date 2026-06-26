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

use aws_smithy_runtime_api::client::http::{
    HttpConnector, HttpConnectorFuture, SharedHttpClient, SharedHttpConnector, http_client_fn,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_types::body::SdkBody;
use hyper_util::client::legacy::connect::HttpConnector as HyperHttpConnector;
use hyper_util::rt::{TokioExecutor, TokioTimer};

/// HTTP/1.1 read buffer size for S3 responses: 2^19 = 512 KiB.
///
/// The hyper default is 8 KiB. A larger buffer reduces syscall overhead when
/// reading large objects from S3.
const S3_HTTP1_READ_BUF_SIZE: usize = 1 << 19;

/// Builds an HTTP client for S3 with a 512 KiB HTTP/1.1 read buffer.
///
/// `aws_smithy_http_client::Builder` does not expose a public API for the
/// underlying `hyper_util::client::legacy::Builder`, so we build the hyper
/// client directly and wrap it in the smithy `HttpConnector` trait.
pub fn build_s3_http_client() -> SharedHttpClient {
    let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .expect("failed to load native TLS roots for S3 HTTP client")
        .https_or_http()
        .enable_http1()
        .build();
    let mut builder = hyper_util::client::legacy::Builder::new(TokioExecutor::new());
    builder.pool_timer(TokioTimer::new());
    builder.http1_read_buf_exact_size(S3_HTTP1_READ_BUF_SIZE);
    let hyper_client = builder.build(https_connector);
    let connector = S3Connector {
        client: hyper_client,
    };
    http_client_fn(move |_, _| SharedHttpConnector::new(connector.clone()))
}

#[derive(Clone, Debug)]
struct S3Connector {
    client: hyper_util::client::legacy::Client<
        hyper_rustls::HttpsConnector<HyperHttpConnector>,
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
