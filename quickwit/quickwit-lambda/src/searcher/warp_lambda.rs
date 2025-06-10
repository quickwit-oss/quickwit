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

// Based on https://github.com/aslamplr/warp_lambda under MIT license

use core::future::Future;
use std::collections::HashSet;
use std::convert::Infallible;
use std::marker::PhantomData;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};

use anyhow::anyhow;
use lambda_http::http::HeaderValue;
use lambda_http::{
    Adapter, Body as LambdaBody, Error as LambdaError, Request, RequestExt, Response, Service,
    lambda_runtime,
};
use mime_guess::{Mime, mime};
use once_cell::sync::Lazy;
use tracing::{Instrument, info_span};
use warp::hyper::Body as WarpBody;
pub use {lambda_http, warp};

pub type WarpRequest = warp::http::Request<warp::hyper::Body>;
pub type WarpResponse = warp::http::Response<warp::hyper::Body>;

static PLAINTEXT_MIMES: Lazy<HashSet<Mime>> = Lazy::new(|| {
    HashSet::from_iter([
        mime::APPLICATION_JAVASCRIPT,
        mime::APPLICATION_JAVASCRIPT_UTF_8,
        mime::APPLICATION_JSON,
    ])
});

pub async fn run<'a, S>(service: S) -> Result<(), LambdaError>
where
    S: Service<WarpRequest, Response = WarpResponse, Error = Infallible> + Send + 'a,
    S::Future: Send + 'a,
{
    lambda_runtime::run(Adapter::from(WarpAdapter::new(service))).await
}

#[derive(Clone)]
pub struct WarpAdapter<'a, S>
where
    S: Service<WarpRequest, Response = WarpResponse, Error = Infallible>,
    S::Future: Send + 'a,
{
    warp_service: S,
    _phantom_data: PhantomData<&'a WarpResponse>,
}

impl<'a, S> WarpAdapter<'a, S>
where
    S: Service<WarpRequest, Response = WarpResponse, Error = Infallible>,
    S::Future: Send + 'a,
{
    pub fn new(warp_service: S) -> Self {
        Self {
            warp_service,
            _phantom_data: PhantomData,
        }
    }
}

impl<'a, S> Service<Request> for WarpAdapter<'a, S>
where
    S: Service<WarpRequest, Response = WarpResponse, Error = Infallible> + 'a,
    S::Future: Send + 'a,
{
    type Response = Response<LambdaBody>;
    type Error = LambdaError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'a>>;

    fn poll_ready(&mut self, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.warp_service
            .poll_ready(ctx)
            .map_err(|error| match error {})
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let query_params = request.query_string_parameters();
        let request_id = request.lambda_context().request_id.clone();
        let (parts, body) = request.into_parts();
        let mut warp_parts = lambda_parts_to_warp_parts(&parts);
        let (content_len, warp_body) = match body {
            LambdaBody::Empty => (0, WarpBody::empty()),
            LambdaBody::Text(text) => (text.len(), WarpBody::from(text.into_bytes())),
            LambdaBody::Binary(bytes) => (bytes.len(), WarpBody::from(bytes)),
        };
        let mut uri = format!("http://{}{}", "127.0.0.1", parts.uri.path());
        if !query_params.is_empty() {
            let url_res = reqwest::Url::parse_with_params(&uri, query_params.iter());
            if let Ok(url) = url_res {
                uri = url.into();
            } else {
                return Box::pin(async move { Err(anyhow!("invalid url: {uri}").into()) });
            }
        }
        warp_parts.uri = warp::hyper::Uri::from_str(uri.as_str()).unwrap();
        // REST API Gateways swallow the content-length header which is required
        // by many Quickwit routes (`warp::body::content_length_limit(xxx)`)
        if let warp::http::header::Entry::Vacant(entry) = warp_parts.headers.entry("Content-Length")
        {
            entry.insert(content_len.into());
        }
        let warp_request = WarpRequest::from_parts(warp_parts, warp_body);

        // Call warp service with warp request, save future
        let warp_fut = self.warp_service.call(warp_request);

        // Create lambda future
        let fut = async move {
            let warp_response = warp_fut.await?;
            let (warp_parts, warp_body): (_, _) = warp_response.into_parts();
            let parts = warp_parts_to_lambda_parts(&warp_parts);
            let body = warp_body_to_lambda_body(&parts, warp_body).await?;
            let lambda_response = Response::from_parts(parts, body);
            Ok::<Self::Response, Self::Error>(lambda_response)
        }
        .instrument(info_span!("searcher request", request_id));
        Box::pin(fut)
    }
}

fn lambda_parts_to_warp_parts(
    parts: &lambda_http::http::request::Parts,
) -> warp::http::request::Parts {
    let mut builder = warp::http::Request::builder()
        .method(lambda_method_to_warp_method(&parts.method))
        .uri(lambda_uri_to_warp_uri(&parts.uri))
        .version(lambda_version_to_warp_version(parts.version));

    for (name, value) in parts.headers.iter() {
        builder = builder.header(name.as_str(), value.as_bytes());
    }
    let request = builder.body(()).unwrap();
    let (parts, _) = request.into_parts();
    parts
}

fn warp_parts_to_lambda_parts(
    parts: &warp::http::response::Parts,
) -> lambda_http::http::response::Parts {
    let mut builder = lambda_http::http::Response::builder()
        .status(parts.status.as_u16())
        .version(warp_version_to_lambda_version(parts.version));

    for (name, value) in parts.headers.iter() {
        builder = builder.header(name.as_str(), value.as_bytes());
    }
    let response = builder.body(()).unwrap();
    let (parts, _) = response.into_parts();
    parts
}

async fn warp_body_to_lambda_body(
    parts: &lambda_http::http::response::Parts,
    warp_body: WarpBody,
) -> Result<LambdaBody, LambdaError> {
    // Concatenate all bytes into a single buffer
    let body_bytes = warp::hyper::body::to_bytes(warp_body).await?.to_vec();

    // Attempt to determine the Content-Type
    let content_type_opt: Option<&HeaderValue> = parts.headers.get("Content-Type");
    let content_encoding_opt: Option<&HeaderValue> = parts.headers.get("Content-Encoding");

    // If Content-Encoding is present, assume compression
    // If Content-Type is not present, don't assume is a string
    if let (Some(content_type), None) = (content_type_opt, content_encoding_opt) {
        let content_type_str = content_type.to_str()?;
        let mime = content_type_str.parse::<Mime>()?;

        if PLAINTEXT_MIMES.contains(&mime) || mime.type_() == mime::TEXT {
            return Ok(LambdaBody::Text(String::from_utf8(body_bytes)?));
        }
    }
    // Not a text response, make binary
    Ok(LambdaBody::Binary(body_bytes))
}

fn lambda_method_to_warp_method(method: &lambda_http::http::Method) -> warp::http::Method {
    method.as_str().parse::<warp::http::Method>().unwrap()
}

fn lambda_uri_to_warp_uri(uri: &lambda_http::http::Uri) -> warp::http::Uri {
    uri.to_string().parse::<warp::http::Uri>().unwrap()
}

fn lambda_version_to_warp_version(version: lambda_http::http::Version) -> warp::http::Version {
    if version == lambda_http::http::Version::HTTP_09 {
        warp::http::Version::HTTP_09
    } else if version == lambda_http::http::Version::HTTP_10 {
        warp::http::Version::HTTP_10
    } else if version == lambda_http::http::Version::HTTP_11 {
        warp::http::Version::HTTP_11
    } else if version == lambda_http::http::Version::HTTP_2 {
        warp::http::Version::HTTP_2
    } else if version == lambda_http::http::Version::HTTP_3 {
        warp::http::Version::HTTP_3
    } else {
        panic!("invalid HTTP version: {version:?}");
    }
}

fn warp_version_to_lambda_version(version: warp::http::Version) -> lambda_http::http::Version {
    if version == warp::http::Version::HTTP_09 {
        lambda_http::http::Version::HTTP_09
    } else if version == warp::http::Version::HTTP_10 {
        lambda_http::http::Version::HTTP_10
    } else if version == warp::http::Version::HTTP_11 {
        lambda_http::http::Version::HTTP_11
    } else if version == warp::http::Version::HTTP_2 {
        lambda_http::http::Version::HTTP_2
    } else if version == warp::http::Version::HTTP_3 {
        lambda_http::http::Version::HTTP_3
    } else {
        panic!("invalid HTTP version: {version:?}");
    }
}
