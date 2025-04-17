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
use http::header::Entry;
use lambda_http::http::HeaderValue;
use lambda_http::http::response::Parts;
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

static PLAINTEXT_MIMES: Lazy<HashSet<Mime>> = Lazy::new(|| {
    vec![
        mime::APPLICATION_JAVASCRIPT,
        mime::APPLICATION_JAVASCRIPT_UTF_8,
        mime::APPLICATION_JSON,
    ]
    .into_iter()
    .collect()
});

async fn warp_body_as_lambda_body(
    warp_body: WarpBody,
    parts: &Parts,
) -> Result<LambdaBody, LambdaError> {
    // Concatenate all bytes into a single buffer
    let raw_bytes = warp::hyper::body::to_bytes(warp_body).await?;

    // Attempt to determine the Content-Type
    let content_type: Option<&HeaderValue> = parts.headers.get("Content-Type");
    let content_encoding: Option<&HeaderValue> = parts.headers.get("Content-Encoding");

    // If Content-Encoding is present, assume compression
    // If Content-Type is not present, don't assume is a string
    let body = if let (Some(typ), None) = (content_type, content_encoding) {
        let typ = typ.to_str()?;
        let m = typ.parse::<Mime>()?;
        if PLAINTEXT_MIMES.contains(&m) || m.type_() == mime::TEXT {
            Some(String::from_utf8(raw_bytes.to_vec()).map(LambdaBody::Text)?)
        } else {
            None
        }
    } else {
        None
    };

    // Not a text response, make binary
    Ok(body.unwrap_or_else(|| LambdaBody::Binary(raw_bytes.to_vec())))
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
            .map_err(|err| match err {})
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let query_params = req.query_string_parameters();
        let request_id = req.lambda_context().request_id.clone();
        let (mut parts, body) = req.into_parts();
        let (content_len, body) = match body {
            LambdaBody::Empty => (0, WarpBody::empty()),
            LambdaBody::Text(t) => (t.len(), WarpBody::from(t.into_bytes())),
            LambdaBody::Binary(b) => (b.len(), WarpBody::from(b)),
        };

        let mut uri = format!("http://{}{}", "127.0.0.1", parts.uri.path());
        if !query_params.is_empty() {
            let url_res = reqwest::Url::parse_with_params(&uri, query_params.iter());
            if let Ok(url) = url_res {
                uri = url.into();
            } else {
                return Box::pin(async { Err(anyhow!("Invalid url").into()) });
            }
        }

        // REST API Gateways swallow the content-length header which is required
        // by many Quickwit routes (`warp::body::content_length_limit(xxx)`)
        if let Entry::Vacant(v) = parts.headers.entry("Content-Length") {
            v.insert(content_len.into());
        }

        parts.uri = warp::hyper::Uri::from_str(uri.as_str()).unwrap();
        let warp_request = WarpRequest::from_parts(parts, body);

        // Call warp service with warp request, save future
        let warp_fut = self.warp_service.call(warp_request);

        // Create lambda future
        let fut = async move {
            let warp_response = warp_fut.await?;
            let (parts, res_body): (_, _) = warp_response.into_parts();
            let body = warp_body_as_lambda_body(res_body, &parts).await?;
            let lambda_response = Response::from_parts(parts, body);
            Ok::<Self::Response, Self::Error>(lambda_response)
        }
        .instrument(info_span!("searcher request", request_id));
        Box::pin(fut)
    }
}
