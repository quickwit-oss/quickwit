// Copyright (C) 2024 Quickwit, Inc.
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
use lambda_http::http::response::Parts;
use lambda_http::http::HeaderValue;
use lambda_http::request::RequestContext;
use lambda_http::{
    lambda_runtime, Adapter, Body as LambdaBody, Error as LambdaError, Request, RequestExt,
    Response, Service,
};
use mime_guess::{mime, Mime};
use once_cell::sync::Lazy;
use warp::hyper::Body as WarpBody;
pub use {lambda_http, warp};

use super::LAMBDA_REQUEST_ID_HEADER;

pub type WarpRequest = warp::http::Request<warp::hyper::Body>;
pub type WarpResponse = warp::http::Response<warp::hyper::Body>;

#[derive(thiserror::Error, Debug)]
pub enum WarpAdapterError {
    #[error("This may never occur, it's infallible!!")]
    Infallible(#[from] std::convert::Infallible),
    #[error("Warp error: `{0:#?}`")]
    HyperError(#[from] warp::hyper::Error),
    #[error("Unexpected error: `{0:#?}`")]
    Unexpected(#[from] LambdaError),
}

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

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        core::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let query_params = req.query_string_parameters();
        let request_id_opt = match req.request_context() {
            RequestContext::ApiGatewayV2(ctx) => ctx.request_id.clone(),
            RequestContext::ApiGatewayV1(ctx) => ctx.request_id.clone(),
            RequestContext::Alb(_) => None,
            RequestContext::WebSocket(ctx) => ctx.request_id.clone(),
        };
        let request_id_header_opt = match request_id_opt.as_ref().map(|a| HeaderValue::from_str(a))
        {
            Some(Ok(rid)) => Some(rid),
            Some(Err(_)) => None,
            None => None,
        };
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
        if let Some(rid) = &request_id_header_opt {
            parts.headers.insert(LAMBDA_REQUEST_ID_HEADER, rid.clone());
        }

        parts.uri = warp::hyper::Uri::from_str(uri.as_str())
            .map_err(|e| e)
            .unwrap();
        let warp_request = WarpRequest::from_parts(parts, body);

        // Call warp service with warp request, save future
        let warp_fut = self.warp_service.call(warp_request);

        // Create lambda future
        let fut = async move {
            let warp_response = warp_fut.await?;
            let (mut parts, res_body): (_, _) = warp_response.into_parts();
            let body = warp_body_as_lambda_body(res_body, &parts).await?;
            if let Some(rid) = request_id_header_opt {
                parts.headers.insert(LAMBDA_REQUEST_ID_HEADER, rid);
            }
            let lambda_response = Response::from_parts(parts, body);
            Ok::<Self::Response, Self::Error>(lambda_response)
        };
        Box::pin(fut)
    }
}
