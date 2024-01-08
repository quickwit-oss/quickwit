// Copyright (C) 2023 Quickwit, Inc.
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

use anyhow::Context;
use flate2::write::GzEncoder;
use flate2::Compression;
use lambda_http::http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use lambda_http::{Body, Error, IntoResponse, Request, RequestExt, RequestPayloadExt, Response};
use quickwit_search::SearchResponseRest;
use quickwit_serve::SearchRequestQueryString;
use tracing::{debug_span, error, info_span, instrument, Instrument};

use super::environment::{DISABLE_SEARCH_CACHE, INDEX_ID};
use super::search::{search, SearchArgs};
use crate::logger;
use crate::utils::LambdaContainerContext;

#[instrument(skip_all)]
fn deflate_serialize(resp: SearchResponseRest) -> anyhow::Result<Vec<u8>> {
    let value = serde_json::to_value(resp)?;
    let mut buffer = Vec::new();
    let mut gz = GzEncoder::new(&mut buffer, Compression::default());
    serde_json::to_writer(&mut gz, &value)?;
    gz.finish()?;
    Ok(buffer)
}

pub async fn searcher_handler(request: Request) -> Result<impl IntoResponse, Error> {
    let container_ctx = LambdaContainerContext::load();
    let memory = request.lambda_context().env_config.memory;
    let payload = request
        .payload::<SearchRequestQueryString>()?
        .context("Empty payload")?;

    let search_res = search(SearchArgs { query: payload })
        .instrument(debug_span!(
            "search",
            memory,
            env.INDEX_ID = *INDEX_ID,
            env.DISABLE_SEARCH_CACHE = *DISABLE_SEARCH_CACHE,
            cold = container_ctx.cold,
            container_id = container_ctx.container_id,
        ))
        .await?;

    let response_body = deflate_serialize(search_res)?;

    let response = Response::builder()
        .header(CONTENT_ENCODING, "gzip")
        .header(CONTENT_TYPE, "application/json")
        .header("x-lambda-request-id", request.lambda_context().request_id)
        .body(Body::Binary(response_body))
        .context("Could not build response")?;
    Ok(response)
}

pub async fn handler(request: Request) -> Result<impl IntoResponse, Error> {
    let request_id = request.lambda_context().request_id.clone();
    let response = searcher_handler(request)
        .instrument(info_span!("searcher_handler", request_id))
        .await;

    if let Err(e) = &response {
        error!(err=?e, "Handler failed");
    }
    logger::flush_tracer();
    response
}
