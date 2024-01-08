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

use std::sync::atomic::Ordering::SeqCst;

use anyhow::Context;
use flate2::write::GzEncoder;
use flate2::Compression;
use lambda_http::http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use lambda_http::{Body, Error, IntoResponse, Request, RequestExt, RequestPayloadExt, Response};
use quickwit_search::SearchResponseRest;
use quickwit_serve::SearchRequestQueryString;
use tracing::{instrument, span, Instrument, Level};

use super::environment::{ENABLE_SEARCH_CACHE, INDEX_ID};
use super::search::{search, SearchArgs};
use crate::logger;
use crate::utils::ALREADY_EXECUTED;

#[instrument(skip_all)]
fn deflate_serialize(resp: SearchResponseRest) -> anyhow::Result<Vec<u8>> {
    let value = serde_json::to_value(resp)?;
    let mut buffer = Vec::new();
    let mut gz = GzEncoder::new(&mut buffer, Compression::default());
    serde_json::to_writer(&mut gz, &value)?;
    gz.finish()?;
    Ok(buffer)
}

pub async fn handler(request: Request) -> Result<impl IntoResponse, Error> {
    let cold = !ALREADY_EXECUTED.swap(true, SeqCst);
    let memory = request.lambda_context().env_config.memory;
    let payload = request
        .payload::<SearchRequestQueryString>()?
        .context("Empty payload")?;
    let paylog_dbg = format!("{:?}", payload);

    // TODO better handle errors (e.g print them)

    let search_res = search(SearchArgs { query: payload })
        .instrument(span!(
            parent: &span!(Level::INFO, "searcher_handler", cold),
            Level::TRACE,
            logger::RUNTIME_CONTEXT_SPAN,
            memory,
            payload = paylog_dbg,
            env.INDEX_ID = *INDEX_ID,
            env.ENABLE_SEARCH_CACHE = *ENABLE_SEARCH_CACHE
        ))
        .await?;

    let response_body = deflate_serialize(search_res)?;

    logger::flush_tracer();
    let response = Response::builder()
        .header(CONTENT_ENCODING, "gzip")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::Binary(response_body))
        .context("Could not build response")?;
    Ok(response)
}
