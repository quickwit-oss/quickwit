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

use lambda_runtime::{Error, LambdaEvent};
use quickwit_serve::SearchRequestQueryString;
use serde_json::Value;
use tracing::{debug, error, span, Instrument, Level};

use super::environment::{ENABLE_SEARCH_CACHE, INDEX_ID};
use super::search::{search, SearchArgs};
use crate::logger;
use crate::utils::ALREADY_EXECUTED;

async fn searcher_handler(event: LambdaEvent<SearchRequestQueryString>) -> Result<Value, Error> {
    let ingest_res = search(SearchArgs {
        query: event.payload,
    })
    .await;
    let result = match ingest_res {
        Ok(resp) => {
            debug!(resp=?resp, "Search succeeded");
            Ok(serde_json::to_value(resp)?)
        }
        Err(e) => {
            error!(err=?e, "Search failed");
            return Err(anyhow::anyhow!("Query failed").into());
        }
    };
    logger::flush_tracer();
    result
}

pub async fn handler(event: LambdaEvent<SearchRequestQueryString>) -> Result<Value, Error> {
    let cold = !ALREADY_EXECUTED.swap(true, SeqCst);
    let memory = event.context.env_config.memory;
    let payload = format!("{:?}", event.payload);

    let result = searcher_handler(event)
        .instrument(span!(
            parent: &span!(Level::INFO, "searcher_handler", cold),
            Level::TRACE,
            logger::RUNTIME_CONTEXT_SPAN,
            memory,
            payload,
            env.INDEX_ID = *INDEX_ID,
            env.ENABLE_SEARCH_CACHE = *ENABLE_SEARCH_CACHE
        ))
        .await;

    logger::flush_tracer();
    result
}
