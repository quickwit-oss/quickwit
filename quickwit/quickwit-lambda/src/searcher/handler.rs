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

use lambda_runtime::{Error, LambdaEvent};
use quickwit_serve::SearchRequestQueryString;
use serde_json::Value;
use tracing::{debug, error};

use super::search::{search, SearchArgs};
use crate::logger;

pub async fn handler(event: LambdaEvent<SearchRequestQueryString>) -> Result<Value, Error> {
    debug!(payload = ?event.payload, "Received query");
    let ingest_res = search(SearchArgs {
        index_id: std::env::var("QW_LAMBDA_INDEX_ID")?,
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
