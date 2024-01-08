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

use lambda_runtime::{service_fn, Error, LambdaEvent};
use quickwit_lambda::{search, setup_lambda_tracer, SearchArgs};
use serde_json::Value;
use tracing::{debug, error};

pub async fn handler(_event: LambdaEvent<Value>) -> Result<Value, Error> {
    let ingest_res = search(SearchArgs {
        index_id: std::env::var("INDEX_ID")?,
        query: String::new(),
        aggregation: None,
        max_hits: 10,
        start_offset: 0,
        search_fields: None,
        snippet_fields: None,
        start_timestamp: None,
        end_timestamp: None,
        sort_by_field: None,
    })
    .await;
    match ingest_res {
        Ok(resp) => {
            debug!(resp=?resp, "Search succeeded");
            Ok(serde_json::to_value(resp)?)
        }
        Err(e) => {
            error!(err=?e, "Search failed");
            return Err(anyhow::anyhow!("Query failed").into());
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_lambda_tracer()?;
    let func = service_fn(handler);
    lambda_runtime::run(func)
        .await
        .map_err(|e| anyhow::anyhow!(e))
}
