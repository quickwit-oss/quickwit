// Copyright (C) 2021 Quickwit, Inc.
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

use serde::Serialize;
use tracing::error;

/// SearchResultsJson represents the result returned by the rest search API
/// and is meant to be serialized into Json.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResultJson {
    /// Overall number of document matching the query.
    pub num_hits: u64,
    /// List of hits returned.
    pub hits: Vec<serde_json::Value>,
    /// Elapsed time.
    pub num_microsecs: u64,
}

impl From<quickwit_proto::SearchResult> for SearchResultJson {
    fn from(search_result: quickwit_proto::SearchResult) -> Self {
        let hits: Vec<serde_json::Value> = search_result
            .hits
            .into_iter()
            .map(|hit| match serde_json::from_str(&hit.json) {
                Ok(hit_json) => hit_json,
                Err(invalid_json) => {
                    error!(err=?invalid_json, "Invalid json in hit");
                    serde_json::json!({})
                }
            })
            .collect();
        SearchResultJson {
            num_hits: search_result.num_hits,
            hits,
            num_microsecs: search_result.elapsed_time_micros,
        }
    }
}
