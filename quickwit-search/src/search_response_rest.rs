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

use std::convert::TryFrom;

use serde::Serialize;

use crate::error::SearchError;

/// SearchResponseRest represents the response returned by the REST search API
/// and is meant to be serialized into JSON.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResponseRest {
    /// Overall number of documents matching the query.
    pub num_hits: u64,
    /// List of hits returned.
    pub hits: Vec<serde_json::Value>,
    /// Elapsed time.
    pub elapsed_time_micros: u64,
    /// Search errors.
    pub errors: Vec<String>,
}

impl TryFrom<quickwit_proto::SearchResponse> for SearchResponseRest {
    type Error = SearchError;

    fn try_from(search_response: quickwit_proto::SearchResponse) -> Result<Self, Self::Error> {
        let hits = search_response
            .hits
            .into_iter()
            .map(|hit| {
                serde_json::from_str(&hit.json).map_err(|err| {
                    SearchError::InternalError(format!(
                        "Failed to serialize document `{}` to JSON: `{}`.",
                        hit.json, err
                    ))
                })
            })
            .collect::<crate::Result<Vec<serde_json::Value>>>()?;
        Ok(SearchResponseRest {
            num_hits: search_response.num_hits,
            hits,
            elapsed_time_micros: search_response.elapsed_time_micros,
            errors: search_response.errors,
        })
    }
}
