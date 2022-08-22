// Copyright (C) 2022 Quickwit, Inc.
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
use serde_json::{Map, Value};

use crate::error::SearchError;

/// SearchResponseRest represents the response returned by the REST search API
/// and is meant to be serialized into JSON.
#[derive(Serialize)]
pub struct SearchResponseRest {
    /// Overall number of documents matching the query.
    pub num_hits: u64,
    /// List of hits returned.
    pub hits: Vec<serde_json::Value>,
    /// Elapsed time.
    pub elapsed_time_micros: u64,
    /// Search errors.
    pub errors: Vec<String>,
    /// Aggregations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregations: Option<serde_json::Value>,
}

impl TryFrom<quickwit_proto::SearchResponse> for SearchResponseRest {
    type Error = SearchError;

    fn try_from(search_response: quickwit_proto::SearchResponse) -> Result<Self, Self::Error> {
        let hits = search_response
            .hits
            .into_iter()
            .map(|hit| {
                let mut hit_value = Map::with_capacity(2);
                let document: serde_json::Value =
                    serde_json::from_str(&hit.json).map_err(|err| {
                        SearchError::InternalError(format!(
                            "Failed to serialize document `{}` to JSON: `{}`.",
                            &hit.json[..100],
                            err
                        ))
                    })?;
                hit_value.insert("document".to_string(), document);

                if let Some(snippet_json) = &hit.snippet {
                    let snippet: Value = serde_json::from_str(snippet_json).map_err(|err| {
                        SearchError::InternalError(format!(
                            "Failed to serialize snippet `{}` to JSON: `{}`.",
                            snippet_json, err
                        ))
                    })?;
                    hit_value.insert("snippet".to_string(), snippet);
                }
                Ok(Value::Object(hit_value))
            })
            .collect::<crate::Result<Vec<serde_json::Value>>>()?;
        Ok(SearchResponseRest {
            num_hits: search_response.num_hits,
            hits,
            elapsed_time_micros: search_response.elapsed_time_micros,
            errors: search_response.errors,
            aggregations: search_response
                .aggregation
                .map(|agg| serde_json::from_str(&agg))
                .transpose()
                .map_err(|err| SearchError::InternalError(err.to_string()))?,
        })
    }
}
