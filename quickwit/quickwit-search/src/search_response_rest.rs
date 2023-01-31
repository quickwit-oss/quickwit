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

use quickwit_common::truncate_str;
use quickwit_proto::SearchResponse;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::error::SearchError;

/// SearchResponseRest represents the response returned by the REST search API
/// and is meant to be serialized into JSON.
#[derive(Serialize, Deserialize, PartialEq, Debug, utoipa::ToSchema)]
pub struct SearchResponseRest {
    /// Overall number of documents matching the query.
    pub num_hits: u64,
    #[schema(value_type = Vec<Object>)]
    /// List of hits returned.
    pub hits: Vec<JsonValue>,
    /// List of snippets
    #[schema(value_type = Vec<Object>)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippets: Option<Vec<JsonValue>>,
    /// Elapsed time.
    pub elapsed_time_micros: u64,
    /// Search errors.
    pub errors: Vec<String>,
    /// Aggregations.
    #[schema(value_type = Object)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregations: Option<JsonValue>,
}

impl TryFrom<SearchResponse> for SearchResponseRest {
    type Error = SearchError;

    fn try_from(search_response: SearchResponse) -> Result<Self, Self::Error> {
        let mut documents = Vec::with_capacity(search_response.hits.len());
        let mut snippets = Vec::new();
        for hit in search_response.hits {
            let document: JsonValue = serde_json::from_str(&hit.json).map_err(|err| {
                SearchError::InternalError(format!(
                    "Failed to serialize document `{}` to JSON: `{}`.",
                    truncate_str(&hit.json, 100),
                    err
                ))
            })?;
            documents.push(document);

            if let Some(snippet_json) = hit.snippet {
                let snippet_opt: JsonValue =
                    serde_json::from_str(&snippet_json).map_err(|err| {
                        SearchError::InternalError(format!(
                            "Failed to serialize snippet `{}` to JSON: `{}`.",
                            snippet_json, err
                        ))
                    })?;
                snippets.push(snippet_opt);
            }
        }

        let snippet_opt = if !snippets.is_empty() {
            Some(snippets)
        } else {
            None
        };

        let aggregations_opt = if let Some(aggregation_json) = search_response.aggregation {
            let aggregation: JsonValue = serde_json::from_str(&aggregation_json)
                .map_err(|err| SearchError::InternalError(err.to_string()))?;
            Some(aggregation)
        } else {
            None
        };

        Ok(SearchResponseRest {
            num_hits: search_response.num_hits,
            hits: documents,
            snippets: snippet_opt,
            elapsed_time_micros: search_response.elapsed_time_micros,
            errors: search_response.errors,
            aggregations: aggregations_opt,
        })
    }
}
