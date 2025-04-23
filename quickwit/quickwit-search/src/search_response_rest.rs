// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::convert::TryFrom;

use quickwit_common::truncate_str;
use quickwit_proto::search::SearchResponse;
use quickwit_query::aggregations::AggregationResults as AggregationResultsProxy;
use quickwit_query::query_ast::QueryAst;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::error::SearchError;

/// A classic ES aggregation result ast
// TODO previously, we were using zero-copy when possible, which we are no longer doing:
// is that problematic? How can we return to zero/low-copy without it being painful?
#[derive(Serialize, PartialEq, Debug)]
pub struct AggregationResults(tantivy::aggregation::agg_result::AggregationResults);

impl AggregationResults {
    /// Parse an ES aggregation result ast from our non-ambiguous postcard format
    pub fn from_postcard(postcard_bytes: &[u8]) -> anyhow::Result<Self> {
        let aggregation_result: AggregationResultsProxy = postcard::from_bytes(postcard_bytes)?;
        Ok(AggregationResults(aggregation_result.into()))
    }
}

/// SearchResponseRest represents the response returned by the REST search API
/// and is meant to be serialized into JSON.
#[derive(Serialize, PartialEq, Debug, utoipa::ToSchema)]
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
    pub aggregations: Option<AggregationResults>,
}

impl TryFrom<SearchResponse> for SearchResponseRest {
    type Error = SearchError;

    fn try_from(search_response: SearchResponse) -> Result<Self, Self::Error> {
        let mut documents = Vec::with_capacity(search_response.hits.len());
        let mut snippets = Vec::new();
        for hit in search_response.hits {
            let document: JsonValue = serde_json::from_str(&hit.json).map_err(|err| {
                SearchError::Internal(format!(
                    "failed to serialize document `{}` to JSON: `{}`",
                    truncate_str(&hit.json, 100),
                    err
                ))
            })?;
            documents.push(document);

            if let Some(snippet_json) = hit.snippet {
                let snippet_opt: JsonValue =
                    serde_json::from_str(&snippet_json).map_err(|err| {
                        SearchError::Internal(format!(
                            "failed to serialize snippet `{snippet_json}` to JSON: `{err}`"
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

        let aggregations_opt =
            if let Some(aggregation_postcard) = search_response.aggregation_postcard {
                let aggregation = AggregationResults::from_postcard(&aggregation_postcard)
                    .map_err(|err| SearchError::Internal(err.to_string()))?;
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

/// Details on how a query would be executed.
#[derive(Serialize, Deserialize, PartialEq, Debug, utoipa::ToSchema)]
pub struct SearchPlanResponseRest {
    /// Quickwit AST of the query.
    #[schema(value_type = Object)]
    pub quickwit_ast: QueryAst,
    /// Resolved Tantivy AST of the query, according to the latest docmapping.
    ///
    /// It's possible older splits actually resolve to a different ast.
    pub tantivy_ast: String,
    /// List of splits that would be searched by this query
    pub searched_splits: Vec<String>,
    /// Requests expected for each split
    #[schema(value_type = Object)]
    pub storage_requests: StorageRequestCount,
}

/// Number of expected storage requests, per request kind.
///
/// These figures do not take in account whether the data is already cached or not.
#[derive(Serialize, Deserialize, PartialEq, Debug, Default)]
pub struct StorageRequestCount {
    /// Number of split footer downloaded, always 1
    pub footer: usize,
    /// Number of fastfields downloaded
    pub fastfield: usize,
    /// Number of fieldnorm downloaded
    pub fieldnorm: usize,
    /// Number of sstable downloaded
    pub sstable: usize,
    /// Number of posting list downloaded
    pub posting: usize,
    /// Number of position list downloaded
    pub position: usize,
}
