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
use std::fmt;
use std::str::FromStr;

use base64::Engine;
use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use prost::Message;
use quickwit_common::truncate_str;
use quickwit_proto::search::{PartialHit, SearchResponse};
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
    /// [`SearchAfterCursor`] of each hit, in the same order as `hits`.
    pub cursors: Vec<String>,
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
        let mut cursors = Vec::with_capacity(search_response.hits.len());
        let mut snippets = Vec::new();
        for hit in search_response.hits {
            let partial_hit = hit.partial_hit.ok_or_else(|| {
                SearchError::Internal("search response hit is missing its partial hit".to_string())
            })?;
            cursors.push(SearchAfterCursor(partial_hit).to_string());
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
            cursors,
            snippets: snippet_opt,
            elapsed_time_micros: search_response.elapsed_time_micros,
            errors: search_response.errors,
            aggregations: aggregations_opt,
        })
    }
}

/// Opaque cursor of a hit, i.e. its sort values and address, passed as `search_after` to get the
/// hits sorted after it.
///
/// It is written as the URL-safe base64 of the `PartialHit` protobuf. Datetime sort values are
/// kept as returned by the root search, i.e. in the sort datetime format of the request, which is
/// also how the root parses `search_after`. A cursor is only meaningful for the indexes, query, and
/// sort fields of the search that returned it: the root search rejects the sort values that cannot
/// match the sort fields, but cannot tell whether the cursor comes from another sort.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchAfterCursor(pub PartialHit);

impl fmt::Display for SearchAfterCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let b64_payload = BASE64_URL_SAFE_NO_PAD.encode(self.0.encode_to_vec());
        write!(formatter, "{b64_payload}")
    }
}

impl FromStr for SearchAfterCursor {
    type Err = &'static str;

    fn from_str(cursor_str: &str) -> Result<Self, Self::Err> {
        let base64_decoded: Vec<u8> = BASE64_URL_SAFE_NO_PAD
            .decode(cursor_str)
            .map_err(|_| "search_after cursor is invalid base64")?;
        let partial_hit = PartialHit::decode(base64_decoded.as_slice())
            .map_err(|_| "search_after cursor is malformed")?;
        Ok(SearchAfterCursor(partial_hit))
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

#[cfg(test)]
mod tests {
    use quickwit_proto::search::{Hit, SortByValue, SortValue};

    use super::*;

    fn partial_hit(sort_value: Option<SortValue>, sort_value2: Option<SortValue>) -> PartialHit {
        PartialHit {
            sort_value: sort_value.map(SortByValue::from),
            sort_value2: sort_value2.map(SortByValue::from),
            split_id: "01HZ7Q0K9Y5X2M3N4P5Q6R7S8T".to_string(),
            segment_ord: 1,
            doc_id: 42,
        }
    }

    #[test]
    fn test_search_after_cursor_round_trip() {
        let partial_hits = [
            partial_hit(None, None),
            partial_hit(Some(SortValue::I64(1_695_890_000_123)), None),
            partial_hit(Some(SortValue::I64(-5)), Some(SortValue::U64(u64::MAX))),
            partial_hit(Some(SortValue::F64(0.1)), Some(SortValue::Boolean(true))),
        ];
        for partial_hit in partial_hits {
            let cursor = SearchAfterCursor(partial_hit.clone()).to_string();
            assert!(
                cursor
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'),
                "cursor `{cursor}` is not URL-safe"
            );
            assert_eq!(
                SearchAfterCursor::from_str(&cursor).unwrap(),
                SearchAfterCursor(partial_hit)
            );
        }
    }

    #[test]
    fn test_search_after_cursor_encoding_is_stable() {
        // Cursors outlive the node that issued them (rolling upgrades, clients holding them), so
        // the encoding is a compatibility contract.
        let partial_hit = PartialHit {
            sort_value: Some(SortValue::I64(5).into()),
            sort_value2: None,
            split_id: "a".to_string(),
            segment_ord: 1,
            doc_id: 2,
        };
        assert_eq!(
            SearchAfterCursor(partial_hit).to_string(),
            "EgFhGAEgAlICEAU"
        );
    }

    #[test]
    fn test_search_after_cursor_rejects_invalid_cursors() {
        assert_eq!(
            SearchAfterCursor::from_str("not a cursor!").unwrap_err(),
            "search_after cursor is invalid base64"
        );
        // `_w` is valid base64 for the truncated varint `0xff`.
        assert_eq!(
            SearchAfterCursor::from_str("_w").unwrap_err(),
            "search_after cursor is malformed"
        );
    }

    #[test]
    fn test_search_response_rest_has_one_cursor_per_hit() {
        let first_partial_hit = partial_hit(Some(SortValue::I64(2)), None);
        let second_partial_hit = PartialHit {
            doc_id: 43,
            ..partial_hit(Some(SortValue::I64(1)), None)
        };
        let search_response = SearchResponse {
            num_hits: 2,
            hits: vec![
                Hit {
                    json: r#"{"body": "first"}"#.to_string(),
                    partial_hit: Some(first_partial_hit.clone()),
                    ..Default::default()
                },
                Hit {
                    json: r#"{"body": "second"}"#.to_string(),
                    partial_hit: Some(second_partial_hit.clone()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let search_response_rest = SearchResponseRest::try_from(search_response).unwrap();
        let decoded_partial_hits: Vec<PartialHit> = search_response_rest
            .cursors
            .iter()
            .map(|cursor| SearchAfterCursor::from_str(cursor).unwrap().0)
            .collect();
        assert_eq!(
            decoded_partial_hits,
            vec![first_partial_hit, second_partial_hit]
        );
    }

    #[test]
    fn test_search_response_rest_rejects_hit_without_partial_hit() {
        let search_response = SearchResponse {
            num_hits: 1,
            hits: vec![Hit {
                json: "{}".to_string(),
                partial_hit: None,
                ..Default::default()
            }],
            ..Default::default()
        };
        let error = SearchResponseRest::try_from(search_response).unwrap_err();
        assert!(matches!(error, SearchError::Internal(_)), "{error:?}");
    }
}
