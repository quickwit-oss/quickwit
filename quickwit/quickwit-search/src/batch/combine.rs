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

//! Pure functions for combining and unbatching search requests and responses.
//!
//! All functions here expect **pre-normalized** requests (see `normalize.rs`).

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use quickwit_proto::search::{SearchRequest, SearchResponse};
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;

/// Computes a grouping key for a pre-normalized search request.
///
/// Requests with the same hash are *candidates* for batching. Additional
/// compatibility checks (list sort_fields, scroll, snippets, etc.) are done
/// separately before adding to a batch.
pub(super) fn batch_grouping_key(request: &SearchRequest) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    request.query_ast.hash(&mut hasher);
    request.start_timestamp.hash(&mut hasher);
    request.end_timestamp.hash(&mut hasher);
    request.index_id_patterns.hash(&mut hasher);
    hasher.finish()
}

/// Validates a batch of pre-normalized requests and builds a single combined
/// `SearchRequest`.
///
/// All requests must share the same query predicate, timestamps, and index
/// patterns. Returns an error string if validation fails.
///
/// Aggregation keys are prefixed with `__b{idx}_` to avoid collisions.
/// `unbatch_response` recovers individual results by filtering on that prefix.
pub(super) fn build_combined_request(requests: &[SearchRequest]) -> Result<SearchRequest, String> {
    assert!(!requests.is_empty());

    let first = &requests[0];

    let mut list_params: Option<(
        Vec<quickwit_proto::search::SortField>,
        u64,
        Option<quickwit_proto::search::PartialHit>,
    )> = None;
    let mut combined_aggs: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    let mut max_hits: u64 = 0;

    for (idx, req) in requests.iter().enumerate() {
        if req.index_id_patterns != first.index_id_patterns {
            return Err(format!("request {idx} has mismatched index_id_patterns"));
        }
        if req.query_ast != first.query_ast {
            return Err(format!("request {idx} has mismatched query_ast"));
        }
        if req.start_timestamp != first.start_timestamp || req.end_timestamp != first.end_timestamp
        {
            return Err(format!("request {idx} has mismatched timestamps"));
        }
        if req.scroll_ttl_secs.is_some() {
            return Err(format!(
                "request {idx} uses scroll pagination, which cannot be batched"
            ));
        }
        if !req.snippet_fields.is_empty() {
            // TODO: it could be batched with a bit of effort
            return Err(format!(
                "request {idx} uses snippet_fields, which cannot be batched"
            ));
        }
        if req.ignore_missing_indexes != first.ignore_missing_indexes {
            return Err(format!(
                "request {idx} has mismatched ignore_missing_indexes"
            ));
        }

        // list requests (max_hits > 0) must share sort_fields/offset/search_after
        if req.max_hits > 0 {
            let params = (
                req.sort_fields.clone(),
                req.start_offset,
                req.search_after.clone(),
            );
            if let Some(ref existing) = list_params {
                if params != *existing {
                    return Err(format!(
                        "list request {idx} has incompatible sort_fields/offset/search_after"
                    ));
                }
            } else {
                list_params = Some(params);
            }
            if req.max_hits > max_hits {
                max_hits = req.max_hits;
            }
        }

        if let Some(agg_json) = &req.aggregation_request {
            if !req.skip_aggregation_finalization {
                // could be supported with extra work, but all CloudPrem aggregation requests
                // use skip_aggregation_finalization=true so it's not worth the complexity
                return Err(format!(
                    "request {idx} requires aggregation finalization, which cannot be batched"
                ));
            }
            let agg_map: BTreeMap<String, serde_json::Value> = serde_json::from_str(agg_json)
                .map_err(|err| format!("request {idx} has invalid aggregation JSON: {err}"))?;
            for (key, value) in agg_map {
                combined_aggs.insert(format!("__b{idx}_{key}"), value);
            }
        }
    }

    let combined_agg_json = if combined_aggs.is_empty() {
        None
    } else {
        Some(serde_json::to_string(&combined_aggs).unwrap())
    };

    let (sort_fields, start_offset, search_after) = list_params.unwrap_or_default();

    let count_hits = if requests
        .iter()
        .any(|req| req.count_hits == quickwit_proto::search::CountHits::CountAll as i32)
    {
        quickwit_proto::search::CountHits::CountAll as i32
    } else {
        quickwit_proto::search::CountHits::Underestimate as i32
    };

    Ok(SearchRequest {
        index_id_patterns: first.index_id_patterns.clone(),
        query_ast: first.query_ast.clone(),
        start_timestamp: first.start_timestamp,
        end_timestamp: first.end_timestamp,
        max_hits,
        start_offset,
        search_after,
        aggregation_request: combined_agg_json,
        sort_fields,
        count_hits,
        skip_aggregation_finalization: true,
        ignore_missing_indexes: first.ignore_missing_indexes,
        ..Default::default()
    })
}

/// Unbatches a combined `SearchResponse` back into individual responses,
/// in the same order as the input `requests` slice.
///
/// Each request gets:
/// - `num_hits`, `elapsed_time_micros`, `errors`, `failed_splits` from the combined response
/// - Its own aggregation results, recovered by filtering on the `__b{idx}_` prefix
/// - Hits only if `max_hits > 0`, truncated to its `max_hits`
pub(super) fn unbatch_response(
    combined_response: SearchResponse,
    requests: &[SearchRequest],
) -> Vec<crate::Result<SearchResponse>> {
    let mut all_agg_results: Option<IntermediateAggregationResults> = combined_response
        .aggregation_postcard
        .as_ref()
        .and_then(|bytes| postcard::from_bytes(bytes).ok());

    requests
        .iter()
        .enumerate()
        .map(|(idx, req)| {
            let agg_postcard = all_agg_results
                .as_mut()
                .and_then(|results| {
                    let prefix = format!("__b{idx}_");
                    let keys: Vec<String> = results
                        .keys()
                        .filter(|k| k.starts_with(&prefix))
                        .cloned()
                        .collect();
                    if keys.is_empty() {
                        return None;
                    }
                    let mut per_request = IntermediateAggregationResults::default();
                    for key in keys {
                        if let Some(val) = results.remove(&key) {
                            let original_key = key.strip_prefix(&prefix).unwrap_or(&key);
                            let _ = per_request.push(original_key.to_string(), val);
                        }
                    }
                    postcard::to_stdvec(&per_request)
                        .map_err(|err| {
                            tracing::error!("failed to serialize aggregation result: {err}")
                        })
                        .ok()
                })
                // A sub-request with aggregation_request = Some("{}") (count-only, no actual
                // aggregation keys) produces no __b{idx}_ entries in the combined result.
                // The caller still expects a non-None postcard; return an empty one so it does
                // not treat the missing postcard as a request failure.
                .or_else(|| {
                    if req.aggregation_request.is_some() {
                        postcard::to_stdvec(&IntermediateAggregationResults::default()).ok()
                    } else {
                        None
                    }
                });

            Ok(SearchResponse {
                num_hits: combined_response.num_hits,
                hits: if req.max_hits > 0 {
                    let mut hits = combined_response.hits.clone();
                    hits.truncate(req.max_hits as usize);
                    hits
                } else {
                    Vec::new()
                },
                elapsed_time_micros: combined_response.elapsed_time_micros,
                errors: combined_response.errors.clone(),
                aggregation_postcard: agg_postcard,
                scroll_id: None,
                failed_splits: combined_response.failed_splits.clone(),
                num_successful_splits: combined_response.num_successful_splits,
                resource_stats: combined_response.resource_stats.clone(),
            })
        })
        .collect()
}
