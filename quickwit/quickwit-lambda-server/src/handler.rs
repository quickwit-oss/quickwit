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

use base64::prelude::*;
use prost::Message;
use quickwit_proto::search::{
    LeafSearchRequest, LeafSearchResponse, LeafSearchResponses, SplitIdAndFooterOffsets,
};
use quickwit_search::leaf::multi_index_leaf_search;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use tracing::{info, instrument};

use crate::context::LambdaSearcherContext;
use crate::error::{LambdaError, LambdaResult};

/// Payload for leaf search Lambda invocation.
#[derive(Debug, Serialize, Deserialize)]
pub struct LeafSearchPayload {
    /// Base64-encoded serialized LeafSearchRequest protobuf.
    pub payload: String,
}

/// Response from leaf search Lambda invocation.
#[derive(Debug, Serialize, Deserialize)]
pub struct LeafSearchResponsePayload {
    /// Base64-encoded serialized `LeafSearchResponses` protobuf (one per split).
    pub payload: String,
}

/// Handle a leaf search request in Lambda.
///
/// Returns one `LeafSearchResponse` per split. Each split is processed
/// independently so that the caller can cache and merge results individually.
#[instrument(skip(ctx), fields(request_id))]
pub async fn handle_leaf_search(
    event: LeafSearchPayload,
    ctx: &LambdaSearcherContext,
) -> LambdaResult<LeafSearchResponsePayload> {
    // Decode base64 payload
    let request_bytes = BASE64_STANDARD
        .decode(&event.payload)
        .map_err(|e| LambdaError::Serialization(format!("Base64 decode error: {}", e)))?;

    // Deserialize LeafSearchRequest
    let leaf_search_request = LeafSearchRequest::decode(&request_bytes[..])?;

    let all_splits: Vec<(usize, SplitIdAndFooterOffsets)> =
        leaf_search_request
            .leaf_requests
            .iter()
            .enumerate()
            .flat_map(|(leaf_req_idx, leaf_request_ref)| {
                leaf_request_ref.split_offsets.iter().cloned().map(
                    move |split_id_and_footer_offsets| (leaf_req_idx, split_id_and_footer_offsets),
                )
            })
            .collect();

    let num_splits = all_splits.len();
    info!(num_splits, "processing leaf search request (per-split)");

    // Process each split in parallel. The SearchPermitProvider inside
    // SearcherContext gates concurrency based on memory budget.
    let mut join_set = JoinSet::new();
    for (split_idx, (leaf_req_idx, split)) in all_splits.into_iter().enumerate() {
        let leaf_request_ref = &leaf_search_request.leaf_requests[leaf_req_idx];
        let single_split_request = LeafSearchRequest {
            search_request: leaf_search_request.search_request.clone(),
            doc_mappers: leaf_search_request.doc_mappers.clone(),
            index_uris: leaf_search_request.index_uris.clone(),
            leaf_requests: vec![quickwit_proto::search::LeafRequestRef {
                index_uri_ord: leaf_request_ref.index_uri_ord,
                doc_mapper_ord: leaf_request_ref.doc_mapper_ord,
                split_offsets: vec![split],
            }],
        };

        let searcher_context = ctx.searcher_context.clone();
        let storage_resolver = ctx.storage_resolver.clone();
        join_set.spawn(async move {
            let response =
                multi_index_leaf_search(searcher_context, single_split_request, &storage_resolver)
                    .await;
            (split_idx, response)
        });
    }

    // Collect results, preserving split order.
    let mut indexed_responses: Vec<(usize, LeafSearchResponse)> = Vec::with_capacity(num_splits);
    while let Some(join_result) = join_set.join_next().await {
        let (split_idx, search_result) = join_result
            .map_err(|e| LambdaError::Internal(format!("split search task failed: {e}")))?;
        let response =
            search_result.map_err(|e| LambdaError::Internal(format!("leaf search failed: {e}")))?;
        indexed_responses.push((split_idx, response));
    }
    indexed_responses.sort_by_key(|(idx, _)| *idx);
    let responses: Vec<LeafSearchResponse> =
        indexed_responses.into_iter().map(|(_, r)| r).collect();

    info!(
        num_responses = responses.len(),
        "leaf search completed (per-split)"
    );

    // Serialize as LeafSearchResponses wrapper
    let wrapper = LeafSearchResponses { responses };
    let response_bytes = wrapper.encode_to_vec();
    let payload = BASE64_STANDARD.encode(&response_bytes);

    Ok(LeafSearchResponsePayload { payload })
}

/// Decode a LeafSearchRequest from base64-encoded protobuf bytes.
#[allow(dead_code)]
pub fn decode_leaf_search_request(payload: &str) -> LambdaResult<LeafSearchRequest> {
    let bytes = BASE64_STANDARD
        .decode(payload)
        .map_err(|e| LambdaError::Serialization(format!("Base64 decode error: {}", e)))?;
    LeafSearchRequest::decode(&bytes[..]).map_err(LambdaError::from)
}

/// Encode a LeafSearchResponse to base64-encoded protobuf bytes.
#[allow(dead_code)]
pub fn encode_leaf_search_response(response: &LeafSearchResponse) -> String {
    BASE64_STANDARD.encode(response.encode_to_vec())
}
