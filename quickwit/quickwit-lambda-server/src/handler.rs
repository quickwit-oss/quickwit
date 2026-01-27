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
use tracing::{error, info, instrument, warn};

use crate::context::LambdaSearcherContext;
use crate::error::{LambdaError, LambdaResult};

/// Payload for leaf search Lambda invocation.
#[derive(Debug, Serialize, Deserialize)]
pub struct LeafSearchRequestPayload {
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
    event: LeafSearchRequestPayload,
    ctx: &LambdaSearcherContext,
) -> LambdaResult<LeafSearchResponsePayload> {
    // Decode base64 payload
    let request_bytes = BASE64_STANDARD
        .decode(&event.payload)
        .map_err(|e| LambdaError::Serialization(format!("base64 decode error: {}", e)))?;

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
    let mut split_search_futures: Vec<tokio::task::JoinHandle<_>> =
        Vec::with_capacity(all_splits.len());
    for (leaf_req_idx, split) in all_splits {
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
        split_search_futures.push(tokio::task::spawn(multi_index_leaf_search(
            searcher_context,
            single_split_request,
            storage_resolver,
        )));
    }

    // Collect results, preserving split order.
    let mut responses: Vec<LeafSearchResponse> = Vec::with_capacity(num_splits);
    for split_search_fut in split_search_futures {
        match split_search_fut.await {
            Ok(Ok(response)) => responses.push(response),
            Ok(Err(e)) => {
                return Err(LambdaError::Internal(format!("leaf search failed: {e}")));
            }
            Err(join_error) if join_error.is_cancelled() => {
                warn!("search task was cancelled");
                return Err(LambdaError::Cancelled);
            }
            Err(join_error) => {
                error!(error = %join_error, "search task panicked");
                return Err(LambdaError::Internal(format!(
                    "search task panicked: {join_error}"
                )));
            }
        }
    }

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
