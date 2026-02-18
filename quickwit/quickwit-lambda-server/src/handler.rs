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
use quickwit_proto::search::lambda_single_split_result::Outcome;
use quickwit_proto::search::{
    LambdaSearchResponses, LambdaSingleSplitResult, LeafSearchRequest, SplitIdAndFooterOffsets,
};
use quickwit_search::leaf::multi_index_leaf_search;
use serde::{Deserialize, Serialize};
use tracing::{error, info, instrument, warn};

use crate::context::LambdaSearcherContext;
use crate::error::{LambdaError, LambdaResult};

/// Payload for leaf search Lambda invocation.
#[derive(Debug, Serialize, Deserialize)]
pub struct LambdaSearchRequestPayload {
    /// Base64-encoded serialized LeafSearchRequest protobuf.
    pub payload: String,
}

/// Response from leaf search Lambda invocation.
#[derive(Debug, Serialize, Deserialize)]
pub struct LambdaSearchResponsePayload {
    /// Base64-encoded serialized `LambdaSearchResponses` protobuf (one per split).
    pub payload: String,
}

/// Handle a leaf search request in Lambda.
///
/// Returns one `LambdaSingleSplitResult` per split, each tagged with its
/// split_id. Individual split failures are reported per-split rather than
/// failing the entire invocation, so the caller can retry only failed splits.
#[instrument(skip(ctx), fields(request_id))]
pub async fn handle_leaf_search(
    event: LambdaSearchRequestPayload,
    ctx: &LambdaSearcherContext,
) -> LambdaResult<LambdaSearchResponsePayload> {
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

    // Process each split in parallel using a JoinSet. The SearchPermitProvider
    // inside SearcherContext gates concurrency based on memory budget.
    let mut split_search_joinset: tokio::task::JoinSet<(String, Result<_, String>)> =
        tokio::task::JoinSet::new();
    for (leaf_req_idx, split) in all_splits {
        let split_id = split.split_id.clone();
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
        split_search_joinset.spawn(async move {
            let result =
                multi_index_leaf_search(searcher_context, single_split_request, storage_resolver)
                    .await
                    .map_err(|err| format!("{err}"));
            (split_id, result)
        });
    }

    // Collect results. Order is irrelevant: each result is tagged with its split_id.
    let mut split_results: Vec<LambdaSingleSplitResult> = Vec::with_capacity(num_splits);
    let mut num_successes: usize = 0;
    let mut num_failures: usize = 0;
    while let Some(join_result) = split_search_joinset.join_next().await {
        match join_result {
            Ok((split_id, Ok(response))) => {
                num_successes += 1;
                split_results.push(LambdaSingleSplitResult {
                    split_id,
                    outcome: Some(Outcome::Response(response)),
                });
            }
            Ok((split_id, Err(error_msg))) => {
                num_failures += 1;
                warn!(split_id = %split_id, error = %error_msg, "split search failed");
                split_results.push(LambdaSingleSplitResult {
                    split_id,
                    outcome: Some(Outcome::Error(error_msg)),
                });
            }
            Err(join_error) if join_error.is_cancelled() => {
                warn!("search task was cancelled");
                return Err(LambdaError::Cancelled);
            }
            Err(join_error) => {
                // Panics lose the captured split_id, so we fail the entire invocation.
                error!(error = %join_error, "search task panicked");
                return Err(LambdaError::Internal(format!(
                    "search task panicked: {join_error}"
                )));
            }
        }
    }
    info!(
        num_successes,
        num_failures, "leaf search completed (per-split)"
    );

    let wrapper = LambdaSearchResponses { split_results };
    let response_bytes = wrapper.encode_to_vec();
    let payload = BASE64_STANDARD.encode(&response_bytes);

    Ok(LambdaSearchResponsePayload { payload })
}
