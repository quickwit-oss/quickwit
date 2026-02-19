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

use std::str::FromStr;
use std::sync::Arc;

use base64::prelude::*;
use prost::Message;
use quickwit_common::uri::Uri;
use quickwit_doc_mapper::DocMapper;
use quickwit_proto::search::lambda_single_split_result::Outcome;
use quickwit_proto::search::{
    LambdaSearchResponses, LambdaSingleSplitResult, LeafRequestRef, LeafSearchRequest,
    SearchRequest,
};
use quickwit_search::leaf::single_doc_mapping_leaf_search;
use quickwit_storage::Storage;
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
    let request_bytes: Vec<u8> = BASE64_STANDARD
        .decode(&event.payload)
        .map_err(|err| LambdaError::Serialization(format!("base64 decode error: {}", err)))?;

    // Deserialize LeafSearchRequest
    let leaf_search_request = LeafSearchRequest::decode(&request_bytes[..])?;

    // Unpack the shared fields once instead of cloning per split.
    let search_request: Arc<SearchRequest> = leaf_search_request
        .search_request
        .ok_or_else(|| LambdaError::Internal("no search request".to_string()))?
        .into();

    let doc_mappers: Vec<Arc<DocMapper>> = leaf_search_request
        .doc_mappers
        .iter()
        .map(String::as_str)
        .map(serde_json::from_str::<Arc<DocMapper>>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| {
            LambdaError::Internal(format!("failed to deserialize doc mapper: `{err}`"))
        })?;

    // Resolve storage for every index URI upfront.
    let mut storages: Vec<Arc<dyn quickwit_storage::Storage>> =
        Vec::with_capacity(leaf_search_request.index_uris.len());
    for uri_str in &leaf_search_request.index_uris {
        let uri = Uri::from_str(uri_str)
            .map_err(|err| LambdaError::Internal(format!("invalid index uri: {err}")))?;
        let storage =
            ctx.storage_resolver.resolve(&uri).await.map_err(|err| {
                LambdaError::Internal(format!("failed to resolve storage: {err}"))
            })?;
        storages.push(storage);
    }

    let split_results: Vec<LambdaSingleSplitResult> = lambda_leaf_search(
        search_request,
        leaf_search_request.leaf_requests,
        &doc_mappers[..],
        &storages[..],
        ctx,
    )
    .await?;
    let wrapper = LambdaSearchResponses { split_results };
    let response_bytes = wrapper.encode_to_vec();
    let payload = BASE64_STANDARD.encode(&response_bytes);

    Ok(LambdaSearchResponsePayload { payload })
}

/// Lambda leaf search returns individual split results.
async fn lambda_leaf_search(
    search_request: Arc<SearchRequest>,
    leaf_req_ref: Vec<LeafRequestRef>,
    doc_mappers: &[Arc<DocMapper>],
    storages: &[Arc<dyn Storage>],
    ctx: &LambdaSearcherContext,
) -> LambdaResult<Vec<LambdaSingleSplitResult>> {
    // Flatten leaf_requests into per-split tasks using pre-resolved Arc references.
    let mut split_search_joinset: tokio::task::JoinSet<(String, Result<_, String>)> =
        tokio::task::JoinSet::new();

    for leaf_req in leaf_req_ref {
        let doc_mapper = doc_mappers
            .get(leaf_req.doc_mapper_ord as usize)
            .ok_or_else(|| {
                LambdaError::Internal(format!(
                    "doc_mapper_ord out of bounds: {}",
                    leaf_req.doc_mapper_ord
                ))
            })?
            .clone();
        let storage = storages[leaf_req.index_uri_ord as usize].clone();

        for split_id_and_footer_offsets in leaf_req.split_offsets {
            let split_id = split_id_and_footer_offsets.split_id.clone();
            let searcher_context = ctx.searcher_context.clone();
            let search_request = search_request.clone();
            let doc_mapper = doc_mapper.clone();
            let storage = storage.clone();
            let split = split_id_and_footer_offsets.clone();
            split_search_joinset.spawn(async move {
                let result = single_doc_mapping_leaf_search(
                    searcher_context,
                    search_request,
                    storage,
                    vec![split],
                    doc_mapper,
                )
                .await
                .map_err(|err| format!("{err}"));
                (split_id, result)
            });
        }
    }

    let num_splits = split_search_joinset.len();
    info!(num_splits, "processing leaf search request (per-split)");

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

    Ok(split_results)
}
