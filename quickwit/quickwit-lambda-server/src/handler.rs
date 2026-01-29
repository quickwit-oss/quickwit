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
use quickwit_proto::search::{LeafSearchRequest, LeafSearchResponse};
use quickwit_search::leaf::multi_index_leaf_search;
use serde::{Deserialize, Serialize};
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
    /// Base64-encoded serialized LeafSearchResponse protobuf.
    pub payload: String,
}

/// Handle a leaf search request in Lambda.
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

    let num_splits: usize = leaf_search_request
        .leaf_requests
        .iter()
        .map(|leaf_request_ref| leaf_request_ref.split_offsets.len())
        .sum();

    info!(num_splits, "processing leaf search request");

    // Execute leaf search
    let leaf_search_response = multi_index_leaf_search(
        ctx.searcher_context.clone(),
        leaf_search_request,
        &ctx.storage_resolver,
    )
    .await?;

    info!(
        num_hits = leaf_search_response.num_hits,
        num_successful_splits = leaf_search_response.num_successful_splits,
        "leaf search completed"
    );

    // Serialize response
    let response_bytes = leaf_search_response.encode_to_vec();
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
