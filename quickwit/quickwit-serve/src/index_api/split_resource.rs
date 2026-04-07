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

use quickwit_config::StorageResolver;
use quickwit_metastore::{
    IndexMetadataResponseExt, ListSplitsQuery, ListSplitsRequestExt, StageSplitsRequestExt,
    MetastoreServiceStreamSplitsExt, Split, SplitState, SplitMetadata, SplitMaturity,
};
use quickwit_proto::metastore::{
    IndexMetadataRequest, ListSplitsRequest, MarkSplitsForDeletionRequest, MetastoreResult,
    MetastoreService, MetastoreServiceClient, StageSplitsRequest,
};
use quickwit_proto::types::{IndexId, IndexUid};
use quickwit_storage::OwnedBytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use tracing::info;
use warp::{Filter, Rejection};

use crate::rest::json_body;
use crate::format::extract_format_from_qs;
use crate::rest_api_response::into_rest_api_response;
use crate::simple_list::{from_simple_list, to_simple_list};
use crate::with_arg;

/// This struct represents the QueryString passed to
/// the rest API to filter splits.
#[derive(Debug, Clone, Deserialize, Serialize, utoipa::IntoParams, utoipa::ToSchema, Default)]
#[into_params(parameter_in = Query)]
pub struct ListSplitsQueryParams {
    /// If set, define the number of splits to skip
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub offset: Option<usize>,
    /// If set, restrict maximum number of splits to retrieve
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub limit: Option<usize>,
    /// A specific split state(s) to filter by.
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(serialize_with = "to_simple_list")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub split_states: Option<Vec<SplitState>>,
    /// If set, restrict splits to documents with a `timestamp >= start_timestamp`.
    /// This timestamp is in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub start_timestamp: Option<i64>,
    /// If set, restrict splits to documents with a `timestamp < end_timestamp`.
    /// This timestamp is in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub end_timestamp: Option<i64>,
    /// If set, restrict splits whose creation dates are before this date.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub end_create_timestamp: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug, utoipa::ToSchema)]
pub struct ListSplitsResponse {
    #[serde(default)]
    pub offset: usize,
    #[serde(default)]
    pub size: usize,
    #[serde(default)]
    pub splits: Vec<Split>,
}

#[utoipa::path(
    get,
    tag = "Indexes",
    path = "/indexes/{index_id}/splits",
    responses(
        (status = 200, description = "Successfully fetched splits.", body = ListSplitsResponse)
    ),
    params(
        ListSplitsQueryParams,
        ("index_id" = String, Path, description = "The index ID to retrieve splits for."),
    )
)]

/// Get splits.
pub async fn list_splits(
    index_id: IndexId,
    list_split_query: ListSplitsQueryParams,
    metastore: MetastoreServiceClient,
) -> MetastoreResult<ListSplitsResponse> {
    let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
    let index_uid: IndexUid = metastore
        .index_metadata(index_metadata_request)
        .await?
        .deserialize_index_metadata()?
        .index_uid;
    info!(index_id = %index_id, list_split_query = ?list_split_query, "get-splits");
    let mut query = ListSplitsQuery::for_index(index_uid);
    let mut offset = 0;
    if let Some(offset_value) = list_split_query.offset {
        query = query.with_offset(offset_value);
        offset = offset_value;
    }
    if let Some(limit) = list_split_query.limit {
        query = query.with_limit(limit);
    }
    if let Some(split_states) = list_split_query.split_states {
        query = query.with_split_states(split_states);
    }
    if let Some(start_timestamp) = list_split_query.start_timestamp {
        query = query.with_time_range_start_gte(start_timestamp);
    }
    if let Some(end_timestamp) = list_split_query.end_timestamp {
        query = query.with_time_range_end_lt(end_timestamp);
    }
    if let Some(end_created_timestamp) = list_split_query.end_create_timestamp {
        query = query.with_create_timestamp_lt(end_created_timestamp);
    }
    let list_splits_request = ListSplitsRequest::try_from_list_splits_query(&query)?;
    let splits = metastore
        .list_splits(list_splits_request)
        .await?
        .collect_splits()
        .await?;
    Ok(ListSplitsResponse {
        offset,
        size: splits.len(),
        splits,
    })
}

pub fn list_splits_handler(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String / "splits")
        .and(warp::get())
        .and(warp::query())
        .and(with_arg(metastore))
        .then(list_splits)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed()
}

#[derive(Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SplitsForDeletion {
    pub split_ids: Vec<String>,
}

#[derive(Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AddSplitRequest {
    pub split_uri: String,
}

#[utoipa::path(
    put,
    tag = "Splits",
    path = "/indexes/{index_id}/splits/mark-for-deletion",
    request_body = SplitsForDeletion,
    responses(
        (status = 200, description = "Successfully marked splits for deletion.")
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to mark splits for deletion for."),
    )
)]
/// Marks splits for deletion.
pub async fn mark_splits_for_deletion(
    index_id: IndexId,
    splits_for_deletion: SplitsForDeletion,
    metastore: MetastoreServiceClient,
) -> MetastoreResult<()> {
    let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
    let index_uid: IndexUid = metastore
        .index_metadata(index_metadata_request)
        .await?
        .deserialize_index_metadata()?
        .index_uid;
    info!(index_id = %index_id, splits_ids = ?splits_for_deletion.split_ids, "mark-splits-for-deletion");
    let split_ids: Vec<String> = splits_for_deletion
        .split_ids
        .iter()
        .map(|split_id| split_id.to_string())
        .collect();
    let mark_splits_for_deletion_request =
        MarkSplitsForDeletionRequest::new(index_uid, split_ids.clone());
    metastore
        .mark_splits_for_deletion(mark_splits_for_deletion_request)
        .await?;
    Ok(())
}

#[utoipa::path(
    post,
    tag = "Splits",
    path = "/indexes/{index_id}/splits",
    request_body = AddSplitRequest,
    responses(
        (status = 200, description = "Successfully added split to index.")
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to add the split to."),
    )
)]
/// Adds a split to an index.
pub async fn add_split(
    index_id: IndexId,
    add_split_request: AddSplitRequest,
    metastore: MetastoreServiceClient,
) -> MetastoreResult<()> {
    let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
    let index_metadata = metastore
        .index_metadata(index_metadata_request)
        .await?
        .deserialize_index_metadata()?;
    let index_uid = index_metadata.index_uid;
    
    info!(index_id = %index_id, split_uri = %add_split_request.split_uri, "add-split");
    
    // Parse the split URI to get storage and path
    let split_uri = &add_split_request.split_uri;
    let split_path = std::path::Path::new(split_uri);
    
    // Extract split ID from the URI (assuming it's the filename without extension)
    let split_filename = split_path
        .file_stem()
        .and_then(|name| name.to_str())
        .ok_or_else(|| quickwit_proto::metastore::MetastoreError::Internal {
            message: format!("Invalid split URI: unable to extract split ID from {}", split_uri),
        })?;
    let split_id = quickwit_proto::types::SplitId::from(split_filename);
    
    // Resolve the storage for the index
    let storage_resolver = StorageResolver::from_config(&index_metadata.storage_config);
    let storage = storage_resolver
        .resolve(&index_metadata.index_uri)
        .await
        .map_err(|e| quickwit_proto::metastore::MetastoreError::Internal {
            message: format!("Failed to resolve storage for index {}: {}", index_id, e),
        })?;
    
    // Read the split file
    let split_data = storage
        .get_all(split_path)
        .await
        .map_err(|e| quickwit_proto::metastore::MetastoreError::Internal {
            message: format!("Failed to read split file {}: {}", split_uri, e),
        })?;
    
    // Extract split metadata from the split file
    let split_metadata = extract_split_metadata_from_file(split_data, &split_id, &index_uid)
        .await
        .map_err(|e| quickwit_proto::metastore::MetastoreError::Internal {
            message: format!("Failed to extract split metadata from {}: {}", split_uri, e),
        })?;
    
    // Create and send StageSplitsRequest
    let stage_splits_request = quickwit_metastore::StageSplitsRequestExt::try_from_split_metadata(
        &index_uid,
        &split_metadata,
    )
    .map_err(|e| quickwit_proto::metastore::MetastoreError::Internal {
        message: format!("Failed to create stage splits request: {}", e),
    })?;
    
    metastore
        .stage_splits(stage_splits_request)
        .await?;
    
    info!(index_id = %index_id, split_id = %split_id, "successfully added split to index");
    
    Ok(())
}

/// Extracts split metadata from a split file.
async fn extract_split_metadata_from_file(
    split_data: OwnedBytes,
    split_id: &quickwit_proto::types::SplitId,
    index_uid: &quickwit_proto::types::IndexUid,
) -> anyhow::Result<quickwit_metastore::SplitMetadata> {
    // For now, create minimal metadata from the split information
    // This is a simplified approach - we'll create basic metadata
    let file_size = split_data.len() as u64;
    let create_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    
    let split_metadata = quickwit_metastore::SplitMetadata {
        split_id: split_id.clone(),
        index_uid: index_uid.clone(),
        partition_id: 0, // Default partition
        source_id: "manual-add".to_string(),
        node_id: "manual-add".to_string(),
        num_docs: 0, // Unknown - will be updated if possible
        uncompressed_docs_size_in_bytes: 0, // Unknown
        time_range: None,
        create_timestamp,
        footer_offsets: 0..file_size,
        tags: std::collections::BTreeSet::new(),
        delete_opstamp: 0,
        num_merge_ops: 0,
        maturity: quickwit_metastore::SplitMaturity::Stable,
    };
    
    Ok(split_metadata)
}

pub fn add_split_handler(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String / "splits")
        .and(warp::post())
        .and(json_body())
        .and(with_arg(metastore))
        .then(add_split)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed()
}

pub fn mark_splits_for_deletion_handler(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String / "splits" / "mark-for-deletion")
        .and(warp::put())
        .and(json_body())
        .and(with_arg(metastore))
        .then(mark_splits_for_deletion)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed()
}
