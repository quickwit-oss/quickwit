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

use quickwit_metastore::{
    IndexMetadataResponseExt, ListSplitsQuery, ListSplitsRequestExt,
    MetastoreServiceStreamSplitsExt, Split, SplitState,
};
use quickwit_proto::metastore::{
    IndexMetadataRequest, ListSplitsRequest, MarkSplitsForDeletionRequest, MetastoreResult,
    MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::{IndexId, IndexUid};
use serde::{Deserialize, Serialize};
use tracing::info;
use warp::{Filter, Rejection};

use super::rest_handler::json_body;
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
