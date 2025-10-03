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

use std::sync::Arc;

use bytes::Bytes;
use quickwit_common::uri::Uri;
use quickwit_config::{
    ConfigFormat, NodeConfig, load_index_config_update, validate_index_id_pattern,
};
use quickwit_index_management::{IndexService, IndexServiceError};
use quickwit_metastore::{
    IndexMetadata, IndexMetadataResponseExt, ListIndexesMetadataResponseExt, ListSplitsQuery,
    ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, Split, SplitInfo, SplitState,
};
use quickwit_proto::metastore::{
    IndexMetadataRequest, ListIndexesMetadataRequest, ListSplitsRequest, MetastoreError,
    MetastoreResult, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::IndexId;
use serde::{Deserialize, Serialize};
use tracing::info;
use warp::{Filter, Rejection};

use super::rest_handler::log_failure;
use crate::format::{extract_config_format, extract_format_from_qs};
use crate::rest_api_response::into_rest_api_response;
use crate::simple_list::from_simple_list;
use crate::with_arg;

pub fn get_index_metadata_handler(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String)
        .and(warp::get())
        .and(with_arg(metastore))
        .then(get_index_metadata)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed()
}

pub async fn get_index_metadata(
    index_id: IndexId,
    metastore: MetastoreServiceClient,
) -> MetastoreResult<IndexMetadata> {
    info!(index_id = %index_id, "get-index-metadata");
    let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
    let index_metadata = metastore
        .index_metadata(index_metadata_request)
        .await?
        .deserialize_index_metadata()?;
    Ok(index_metadata)
}

/// This struct represents the QueryString passed to
/// the rest API to filter indexes.
#[derive(Debug, Clone, Deserialize, Serialize, utoipa::IntoParams, utoipa::ToSchema, Default)]
#[into_params(parameter_in = Query)]
pub struct ListIndexesQueryParams {
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub index_id_patterns: Option<Vec<String>>,
}

pub fn list_indexes_metadata_handler(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes")
        .and(warp::get())
        .and(warp::query())
        .and(with_arg(metastore))
        .then(list_indexes_metadata)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed()
}

/// Describes an index with its main information and statistics.
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
pub struct IndexStats {
    #[schema(value_type = String)]
    pub index_id: IndexId,
    #[schema(value_type = String)]
    pub index_uri: Uri,
    pub num_published_splits: usize,
    pub size_published_splits: u64,
    pub num_published_docs: u64,
    pub size_published_docs_uncompressed: u64,
    pub timestamp_field_name: Option<String>,
    pub min_timestamp: Option<i64>,
    pub max_timestamp: Option<i64>,
}

#[utoipa::path(
    get,
    tag = "Indexes",
    path = "/indexes/{index_id}/describe",
    responses(
        (status = 200, description = "Successfully fetched stats about Index.", body = IndexStats)
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to describe."),
    )
)]

/// Describes an index.
pub async fn describe_index(
    index_id: IndexId,
    metastore: MetastoreServiceClient,
) -> MetastoreResult<IndexStats> {
    let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
    let index_metadata = metastore
        .index_metadata(index_metadata_request)
        .await?
        .deserialize_index_metadata()?;
    let query = ListSplitsQuery::for_index(index_metadata.index_uid.clone());
    let list_splits_request = ListSplitsRequest::try_from_list_splits_query(&query)?;
    let splits = metastore
        .list_splits(list_splits_request)
        .await?
        .collect_splits()
        .await?;
    let published_splits: Vec<Split> = splits
        .into_iter()
        .filter(|split| split.split_state == SplitState::Published)
        .collect();
    let mut total_num_docs = 0;
    let mut total_num_bytes = 0;
    let mut total_uncompressed_num_bytes = 0;
    let mut min_timestamp: Option<i64> = None;
    let mut max_timestamp: Option<i64> = None;

    for split in &published_splits {
        total_num_docs += split.split_metadata.num_docs as u64;
        total_num_bytes += split.split_metadata.footer_offsets.end;
        total_uncompressed_num_bytes += split.split_metadata.uncompressed_docs_size_in_bytes;

        if let Some(time_range) = &split.split_metadata.time_range {
            min_timestamp = min_timestamp
                .min(Some(*time_range.start()))
                .or(Some(*time_range.start()));
            max_timestamp = max_timestamp
                .max(Some(*time_range.end()))
                .or(Some(*time_range.end()));
        }
    }

    let index_config = index_metadata.into_index_config();
    let index_stats = IndexStats {
        index_id,
        index_uri: index_config.index_uri.clone(),
        num_published_splits: published_splits.len(),
        size_published_splits: total_num_bytes,
        num_published_docs: total_num_docs,
        size_published_docs_uncompressed: total_uncompressed_num_bytes,
        timestamp_field_name: index_config.doc_mapping.timestamp_field,
        min_timestamp,
        max_timestamp,
    };

    Ok(index_stats)
}

pub fn describe_index_handler(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String / "describe")
        .and(warp::get())
        .and(with_arg(metastore))
        .then(describe_index)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed()
}

#[utoipa::path(
    get,
    tag = "Indexes",
    path = "/indexes",
    responses(
        // We return `VersionedIndexMetadata` as it's the serialized model view.
        (status = 200, description = "Successfully fetched all indexes.", body = [VersionedIndexMetadata])
    ),
    params(
        ListIndexesQueryParams,
        ("index_id_patterns" = String, Path, description = "The index ID pattern to retrieve indexes for."),
    )
)]
/// Gets indexes metadata.
pub async fn list_indexes_metadata(
    list_indexes_params: ListIndexesQueryParams,
    metastore: MetastoreServiceClient,
) -> MetastoreResult<Vec<IndexMetadata>> {
    let list_indexes_metata_request =
        if let Some(index_id_patterns) = list_indexes_params.index_id_patterns {
            for index_id_pattern in &index_id_patterns {
                validate_index_id_pattern(index_id_pattern, true).map_err(|error| {
                    MetastoreError::InvalidArgument {
                        message: error.to_string(),
                    }
                })?;
            }
            ListIndexesMetadataRequest { index_id_patterns }
        } else {
            ListIndexesMetadataRequest::all()
        };
    metastore
        .list_indexes_metadata(list_indexes_metata_request)
        .await?
        .deserialize_indexes_metadata()
        .await
}

#[derive(Deserialize, utoipa::IntoParams, utoipa::ToSchema)]
#[into_params(parameter_in = Query)]
pub struct CreateIndexQueryParams {
    #[serde(default)]
    overwrite: bool,
}

pub fn create_index_handler(
    index_service: IndexService,
    node_config: Arc<NodeConfig>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes")
        .and(warp::post())
        .and(warp::query())
        .and(extract_config_format())
        .and(warp::body::content_length_limit(1024 * 1024))
        .and(warp::filters::body::bytes())
        .and(with_arg(index_service))
        .and(with_arg(node_config))
        .then(create_index)
        .map(log_failure("failed to create index"))
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed()
}

#[utoipa::path(
    post,
    tag = "Indexes",
    path = "/indexes",
    request_body = VersionedIndexConfig,
    responses(
        // We return `VersionedIndexMetadata` as it's the serialized model view.
        (status = 200, description = "Successfully created index.", body = VersionedIndexMetadata)
    ),
    params(
        CreateIndexQueryParams,
    )
)]
/// Creates index.
pub async fn create_index(
    create_index_query_params: CreateIndexQueryParams,
    config_format: ConfigFormat,
    index_config_bytes: Bytes,
    mut index_service: IndexService,
    node_config: Arc<NodeConfig>,
) -> Result<IndexMetadata, IndexServiceError> {
    let index_config = quickwit_config::load_index_config_from_user_config(
        config_format,
        &index_config_bytes,
        &node_config.default_index_root_uri,
    )
    .map_err(IndexServiceError::InvalidConfig)?;
    info!(index_id = %index_config.index_id, overwrite = create_index_query_params.overwrite, "create-index");
    index_service
        .create_index(index_config, create_index_query_params.overwrite)
        .await
}

/// Query parameters for update index queries
#[derive(Deserialize, Debug, Eq, PartialEq, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct UpdateQueryParams {
    /// Create the index if it doesn't exist yet
    #[serde(default)]
    pub create: bool,
}

fn update_index_qp() -> impl Filter<Extract = (UpdateQueryParams,), Error = Rejection> + Clone {
    warp::query::<UpdateQueryParams>()
}

pub fn update_index_handler(
    index_service: IndexService,
    node_config: Arc<NodeConfig>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String)
        .and(warp::put())
        .and(extract_config_format())
        .and(update_index_qp())
        .and(warp::body::content_length_limit(1024 * 1024))
        .and(warp::filters::body::bytes())
        .and(with_arg(index_service))
        .and(with_arg(node_config))
        .then(update_index)
        .map(log_failure("failed to update index"))
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed()
}

#[utoipa::path(
    put,
    tag = "Indexes",
    path = "/indexes/{index_id}",
    request_body = VersionedIndexConfig,
    responses(
        (status = 200, description = "Successfully updated the index configuration.", body = VersionedIndexMetadata)
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to update."),
        UpdateQueryParams,
    )
)]
/// Updates an existing index.
///
/// This endpoint follows PUT semantics, which means that all the fields of the
/// current configuration are replaced by the values specified in this request
/// or the associated defaults. In particular, if the field is optional (e.g.
/// `retention_policy`), omitting it will delete the associated configuration.
/// If the new configuration file contains updates that cannot be applied, the
/// request fails, and none of the updates are applied.
pub async fn update_index(
    target_index_id: IndexId,
    config_format: ConfigFormat,
    query_params: UpdateQueryParams,
    index_config_bytes: Bytes,
    mut index_service: IndexService,
    node_config: Arc<NodeConfig>,
) -> Result<IndexMetadata, IndexServiceError> {
    info!(index_id = %target_index_id, "update-index");

    let metastore = index_service.metastore();
    let index_metadata_request = IndexMetadataRequest::for_index_id(target_index_id.to_string());
    let current_index_metadata_res = metastore.index_metadata(index_metadata_request).await;

    let current_index_metadata_ser = match current_index_metadata_res {
        Ok(index_metadata) => index_metadata,
        Err(MetastoreError::NotFound(_)) if query_params.create => {
            let index_config = quickwit_config::load_index_config_from_user_config(
                config_format,
                &index_config_bytes,
                &node_config.default_index_root_uri,
            )
            .map_err(IndexServiceError::InvalidConfig)?;
            if index_config.index_id != target_index_id {
                return Err(IndexServiceError::InvalidConfig(anyhow::anyhow!(
                    "`index_id` in config file does not match index_id from query path"
                )));
            }
            info!(index_id = %index_config.index_id, "create-index-on-update");
            match index_service.create_index(index_config, false).await {
                Err(IndexServiceError::Metastore(MetastoreError::AlreadyExists(_))) => {
                    // If the index was created just after we tried to update it, try to update as
                    // if nothing happened. But if it gets deleted again before we update it, just
                    // error out
                    let index_metadata_request =
                        IndexMetadataRequest::for_index_id(target_index_id.to_string());
                    metastore.index_metadata(index_metadata_request).await?
                }
                other => return other,
            }
        }
        Err(e) => return Err(e.into()),
    };
    let current_index_metadata = current_index_metadata_ser.deserialize_index_metadata()?;
    let index_uid = current_index_metadata.index_uid.clone();
    let current_index_config = current_index_metadata.into_index_config();

    let new_index_config = load_index_config_update(
        config_format,
        &index_config_bytes,
        &node_config.default_index_root_uri,
        &current_index_config,
    )
    .map_err(IndexServiceError::InvalidConfig)?;

    let index_metadata = index_service
        .update_index(index_uid, new_index_config)
        .await?;
    Ok(index_metadata)
}

pub fn clear_index_handler(
    index_service: IndexService,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String / "clear")
        .and(warp::put())
        .and(with_arg(index_service))
        .then(clear_index)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed()
}

#[utoipa::path(
    put,
    tag = "Indexes",
    path = "/indexes/{index_id}/clear",
    responses(
        (status = 200, description = "Successfully cleared index.")
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to clear."),
    )
)]
/// Removes all of the data (splits, queued document) associated with the index, but keeps the index
/// configuration. (See also, `delete-index`).
pub async fn clear_index(
    index_id: IndexId,
    mut index_service: IndexService,
) -> Result<(), IndexServiceError> {
    info!(index_id = %index_id, "clear-index");
    index_service.clear_index(&index_id).await
}

#[derive(Deserialize, utoipa::IntoParams, utoipa::ToSchema)]
#[into_params(parameter_in = Query)]
pub struct DeleteIndexQueryParam {
    #[serde(default)]
    dry_run: bool,
}

pub fn delete_index_handler(
    index_service: IndexService,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String)
        .and(warp::delete())
        .and(warp::query())
        .and(with_arg(index_service))
        .then(delete_index)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed()
}

#[utoipa::path(
    delete,
    tag = "Indexes",
    path = "/indexes/{index_id}",
    responses(
        // We return `VersionedIndexMetadata` as it's the serialized model view.
        (status = 200, description = "Successfully deleted index.", body = [FileEntry])
    ),
    params(
        DeleteIndexQueryParam,
        ("index_id" = String, Path, description = "The index ID to delete."),
    )
)]
/// Deletes index.
pub async fn delete_index(
    index_id: IndexId,
    delete_index_query_param: DeleteIndexQueryParam,
    mut index_service: IndexService,
) -> Result<Vec<SplitInfo>, IndexServiceError> {
    info!(index_id = %index_id, dry_run = delete_index_query_param.dry_run, "delete-index");
    index_service
        .delete_index(&index_id, delete_index_query_param.dry_run)
        .await
}
