// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::sync::Arc;

use bytes::Bytes;
use hyper::header::CONTENT_TYPE;
use quickwit_common::uri::Uri;
use quickwit_common::FileEntry;
use quickwit_config::{
    load_source_config_from_user_config, ConfigFormat, QuickwitConfig, SourceConfig, SourceParams,
    CLI_INGEST_SOURCE_ID, INGEST_API_SOURCE_ID,
};
use quickwit_core::{IndexService, IndexServiceError};
use quickwit_metastore::{
    IndexMetadata, ListSplitsQuery, Metastore, MetastoreError, Split, SplitState,
};
use quickwit_proto::IndexUid;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::info;
use warp::{Filter, Rejection};

use crate::format::extract_format_from_qs;
use crate::json_api_response::make_json_api_response;
use crate::simple_list::{from_simple_list, to_simple_list};
use crate::with_arg;

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        create_index,
        clear_index,
        delete_index,
        get_indexes_metadatas,
        list_splits,
        describe_index,
        mark_splits_for_deletion,
        create_source,
        reset_source_checkpoint,
        toggle_source,
        delete_source,
    ),
    components(schemas(ToggleSource, SplitsForDeletion, IndexStats))
)]
pub struct IndexApi;

pub fn index_management_handlers(
    index_service: Arc<IndexService>,
    quickwit_config: Arc<QuickwitConfig>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    // Indexes handlers.
    get_index_metadata_handler(index_service.metastore())
        .or(get_indexes_metadatas_handler(index_service.metastore()))
        .or(create_index_handler(index_service.clone(), quickwit_config))
        .or(clear_index_handler(index_service.clone()))
        .or(delete_index_handler(index_service.clone()))
        // Splits handlers
        .or(list_splits_handler(index_service.metastore()))
        .or(describe_index_handler(index_service.metastore()))
        .or(mark_splits_for_deletion_handler(index_service.metastore()))
        // Sources handlers.
        .or(reset_source_checkpoint_handler(index_service.metastore()))
        .or(toggle_source_handler(index_service.metastore()))
        .or(create_source_handler(index_service.clone()))
        .or(get_source_handler(index_service.metastore()))
        .or(delete_source_handler(index_service.metastore()))
}

fn json_body<T: DeserializeOwned + Send>(
) -> impl Filter<Extract = (T,), Error = warp::Rejection> + Clone {
    warp::body::content_length_limit(1024 * 1024).and(warp::body::json())
}

#[derive(Debug, Error)]
#[error(
    "Unsupported content-type header. Choices are application/json, application/toml and \
     application/yaml."
)]
pub struct UnsupportedContentType;
impl warp::reject::Reject for UnsupportedContentType {}

pub fn config_format_filter() -> impl Filter<Extract = (ConfigFormat,), Error = Rejection> + Copy {
    warp::filters::header::optional::<mime_guess::Mime>(CONTENT_TYPE.as_str()).and_then(
        |mime_opt: Option<mime_guess::Mime>| {
            if let Some(mime) = mime_opt {
                let config_format = match mime.subtype().as_str() {
                    "json" => ConfigFormat::Json,
                    "yaml" => ConfigFormat::Yaml,
                    "toml" => ConfigFormat::Toml,
                    _ => {
                        return futures::future::err(warp::reject::custom(UnsupportedContentType));
                    }
                };
                return futures::future::ok(config_format);
            }
            futures::future::ok(ConfigFormat::Json)
        },
    )
}

fn get_index_metadata_handler(
    metastore: Arc<dyn Metastore>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String)
        .and(warp::get())
        .and(with_arg(metastore))
        .then(get_index_metadata)
        .and(extract_format_from_qs())
        .map(make_json_api_response)
}

async fn get_index_metadata(
    index_id: String,
    metastore: Arc<dyn Metastore>,
) -> Result<IndexMetadata, MetastoreError> {
    info!(index_id = %index_id, "get-index-metadata");
    metastore.index_metadata(&index_id).await
}

fn get_indexes_metadatas_handler(
    metastore: Arc<dyn Metastore>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes")
        .and(warp::get())
        .and(with_arg(metastore))
        .then(get_indexes_metadatas)
        .and(extract_format_from_qs())
        .map(make_json_api_response)
}

/// Describes an index with its main information and statistics.
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
struct IndexStats {
    pub index_id: String,
    #[schema(value_type = String)]
    pub index_uri: Uri,
    pub num_published_splits: usize,
    pub num_published_docs: u64,
    pub size_published_docs: u64,
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
async fn describe_index(
    index_id: String,
    metastore: Arc<dyn Metastore>,
) -> Result<IndexStats, MetastoreError> {
    let index_metadata = metastore.index_metadata(&index_id).await?;
    let query = ListSplitsQuery::for_index(index_metadata.index_uid.clone());
    let splits = metastore.list_splits(query).await?;
    let published_splits: Vec<Split> = splits
        .into_iter()
        .filter(|split| split.split_state == SplitState::Published)
        .collect();
    let mut total_num_docs = 0;
    let mut total_bytes = 0;
    let mut min_timestamp: Option<i64> = None;
    let mut max_timestamp: Option<i64> = None;

    for split in &published_splits {
        total_num_docs += split.split_metadata.num_docs as u64;
        total_bytes += split.split_metadata.footer_offsets.end;

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
        num_published_docs: total_num_docs,
        size_published_docs: total_bytes,
        timestamp_field_name: index_config.doc_mapping.timestamp_field,
        min_timestamp,
        max_timestamp,
    };

    Ok(index_stats)
}

fn describe_index_handler(
    metastore: Arc<dyn Metastore>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String / "describe")
        .and(warp::get())
        .and(with_arg(metastore))
        .then(describe_index)
        .and(extract_format_from_qs())
        .map(make_json_api_response)
}

/// This struct represents the QueryString passed to
/// the rest API to filter splits.
#[derive(Debug, Clone, Deserialize, Serialize, utoipa::IntoParams, utoipa::ToSchema, Default)]
#[into_params(parameter_in = Query)]
pub struct ListSplitsQueryParams {
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

#[utoipa::path(
    get,
    tag = "Indexes",
    path = "/indexes/{index_id}/splits",
    responses(
        (status = 200, description = "Successfully fetched splits.", body = [Split])
    ),
    params(
        ListSplitsQueryParams,
        ("index_id" = String, Path, description = "The index ID to retrieve delete tasks for."),
    )
)]

/// Get splits.
async fn list_splits(
    index_id: String,
    list_split_query: ListSplitsQueryParams,
    metastore: Arc<dyn Metastore>,
) -> Result<Vec<Split>, MetastoreError> {
    let index_uid: IndexUid = metastore.index_metadata(&index_id).await?.index_uid;
    info!(index_id = %index_id, list_split_query = ?list_split_query, "get-splits");
    let mut query = ListSplitsQuery::for_index(index_uid);
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
    metastore.list_splits(query).await
}

fn list_splits_handler(
    metastore: Arc<dyn Metastore>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String / "splits")
        .and(warp::get())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
        .and(with_arg(metastore))
        .then(list_splits)
        .and(extract_format_from_qs())
        .map(make_json_api_response)
}

#[derive(Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
struct SplitsForDeletion {
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
async fn mark_splits_for_deletion(
    index_id: String,
    splits_for_deletion: SplitsForDeletion,
    metastore: Arc<dyn Metastore>,
) -> Result<(), MetastoreError> {
    let index_uid: IndexUid = metastore.index_metadata(&index_id).await?.index_uid;
    info!(index_id = %index_id, splits_ids = ?splits_for_deletion.split_ids, "mark-splits-for-deletion");
    let split_ids: Vec<&str> = splits_for_deletion
        .split_ids
        .iter()
        .map(|split_id| split_id.as_ref())
        .collect();
    metastore
        .mark_splits_for_deletion(index_uid, &split_ids)
        .await
}

fn mark_splits_for_deletion_handler(
    metastore: Arc<dyn Metastore>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String / "splits" / "mark-for-deletion")
        .and(warp::put())
        .and(json_body())
        .and(with_arg(metastore))
        .then(mark_splits_for_deletion)
        .and(extract_format_from_qs())
        .map(make_json_api_response)
}

#[utoipa::path(
    get,
    tag = "Indexes",
    path = "/indexes",
    responses(
        // We return `VersionedIndexMetadata` as it's the serialized model view.
        (status = 200, description = "Successfully fetched all indexes.", body = [VersionedIndexMetadata])
    ),
)]
/// Gets indexes metadata.
async fn get_indexes_metadatas(
    metastore: Arc<dyn Metastore>,
) -> Result<Vec<IndexMetadata>, MetastoreError> {
    info!("get-indexes-metadatas");
    metastore.list_indexes_metadatas().await
}

#[derive(Deserialize, utoipa::IntoParams, utoipa::ToSchema)]
#[into_params(parameter_in = Query)]
struct CreateIndexQueryParams {
    #[serde(default)]
    overwrite: bool,
}

fn create_index_handler(
    index_service: Arc<IndexService>,
    quickwit_config: Arc<QuickwitConfig>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes")
        .and(warp::post())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
        .and(config_format_filter())
        .and(warp::body::content_length_limit(1024 * 1024))
        .and(warp::filters::body::bytes())
        .and(with_arg(index_service))
        .and(with_arg(quickwit_config))
        .then(create_index)
        .and(extract_format_from_qs())
        .map(make_json_api_response)
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
async fn create_index(
    create_index_query_params: CreateIndexQueryParams,
    config_format: ConfigFormat,
    index_config_bytes: Bytes,
    index_service: Arc<IndexService>,
    quickwit_config: Arc<QuickwitConfig>,
) -> Result<IndexMetadata, IndexServiceError> {
    let index_config = quickwit_config::load_index_config_from_user_config(
        config_format,
        &index_config_bytes,
        &quickwit_config.default_index_root_uri,
    )
    .map_err(IndexServiceError::InvalidConfig)?;
    info!(index_id = %index_config.index_id, overwrite = create_index_query_params.overwrite, "create-index");
    index_service
        .create_index(index_config, create_index_query_params.overwrite)
        .await
}

fn clear_index_handler(
    index_service: Arc<IndexService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String / "clear")
        .and(warp::put())
        .and(with_arg(index_service))
        .then(clear_index)
        .and(extract_format_from_qs())
        .map(make_json_api_response)
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
async fn clear_index(
    index_id: String,
    index_service: Arc<IndexService>,
) -> Result<(), IndexServiceError> {
    info!(index_id = %index_id, "clear-index");
    index_service.clear_index(&index_id).await
}

#[derive(Deserialize, utoipa::IntoParams, utoipa::ToSchema)]
#[into_params(parameter_in = Query)]
struct DeleteIndexQueryParam {
    #[serde(default)]
    dry_run: bool,
}

fn delete_index_handler(
    index_service: Arc<IndexService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String)
        .and(warp::delete())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
        .and(with_arg(index_service))
        .then(delete_index)
        .and(extract_format_from_qs())
        .map(make_json_api_response)
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
async fn delete_index(
    index_id: String,
    delete_index_query_param: DeleteIndexQueryParam,
    index_service: Arc<IndexService>,
) -> Result<Vec<FileEntry>, IndexServiceError> {
    info!(index_id = %index_id, dry_run = delete_index_query_param.dry_run, "delete-index");
    index_service
        .delete_index(&index_id, delete_index_query_param.dry_run)
        .await
}

fn create_source_handler(
    index_service: Arc<IndexService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String / "sources")
        .and(warp::post())
        .and(config_format_filter())
        .and(warp::body::content_length_limit(1024 * 1024))
        .and(warp::filters::body::bytes())
        .and(with_arg(index_service))
        .then(create_source)
        .and(extract_format_from_qs())
        .map(make_json_api_response)
}

#[utoipa::path(
    post,
    tag = "Sources",
    path = "/indexes/{index_id}/sources",
    request_body = VersionedSourceConfig,
    responses(
        // We return `VersionedSourceConfig` as it's the serialized model view.
        (status = 200, description = "Successfully created source.", body = VersionedSourceConfig)
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to create a source for."),
    )
)]
/// Creates Source.
async fn create_source(
    index_id: String,
    config_format: ConfigFormat,
    source_config_bytes: Bytes,
    index_service: Arc<IndexService>,
) -> Result<SourceConfig, IndexServiceError> {
    let source_config: SourceConfig =
        load_source_config_from_user_config(config_format, &source_config_bytes)
            .map_err(IndexServiceError::InvalidConfig)?;
    if let SourceParams::File(_) = &source_config.source_params {
        return Err(IndexServiceError::OperationNotAllowed(
            "File sources are limited to a local usage. Please use the CLI command `quickwit tool \
             local-ingest` to ingest data from a file."
                .to_string(),
        ));
    }
    let index_uid: IndexUid = index_service
        .metastore()
        .index_metadata(&index_id)
        .await?
        .index_uid;
    info!(index_id = %index_id, source_id = %source_config.source_id, "create-source");
    index_service.create_source(index_uid, source_config).await
}

fn get_source_handler(
    metastore: Arc<dyn Metastore>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String / "sources" / String)
        .and(warp::get())
        .and(with_arg(metastore))
        .then(get_source)
        .and(extract_format_from_qs())
        .map(make_json_api_response)
}

async fn get_source(
    index_id: String,
    source_id: String,
    metastore: Arc<dyn Metastore>,
) -> Result<SourceConfig, MetastoreError> {
    info!(index_id = %index_id, source_id = %source_id, "get-source");
    let source_config = metastore
        .index_metadata(&index_id)
        .await?
        .sources
        .get(&source_id)
        .ok_or_else(|| MetastoreError::SourceDoesNotExist {
            source_id: source_id.to_string(),
        })?
        .clone();
    Ok(source_config)
}

fn reset_source_checkpoint_handler(
    metastore: Arc<dyn Metastore>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String / "sources" / String / "reset-checkpoint")
        .and(warp::put())
        .and(with_arg(metastore))
        .then(reset_source_checkpoint)
        .and(extract_format_from_qs())
        .map(make_json_api_response)
}

#[utoipa::path(
    put,
    tag = "Sources",
    path = "/indexes/{index_id}/sources/{source_id}/reset-checkpoint",
    responses(
        (status = 200, description = "Successfully reset source checkpoint.")
    ),
    params(
        ("index_id" = String, Path, description = "The index ID of the source."),
        ("source_id" = String, Path, description = "The source ID whose checkpoint is reset."),
    )
)]
/// Resets source checkpoint.
async fn reset_source_checkpoint(
    index_id: String,
    source_id: String,
    metastore: Arc<dyn Metastore>,
) -> Result<(), MetastoreError> {
    let index_uid: IndexUid = metastore.index_metadata(&index_id).await?.index_uid;
    info!(index_id = %index_id, source_id = %source_id, "reset-checkpoint");
    metastore
        .reset_source_checkpoint(index_uid, &source_id)
        .await
}

fn toggle_source_handler(
    metastore: Arc<dyn Metastore>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String / "sources" / String / "toggle")
        .and(warp::put())
        .and(json_body())
        .and(with_arg(metastore))
        .then(toggle_source)
        .and(extract_format_from_qs())
        .map(make_json_api_response)
}

#[derive(Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
struct ToggleSource {
    enable: bool,
}

#[utoipa::path(
    put,
    tag = "Sources",
    path = "/indexes/{index_id}/sources/{source_id}/toggle",
    request_body = ToggleSource,
    responses(
        (status = 200, description = "Successfully toggled source.")
    ),
    params(
        ("index_id" = String, Path, description = "The index ID of the source."),
        ("source_id" = String, Path, description = "The source ID to toggle."),
    )
)]
/// Toggles source.
async fn toggle_source(
    index_id: String,
    source_id: String,
    toggle_source: ToggleSource,
    metastore: Arc<dyn Metastore>,
) -> Result<(), IndexServiceError> {
    info!(index_id = %index_id, source_id = %source_id, enable = toggle_source.enable, "toggle-source");
    let index_uid: IndexUid = metastore.index_metadata(&index_id).await?.index_uid;
    if [CLI_INGEST_SOURCE_ID, INGEST_API_SOURCE_ID].contains(&source_id.as_str()) {
        return Err(IndexServiceError::OperationNotAllowed(format!(
            "Source `{source_id}` is managed by Quickwit, you cannot enable or disable a source \
             managed by Quickwit."
        )));
    }
    metastore
        .toggle_source(index_uid, &source_id, toggle_source.enable)
        .await?;
    Ok(())
}

fn delete_source_handler(
    metastore: Arc<dyn Metastore>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String / "sources" / String)
        .and(warp::delete())
        .and(with_arg(metastore))
        .then(delete_source)
        .and(extract_format_from_qs())
        .map(make_json_api_response)
}

#[utoipa::path(
    delete,
    tag = "Sources",
    path = "/indexes/{index_id}/sources/{source_id}",
    responses(
        (status = 200, description = "Successfully deleted source.")
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to remove the source from."),
        ("source_id" = String, Path, description = "The source ID to remove from the index."),
    )
)]
/// Deletes source.
async fn delete_source(
    index_id: String,
    source_id: String,
    metastore: Arc<dyn Metastore>,
) -> Result<(), IndexServiceError> {
    info!(index_id = %index_id, source_id = %source_id, "delete-source");
    let index_uid: IndexUid = metastore.index_metadata(&index_id).await?.index_uid;
    if [INGEST_API_SOURCE_ID, CLI_INGEST_SOURCE_ID].contains(&source_id.as_str()) {
        return Err(IndexServiceError::OperationNotAllowed(format!(
            "Source `{source_id}` is managed by Quickwit, you cannot delete a source managed by \
             Quickwit."
        )));
    }
    metastore.delete_source(index_uid, &source_id).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ops::{Bound, RangeInclusive};

    use assert_json_diff::assert_json_include;
    use quickwit_common::uri::{Protocol, Uri};
    use quickwit_config::{SourceParams, VecSourceParams};
    use quickwit_indexing::mock_split;
    use quickwit_metastore::file_backed_metastore::FileBackedMetastoreFactory;
    use quickwit_metastore::{
        IndexMetadata, Metastore, MetastoreError, MetastoreUriResolver, MockMetastore,
    };
    use quickwit_storage::StorageUriResolver;
    use serde::__private::from_utf8_lossy;
    use serde_json::Value as JsonValue;

    use super::*;
    use crate::recover_fn;

    async fn build_metastore_for_test() -> Arc<dyn Metastore> {
        let storage_resolver = StorageUriResolver::for_test();
        let metastore_uri_resolver = MetastoreUriResolver::builder()
            .register(
                Protocol::Ram,
                FileBackedMetastoreFactory::new(storage_resolver.clone()),
            )
            .build();
        metastore_uri_resolver
            .resolve(&Uri::from_well_formed("ram://quickwit-test-indexes"))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_get_index() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .return_once(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        let index_service = IndexService::new(Arc::new(metastore), StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/test-index")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let actual_response_json: JsonValue = serde_json::from_slice(resp.body())?;
        let expected_response_json = serde_json::json!({
            "index_id": "test-index",
            "index_uri": "ram:///indexes/test-index",
        });
        assert_json_include!(
            actual: actual_response_json.get("index_config").unwrap(),
            expected: expected_response_json
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_get_non_existing_index() {
        let metastore = build_metastore_for_test().await;
        let index_service = IndexService::new(metastore, StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/test-index")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 404);
    }

    #[tokio::test]
    async fn test_get_splits() {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "quickwit-demo-index",
                    "ram:///indexes/quickwit-demo-index",
                ))
            })
            .times(2);
        metastore
            .expect_list_splits()
            .returning(|list_split_query: ListSplitsQuery| {
                if list_split_query.index_uid.index_id() == "quickwit-demo-index"
                    && list_split_query.split_states
                        == vec![SplitState::Published, SplitState::Staged]
                    && list_split_query.time_range.start == Bound::Included(10)
                    && list_split_query.time_range.end == Bound::Excluded(20)
                    && list_split_query.create_timestamp.end == Bound::Excluded(2)
                {
                    return Ok(vec![mock_split("split_1")]);
                }
                Err(MetastoreError::InternalError {
                    message: "".to_string(),
                    cause: "".to_string(),
                })
            })
            .times(2);
        let index_service = IndexService::new(Arc::new(metastore), StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        {
            let resp = warp::test::request()
                .path(
                    "/indexes/quickwit-demo-index/splits?split_states=Published,Staged&\
                     start_timestamp=10&end_timestamp=20&end_create_timestamp=2",
                )
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 200);
            let actual_response_json: JsonValue = serde_json::from_slice(resp.body()).unwrap();
            let expected_response_json = serde_json::json!([{
                "create_timestamp": 0,
                "split_id": "split_1",
            }]);
            assert_json_include!(
                actual: actual_response_json,
                expected: expected_response_json
            );
        }
        {
            let resp = warp::test::request()
                .path(
                    "/indexes/quickwit-demo-index/splits?split_states=Published&\
                     start_timestamp=11&end_timestamp=20&end_create_timestamp=2",
                )
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 500);
        }
    }

    #[tokio::test]
    async fn test_describe_index() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .return_once(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        let split_1 = mock_split("split_1");
        let split_1_time_range = split_1.split_metadata.time_range.clone().unwrap();
        let mut split_2 = mock_split("split_2");
        split_2.split_metadata.time_range = Some(RangeInclusive::new(
            split_1_time_range.start() - 10,
            split_1_time_range.end() + 10,
        ));
        metastore
            .expect_list_splits()
            .return_once(|list_split_query: ListSplitsQuery| {
                if list_split_query.index_uid.index_id() == "test-index" {
                    return Ok(vec![split_1, split_2]);
                }
                Err(MetastoreError::InternalError {
                    message: "".to_string(),
                    cause: "".to_string(),
                })
            });

        let index_service = IndexService::new(Arc::new(metastore), StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/test-index/describe")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);

        let actual_response_json: JsonValue = serde_json::from_slice(resp.body()).unwrap();
        let expected_response_json = serde_json::json!({
            "index_id": "test-index",
            "index_uri": "ram:///indexes/test-index",
            "num_published_splits": 2,
            "num_published_docs": 20,
            "size_published_docs": 1600,
            "timestamp_field_name": "timestamp",
            "min_timestamp": split_1_time_range.start() - 10,
            "max_timestamp": split_1_time_range.end() + 10,
        });

        assert_eq!(actual_response_json, expected_response_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_all_splits() {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .return_once(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "quickwit-demo-index",
                    "ram:///indexes/quickwit-demo-index",
                ))
            });
        metastore
            .expect_list_splits()
            .return_once(|list_split_query: ListSplitsQuery| {
                if list_split_query.index_uid.index_id() == "quickwit-demo-index"
                    && list_split_query.split_states.is_empty()
                    && list_split_query.time_range.is_unbounded()
                    && list_split_query.create_timestamp.is_unbounded()
                {
                    return Ok(Vec::new());
                }
                Err(MetastoreError::InternalError {
                    message: "".to_string(),
                    cause: "".to_string(),
                })
            });
        let index_service = IndexService::new(Arc::new(metastore), StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/splits")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn test_mark_splits_for_deletion() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "quickwit-demo-index",
                    "ram:///indexes/quickwit-demo-index",
                ))
            })
            .times(2);
        metastore
            .expect_mark_splits_for_deletion()
            .returning(|index_uid: IndexUid, split_ids: &[&str]| {
                if index_uid.index_id() == "quickwit-demo-index"
                    && split_ids == ["split-1", "split-2"]
                {
                    return Ok(());
                }
                Err(MetastoreError::InternalError {
                    message: "".to_string(),
                    cause: "".to_string(),
                })
            })
            .times(2);
        let index_service = IndexService::new(Arc::new(metastore), StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/splits/mark-for-deletion")
            .method("PUT")
            .json(&true)
            .body(r#"{"split_ids": ["split-1", "split-2"]}"#)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/splits/mark-for-deletion")
            .json(&true)
            .body(r#"{"split_ids": [""]}"#)
            .method("PUT")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 500);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_list_indexes() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        metastore.expect_list_indexes_metadatas().return_once(|| {
            Ok(vec![IndexMetadata::for_test(
                "test-index",
                "ram:///indexes/test-index",
            )])
        });
        let index_service = IndexService::new(Arc::new(metastore), StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let actual_response_json: JsonValue = serde_json::from_slice(resp.body())?;
        let actual_response_arr: &Vec<JsonValue> = actual_response_json.as_array().unwrap();
        assert_eq!(actual_response_arr.len(), 1);
        let actual_index_metadata_json: &JsonValue = &actual_response_arr[0];
        let expected_response_json = serde_json::json!({
            "index_id": "test-index",
            "index_uri": "ram:///indexes/test-index",
        });
        assert_json_include!(
            actual: actual_index_metadata_json.get("index_config").unwrap(),
            expected: expected_response_json
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_clear_index() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .return_once(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "quickwit-demo-index",
                    "file:///path/to/index/quickwit-demo-index",
                ))
            });
        metastore
            .expect_list_all_splits()
            .return_once(|_| Ok(vec![mock_split("split_1")]));
        metastore
            .expect_mark_splits_for_deletion()
            .return_once(|_index_id: IndexUid, _splits: &[&str]| Ok(()));
        metastore
            .expect_delete_splits()
            .return_once(|_index_id: IndexUid, _splits: &[&str]| Ok(()));
        metastore
            .expect_reset_source_checkpoint()
            .return_once(|_index_id: IndexUid, _source_id: &str| Ok(()));
        let index_service = IndexService::new(Arc::new(metastore), StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/clear")
            .method("PUT")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_index() {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "quickwit-demo-index",
                    "file:///path/to/index/quickwit-demo-index",
                ))
            })
            .times(2);
        metastore
            .expect_list_all_splits()
            .return_once(|_| Ok(vec![mock_split("split_1")]));
        metastore
            .expect_list_splits()
            .returning(|_| Ok(vec![mock_split("split_1")]))
            .times(2);
        metastore
            .expect_mark_splits_for_deletion()
            .return_once(|_index_uid: IndexUid, _splits: &[&str]| Ok(()));
        metastore
            .expect_delete_splits()
            .return_once(|_index_uid: IndexUid, _splits: &[&str]| Ok(()));
        metastore
            .expect_delete_index()
            .return_once(|_index_uid: IndexUid| Ok(()));
        let index_service = IndexService::new(Arc::new(metastore), StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        {
            // Dry run
            let resp = warp::test::request()
                .path("/indexes/quickwit-demo-index?dry_run=true")
                .method("DELETE")
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 200);
            let resp_json: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
            let expected_response_json = serde_json::json!([{
                "file_name": "split_1.split",
                "file_size_in_bytes": 800,
            }]);
            assert_json_include!(actual: resp_json, expected: expected_response_json);
        }
        {
            let resp = warp::test::request()
                .path("/indexes/quickwit-demo-index")
                .method("DELETE")
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 200);
            let resp_json: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
            let expected_response_json = serde_json::json!([{
                "file_name": "split_1.split",
                "file_size_in_bytes": 800,
            }]);
            assert_json_include!(actual: resp_json, expected: expected_response_json);
        }
    }

    #[tokio::test]
    async fn test_delete_on_non_existing_index() {
        let metastore = build_metastore_for_test().await;
        let index_service = IndexService::new(metastore, StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index")
            .method("DELETE")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 404);
    }

    #[tokio::test]
    async fn test_create_index_with_overwrite() {
        let metastore = build_metastore_for_test().await;
        let index_service = IndexService::new(metastore.clone(), StorageUriResolver::for_test());
        let mut quickwit_config = QuickwitConfig::for_test();
        quickwit_config.default_index_root_uri =
            Uri::from_well_formed("file:///default-index-root-uri");
        let index_management_handler =
            super::index_management_handlers(Arc::new(index_service), Arc::new(quickwit_config));
        {
            let resp = warp::test::request()
                .path("/indexes?overwrite=true")
                .method("POST")
                .json(&true)
                .body(r#"{"version": "0.5", "index_id": "hdfs-logs", "doc_mapping": {"field_mappings":[{"name": "timestamp", "type": "i64", "fast": true, "indexed": true}]}}"#)
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 200);
        }
        {
            let resp = warp::test::request()
                .path("/indexes?overwrite=true")
                .method("POST")
                .json(&true)
                .body(r#"{"version": "0.5", "index_id": "hdfs-logs", "doc_mapping": {"field_mappings":[{"name": "timestamp", "type": "i64", "fast": true, "indexed": true}]}}"#)
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 200);
        }
        {
            let resp = warp::test::request()
                .path("/indexes")
                .method("POST")
                .json(&true)
                .body(r#"{"version": "0.5", "index_id": "hdfs-logs", "doc_mapping": {"field_mappings":[{"name": "timestamp", "type": "i64", "fast": true, "indexed": true}]}}"#)
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 400);
        }
    }

    #[tokio::test]
    async fn test_create_delete_index_and_source() {
        let metastore = build_metastore_for_test().await;
        let index_service = IndexService::new(metastore.clone(), StorageUriResolver::for_test());
        let mut quickwit_config = QuickwitConfig::for_test();
        quickwit_config.default_index_root_uri =
            Uri::from_well_formed("file:///default-index-root-uri");
        let index_management_handler =
            super::index_management_handlers(Arc::new(index_service), Arc::new(quickwit_config));
        let resp = warp::test::request()
            .path("/indexes")
            .method("POST")
            .json(&true)
            .body(r#"{"version": "0.5", "index_id": "hdfs-logs", "doc_mapping": {"field_mappings":[{"name": "timestamp", "type": "i64", "fast": true, "indexed": true}]}}"#)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp_json: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        let expected_response_json = serde_json::json!({
            "index_config": {
                "index_id": "hdfs-logs",
                "index_uri": "file:///default-index-root-uri/hdfs-logs",
            }
        });
        assert_json_include!(actual: resp_json, expected: expected_response_json);

        // Create source.
        let source_config_body = r#"{"version": "0.5", "source_id": "vec-source", "source_type": "vec", "params": {"docs": [], "batch_num_docs": 10}}"#;
        let resp = warp::test::request()
            .path("/indexes/hdfs-logs/sources")
            .method("POST")
            .json(&true)
            .body(source_config_body)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);

        // Get source.
        let resp = warp::test::request()
            .path("/indexes/hdfs-logs/sources/vec-source")
            .method("GET")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);

        // Check that the source has been added to index metadata.
        let index_metadata = metastore.index_metadata("hdfs-logs").await.unwrap();
        assert!(index_metadata.sources.contains_key("vec-source"));
        let source_config = index_metadata.sources.get("vec-source").unwrap();
        assert_eq!(source_config.source_type(), "vec");
        assert_eq!(
            source_config.source_params,
            SourceParams::Vec(VecSourceParams {
                docs: Vec::new(),
                batch_num_docs: 10,
                partition: "".to_string(),
            })
        );

        // Check delete source.
        let resp = warp::test::request()
            .path("/indexes/hdfs-logs/sources/vec-source")
            .method("DELETE")
            .body(source_config_body)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let index_metadata = metastore.index_metadata("hdfs-logs").await.unwrap();
        assert!(!index_metadata.sources.contains_key("file-source"));

        // Check cannot delete source managed by Quickwit.
        let resp = warp::test::request()
            .path(format!("/indexes/hdfs-logs/sources/{INGEST_API_SOURCE_ID}").as_str())
            .method("DELETE")
            .body(source_config_body)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 405);

        let resp = warp::test::request()
            .path(format!("/indexes/hdfs-logs/sources/{CLI_INGEST_SOURCE_ID}").as_str())
            .method("DELETE")
            .body(source_config_body)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 405);

        // Check get a non existing source returns 404.
        let resp = warp::test::request()
            .path("/indexes/hdfs-logs/sources/file-source")
            .method("GET")
            .body(source_config_body)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 404);

        // Check delete index.
        let resp = warp::test::request()
            .path("/indexes/hdfs-logs")
            .method("DELETE")
            .body(source_config_body)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let indexes = metastore.list_indexes_metadatas().await.unwrap();
        assert!(indexes.is_empty());
    }

    #[tokio::test]
    async fn test_create_file_source_returns_405() {
        let metastore = build_metastore_for_test().await;
        let index_service = IndexService::new(metastore.clone(), StorageUriResolver::for_test());
        let mut quickwit_config = QuickwitConfig::for_test();
        quickwit_config.default_index_root_uri =
            Uri::from_well_formed("file:///default-index-root-uri");
        let index_management_handler =
            super::index_management_handlers(Arc::new(index_service), Arc::new(quickwit_config))
                .recover(recover_fn);
        let source_config_body = r#"{"version": "0.5", "source_id": "file-source", "source_type": "file", "params": {"filepath": "FILEPATH"}}"#;
        let resp = warp::test::request()
            .path("/indexes/hdfs-logs/sources")
            .method("POST")
            .body(source_config_body)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 405);
        let response_body = std::str::from_utf8(resp.body()).unwrap();
        assert!(response_body.contains("limited to a local usage"))
    }

    #[tokio::test]
    async fn test_create_index_with_yaml() {
        let metastore = build_metastore_for_test().await;
        let index_service = IndexService::new(metastore.clone(), StorageUriResolver::for_test());
        let mut quickwit_config = QuickwitConfig::for_test();
        quickwit_config.default_index_root_uri =
            Uri::from_well_formed("file:///default-index-root-uri");
        let index_management_handler =
            super::index_management_handlers(Arc::new(index_service), Arc::new(quickwit_config))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes")
            .method("POST")
            .header("content-type", "application/yaml")
            .body(
                r#"
            version: 0.5
            index_id: hdfs-logs
            doc_mapping:
              field_mappings:
                - name: timestamp
                  type: i64
                  fast: true
                  indexed: true
            "#,
            )
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp_json: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        let expected_response_json = serde_json::json!({
            "index_config": {
                "index_id": "hdfs-logs",
                "index_uri": "file:///default-index-root-uri/hdfs-logs",
            }
        });
        assert_json_include!(actual: resp_json, expected: expected_response_json);
    }

    #[tokio::test]
    async fn test_create_index_and_source_with_toml() {
        let metastore = build_metastore_for_test().await;
        let index_service = IndexService::new(metastore.clone(), StorageUriResolver::for_test());
        let mut quickwit_config = QuickwitConfig::for_test();
        quickwit_config.default_index_root_uri =
            Uri::from_well_formed("file:///default-index-root-uri");
        let index_management_handler =
            super::index_management_handlers(Arc::new(index_service), Arc::new(quickwit_config))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes")
            .method("POST")
            .header("content-type", "application/toml")
            .body(
                r#"
            version = "0.5"
            index_id = "hdfs-logs"
            [doc_mapping]
            field_mappings = [
                { name = "timestamp", type = "i64", fast = true, indexed = true}
            ]
            "#,
            )
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp_json: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        let expected_response_json = serde_json::json!({
            "index_config": {
                "index_id": "hdfs-logs",
                "index_uri": "file:///default-index-root-uri/hdfs-logs",
            }
        });
        assert_json_include!(actual: resp_json, expected: expected_response_json);
    }

    #[tokio::test]
    async fn test_create_index_with_wrong_content_type() {
        let metastore = build_metastore_for_test().await;
        let index_service = IndexService::new(metastore.clone(), StorageUriResolver::for_test());
        let mut quickwit_config = QuickwitConfig::for_test();
        quickwit_config.default_index_root_uri =
            Uri::from_well_formed("file:///default-index-root-uri");
        let index_management_handler =
            super::index_management_handlers(Arc::new(index_service), Arc::new(quickwit_config))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes")
            .method("POST")
            .header("content-type", "application/yoml")
            .body(r#""#)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 415);
        let body = from_utf8_lossy(resp.body());
        assert!(body.contains("Unsupported content-type header. Choices are"));
    }

    #[tokio::test]
    async fn test_create_index_with_bad_config() -> anyhow::Result<()> {
        let metastore = MockMetastore::new();
        let index_service = IndexService::new(Arc::new(metastore), StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes")
            .method("POST")
            .json(&true)
            .body(
                r#"{"version": "0.5", "index_id": "hdfs-log", "doc_mapping":
    {"field_mappings":[{"name": "timestamp", "type": "unknown", "fast": true, "indexed":
    true}]}}"#,
            )
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 400);
        let body = from_utf8_lossy(resp.body());
        assert!(body.contains("Field `timestamp` has an unknown type"));
        Ok(())
    }

    #[tokio::test]
    async fn test_create_source_with_bad_config() {
        let metastore = build_metastore_for_test().await;
        let index_service = IndexService::new(metastore, StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        {
            // Source config with bad version.
            let resp = warp::test::request()
                .path("/indexes/my-index/sources")
                .method("POST")
                .json(&true)
                .body(r#"{"version": 0.4, "source_id": "file-source"}"#)
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 400);
            let body = from_utf8_lossy(resp.body());
            assert!(body.contains("invalid type: floating point `0.4`"));
        }
        {
            // Invalid pulsar source config with number of pipelines > 1, not supported yet.
            let resp = warp::test::request()
                .path("/indexes/my-index/sources")
                .method("POST")
                .json(&true)
                .body(r#"{"version": "0.5", "source_id": "pulsar-source", "desired_num_pipelines": 2, "source_type": "pulsar", "params": {"topics": ["my-topic"], "address": "pulsar://localhost:6650" }}"#)
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 400);
            let body = from_utf8_lossy(resp.body());
            assert!(body
                .contains("Quickwit currently supports multiple pipelines only for Kafka sources"));
        }
    }

    #[tokio::test]
    async fn test_delete_non_existing_source() {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .return_once(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "quickwit-demo-index",
                    "file:///path/to/index/quickwit-demo-index",
                ))
            });
        metastore
            .expect_index_exists()
            .return_once(|index_id: &str| Ok(index_id == "quickwit-demo-index"));
        metastore
            .expect_delete_source()
            .return_once(|index_uid, source_id| {
                assert_eq!(index_uid.index_id(), "quickwit-demo-index");
                Err(MetastoreError::SourceDoesNotExist {
                    source_id: source_id.to_string(),
                })
            });
        let index_service = IndexService::new(Arc::new(metastore), StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/sources/foo-source")
            .method("DELETE")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 404);
    }

    #[tokio::test]
    async fn test_source_reset_checkpoint() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "quickwit-demo-index",
                    "file:///path/to/index/quickwit-demo-index",
                ))
            })
            .times(2);
        metastore
            .expect_reset_source_checkpoint()
            .returning(|index_uid: IndexUid, source_id: &str| {
                if index_uid.index_id() == "quickwit-demo-index" && source_id == "source-to-reset" {
                    return Ok(());
                }
                Err(MetastoreError::InternalError {
                    message: "".to_string(),
                    cause: "".to_string(),
                })
            })
            .times(2);
        let index_service = IndexService::new(Arc::new(metastore), StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/sources/source-to-reset/reset-checkpoint")
            .method("PUT")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/sources/source-to-reset-2/reset-checkpoint")
            .method("PUT")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 500);
        Ok(())
    }

    #[tokio::test]
    async fn test_source_toggle() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "quickwit-demo-index",
                    "file:///path/to/index/quickwit-demo-index",
                ))
            })
            .times(3);
        metastore.expect_toggle_source().return_once(
            |index_uid: IndexUid, source_id: &str, enable: bool| {
                if index_uid.index_id() == "quickwit-demo-index"
                    && source_id == "source-to-toggle"
                    && enable
                {
                    return Ok(());
                }
                Err(MetastoreError::InternalError {
                    message: "".to_string(),
                    cause: "".to_string(),
                })
            },
        );
        let index_service = IndexService::new(Arc::new(metastore), StorageUriResolver::for_test());
        let index_management_handler = super::index_management_handlers(
            Arc::new(index_service),
            Arc::new(QuickwitConfig::for_test()),
        )
        .recover(recover_fn);
        // Check server returns 405 if sources root path is used.
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/sources/source-to-toggle")
            .method("PUT")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 405);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/sources/source-to-toggle/toggle")
            .method("PUT")
            .json(&true)
            .body(r#"{"enable": true}"#)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/sources/source-to-toggle/toggle")
            .method("PUT")
            .json(&true)
            .body(r#"{"toggle": true}"#) // unknown field, should return 400.
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 400);
        // Check cannot toggle source managed by Quickwit.
        let resp = warp::test::request()
            .path(format!("/indexes/hdfs-logs/sources/{INGEST_API_SOURCE_ID}/toggle").as_str())
            .method("PUT")
            .body(r#"{"enable": true}"#)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 405);

        let resp = warp::test::request()
            .path(format!("/indexes/hdfs-logs/sources/{CLI_INGEST_SOURCE_ID}/toggle").as_str())
            .method("PUT")
            .body(r#"{"enable": true}"#)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 405);
        Ok(())
    }
}
