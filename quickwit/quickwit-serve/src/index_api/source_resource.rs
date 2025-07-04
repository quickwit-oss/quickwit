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

use bytes::Bytes;
use quickwit_config::{
    CLI_SOURCE_ID, ConfigFormat, FileSourceParams, INGEST_API_SOURCE_ID, SourceConfig,
    SourceParams, load_source_config_from_user_config, load_source_config_update,
};
use quickwit_index_management::{IndexService, IndexServiceError};
use quickwit_metastore::IndexMetadataResponseExt;
use quickwit_proto::ingest::Shard;
use quickwit_proto::metastore::{
    DeleteSourceRequest, EntityKind, IndexMetadataRequest, ListShardsRequest, ListShardsSubrequest,
    MetastoreError, MetastoreResult, MetastoreService, MetastoreServiceClient,
    ResetSourceCheckpointRequest, ToggleSourceRequest,
};
use quickwit_proto::types::{IndexId, IndexUid, SourceId};
use serde::Deserialize;
use tracing::info;

#[allow(clippy::result_large_err)]
fn check_source_type(source_params: &SourceParams) -> Result<(), IndexServiceError> {
    // Note: This check is performed here instead of the source config serde
    // because many tests use the file source, and can't store that config in
    // the metastore without going through the validation.
    if let SourceParams::File(FileSourceParams::Filepath(_)) = source_params {
        return Err(IndexServiceError::InvalidConfig(anyhow::anyhow!(
            "path based file sources are limited to a local usage, please use the CLI command \
             `quickwit tool local-ingest` to ingest data from a specific file or setup a \
             notification based file source"
        )));
    }
    Ok(())
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
pub async fn create_source(
    index_id: IndexId,
    config_format: ConfigFormat,
    source_config_bytes: Bytes,
    mut index_service: IndexService,
) -> Result<SourceConfig, IndexServiceError> {
    let source_config: SourceConfig =
        load_source_config_from_user_config(config_format, &source_config_bytes)
            .map_err(IndexServiceError::InvalidConfig)?;
    check_source_type(&source_config.source_params)?;
    let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
    let index_uid: IndexUid = index_service
        .metastore()
        .index_metadata(index_metadata_request)
        .await?
        .deserialize_index_metadata()?
        .index_uid;
    info!(index_id = %index_id, source_id = %source_config.source_id, "create-source");
    index_service.add_source(index_uid, source_config).await
}

/// Query parameters for update source queries
#[derive(Deserialize, Debug, Eq, PartialEq, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct UpdateQueryParams {
    /// Create the source if it doesn't exist yet
    #[serde(default)]
    pub create: bool,
}

#[utoipa::path(
    put,
    tag = "Sources",
    path = "/indexes/{index_id}/sources/{source_id}",
    request_body = VersionedSourceConfig,
    responses(
        // We return `VersionedSourceConfig` as it's the serialized model view.
        (status = 200, description = "Successfully updated source.", body = VersionedSourceConfig)
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to create a source for."),
        ("source_id" = String, Path, description = "The source ID to update."),
        UpdateQueryParams,
    )
)]
/// Updates Source.
pub async fn update_source(
    index_id: IndexId,
    source_id: SourceId,
    config_format: ConfigFormat,
    query_params: UpdateQueryParams,
    source_config_bytes: Bytes,
    mut index_service: IndexService,
) -> Result<SourceConfig, IndexServiceError> {
    let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
    let mut current_index_metadata = index_service
        .metastore()
        .index_metadata(index_metadata_request)
        .await?
        .deserialize_index_metadata()?;
    let current_source_config = match current_index_metadata.sources.remove(&source_id) {
        Some(source_config) => source_config,
        None if query_params.create => {
            let source_config: SourceConfig =
                load_source_config_from_user_config(config_format, &source_config_bytes)
                    .map_err(IndexServiceError::InvalidConfig)?;
            if source_config.source_id != source_id {
                return Err(IndexServiceError::InvalidConfig(anyhow::anyhow!(
                    "`source_id` in config file does not match source_id from query path"
                )));
            }
            check_source_type(&source_config.source_params)?;
            info!(index_id = %index_id, source_id = %source_config.source_id, "create-source-on-update");
            // TODO handle already exists?
            return index_service
                .add_source(current_index_metadata.index_uid, source_config)
                .await;
        }
        None => {
            return Err(MetastoreError::NotFound(EntityKind::Source {
                index_id: index_id.to_string(),
                source_id,
            })
            .into());
        }
    };

    let new_source_config: SourceConfig =
        load_source_config_update(config_format, &source_config_bytes, &current_source_config)
            .map_err(IndexServiceError::InvalidConfig)?;

    info!(index_id = %index_id, source_id = %new_source_config.source_id, "update-source");
    index_service
        .update_source(current_index_metadata.index_uid, new_source_config)
        .await
}

pub async fn get_source(
    index_id: IndexId,
    source_id: SourceId,
    metastore: MetastoreServiceClient,
) -> MetastoreResult<SourceConfig> {
    info!(index_id = %index_id, source_id = %source_id, "get-source");
    let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
    let source_config = metastore
        .index_metadata(index_metadata_request)
        .await?
        .deserialize_index_metadata()?
        .sources
        .remove(&source_id)
        .ok_or({
            MetastoreError::NotFound(EntityKind::Source {
                index_id,
                source_id,
            })
        })?;
    Ok(source_config)
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
pub async fn reset_source_checkpoint(
    index_id: IndexId,
    source_id: SourceId,
    metastore: MetastoreServiceClient,
) -> MetastoreResult<()> {
    let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
    let index_uid: IndexUid = metastore
        .index_metadata(index_metadata_request)
        .await?
        .deserialize_index_metadata()?
        .index_uid;
    info!(index_id = %index_id, source_id = %source_id, "reset-checkpoint");
    let reset_source_checkpoint_request = ResetSourceCheckpointRequest {
        index_uid: Some(index_uid),
        source_id: source_id.clone(),
    };
    metastore
        .reset_source_checkpoint(reset_source_checkpoint_request)
        .await?;
    Ok(())
}

#[derive(Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ToggleSource {
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
pub async fn toggle_source(
    index_id: IndexId,
    source_id: SourceId,
    toggle_source: ToggleSource,
    metastore: MetastoreServiceClient,
) -> Result<(), IndexServiceError> {
    info!(index_id = %index_id, source_id = %source_id, enable = toggle_source.enable, "toggle-source");
    let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
    let index_uid: IndexUid = metastore
        .index_metadata(index_metadata_request)
        .await?
        .deserialize_index_metadata()?
        .index_uid;
    if [CLI_SOURCE_ID, INGEST_API_SOURCE_ID].contains(&source_id.as_str()) {
        return Err(IndexServiceError::OperationNotAllowed(format!(
            "source `{source_id}` is managed by Quickwit, you cannot enable or disable a source \
             managed by Quickwit"
        )));
    }
    let toggle_source_request = ToggleSourceRequest {
        index_uid: Some(index_uid),
        source_id: source_id.clone(),
        enable: toggle_source.enable,
    };
    metastore.toggle_source(toggle_source_request).await?;
    Ok(())
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
pub async fn delete_source(
    index_id: IndexId,
    source_id: SourceId,
    metastore: MetastoreServiceClient,
) -> Result<(), IndexServiceError> {
    info!(index_id = %index_id, source_id = %source_id, "delete-source");
    let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
    let index_uid: IndexUid = metastore
        .index_metadata(index_metadata_request)
        .await?
        .deserialize_index_metadata()?
        .index_uid;
    if [INGEST_API_SOURCE_ID, CLI_SOURCE_ID].contains(&source_id.as_str()) {
        return Err(IndexServiceError::OperationNotAllowed(format!(
            "source `{source_id}` is managed by Quickwit, you cannot delete a source managed by \
             Quickwit"
        )));
    }
    let delete_source_request = DeleteSourceRequest {
        index_uid: Some(index_uid),
        source_id: source_id.clone(),
    };
    metastore.delete_source(delete_source_request).await?;
    Ok(())
}

pub async fn get_source_shards(
    index_id: IndexId,
    source_id: SourceId,
    metastore: MetastoreServiceClient,
) -> MetastoreResult<Vec<Shard>> {
    info!(index_id = %index_id, source_id = %source_id, "get-source-shards");
    let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
    let index_uid: IndexUid = metastore
        .index_metadata(index_metadata_request)
        .await?
        .deserialize_index_metadata()?
        .index_uid;
    let response = metastore
        .list_shards(ListShardsRequest {
            subrequests: vec![ListShardsSubrequest {
                index_uid: Some(index_uid),
                source_id: source_id.to_string(),
                ..Default::default()
            }],
        })
        .await?;
    let shards = response
        .subresponses
        .into_iter()
        .flat_map(|resp| resp.shards)
        .collect();
    Ok(shards)
}
