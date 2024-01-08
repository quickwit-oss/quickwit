// Copyright (C) 2024 Quickwit, Inc.
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

use std::path::Path;
use std::time::Duration;

use quickwit_common::fs::{empty_dir, get_cache_directory_path};
use quickwit_config::{validate_identifier, IndexConfig, SourceConfig};
use quickwit_indexing::check_source_connectivity;
use quickwit_metastore::{
    AddSourceRequestExt, CreateIndexRequestExt, IndexMetadata, IndexMetadataResponseExt,
    ListSplitsQuery, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitInfo,
    SplitMetadata, SplitState,
};
use quickwit_proto::metastore::{
    AddSourceRequest, CreateIndexRequest, DeleteIndexRequest, EntityKind, IndexMetadataRequest,
    ListSplitsRequest, MarkSplitsForDeletionRequest, MetastoreError, MetastoreService,
    MetastoreServiceClient, ResetSourceCheckpointRequest,
};
use quickwit_proto::types::{IndexUid, SplitId};
use quickwit_proto::{ServiceError, ServiceErrorCode};
use quickwit_storage::{StorageResolver, StorageResolverError};
use thiserror::Error;
use tracing::{error, info};

use crate::garbage_collection::{
    delete_splits_from_storage_and_metastore, run_garbage_collect, DeleteSplitsError,
    SplitRemovalInfo,
};

#[derive(Error, Debug)]
pub enum IndexServiceError {
    #[error("failed to resolve the storage `{0}`")]
    Storage(#[from] StorageResolverError),
    #[error("metastore error `{0}`")]
    Metastore(#[from] MetastoreError),
    #[error("split deletion error `{0}`")]
    SplitDeletion(#[from] DeleteSplitsError),
    #[error("invalid config: {0:#}")]
    InvalidConfig(anyhow::Error),
    #[error("invalid identifier: {0}")]
    InvalidIdentifier(String),
    #[error("operation not allowed: {0}")]
    OperationNotAllowed(String),
    #[error("internal error: {0}")]
    Internal(String),
}

impl ServiceError for IndexServiceError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Internal(_) => ServiceErrorCode::Internal,
            Self::InvalidConfig(_) => ServiceErrorCode::BadRequest,
            Self::InvalidIdentifier(_) => ServiceErrorCode::BadRequest,
            Self::Metastore(error) => error.error_code(),
            Self::OperationNotAllowed(_) => ServiceErrorCode::MethodNotAllowed,
            Self::SplitDeletion(_) => ServiceErrorCode::Internal,
            Self::Storage(_) => ServiceErrorCode::Internal,
        }
    }
}

/// Index service responsible for creating, updating and deleting indexes.
#[derive(Clone)]
pub struct IndexService {
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
}

impl IndexService {
    /// Creates an `IndexService`.
    pub fn new(metastore: MetastoreServiceClient, storage_resolver: StorageResolver) -> Self {
        Self {
            metastore,
            storage_resolver,
        }
    }

    pub fn metastore(&self) -> MetastoreServiceClient {
        self.metastore.clone()
    }

    /// Creates an index from `IndexConfig`.
    pub async fn create_index(
        &mut self,
        index_config: IndexConfig,
        overwrite: bool,
    ) -> Result<IndexMetadata, IndexServiceError> {
        validate_storage_uri(&self.storage_resolver, &index_config)
            .await
            .map_err(IndexServiceError::InvalidConfig)?;

        // Delete existing index if it exists.
        if overwrite {
            match self.delete_index(&index_config.index_id, false).await {
                Ok(_)
                | Err(IndexServiceError::Metastore(MetastoreError::NotFound(
                    EntityKind::Index { .. },
                ))) => {
                    // Ignore index not found error.
                }
                Err(error) => {
                    return Err(error);
                }
            }
        }

        let mut metastore = self.metastore.clone();

        // Add default ingest-api & cli-ingest sources config.
        let index_id = index_config.index_id.clone();
        let create_index_request = CreateIndexRequest::try_from_index_config(index_config)?;
        let create_index_response = metastore.create_index(create_index_request).await?;
        let index_uid: IndexUid = create_index_response.index_uid.into();
        let add_ingest_api_source_request = AddSourceRequest::try_from_source_config(
            index_uid.clone(),
            SourceConfig::ingest_api_default(),
        )?;
        metastore.add_source(add_ingest_api_source_request).await?;
        let add_ingest_source_request = AddSourceRequest::try_from_source_config(
            index_uid.clone(),
            SourceConfig::ingest_v2_default(),
        )?;
        metastore.add_source(add_ingest_source_request).await?;
        let add_ingest_cli_source_request = AddSourceRequest::try_from_source_config(
            index_uid.clone(),
            SourceConfig::cli_ingest_source(),
        )?;
        metastore.add_source(add_ingest_cli_source_request).await?;
        let index_metadata_request = IndexMetadataRequest::for_index_id(index_id);
        let index_metadata = metastore
            .index_metadata(index_metadata_request)
            .await?
            .deserialize_index_metadata()?;
        Ok(index_metadata)
    }

    /// Deletes the index specified with `index_id`.
    /// This is equivalent to running `rm -rf <index path>` for a local index or
    /// `aws s3 rm --recursive <index path>` for a remote Amazon S3 index.
    ///
    /// * `index_id` - The target index Id.
    /// * `dry_run` - Should this only return a list of affected files without performing deletion.
    pub async fn delete_index(
        &mut self,
        index_id: &str,
        dry_run: bool,
    ) -> Result<Vec<SplitInfo>, IndexServiceError> {
        let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
        let index_metadata = self
            .metastore
            .index_metadata(index_metadata_request)
            .await?
            .deserialize_index_metadata()?;
        let index_uid = index_metadata.index_uid.clone();
        let index_uri = index_metadata.into_index_config().index_uri.clone();
        let storage = self.storage_resolver.resolve(&index_uri).await?;

        if dry_run {
            let list_splits_request = ListSplitsRequest::try_from_index_uid(index_uid)?;
            let splits_to_delete: Vec<SplitInfo> = self
                .metastore
                .list_splits(list_splits_request)
                .await?
                .collect_splits()
                .await?
                .into_iter()
                .map(|split| split.split_metadata.as_split_info())
                .collect();
            return Ok(splits_to_delete);
        }
        // Schedule staged and published splits for deletion.
        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_states([SplitState::Staged, SplitState::Published]);
        let list_splits_request = ListSplitsRequest::try_from_list_splits_query(query)?;
        let split_ids: Vec<SplitId> = self
            .metastore
            .list_splits(list_splits_request)
            .await?
            .collect_split_ids()
            .await?;
        let mark_splits_for_deletion_request =
            MarkSplitsForDeletionRequest::new(index_uid.clone(), split_ids);
        self.metastore
            .mark_splits_for_deletion(mark_splits_for_deletion_request)
            .await?;

        // Select splits to delete
        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::MarkedForDeletion);
        let list_splits_request = ListSplitsRequest::try_from_list_splits_query(query)?;
        let splits_metadata_to_delete: Vec<SplitMetadata> = self
            .metastore
            .list_splits(list_splits_request)
            .await?
            .collect_splits_metadata()
            .await?;

        let deleted_splits = delete_splits_from_storage_and_metastore(
            index_uid.clone(),
            storage,
            self.metastore.clone(),
            splits_metadata_to_delete,
            None,
        )
        .await?;
        let delete_index_request = DeleteIndexRequest {
            index_uid: index_uid.to_string(),
        };
        self.metastore.delete_index(delete_index_request).await?;

        Ok(deleted_splits)
    }

    /// Detect all dangling splits and associated files from the index and removes them.
    ///
    /// * `index_id` - The target index Id.
    /// * `grace_period` -  Threshold period after which a staged split can be garbage collected.
    /// * `dry_run` - Should this only return a list of affected files without performing deletion.
    pub async fn garbage_collect_index(
        &mut self,
        index_id: &str,
        grace_period: Duration,
        dry_run: bool,
    ) -> anyhow::Result<SplitRemovalInfo> {
        let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
        let index_metadata = self
            .metastore
            .index_metadata(index_metadata_request)
            .await?
            .deserialize_index_metadata()?;
        let index_uid = index_metadata.index_uid.clone();
        let index_config = index_metadata.into_index_config();
        let storage = self
            .storage_resolver
            .resolve(&index_config.index_uri)
            .await?;

        let deleted_entries = run_garbage_collect(
            index_uid,
            storage,
            self.metastore.clone(),
            grace_period,
            // deletion_grace_period of zero, so that a cli call directly deletes splits after
            // marking to be deleted.
            Duration::ZERO,
            dry_run,
            None,
        )
        .await?;

        Ok(deleted_entries)
    }

    /// Clears the index by applying the following actions:
    /// - mark all splits for deletion in the metastore.
    /// - delete the files of all splits marked for deletion using garbage collection.
    /// - delete the splits from the metastore.
    /// - reset all the source checkpoints.
    ///
    /// * `metastore` - A metastore object for interacting with the metastore.
    /// * `index_id` - The target index Id.
    /// * `storage_resolver` - A storage resolver object to access the storage.
    pub async fn clear_index(&mut self, index_id: &str) -> Result<(), IndexServiceError> {
        let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
        let index_metadata = self
            .metastore
            .index_metadata(index_metadata_request)
            .await?
            .deserialize_index_metadata()?;
        let index_uid = index_metadata.index_uid.clone();
        let storage = self
            .storage_resolver
            .resolve(index_metadata.index_uri())
            .await?;
        let list_splits_request = ListSplitsRequest::try_from_index_uid(index_uid.clone())?;
        let splits_metadata: Vec<SplitMetadata> = self
            .metastore
            .list_splits(list_splits_request)
            .await?
            .collect_splits_metadata()
            .await?;
        let split_ids: Vec<SplitId> = splits_metadata
            .iter()
            .map(|split| split.split_id.to_string())
            .collect();
        let mark_splits_for_deletion_request =
            MarkSplitsForDeletionRequest::new(index_uid.clone(), split_ids.clone());
        self.metastore
            .mark_splits_for_deletion(mark_splits_for_deletion_request)
            .await?;
        // FIXME: return an error.
        if let Err(err) = delete_splits_from_storage_and_metastore(
            index_uid.clone(),
            storage,
            self.metastore.clone(),
            splits_metadata,
            None,
        )
        .await
        {
            error!(metastore_endpoints=?self.metastore.endpoints(), index_id=%index_id, error=?err, "failed to delete all the split files during garbage collection");
        }
        for source_id in index_metadata.sources.keys() {
            let reset_source_checkpoint_request = ResetSourceCheckpointRequest {
                index_uid: index_uid.to_string(),
                source_id: source_id.to_string(),
            };
            self.metastore
                .reset_source_checkpoint(reset_source_checkpoint_request)
                .await?;
        }
        Ok(())
    }

    /// Creates a source config for index `index_id`.
    pub async fn create_source(
        &mut self,
        index_uid: IndexUid,
        source_config: SourceConfig,
    ) -> Result<SourceConfig, IndexServiceError> {
        let source_id = source_config.source_id.clone();
        // This is a bit redundant, as SourceConfig deserialization also checks
        // that the identifier is valid. However it authorizes the special
        // private names internal to quickwit, so we do an extra check.
        validate_identifier("Source ID", &source_id).map_err(|_| {
            IndexServiceError::InvalidIdentifier(format!("invalid source ID: `{source_id}`"))
        })?;
        check_source_connectivity(&self.storage_resolver, &source_config)
            .await
            .map_err(IndexServiceError::InvalidConfig)?;
        let add_source_request =
            AddSourceRequest::try_from_source_config(index_uid.clone(), source_config.clone())?;
        self.metastore.add_source(add_source_request).await?;
        info!(
            "source `{}` successfully created for index `{}`",
            source_id,
            index_uid.index_id()
        );
        let index_metadata_request =
            IndexMetadataRequest::for_index_id(index_uid.index_id().to_string());
        let source = self
            .metastore
            .index_metadata(index_metadata_request)
            .await?
            .deserialize_index_metadata()?
            .sources
            .get(&source_id)
            .ok_or_else(|| {
                IndexServiceError::Internal(
                    "created source is not in index metadata, this should never happen".to_string(),
                )
            })?
            .clone();
        Ok(source)
    }

    pub async fn get_source(
        &mut self,
        index_id: &str,
        source_id: &str,
    ) -> Result<SourceConfig, IndexServiceError> {
        let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
        let source_config = self
            .metastore
            .index_metadata(index_metadata_request)
            .await?
            .deserialize_index_metadata()?
            .sources
            .get(source_id)
            .ok_or_else(|| {
                IndexServiceError::Metastore(MetastoreError::NotFound(EntityKind::Source {
                    index_id: index_id.to_string(),
                    source_id: source_id.to_string(),
                }))
            })?
            .clone();

        Ok(source_config)
    }
}

/// Clears the cache directory of a given source.
///
/// * `data_dir_path` - Path to directory where data (tmp data, splits kept for caching purpose) is
///   persisted.
pub async fn clear_cache_directory(data_dir_path: &Path) -> anyhow::Result<()> {
    let cache_directory_path = get_cache_directory_path(data_dir_path);
    info!(path = %cache_directory_path.display(), "clearing cache directory");
    empty_dir(&cache_directory_path).await?;
    Ok(())
}

/// Validates the storage URI by effectively resolving it.
pub async fn validate_storage_uri(
    storage_resolver: &StorageResolver,
    index_config: &IndexConfig,
) -> anyhow::Result<()> {
    storage_resolver.resolve(&index_config.index_uri).await?;
    Ok(())
}

#[cfg(test)]
mod tests {

    use quickwit_common::uri::Uri;
    use quickwit_config::IndexConfig;
    use quickwit_metastore::{
        metastore_for_test, MetastoreServiceExt, SplitMetadata, StageSplitsRequestExt,
    };
    use quickwit_proto::metastore::StageSplitsRequest;
    use quickwit_storage::PutPayload;

    use super::*;

    #[tokio::test]
    async fn test_create_index() {
        let mut metastore = metastore_for_test();
        let storage_resolver = StorageResolver::for_test();
        let mut index_service = IndexService::new(metastore.clone(), storage_resolver);
        let index_id = "test-index";
        let index_uri = "ram://indexes/test-index";
        let index_config = IndexConfig::for_test(index_id, index_uri);
        let index_metadata_0 = index_service
            .create_index(index_config.clone(), false)
            .await
            .unwrap();
        assert_eq!(index_metadata_0.index_id(), index_id);
        assert_eq!(index_metadata_0.index_uri(), &index_uri);
        assert!(metastore
            .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
            .await
            .is_ok());

        let error = index_service
            .create_index(index_config.clone(), false)
            .await
            .unwrap_err();
        let IndexServiceError::Metastore(inner_error) = error else {
            panic!("expected `MetastoreError` variant, got {:?}", error)
        };
        assert!(
            matches!(inner_error, MetastoreError::AlreadyExists(EntityKind::Index { index_id }) if index_id == index_metadata_0.index_id())
        );

        let index_metadata_1 = index_service
            .create_index(index_config, true)
            .await
            .unwrap();
        assert_eq!(index_metadata_1.index_id(), index_id);
        assert_eq!(index_metadata_1.index_uri(), &index_uri);
        assert!(index_metadata_0.index_uid != index_metadata_1.index_uid);
    }

    #[tokio::test]
    async fn test_delete_index() {
        let mut metastore = metastore_for_test();
        let storage_resolver = StorageResolver::for_test();
        let storage = storage_resolver
            .resolve(&Uri::for_test("ram://indexes/test-index"))
            .await
            .unwrap();
        let mut index_service = IndexService::new(metastore.clone(), storage_resolver);
        let index_id = "test-index";
        let index_uri = "ram://indexes/test-index";
        let index_config = IndexConfig::for_test(index_id, index_uri);
        let index_uid = index_service
            .create_index(index_config.clone(), false)
            .await
            .unwrap()
            .index_uid;

        let split_id = "test-split";
        let split_metadata = SplitMetadata {
            split_id: split_id.to_string(),
            index_uid: index_uid.clone(),
            ..Default::default()
        };
        let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
            index_uid.clone(),
            vec![split_metadata.clone()],
        )
        .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert_eq!(splits.len(), 1);

        let split_path_str = format!("{}.split", split_id);
        let split_path = Path::new(&split_path_str);
        let payload: Box<dyn PutPayload> = Box::new(vec![0]);
        storage.put(split_path, payload).await.unwrap();
        assert!(storage.exists(split_path).await.unwrap());

        let split_infos = index_service.delete_index(index_id, false).await.unwrap();
        assert_eq!(split_infos.len(), 1);

        assert!(!metastore.index_exists(index_id).await.unwrap());
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert!(splits.is_empty());
        assert!(!storage.exists(split_path).await.unwrap());
    }
}
