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

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use quickwit_common::fs::{empty_dir, get_cache_directory_path};
use quickwit_config::{validate_identifier, IndexConfig, SourceConfig};
use quickwit_indexing::check_source_connectivity;
use quickwit_metastore::{
    IndexMetadata, ListSplitsQuery, Metastore, SplitInfo, SplitMetadata, SplitState,
};
use quickwit_proto::metastore::{EntityKind, MetastoreError};
use quickwit_proto::{IndexUid, ServiceError, ServiceErrorCode};
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
    metastore: Arc<dyn Metastore>,
    storage_resolver: StorageResolver,
}

impl IndexService {
    /// Creates an `IndexService`.
    pub fn new(metastore: Arc<dyn Metastore>, storage_resolver: StorageResolver) -> Self {
        Self {
            metastore,
            storage_resolver,
        }
    }

    pub fn metastore(&self) -> Arc<dyn Metastore> {
        self.metastore.clone()
    }

    /// Creates an index from `IndexConfig`.
    pub async fn create_index(
        &self,
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

        // Add default ingest-api & cli-ingest sources config.
        let index_id = index_config.index_id.clone();
        let index_uid = self.metastore.create_index(index_config).await?;
        self.metastore
            .add_source(index_uid.clone(), SourceConfig::ingest_api_default())
            .await?;
        self.metastore
            .add_source(index_uid.clone(), SourceConfig::ingest_default())
            .await?;
        self.metastore
            .add_source(index_uid, SourceConfig::cli_ingest_source())
            .await?;
        let index_metadata = self.metastore.index_metadata(&index_id).await?;
        Ok(index_metadata)
    }

    /// Deletes the index specified with `index_id`.
    /// This is equivalent to running `rm -rf <index path>` for a local index or
    /// `aws s3 rm --recursive <index path>` for a remote Amazon S3 index.
    ///
    /// * `index_id` - The target index Id.
    /// * `dry_run` - Should this only return a list of affected files without performing deletion.
    pub async fn delete_index(
        &self,
        index_id: &str,
        dry_run: bool,
    ) -> Result<Vec<SplitInfo>, IndexServiceError> {
        let index_metadata = self.metastore.index_metadata(index_id).await?;
        let index_uid = index_metadata.index_uid.clone();
        let index_uri = index_metadata.into_index_config().index_uri.clone();
        let storage = self.storage_resolver.resolve(&index_uri).await?;

        if dry_run {
            let splits_to_delete = self
                .metastore
                .list_all_splits(index_uid.clone())
                .await?
                .into_iter()
                .map(|split| split.split_metadata.as_split_info())
                .collect::<Vec<_>>();
            return Ok(splits_to_delete);
        }
        // Schedule staged and published splits for deletion.
        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_states([SplitState::Staged, SplitState::Published]);
        let splits = self.metastore.list_splits(query).await?;
        let split_ids = splits
            .iter()
            .map(|split| split.split_id())
            .collect::<Vec<_>>();
        self.metastore
            .mark_splits_for_deletion(index_uid.clone(), &split_ids)
            .await?;

        // Select splits to delete
        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_state(SplitState::MarkedForDeletion);
        let splits_to_delete = self
            .metastore
            .list_splits(query)
            .await?
            .into_iter()
            .map(|split| split.split_metadata)
            .collect::<Vec<_>>();

        let deleted_splits = delete_splits_from_storage_and_metastore(
            index_uid.clone(),
            storage,
            self.metastore.clone(),
            splits_to_delete,
            None,
        )
        .await?;
        self.metastore.delete_index(index_uid).await?;

        Ok(deleted_splits)
    }

    /// Detect all dangling splits and associated files from the index and removes them.
    ///
    /// * `index_id` - The target index Id.
    /// * `grace_period` -  Threshold period after which a staged split can be garbage collected.
    /// * `dry_run` - Should this only return a list of affected files without performing deletion.
    pub async fn garbage_collect_index(
        &self,
        index_id: &str,
        grace_period: Duration,
        dry_run: bool,
    ) -> anyhow::Result<SplitRemovalInfo> {
        let index_metadata = self.metastore.index_metadata(index_id).await?;
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
    pub async fn clear_index(&self, index_id: &str) -> Result<(), IndexServiceError> {
        let index_metadata = self.metastore.index_metadata(index_id).await?;
        let index_uid = index_metadata.index_uid.clone();
        let storage = self
            .storage_resolver
            .resolve(index_metadata.index_uri())
            .await?;
        let splits = self.metastore.list_all_splits(index_uid.clone()).await?;
        let split_ids: Vec<&str> = splits.iter().map(|split| split.split_id()).collect();
        self.metastore
            .mark_splits_for_deletion(index_uid.clone(), &split_ids)
            .await?;
        let split_metas: Vec<SplitMetadata> = splits
            .into_iter()
            .map(|split| split.split_metadata)
            .collect();
        // FIXME: return an error.
        if let Err(err) = delete_splits_from_storage_and_metastore(
            index_uid.clone(),
            storage,
            self.metastore.clone(),
            split_metas,
            None,
        )
        .await
        {
            error!(metastore_uri=%self.metastore.uri(), index_id=%index_id, error=?err, "failed to delete all the split files during garbage collection");
        }
        for source_id in index_metadata.sources.keys() {
            self.metastore
                .reset_source_checkpoint(index_uid.clone(), source_id)
                .await?;
        }
        Ok(())
    }

    /// Creates a source config for index `index_id`.
    pub async fn create_source(
        &self,
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
        self.metastore
            .add_source(index_uid.clone(), source_config)
            .await?;
        info!(
            "source `{}` successfully created for index `{}`",
            source_id,
            index_uid.index_id()
        );
        let source = self
            .metastore
            .index_metadata(index_uid.index_id())
            .await?
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
        &self,
        index_id: &str,
        source_id: &str,
    ) -> Result<SourceConfig, IndexServiceError> {
        let source_config = self
            .metastore
            .index_metadata(index_id)
            .await?
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
    use quickwit_metastore::{metastore_for_test, SplitMetadata};
    use quickwit_storage::PutPayload;

    use super::*;

    #[tokio::test]
    async fn test_create_index() {
        let metastore = metastore_for_test();
        let storage_resolver = StorageResolver::ram_and_file_for_test();
        let index_service = IndexService::new(metastore.clone(), storage_resolver);
        let index_id = "test-index";
        let index_uri = "ram://indexes/test-index";
        let index_config = IndexConfig::for_test(index_id, index_uri);
        let index_metadata_0 = index_service
            .create_index(index_config.clone(), false)
            .await
            .unwrap();
        assert_eq!(index_metadata_0.index_id(), index_id);
        assert_eq!(index_metadata_0.index_uri(), &index_uri);
        assert!(metastore.index_exists(index_id).await.unwrap());

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
        let metastore = metastore_for_test();
        let storage_resolver = StorageResolver::ram_and_file_for_test();
        let storage = storage_resolver
            .resolve(&Uri::for_test("ram://indexes/test-index"))
            .await
            .unwrap();
        let index_service = IndexService::new(metastore.clone(), storage_resolver);
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
        metastore
            .stage_splits(index_uid.clone(), vec![split_metadata.clone()])
            .await
            .unwrap();

        let splits = metastore.list_all_splits(index_uid.clone()).await.unwrap();
        assert_eq!(splits.len(), 1);

        let split_path_str = format!("{}.split", split_id);
        let split_path = Path::new(&split_path_str);
        let payload: Box<dyn PutPayload> = Box::new(vec![0]);
        storage.put(split_path, payload).await.unwrap();
        assert!(storage.exists(split_path).await.unwrap());

        let split_infos = index_service.delete_index(index_id, false).await.unwrap();
        assert_eq!(split_infos.len(), 1);

        let error = metastore
            .list_all_splits(index_uid.clone())
            .await
            .unwrap_err();
        assert!(
            matches!(error, MetastoreError::NotFound(EntityKind::Index { index_id }) if index_id == index_uid.index_id())
        );
        assert!(!storage.exists(split_path).await.unwrap());
    }
}
