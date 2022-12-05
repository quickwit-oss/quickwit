// Copyright (C) 2022 Quickwit, Inc.
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

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use quickwit_common::fs::{empty_dir, get_cache_directory_path};
use quickwit_config::{validate_identifier, IndexConfig, QuickwitConfig, SourceConfig};
use quickwit_indexing::actors::INDEXING_DIR_NAME;
use quickwit_indexing::check_source_connectivity;
use quickwit_janitor::{
    delete_splits_with_files, run_garbage_collect, FileEntry, SplitDeletionError, SplitRemovalInfo,
};
use quickwit_metastore::{
    quickwit_metastore_uri_resolver, IndexMetadata, ListSplitsQuery, Metastore, MetastoreError,
    Split, SplitMetadata, SplitState,
};
use quickwit_proto::{ServiceError, ServiceErrorCode};
use quickwit_storage::{quickwit_storage_uri_resolver, StorageResolverError, StorageUriResolver};
use thiserror::Error;
use tracing::{error, info};

#[derive(Error, Debug)]
pub enum IndexServiceError {
    #[error("Failed to resolve the storage `{0}`.")]
    StorageError(#[from] StorageResolverError),
    #[error("Metastore error `{0}`.")]
    MetastoreError(#[from] MetastoreError),
    #[error("Split deletion error `{0}`.")]
    SplitDeletionError(#[from] SplitDeletionError),
    #[error("Invalid config: {0:#}.")]
    InvalidConfig(anyhow::Error),
    #[error("Invalid identifier: {0}.")]
    InvalidIdentifier(String),
    #[error("Internal error: {0}.")]
    InternalError(String),
}

impl ServiceError for IndexServiceError {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            Self::StorageError(_) => ServiceErrorCode::Internal,
            Self::MetastoreError(error) => error.status_code(),
            Self::SplitDeletionError(_) => ServiceErrorCode::Internal,
            Self::InvalidConfig(_) => ServiceErrorCode::BadRequest,
            Self::InvalidIdentifier(_) => ServiceErrorCode::BadRequest,
            Self::InternalError(_) => ServiceErrorCode::Internal,
        }
    }
}

/// Index service responsible for creating, updating and deleting indexes.
pub struct IndexService {
    metastore: Arc<dyn Metastore>,
    storage_resolver: StorageUriResolver,
}

impl IndexService {
    /// Creates an `IndexService`.
    pub fn new(metastore: Arc<dyn Metastore>, storage_resolver: StorageUriResolver) -> Self {
        Self {
            metastore,
            storage_resolver,
        }
    }

    pub async fn from_config(config: QuickwitConfig) -> anyhow::Result<Self> {
        let metastore = quickwit_metastore_uri_resolver()
            .resolve(&config.metastore_uri)
            .await?;
        let storage_resolver = quickwit_storage_uri_resolver().clone();
        let index_service = Self::new(metastore, storage_resolver);
        Ok(index_service)
    }

    /// Get an index from `index_id`.
    pub async fn get_index(&self, index_id: &str) -> Result<IndexMetadata, IndexServiceError> {
        let index_metadata = self.metastore.index_metadata(index_id).await?;
        Ok(index_metadata)
    }

    /// Get all splits from index `index_id`.
    pub async fn get_all_splits(&self, index_id: &str) -> Result<Vec<Split>, IndexServiceError> {
        let splits = self.metastore.list_all_splits(index_id).await?;
        Ok(splits)
    }

    /// Get all indexes.
    pub async fn list_indexes(&self) -> anyhow::Result<Vec<IndexMetadata>> {
        let indexes_metadatas = self.metastore.list_indexes_metadatas().await?;
        Ok(indexes_metadatas)
    }

    /// Creates an index from `IndexConfig`.
    pub async fn create_index(
        &self,
        index_config: IndexConfig,
        overwrite: bool,
    ) -> Result<IndexMetadata, IndexServiceError> {
        validate_storage_uri(quickwit_storage_uri_resolver(), &index_config)
            .await
            .map_err(IndexServiceError::InvalidConfig)?;

        // Delete existing index if it exists.
        if overwrite {
            match self.delete_index(&index_config.index_id, false).await {
                Ok(_)
                | Err(IndexServiceError::MetastoreError(MetastoreError::IndexDoesNotExist {
                    index_id: _,
                })) => {
                    // Ignore IndexDoesNotExist error.
                }
                Err(error) => {
                    return Err(error);
                }
            }
        }

        // Add default ingest-api & cli-ingest sources config.
        let index_id = index_config.index_id.clone();
        self.metastore.create_index(index_config).await?;
        self.metastore
            .add_source(&index_id, SourceConfig::ingest_api_default())
            .await?;
        self.metastore
            .add_source(&index_id, SourceConfig::cli_ingest_source())
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
    ) -> Result<Vec<FileEntry>, IndexServiceError> {
        let index_uri = self
            .metastore
            .index_metadata(index_id)
            .await?
            .into_index_config()
            .index_uri
            .clone();
        let storage = self.storage_resolver.resolve(&index_uri)?;

        if dry_run {
            let all_splits = self
                .metastore
                .list_all_splits(index_id)
                .await?
                .into_iter()
                .map(|metadata| metadata.split_metadata)
                .collect::<Vec<_>>();

            let file_entries_to_delete: Vec<FileEntry> =
                all_splits.iter().map(FileEntry::from).collect();
            return Ok(file_entries_to_delete);
        }

        // Schedule staged and published splits for deletion.
        let query = ListSplitsQuery::for_index(index_id)
            .with_split_states([SplitState::Staged, SplitState::Published]);
        let splits = self.metastore.list_splits(query).await?;
        let split_ids = splits
            .iter()
            .map(|meta| meta.split_id())
            .collect::<Vec<_>>();
        self.metastore
            .mark_splits_for_deletion(index_id, &split_ids)
            .await?;

        // Select splits to delete
        let query =
            ListSplitsQuery::for_index(index_id).with_split_state(SplitState::MarkedForDeletion);
        let splits_to_delete = self
            .metastore
            .list_splits(query)
            .await?
            .into_iter()
            .map(|metadata| metadata.split_metadata)
            .collect::<Vec<_>>();

        let deleted_entries = delete_splits_with_files(
            index_id,
            storage,
            self.metastore.clone(),
            splits_to_delete,
            None,
        )
        .await?;
        self.metastore.delete_index(index_id).await?;
        Ok(deleted_entries)
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
        let index_config = self
            .metastore
            .index_metadata(index_id)
            .await?
            .into_index_config();
        let storage = self.storage_resolver.resolve(&index_config.index_uri)?;

        let deleted_entries = run_garbage_collect(
            index_id,
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
    pub async fn clear_index(&self, index_id: &str) -> anyhow::Result<()> {
        let index_metadata = self.metastore.index_metadata(index_id).await?;
        let storage = self.storage_resolver.resolve(index_metadata.index_uri())?;
        let splits = self.metastore.list_all_splits(index_id).await?;
        let split_ids: Vec<&str> = splits.iter().map(|split| split.split_id()).collect();
        self.metastore
            .mark_splits_for_deletion(index_id, &split_ids)
            .await?;
        let split_metas: Vec<SplitMetadata> = splits
            .into_iter()
            .map(|split| split.split_metadata)
            .collect();
        // FIXME: return an error.
        if let Err(err) =
            delete_splits_with_files(index_id, storage, self.metastore.clone(), split_metas, None)
                .await
        {
            error!(metastore_uri=%self.metastore.uri(), index_id=%index_id, error=?err, "Failed to delete all the split files during garbage collection.");
        }
        for source_id in index_metadata.sources.keys() {
            self.metastore
                .reset_source_checkpoint(index_id, source_id)
                .await?;
        }
        Ok(())
    }

    /// Creates a source config for index `index_id`.
    pub async fn create_source(
        &self,
        index_id: &str,
        source_config: SourceConfig,
    ) -> Result<SourceConfig, IndexServiceError> {
        let source_id = source_config.source_id.clone();
        // This is a bit redundant, as SourceConfig deserialization also checks
        // that the indentifier is valid. However it authorizes the special
        // private names internal to quickwit, so we do an extra check.
        validate_identifier("Source ID", &source_id).map_err(|_| {
            IndexServiceError::InvalidIdentifier(format!("Invalid source ID: `{}`", source_id))
        })?;
        check_source_connectivity(&source_config)
            .await
            .map_err(IndexServiceError::InvalidConfig)?;
        self.metastore.add_source(index_id, source_config).await?;
        info!(
            "Source `{}` successfully created for index `{}`.",
            source_id, index_id
        );
        let source = self
            .metastore
            .index_metadata(index_id)
            .await?
            .sources
            .get(&source_id)
            .ok_or_else(|| {
                IndexServiceError::InternalError(
                    "Created source is not in index metadata, this should never happen."
                        .to_string(),
                )
            })?
            .clone();
        Ok(source)
    }

    pub async fn delete_source(
        &self,
        index_id: &str,
        source_id: &str,
    ) -> Result<(), IndexServiceError> {
        self.metastore.delete_source(index_id, source_id).await?;
        info!(
            "Source `{}` successfully deleted for index `{}`.",
            source_id, index_id
        );
        Ok(())
    }
}

/// Clears the cache directory of a given source.
///
/// * `data_dir_path` - Path to directory where data (tmp data, splits kept for caching purpose) is
///   persisted.
pub async fn clear_cache_directory(data_dir_path: &Path) -> anyhow::Result<()> {
    let cache_directory_path = get_cache_directory_path(data_dir_path);
    info!(path = %cache_directory_path.display(), "Clearing cache directory.");
    empty_dir(&cache_directory_path).await?;
    Ok(())
}

/// Removes the indexing directory of a given index.
///
/// * `data_dir_path` - Path to directory where data (tmp data, splits kept for caching purpose) is
///   persisted.
/// * `index_id` - The target index ID.
pub async fn remove_indexing_directory(data_dir_path: &Path, index_id: String) -> io::Result<()> {
    let indexing_directory_path = data_dir_path.join(INDEXING_DIR_NAME).join(index_id);
    info!(path = %indexing_directory_path.display(), "Clearing indexing directory.");
    match tokio::fs::remove_dir_all(indexing_directory_path.as_path()).await {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Resolve storage endpoints to validate.
pub async fn validate_storage_uri(
    storage_uri_resolver: &StorageUriResolver,
    index_config: &IndexConfig,
) -> anyhow::Result<()> {
    storage_uri_resolver.resolve(&index_config.index_uri)?;
    Ok(())
}
