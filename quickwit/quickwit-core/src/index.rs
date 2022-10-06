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
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use quickwit_common::fs::empty_dir;
use quickwit_common::uri::Uri;
use quickwit_config::{ingest_api_default_source_config, IndexConfig, QuickwitConfig};
use quickwit_indexing::actors::INDEXING_DIR_NAME;
use quickwit_indexing::models::CACHE;
use quickwit_janitor::{
    delete_splits_with_files, run_garbage_collect, FileEntry, SplitDeletionError,
};
use quickwit_metastore::{
    quickwit_metastore_uri_resolver, IndexMetadata, Metastore, MetastoreError, Split,
    SplitMetadata, SplitState,
};
use quickwit_proto::{ServiceError, ServiceErrorCode};
use quickwit_storage::{quickwit_storage_uri_resolver, StorageResolverError, StorageUriResolver};
use tantivy::time::OffsetDateTime;
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
    #[error("Invalid index config: {0}.")]
    InvalidIndexConfig(String),
}

impl ServiceError for IndexServiceError {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            Self::StorageError(_) => ServiceErrorCode::Internal,
            Self::MetastoreError(_) => ServiceErrorCode::Internal,
            Self::SplitDeletionError(_) => ServiceErrorCode::Internal,
            Self::InvalidIndexConfig(_) => ServiceErrorCode::BadRequest,
        }
    }
}

/// Index service responsible for creating, updating and deleting indexes.
pub struct IndexService {
    metastore: Arc<dyn Metastore>,
    storage_resolver: StorageUriResolver,
    default_index_root_uri: Uri,
}

impl IndexService {
    /// Creates an `IndexService`.
    pub fn new(
        metastore: Arc<dyn Metastore>,
        storage_resolver: StorageUriResolver,
        default_index_root_uri: Uri,
    ) -> Self {
        Self {
            metastore,
            storage_resolver,
            default_index_root_uri,
        }
    }

    pub async fn from_config(config: QuickwitConfig) -> anyhow::Result<Self> {
        let metastore = quickwit_metastore_uri_resolver()
            .resolve(&config.metastore_uri)
            .await?;
        let storage_resolver = quickwit_storage_uri_resolver().clone();
        let index_service = Self::new(metastore, storage_resolver, config.default_index_root_uri);
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
        index_config
            .validate()
            .map_err(|error| IndexServiceError::InvalidIndexConfig(error.to_string()))?;
        let index_id = index_config.index_id.clone();
        let index_uri = if let Some(index_uri) = &index_config.index_uri {
            index_uri.clone()
        } else {
            let index_uri = self.default_index_root_uri.join(&index_id).expect(
                "Failed to create default index URI. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.",
            );
            info!(
                index_id = %index_id,
                index_uri = %index_uri,
                "Index config does not specify `index_uri`, falling back to default value.",
            );
            index_uri
        };
        let index_metadata = IndexMetadata {
            index_id,
            index_uri,
            checkpoint: Default::default(),
            sources: index_config.sources(),
            doc_mapping: index_config.doc_mapping,
            indexing_settings: index_config.indexing_settings,
            search_settings: index_config.search_settings,
            retention_policy: index_config.retention_policy,
            create_timestamp: OffsetDateTime::now_utc().unix_timestamp(),
            update_timestamp: OffsetDateTime::now_utc().unix_timestamp(),
        };
        self.metastore.create_index(index_metadata).await?;

        // Create a default enabled ingest-api source.
        let source_config = ingest_api_default_source_config();
        self.metastore
            .add_source(&index_config.index_id, source_config)
            .await?;
        let index_metadata = self
            .metastore
            .index_metadata(&index_config.index_id)
            .await?;
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
        let index_uri = self.metastore.index_metadata(index_id).await?.index_uri;
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
        let staged_splits = self
            .metastore
            .list_splits(index_id, SplitState::Staged, None, None)
            .await?;
        let published_splits = self
            .metastore
            .list_splits(index_id, SplitState::Published, None, None)
            .await?;
        let split_ids = staged_splits
            .iter()
            .chain(published_splits.iter())
            .map(|meta| meta.split_id())
            .collect::<Vec<_>>();
        self.metastore
            .mark_splits_for_deletion(index_id, &split_ids)
            .await?;

        // Select splits to delete
        let splits_to_delete = self
            .metastore
            .list_splits(index_id, SplitState::MarkedForDeletion, None, None)
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
    ) -> anyhow::Result<Vec<FileEntry>> {
        let index_uri = self.metastore.index_metadata(index_id).await?.index_uri;
        let storage = self.storage_resolver.resolve(&index_uri)?;

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
        let storage = self.storage_resolver.resolve(&index_metadata.index_uri)?;
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
}

/// Helper function to get the cache path.
pub fn get_cache_directory_path(data_dir_path: &Path, index_id: &str, source_id: &str) -> PathBuf {
    data_dir_path
        .join(INDEXING_DIR_NAME)
        .join(index_id)
        .join(source_id)
        .join(CACHE)
}

/// Clears the cache directory of a given source.
///
/// * `data_dir_path` - Path to directory where data (tmp data, splits kept for caching purpose) is
///   persisted.
/// * `index_id` - The target index Id.
/// * `source_id` -  The source Id.
pub async fn clear_cache_directory(
    data_dir_path: &Path,
    index_id: String,
    source_id: String,
) -> anyhow::Result<()> {
    let cache_directory_path = get_cache_directory_path(data_dir_path, &index_id, &source_id);
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
    quickwit_config: &QuickwitConfig,
    index_config: &IndexConfig,
) -> anyhow::Result<()> {
    storage_uri_resolver.resolve(&quickwit_config.default_index_root_uri)?;

    // Optional: check custom index uri
    if let Some(index_uri) = index_config.index_uri.as_ref() {
        storage_uri_resolver.resolve(index_uri)?;
    }
    Ok(())
}
