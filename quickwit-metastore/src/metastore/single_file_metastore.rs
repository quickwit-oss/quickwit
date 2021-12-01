// Copyright (C) 2021 Quickwit, Inc.
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

use std::collections::HashMap;
use std::ops::{Range, RangeInclusive};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use quickwit_storage::{
    quickwit_storage_uri_resolver, Storage, StorageErrorKind, StorageResolverError,
    StorageUriResolver,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::checkpoint::CheckpointDelta;
use crate::metastore::match_tags_filter;
use crate::{
    IndexMetadata, Metastore, MetastoreError, MetastoreFactory, MetastoreResolverError,
    MetastoreResult, SplitInfo, SplitMetadata, SplitState,
};

/// Metadata file managed by [`SingleFileMetastore`].
const META_FILENAME: &str = "quickwit.json";

/// A MetadataSet carries an index metadata and its split metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataSet {
    /// Metadata specific to the index.
    pub index: IndexMetadata,
    /// List of splits belonging to the index.
    pub splits: HashMap<String, SplitInfo>,
}

/// Creates a path to the metadata file from the given index ID.
fn meta_path(index_id: &str) -> PathBuf {
    Path::new(index_id).join(Path::new(META_FILENAME))
}

/// Takes 2 intervals and returns true iff their intersection is empty
fn is_disjoint(left: &Range<i64>, right: &RangeInclusive<i64>) -> bool {
    left.end <= *right.start() || *right.end() < left.start
}

/// The underlying single file metastore implementation.
struct InnerSingleFileMetastore {
    storage: Arc<dyn Storage>,
    cache: HashMap<String, MetadataSet>,
}

impl InnerSingleFileMetastore {
    fn new(storage: Arc<dyn Storage>) -> Self {
        InnerSingleFileMetastore {
            storage,
            cache: HashMap::new(),
        }
    }

    /// Checks whether the index exists in storage.
    async fn index_exists(&self, index_id: &str) -> MetastoreResult<bool> {
        let metadata_path = meta_path(index_id);

        let exists = self
            .storage
            .exists(&metadata_path)
            .await
            .map_err(|storage_err| match storage_err.kind() {
                StorageErrorKind::DoesNotExist => MetastoreError::IndexDoesNotExist {
                    index_id: index_id.to_string(),
                },
                StorageErrorKind::Unauthorized => MetastoreError::Forbidden {
                    message: "The request credentials do not allow this operation.".to_string(),
                },
                _ => MetastoreError::InternalError {
                    message: "Failed to check index file existence.".to_string(),
                    cause: anyhow::anyhow!(storage_err),
                },
            })?;

        Ok(exists)
    }

    /// Returns all of the data associated with the given index.
    ///
    /// If the value is already in cache, then the call returns right away.
    /// If not, it is fetched from the storage.
    async fn get_index(&mut self, index_id: &str) -> MetastoreResult<MetadataSet> {
        // We first check if the index is in the cache...
        {
            if let Some(index_metadata) = self.cache.get(index_id) {
                return Ok(index_metadata.clone());
            }
        }

        // It is not in the cache yet, let's fetch it from the storage...
        let metadata_path = meta_path(index_id);
        let content = self
            .storage
            .get_all(&metadata_path)
            .await
            .map_err(|storage_err| match storage_err.kind() {
                StorageErrorKind::DoesNotExist => MetastoreError::IndexDoesNotExist {
                    index_id: index_id.to_string(),
                },
                StorageErrorKind::Unauthorized => MetastoreError::Forbidden {
                    message: "The request credentials do not allow for this operation.".to_string(),
                },
                _ => MetastoreError::InternalError {
                    message: "Failed to get index files.".to_string(),
                    cause: anyhow::anyhow!(storage_err),
                },
            })?;

        let metadata_set = serde_json::from_slice::<MetadataSet>(&content[..])
            .map_err(|serde_err| MetastoreError::InvalidManifest { cause: serde_err })?;

        if metadata_set.index.index_id != index_id {
            return Err(MetastoreError::InternalError {
                message: "Inconsistent manifest: index_id mismatch.".to_string(),
                cause: anyhow::anyhow!(
                    "Expected index_id `{}`, but found `{}`",
                    index_id,
                    metadata_set.index.index_id
                ),
            });
        }

        // Finally, update the cache accordingly
        self.cache
            .insert(index_id.to_string(), metadata_set.clone());

        Ok(metadata_set)
    }

    /// Serializes the metadata set and stores the data on the storage.
    async fn put_index(&mut self, metadata_set: MetadataSet) -> MetastoreResult<()> {
        // Serialize metadata set.
        let content: Vec<u8> = serde_json::to_vec_pretty(&metadata_set).map_err(|serde_err| {
            MetastoreError::InternalError {
                message: "Failed to serialize Metadata set".to_string(),
                cause: anyhow::anyhow!(serde_err),
            }
        })?;

        let index_id = metadata_set.index.index_id.clone();
        let metadata_path = meta_path(&index_id);

        // Put data back into storage.
        self.storage
            .put(&metadata_path, Box::new(content))
            .await
            .map_err(|storage_err| match storage_err.kind() {
                StorageErrorKind::Unauthorized => MetastoreError::Forbidden {
                    message: "The request credentials do not allow for this operation.".to_string(),
                },
                _ => MetastoreError::InternalError {
                    message: format!(
                        "Failed to write metastore file to `{}`.",
                        metadata_path.display()
                    ),
                    cause: anyhow::anyhow!(storage_err),
                },
            })?;

        // Update the internal data if the storage is successfully updated.
        self.cache.insert(index_id, metadata_set);

        Ok(())
    }

    /// Helper to publish a list of splits.
    fn publish_splits_helper<'a>(
        split_ids: &[&'a str],
        metadata_set: &mut MetadataSet,
    ) -> MetastoreResult<()> {
        let mut split_not_found_ids = vec![];
        let mut split_not_staged_ids = vec![];
        for &split_id in split_ids {
            // Check for the existence of split.
            let metadata = match metadata_set.splits.get_mut(split_id) {
                Some(metadata) => metadata,
                _ => {
                    split_not_found_ids.push(split_id.to_string());
                    continue;
                }
            };

            match metadata.split_state {
                SplitState::Published => {
                    // Split is already published. This is fine, we just skip it.
                    continue;
                }
                SplitState::Staged => {
                    // The split state needs to be updated.
                    metadata.split_state = SplitState::Published;
                    metadata.update_timestamp = Utc::now().timestamp();
                }
                _ => {
                    split_not_staged_ids.push(split_id.to_string());
                }
            }
        }

        if !split_not_found_ids.is_empty() {
            return Err(MetastoreError::SplitsDoNotExist {
                split_ids: split_not_found_ids,
            });
        }

        if !split_not_staged_ids.is_empty() {
            return Err(MetastoreError::SplitsNotStaged {
                split_ids: split_not_staged_ids,
            });
        }

        Ok(())
    }

    /// Helper to mark a list of splits for deletion.
    fn mark_splits_for_deletion_helper<'a>(
        split_ids: &[&'a str],
        metadata_set: &mut MetadataSet,
    ) -> MetastoreResult<bool> {
        let mut is_modified = false;
        let mut split_not_found_ids = vec![];
        for &split_id in split_ids {
            // Check for the existence of split.
            let metadata = match metadata_set.splits.get_mut(split_id) {
                Some(metadata) => metadata,
                None => {
                    split_not_found_ids.push(split_id.to_string());
                    continue;
                }
            };

            if metadata.split_state == SplitState::ScheduledForDeletion {
                // If the split is already scheduled for deletion, This is fine, we just skip it.
                continue;
            }

            metadata.split_state = SplitState::ScheduledForDeletion;
            metadata.update_timestamp = Utc::now().timestamp();
            is_modified = true;
        }

        if !split_not_found_ids.is_empty() {
            return Err(MetastoreError::SplitsDoNotExist {
                split_ids: split_not_found_ids,
            });
        }

        Ok(is_modified)
    }

    async fn create_index(&mut self, index_metadata: IndexMetadata) -> MetastoreResult<()> {
        // Check for the existence of index.
        let exists = self.index_exists(&index_metadata.index_id).await?;

        if exists {
            return Err(MetastoreError::IndexAlreadyExists {
                index_id: index_metadata.index_id.clone(),
            });
        }

        let metadata_set = MetadataSet {
            index: index_metadata,
            splits: HashMap::new(),
        };
        self.put_index(metadata_set).await?;

        Ok(())
    }

    async fn delete_index(&mut self, index_id: &str) -> MetastoreResult<()> {
        // Check whether the index exists.
        let exists = self.index_exists(index_id).await?;

        if !exists {
            return Err(MetastoreError::IndexDoesNotExist {
                index_id: index_id.to_string(),
            });
        }

        let metadata_path = meta_path(index_id);

        // Delete metadata set from storage.
        self.storage
            .delete(&metadata_path)
            .await
            .map_err(|storage_err| match storage_err.kind() {
                StorageErrorKind::DoesNotExist => MetastoreError::IndexDoesNotExist {
                    index_id: index_id.to_string(),
                },
                StorageErrorKind::Unauthorized => MetastoreError::Forbidden {
                    message: "The request credentials do not allow for this operation.".to_string(),
                },
                _ => MetastoreError::InternalError {
                    message: "Failed to delete metadata set from storage.".to_string(),
                    cause: anyhow::anyhow!(storage_err),
                },
            })?;

        // Update the internal data if the storage is successfully updated.
        self.cache.remove(index_id);

        Ok(())
    }

    async fn stage_split(
        &mut self,
        index_id: &str,
        split_metadata: SplitMetadata,
    ) -> MetastoreResult<()> {
        let mut metadata_set = self.get_index(index_id).await?;

        // Check whether the split exists.
        // If the split exists, return an error to prevent the split from being registered.
        if metadata_set.splits.contains_key(split_metadata.split_id()) {
            return Err(MetastoreError::InternalError {
                message: format!(
                    "Try to stage split that already exists ({})",
                    split_metadata.split_id()
                ),
                cause: anyhow::anyhow!(""),
            });
        }

        let metadata = SplitInfo {
            split_state: SplitState::Staged,

            update_timestamp: Utc::now().timestamp(),

            split_metadata,
        };

        metadata_set
            .splits
            .insert(metadata.split_id().to_string(), metadata);

        self.put_index(metadata_set).await?;

        Ok(())
    }

    async fn publish_splits<'a>(
        &mut self,
        index_id: &str,
        split_ids: &[&'_ str],
        checkpoint_delta: CheckpointDelta,
    ) -> MetastoreResult<()> {
        let mut metadata_set = self.get_index(index_id).await?;
        metadata_set
            .index
            .checkpoint
            .try_apply_delta(checkpoint_delta)?;

        Self::publish_splits_helper(split_ids, &mut metadata_set)?;
        self.put_index(metadata_set).await?;
        Ok(())
    }

    async fn replace_splits<'a>(
        &mut self,
        index_id: &str,
        new_split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        let mut metadata_set = self.get_index(index_id).await?;

        // Try to publish splits.
        Self::publish_splits_helper(new_split_ids, &mut metadata_set)?;

        // Mark splits for deletion.
        Self::mark_splits_for_deletion_helper(replaced_split_ids, &mut metadata_set)?;

        self.put_index(metadata_set).await?;
        Ok(())
    }

    async fn list_splits(
        &mut self,
        index_id: &str,
        state: SplitState,
        time_range_opt: Option<Range<i64>>,
        tags: &[String],
    ) -> MetastoreResult<Vec<SplitInfo>> {
        let time_range_filter = |split_metadata: &SplitMetadata| match (
            time_range_opt.as_ref(),
            split_metadata.time_range.as_ref(),
        ) {
            (Some(filter_time_range), Some(split_time_range)) => {
                !is_disjoint(filter_time_range, split_time_range)
            }
            _ => true, // Return `true` if `time_range` is omitted or the split has no time range.
        };

        let tag_filter = |split_metadata: &SplitMetadata| {
            let split_tags = split_metadata
                .tags
                .clone()
                .into_iter()
                .collect::<Vec<String>>();
            match_tags_filter(split_tags.as_slice(), tags)
        };

        let metadata_set = self.get_index(index_id).await?;
        let splits = metadata_set
            .splits
            .into_values()
            .filter(|metadata| {
                metadata.split_state == state
                    && time_range_filter(&metadata.split_metadata)
                    && tag_filter(&metadata.split_metadata)
            })
            .collect();
        Ok(splits)
    }

    async fn list_all_splits(&mut self, index_id: &str) -> MetastoreResult<Vec<SplitInfo>> {
        let metadata_set = self.get_index(index_id).await?;
        let splits = metadata_set.splits.into_values().collect();
        Ok(splits)
    }

    async fn mark_splits_for_deletion<'a>(
        &mut self,
        index_id: &str,
        split_ids: &[&'_ str],
    ) -> MetastoreResult<()> {
        let mut metadata_set = self.get_index(index_id).await?;

        let is_modified = Self::mark_splits_for_deletion_helper(split_ids, &mut metadata_set)?;
        if is_modified {
            self.put_index(metadata_set).await?;
        }

        Ok(())
    }

    async fn delete_splits<'a>(
        &mut self,
        index_id: &str,
        split_ids: &[&'_ str],
    ) -> MetastoreResult<()> {
        let mut metadata_set = self.get_index(index_id).await?;

        let mut split_not_found_ids = vec![];
        let mut split_not_deletable_ids = vec![];
        for &split_id in split_ids {
            // Check for the existence of split.
            let metadata = match metadata_set.splits.get_mut(split_id) {
                Some(metadata) => metadata,
                None => {
                    split_not_found_ids.push(split_id.to_string());
                    continue;
                }
            };

            match metadata.split_state {
                SplitState::ScheduledForDeletion | SplitState::Staged => {
                    // Only `ScheduledForDeletion` and `Staged` can be deleted
                    metadata_set.splits.remove(split_id);
                }
                _ => {
                    split_not_deletable_ids.push(split_id.to_string());
                }
            }
        }

        if !split_not_found_ids.is_empty() {
            return Err(MetastoreError::SplitsDoNotExist {
                split_ids: split_not_found_ids,
            });
        }

        if !split_not_deletable_ids.is_empty() {
            return Err(MetastoreError::SplitsNotDeletable {
                split_ids: split_not_deletable_ids,
            });
        }

        self.put_index(metadata_set).await?;

        Ok(())
    }

    async fn index_metadata(&mut self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        let metadata_set = self.get_index(index_id).await?;
        Ok(metadata_set.index)
    }
}

/// Single file metastore implementation.
pub struct SingleFileMetastore {
    uri: String,
    inner: Arc<Mutex<InnerSingleFileMetastore>>,
}

impl SingleFileMetastore {
    /// Creates a [`SingleFileMetastore`] for tests.
    #[doc(hidden)]
    pub fn for_test() -> Self {
        use quickwit_storage::RamStorage;
        SingleFileMetastore::new(Arc::new(RamStorage::default()))
    }

    /// Creates a [`SingleFileMetastore`] for a specified storage.
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self {
            uri: storage.uri(),
            inner: Arc::new(Mutex::new(InnerSingleFileMetastore::new(storage))),
        }
    }

    /// Helper used for testing to checks whether the index exists.
    #[cfg(test)]
    async fn index_exists(&self, index_id: &str) -> MetastoreResult<bool> {
        let inner_metastore = self.inner.lock().await;
        inner_metastore.index_exists(index_id).await
    }

    /// Helper used for testing to obtain the data associated with the given index.
    #[cfg(test)]
    async fn get_index(&self, index_id: &str) -> MetastoreResult<MetadataSet> {
        let mut inner_metastore = self.inner.lock().await;
        inner_metastore.get_index(index_id).await
    }
}

#[async_trait]
impl Metastore for SingleFileMetastore {
    async fn create_index(&self, index_metadata: IndexMetadata) -> MetastoreResult<()> {
        let mut inner_metastore = self.inner.lock().await;
        inner_metastore.create_index(index_metadata).await
    }

    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()> {
        let mut inner_metastore = self.inner.lock().await;
        inner_metastore.delete_index(index_id).await
    }

    async fn stage_split(&self, index_id: &str, metadata: SplitMetadata) -> MetastoreResult<()> {
        let mut inner_metastore = self.inner.lock().await;
        inner_metastore.stage_split(index_id, metadata).await
    }

    async fn publish_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        checkpoint_delta: CheckpointDelta,
    ) -> MetastoreResult<()> {
        let mut inner_metastore = self.inner.lock().await;
        inner_metastore
            .publish_splits(index_id, split_ids, checkpoint_delta)
            .await
    }

    async fn replace_splits<'a>(
        &self,
        index_id: &str,
        new_split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        let mut inner_metastore = self.inner.lock().await;
        inner_metastore
            .replace_splits(index_id, new_split_ids, replaced_split_ids)
            .await
    }

    async fn list_splits(
        &self,
        index_id: &str,
        state: SplitState,
        time_range_opt: Option<Range<i64>>,
        tags: &[String],
    ) -> MetastoreResult<Vec<SplitInfo>> {
        let mut inner_metastore = self.inner.lock().await;
        inner_metastore
            .list_splits(index_id, state, time_range_opt, tags)
            .await
    }

    async fn list_all_splits(&self, index_id: &str) -> MetastoreResult<Vec<SplitInfo>> {
        let mut inner_metastore = self.inner.lock().await;
        inner_metastore.list_all_splits(index_id).await
    }

    async fn mark_splits_for_deletion<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        let mut inner_metastore = self.inner.lock().await;
        inner_metastore
            .mark_splits_for_deletion(index_id, split_ids)
            .await
    }

    async fn delete_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        let mut inner_metastore = self.inner.lock().await;
        inner_metastore.delete_splits(index_id, split_ids).await
    }

    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        let mut inner_metastore = self.inner.lock().await;
        inner_metastore.index_metadata(index_id).await
    }

    fn uri(&self) -> String {
        self.uri.clone()
    }

    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.inner
            .lock()
            .await
            .storage
            .check()
            .await
            .map_err(Into::into)
    }
}

/// A single file metastore factory
#[derive(Clone)]
pub struct SingleFileMetastoreFactory {
    storage_uri_resolver: StorageUriResolver,
}

impl Default for SingleFileMetastoreFactory {
    fn default() -> Self {
        SingleFileMetastoreFactory {
            storage_uri_resolver: quickwit_storage_uri_resolver().clone(),
        }
    }
}

#[async_trait]
impl MetastoreFactory for SingleFileMetastoreFactory {
    async fn resolve(&self, uri: &str) -> Result<Arc<dyn Metastore>, MetastoreResolverError> {
        let storage = self
            .storage_uri_resolver
            .resolve(uri)
            .map_err(|err| match err {
                StorageResolverError::InvalidUri { message } => {
                    MetastoreResolverError::InvalidUri(message)
                }
                StorageResolverError::ProtocolUnsupported { protocol } => {
                    MetastoreResolverError::ProtocolUnsupported(protocol)
                }
                StorageResolverError::FailedToOpenStorage { kind, message } => {
                    MetastoreResolverError::FailedToOpenMetastore(MetastoreError::InternalError {
                        message: format!("Failed to open metastore file `{}`.", uri),
                        cause: anyhow::anyhow!("StorageError {:?}: {}.", kind, message),
                    })
                }
            })?;

        Ok(Arc::new(SingleFileMetastore::new(storage)))
    }
}

#[cfg(test)]
#[async_trait]
impl crate::tests::test_suite::DefaultForTest for SingleFileMetastore {
    async fn default_for_test() -> Self {
        use quickwit_storage::RamStorage;
        SingleFileMetastore::new(Arc::new(RamStorage::default()))
    }
}

metastore_test_suite!(crate::SingleFileMetastore);

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::RangeInclusive;
    use std::path::Path;
    use std::sync::Arc;

    use chrono::Utc;
    use quickwit_index_config::WikipediaIndexConfig;
    use quickwit_storage::{MockStorage, StorageErrorKind};
    use rand::Rng;
    use tokio::time::Duration;

    use crate::checkpoint::{Checkpoint, CheckpointDelta};
    use crate::metastore::single_file_metastore::{meta_path, MetadataSet};
    use crate::{
        IndexMetadata, Metastore, MetastoreError, SingleFileMetastore, SplitMetadata, SplitState,
    };

    #[tokio::test]
    async fn test_single_file_metastore_index_exists() {
        let metastore = SingleFileMetastore::for_test();
        let index_id = "my-index";

        {
            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = false;
            assert_eq!(result, expected);

            let index_metadata = IndexMetadata {
                index_id: index_id.to_string(),
                index_uri: "ram://indexes/my-index".to_string(),
                index_config: Arc::new(WikipediaIndexConfig::default()),
                checkpoint: Checkpoint::default(),
            };

            // Create index
            metastore.create_index(index_metadata).await.unwrap();

            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = true;
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_single_file_metastore_get_index() {
        let metastore = SingleFileMetastore::for_test();
        let index_id = "my-index";

        {
            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = false;
            assert_eq!(result, expected);

            let index_metadata = IndexMetadata {
                index_id: index_id.to_string(),
                index_uri: "ram://indexes/my-index".to_string(),
                index_config: Arc::new(WikipediaIndexConfig::default()),
                checkpoint: Checkpoint::default(),
            };

            // Create index
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = true;
            assert_eq!(result, expected);

            // Open index and check its metadata
            let created_index = metastore.get_index(index_id).await.unwrap();
            assert_eq!(created_index.index.index_id, index_metadata.index_id);
            assert_eq!(
                created_index.index.index_uri.clone(),
                index_metadata.index_uri
            );

            assert_eq!(
                format!("{:?}", created_index.index.index_config),
                "WikipediaIndexConfig".to_string()
            );

            // Open a non-existent index.
            let metastore_error = metastore.get_index("non-existent-index").await.unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::IndexDoesNotExist { .. }
            ));
        }
    }

    #[tokio::test]
    async fn test_single_file_metastore_storage_failing() {
        // The single file metastore should not update its internal state if the storage fails.
        let mut mock_storage = MockStorage::default();

        let current_timestamp = Utc::now().timestamp();

        mock_storage
            .expect_uri()
            .times(1)
            .returning(|| String::from("file:///indexes/mocks"));
        mock_storage // remove this if we end up changing the semantics of create.
            .expect_exists()
            .returning(|_| Ok(false));
        mock_storage.expect_put().times(2).returning(|uri, _| {
            assert_eq!(uri, Path::new("my-index/quickwit.json"));
            Ok(())
        });
        mock_storage.expect_put().times(1).returning(|_uri, _| {
            Err(StorageErrorKind::Io
                .with_error(anyhow::anyhow!("Oops. Some network problem maybe?")))
        });

        let metastore = SingleFileMetastore::new(Arc::new(mock_storage));

        let index_id = "my-index";
        let split_id = "split-one";
        let split_metadata = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id.to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(0, 99)),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let index_metadata = IndexMetadata {
            index_id: index_id.to_string(),
            index_uri: "ram://indexes/my-index".to_string(),
            index_config: Arc::new(quickwit_index_config::default_config_for_tests()),
            checkpoint: Checkpoint::default(),
        };

        // create index
        metastore.create_index(index_metadata).await.unwrap();

        // stage split
        metastore
            .stage_split(index_id, split_metadata)
            .await
            .unwrap();

        // publish split fails
        let err = metastore
            .publish_splits(index_id, &[split_id], CheckpointDelta::default())
            .await;
        assert!(err.is_err());

        // empty
        let split = metastore
            .list_splits(index_id, SplitState::Published, None, &[])
            .await
            .unwrap();
        assert!(split.is_empty());

        // not empty
        let split = metastore
            .list_splits(index_id, SplitState::Staged, None, &[])
            .await
            .unwrap();
        assert!(!split.is_empty());
    }

    #[tokio::test]
    async fn test_single_file_metastore_get_index_checks_for_inconsistent_index_id() {
        let metastore = SingleFileMetastore::for_test();
        let index_id = "my-index";

        // put inconsitent index into storage
        let metadata_set = MetadataSet {
            index: IndexMetadata {
                index_id: "inconsistent_index_id".to_string(),
                index_uri: "ram://indexes/my-index".to_string(),
                index_config: Arc::new(WikipediaIndexConfig::default()),
                checkpoint: Checkpoint::default(),
            },
            splits: HashMap::new(),
        };
        let content: Vec<u8> = serde_json::to_vec(&metadata_set).unwrap();
        let metadata_path = meta_path(index_id);
        metastore
            .inner
            .lock()
            .await
            .storage
            .put(&metadata_path, Box::new(content))
            .await
            .unwrap();

        // getting metadatset with inconsistent indexi_id should raise an error.
        let metastore_error = metastore.get_index(index_id).await.unwrap_err();
        assert!(matches!(
            metastore_error,
            MetastoreError::InternalError { .. }
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_single_file_metastore_race_condition() {
        let metastore = Arc::new(SingleFileMetastore::for_test());
        let index_id = "my-index";

        let index_metadata = IndexMetadata {
            index_id: index_id.to_string(),
            index_uri: "ram://indexes/my-index".to_string(),
            index_config: Arc::new(WikipediaIndexConfig::default()),
            checkpoint: Checkpoint::default(),
        };

        // Create index
        metastore.create_index(index_metadata).await.unwrap();

        // Stage the split in multiple threads
        let mut handles = Vec::new();
        let mut random_generator = rand::thread_rng();
        for i in 1..=20 {
            let sleep_duration = Duration::from_millis(random_generator.gen_range(0..=200));
            let metastore = metastore.clone();
            let current_timestamp = Utc::now().timestamp();
            let handle = tokio::spawn(async move {
                let split_metadata = SplitMetadata {
                    footer_offsets: 1000..2000,
                    split_id: format!("split-{}", i),
                    num_docs: 1,
                    original_size_in_bytes: 2,
                    time_range: Some(RangeInclusive::new(0, 99)),
                    create_timestamp: current_timestamp,
                    ..Default::default()
                };
                // stage split
                metastore
                    .stage_split(index_id, split_metadata)
                    .await
                    .unwrap();

                tokio::time::sleep(sleep_duration).await;

                // publish split
                let split_id = format!("split-{}", i);
                metastore
                    .publish_splits(index_id, &[&split_id], CheckpointDelta::default())
                    .await
                    .unwrap();
            });
            handles.push(handle);
        }

        futures::future::try_join_all(handles).await.unwrap();

        let splits = metastore
            .list_splits(index_id, SplitState::Published, None, &[])
            .await
            .unwrap();

        // Make sure that all 20 splits are in `Published`
        assert_eq!(splits.len(), 20);
    }
}
