/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_storage::StorageResolverError;
use quickwit_storage::StorageUriResolver;
use tokio::sync::RwLock;

use quickwit_doc_mapping::DocMapping;
use quickwit_storage::{PutPayload, Storage};

use crate::MetastoreFactory;
use crate::MetastoreResolverError;
use crate::{
    IndexMetadata, MetadataSet, Metastore, MetastoreErrorKind, MetastoreResult, SplitMetadata,
    SplitState,
};

/// A metadata filename that managed by SingleFileMetastore.
const META_FILENAME: &str = "quickwit.json";

/// Create a path to the metadata file from the given index path.
fn meta_path(index_id: &str) -> PathBuf {
    Path::new(index_id).join(Path::new(META_FILENAME))
}

/// Takes 2 semi-open intervals and returns true iff their intersection is empty
fn is_disjoint(left: &Range<u64>, right: &Range<u64>) -> bool {
    left.end <= right.start || right.end <= left.start
}

/// Single file meta store implementation.
pub struct SingleFileMetastore {
    storage: Arc<dyn Storage>,
    cache: Arc<RwLock<HashMap<String, MetadataSet>>>,
}

#[allow(dead_code)]
impl SingleFileMetastore {
    #[cfg(test)]
    pub fn for_test() -> Self {
        use quickwit_storage::RamStorage;
        SingleFileMetastore::new(Arc::new(RamStorage::default()))
    }

    /// Creates a meta store given a storage.
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        SingleFileMetastore {
            storage,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Check the index exists in storage.
    async fn index_exists(&self, index_id: &str) -> MetastoreResult<bool> {
        let path = meta_path(index_id);

        let exist = self.storage.exists(&path).await.map_err(|e| {
            MetastoreErrorKind::InternalError
                .with_error(e.add_context("Failed to check the existence of the index on storage"))
        })?;

        Ok(exist)
    }

    /// Returns all of the data associated with the given index.
    ///
    /// If the value is already in cache, then the call returns right away.
    /// If not, it is fetched from the storage.
    async fn get_index(&self, index_id: &str) -> MetastoreResult<MetadataSet> {
        // We first check if the index is in the cache...
        {
            let cache = self.cache.read().await;
            if let Some(index_metadata) = cache.get(index_id) {
                return Ok(index_metadata.clone());
            }
        }

        // It is not in the cache yet, let's fetch it from the storage...
        let path = meta_path(index_id);
        let contents = self.storage.get_all(&path).await.map_err(|e| {
            MetastoreErrorKind::IndexDoesNotExist.with_error(anyhow::anyhow!(
                "The index does not exist on storage: {:?}",
                e
            ))
        })?;

        let metadata_set =
            serde_json::from_slice::<MetadataSet>(contents.as_slice()).map_err(|e| {
                MetastoreErrorKind::InvalidManifest.with_error(anyhow::anyhow!(
                    "Failed to deserialize metadata set: {:?}",
                    e
                ))
            })?;

        // And update the cache accordingly
        let mut cache = self.cache.write().await;
        cache.insert(index_id.to_string(), metadata_set.clone());

        Ok(metadata_set)
    }

    async fn put_index(&self, metadata_set: MetadataSet) -> MetastoreResult<()> {
        // Serialize metadata set.
        let contents = serde_json::to_vec(&metadata_set).map_err(|e| {
            MetastoreErrorKind::InvalidManifest
                .with_error(anyhow::anyhow!("Failed to serialize meta data: {:?}", e))
        })?;

        let index_id = metadata_set.index.index_id.clone();
        let path = meta_path(&index_id);

        // Put data back into storage.
        self.storage
            .put(&path, PutPayload::from(contents))
            .await
            .map_err(|e| {
                MetastoreErrorKind::InternalError.with_error(anyhow::anyhow!(
                    "Failed to put metadata set back into storage: {:?}",
                    e
                ))
            })?;

        // Update the internal data if the storage is successfully updated.
        let mut cache = self.cache.write().await;
        cache.insert(index_id, metadata_set.clone());

        Ok(())
    }
}

#[async_trait]
impl Metastore for SingleFileMetastore {
    async fn create_index(
        &self,
        index_metadata: IndexMetadata,
        _doc_mapping: DocMapping,
    ) -> MetastoreResult<()> {
        // Check for the existence of index.
        let exists = self
            .index_exists(&index_metadata.index_id)
            .await
            .map_err(|e| {
                MetastoreErrorKind::InternalError.with_error(anyhow::anyhow!(
                    "Failed to check the existence of the index on storage: {:?}",
                    e
                ))
            })?;
        if exists {
            return Err(
                MetastoreErrorKind::ExistingIndexUri.with_error(anyhow::anyhow!(
                    "The index already exists: {:?}",
                    index_metadata.index_id
                )),
            );
        }

        let metadata_set = MetadataSet {
            index: index_metadata,
            splits: HashMap::new(),
        };

        self.put_index(metadata_set).await?;

        Ok(())
    }

    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()> {
        // Check for the existence of index.
        let exists = self.index_exists(index_id).await.map_err(|e| {
            MetastoreErrorKind::InternalError.with_error(anyhow::anyhow!(
                "Failed to check the existence of the index on storage: {:?}",
                e
            ))
        })?;
        if !exists {
            return Err(MetastoreErrorKind::IndexDoesNotExist
                .with_error(anyhow::anyhow!("The index does not exist: {:?}", index_id)));
        }

        let path = meta_path(index_id);

        // Delete metadata set form storage.
        self.storage.delete(&path).await.map_err(|e| {
            MetastoreErrorKind::InternalError.with_error(anyhow::anyhow!(
                "Failed to delete metadata set on storage: {:?}",
                e
            ))
        })?;

        // Update the internal data if the storage is successfully updated.
        let mut cache = self.cache.write().await;
        cache.remove(index_id);

        Ok(())
    }

    async fn stage_split(
        &self,
        index_id: &str,
        mut split_metadata: SplitMetadata,
    ) -> MetastoreResult<()> {
        let mut metadata_set = self.get_index(index_id).await?;

        // Check for the existence of split.
        // If split exists, return an error to prevent the split from being registered.
        if metadata_set.splits.contains_key(&split_metadata.split_id) {
            return Err(
                MetastoreErrorKind::ExistingSplitId.with_error(anyhow::anyhow!(
                    "Split already exists: {:?}",
                    &split_metadata.split_id
                )),
            );
        }

        // Insert a new split metadata as `Staged` state.
        split_metadata.split_state = SplitState::Staged;
        metadata_set
            .splits
            .insert(split_metadata.split_id.to_string(), split_metadata);

        self.put_index(metadata_set).await?;

        Ok(())
    }

    async fn publish_split(&self, index_id: &str, split_id: &str) -> MetastoreResult<()> {
        let mut metadata_set = self.get_index(index_id).await?;

        // Check for the existence of split.
        let split_metadata = metadata_set.splits.get_mut(split_id).ok_or_else(|| {
            MetastoreErrorKind::SplitDoesNotExist
                .with_error(anyhow::anyhow!("Split does not exist: {:?}", split_id))
        })?;

        // Check the split state.
        match split_metadata.split_state {
            SplitState::Published => {
                // If the split is already published, this API call returns a success.
                return Ok(());
            }
            SplitState::Staged => {
                // Update the split state to `Published`.
                split_metadata.split_state = SplitState::Published;
            }
            _ => {
                return Err(MetastoreErrorKind::SplitIsNotStaged
                    .with_error(anyhow::anyhow!("Split ID is not staged: {:?}", split_id)));
            }
        }

        self.put_index(metadata_set).await?;

        Ok(())
    }

    async fn list_splits(
        &self,
        index_id: &str,
        state: SplitState,
        time_range_opt: Option<Range<u64>>,
    ) -> MetastoreResult<Vec<SplitMetadata>> {
        let metadata_set = self.get_index(index_id).await?;

        // filter by split state.
        let split_with_meta_matching_state_it = metadata_set
            .splits
            .iter()
            .filter(|&(_split_id, split_metadata)| split_metadata.split_state == state);

        let mut splits: Vec<SplitMetadata> = Vec::new();
        for (_, split_metadata) in split_with_meta_matching_state_it {
            let match_filter_time_range =
                match (time_range_opt.as_ref(), split_metadata.time_range.as_ref()) {
                    (Some(filter_time_range), Some(split_time_range)) => {
                        !is_disjoint(split_time_range, filter_time_range)
                    }
                    _ => true, //< `time_range` is omitted or regardless of the time range filter,
                               // if a split has no timestamp it is always returned.
                };
            if match_filter_time_range {
                splits.push(split_metadata.clone());
            }
        }

        Ok(splits)
    }

    async fn mark_split_as_deleted(&self, index_id: &str, split_id: &str) -> MetastoreResult<()> {
        let mut metadata_set = self.get_index(index_id).await?;

        // Check for the existence of split.
        let split_metadata = metadata_set.splits.get_mut(split_id).ok_or_else(|| {
            MetastoreErrorKind::SplitDoesNotExist
                .with_error(anyhow::anyhow!("Split does not exists: {:?}", split_id))
        })?;

        if split_metadata.split_state == SplitState::ScheduledForDeletion {
            // If the split is already scheduled for deleted, this API call returns a success.
            return Ok(());
        }

        split_metadata.split_state = SplitState::ScheduledForDeletion;

        self.put_index(metadata_set).await?;

        Ok(())
    }

    async fn delete_split(&self, index_id: &str, split_id: &str) -> MetastoreResult<()> {
        let mut metadata_set = self.get_index(index_id).await?;

        // Check for the existence of split.
        let split_metadata = metadata_set.splits.get_mut(split_id).ok_or_else(|| {
            MetastoreErrorKind::SplitDoesNotExist
                .with_error(anyhow::anyhow!("Split does not exist: {:?}", split_id))
        })?;

        match split_metadata.split_state {
            SplitState::ScheduledForDeletion | SplitState::Staged => {
                // Only `ScheduledForDeletion` and `Staged` can be deleted
                metadata_set.splits.remove(split_id);
            }
            _ => {
                return Err(MetastoreErrorKind::Forbidden.with_error(anyhow::anyhow!(
                    "This split is not a deletable state: {:?}:{:?}",
                    split_id,
                    &split_metadata.split_state
                )));
            }
        };

        self.put_index(metadata_set).await?;

        Ok(())
    }

    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        let index_metadata = self.get_index(index_id).await?;
        Ok(index_metadata.index)
    }
}

/// A SingeFileMetastore factory
#[derive(Debug, Clone)]
pub struct SingleFileMetastoreFactory {}

impl Default for SingleFileMetastoreFactory {
    fn default() -> Self {
        SingleFileMetastoreFactory {}
    }
}

#[async_trait]
impl MetastoreFactory for SingleFileMetastoreFactory {
    fn protocols(&self) -> Vec<String> {
        vec!["file".to_string(), "s3".to_string()]
    }

    async fn resolve(&self, uri: String) -> Result<Arc<dyn Metastore>, MetastoreResolverError> {
        let storage = StorageUriResolver::default()
            .resolve(&uri)
            .map_err(|err| match err {
                StorageResolverError::InvalidUri(err_msg) => {
                    MetastoreResolverError::InvalidUri(err_msg)
                }
                StorageResolverError::ProtocolUnsupported(err_msg) => {
                    MetastoreResolverError::ProtocolUnsupported(err_msg)
                }
                StorageResolverError::FailedToOpenStorage(err) => {
                    MetastoreResolverError::FailedToOpenMetastore(
                        MetastoreErrorKind::InternalError.with_error(err),
                    )
                }
            })?;
        // TODO: remove unwrap
        let metastore = SingleFileMetastore::new(storage);
        Ok(Arc::new(metastore))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::ops::Range;
    use std::path::Path;
    use std::sync::Arc;

    use crate::IndexMetadata;
    use crate::{Metastore, MetastoreErrorKind, SingleFileMetastore, SplitMetadata, SplitState};
    use quickwit_doc_mapping::DocMapping;
    use quickwit_storage::{MockStorage, StorageErrorKind};

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
            };

            // Create index
            metastore
                .create_index(index_metadata, DocMapping::Dynamic)
                .await
                .unwrap();

            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = true;
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_single_file_metastore_create_index() {
        let metastore = SingleFileMetastore::for_test();
        let index_id = "my-index";

        {
            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = false;
            assert_eq!(result, expected);

            let index_metadata = IndexMetadata {
                index_id: index_id.to_string(),
                index_uri: "ram://indexes//my-index".to_string(),
            };

            // Create index
            metastore
                .create_index(index_metadata.clone(), DocMapping::Dynamic)
                .await
                .unwrap();

            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = true;
            assert_eq!(result, expected);

            let result = metastore
                .create_index(index_metadata, DocMapping::Dynamic)
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::ExistingIndexUri;
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
                index_uri: "ram://indexes//my-index".to_string(),
            };
            // Create index
            metastore
                .create_index(index_metadata, DocMapping::Dynamic)
                .await
                .unwrap();

            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = true;
            assert_eq!(result, expected);

            // Open index
            metastore.get_index(index_id).await.unwrap();

            // Open a non-existent index.
            let result = metastore
                .get_index("non-existent-index")
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::IndexDoesNotExist;
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_single_file_metastore_delete_index() {
        let metastore = SingleFileMetastore::for_test();
        let index_id = "my-index";

        {
            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = false;
            assert_eq!(result, expected);

            let index_metadata = IndexMetadata {
                index_id: index_id.to_string(),
                index_uri: "ram://indexes//my-index".to_string(),
            };

            // Create index
            metastore
                .create_index(index_metadata, DocMapping::Dynamic)
                .await
                .unwrap();

            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = true;
            assert_eq!(result, expected);

            // Delete index
            metastore.delete_index(index_id).await.unwrap();

            // Delete a non-existent index.
            let result = metastore
                .delete_index("non-existent-index")
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::IndexDoesNotExist;
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_single_file_metastore_stage_split() {
        let metastore = SingleFileMetastore::for_test();
        let index_id = "my-index";
        let split_id = "one";
        let split_metadata = SplitMetadata {
            split_id: split_id.to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: Some(Range { start: 0, end: 100 }),
            generation: 3,
        };

        {
            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = false;
            assert_eq!(result, expected);

            let index_metadata = IndexMetadata {
                index_id: index_id.to_string(),
                index_uri: "ram://indexes/my-index".to_string(),
            };

            // Create index
            metastore
                .create_index(index_metadata, DocMapping::Dynamic)
                .await
                .unwrap();

            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = true;
            assert_eq!(result, expected);

            // stage split
            metastore
                .stage_split(index_id, split_metadata.clone())
                .await
                .unwrap();
        }

        {
            let cache = metastore.cache.read().await;
            assert_eq!(cache.get(index_id).unwrap().splits.len(), 1);
            assert_eq!(
                cache
                    .get(index_id)
                    .unwrap()
                    .splits
                    .get(split_id)
                    .unwrap()
                    .split_id,
                "one".to_string()
            );
            assert_eq!(
                cache
                    .get(index_id)
                    .unwrap()
                    .splits
                    .get(split_id)
                    .unwrap()
                    .split_state,
                SplitState::Staged
            );
            assert_eq!(
                cache
                    .get(index_id)
                    .unwrap()
                    .splits
                    .get(split_id)
                    .unwrap()
                    .num_records,
                1
            );
            assert_eq!(
                cache
                    .get(index_id)
                    .unwrap()
                    .splits
                    .get(split_id)
                    .unwrap()
                    .size_in_bytes,
                2
            );
            assert_eq!(
                cache
                    .get(index_id)
                    .unwrap()
                    .splits
                    .get(split_id)
                    .unwrap()
                    .time_range,
                Some(Range { start: 0, end: 100 })
            );
            assert_eq!(
                cache
                    .get(index_id)
                    .unwrap()
                    .splits
                    .get(split_id)
                    .unwrap()
                    .generation,
                3
            );
        }

        {
            // stage split (existing split id)
            let result = metastore
                .stage_split(index_id, split_metadata.clone())
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::ExistingSplitId;
            assert_eq!(result, expected);
        }

        {
            // stage split (non-existent index uri)
            let result = metastore
                .stage_split("non-existent-index", split_metadata.clone())
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::IndexDoesNotExist;
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_single_file_metastore_publish_split() {
        let metastore = SingleFileMetastore::for_test();
        let index_id = "my-index";
        let split_id = "one";
        let split_metadata = SplitMetadata {
            split_id: split_id.to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: Some(Range { start: 0, end: 100 }),
            generation: 3,
        };

        {
            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = false;
            assert_eq!(result, expected);

            let index_metadata = IndexMetadata {
                index_id: index_id.to_string(),
                index_uri: "ram://indexes/my-index".to_string(),
            };
            // Create index
            metastore
                .create_index(index_metadata, DocMapping::Dynamic)
                .await
                .unwrap();

            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = true;
            assert_eq!(result, expected);

            // stage split
            metastore
                .stage_split(index_id, split_metadata.clone())
                .await
                .unwrap();

            // publish split
            metastore.publish_split(index_id, split_id).await.unwrap();
        }

        {
            let cache = metastore.cache.read().await;
            assert_eq!(
                cache
                    .get(index_id)
                    .unwrap()
                    .splits
                    .get(split_id)
                    .unwrap()
                    .split_state,
                SplitState::Published
            );
        }

        {
            // publish published split
            metastore.publish_split(index_id, split_id).await.unwrap();

            // publish non-staged split
            let split_id = "one";
            metastore
                .mark_split_as_deleted(index_id, split_id) // mark as deleted
                .await
                .unwrap();
            let result = metastore
                .publish_split(index_id, split_id) // publish
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::SplitIsNotStaged;
            assert_eq!(result, expected);

            // publish non-existent index
            let result = metastore
                .publish_split("non-existent-index", split_id)
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::IndexDoesNotExist;
            assert_eq!(result, expected);

            // publish non-existent split
            let result = metastore
                .publish_split(index_id, "non-existent-split")
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::SplitDoesNotExist;
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_single_file_metastore_list_splits() {
        let metastore = SingleFileMetastore::for_test();
        let index_id = "my-index";

        {
            let index_metadata = IndexMetadata {
                index_id: index_id.to_string(),
                index_uri: "ram://indexes/my-index".to_string(),
            };
            // create index
            metastore
                .create_index(index_metadata, DocMapping::Dynamic)
                .await
                .unwrap();
        }

        {
            // stage split
            let split_metadata_1 = SplitMetadata {
                split_id: "one".to_string(),
                split_state: SplitState::Staged,
                num_records: 1,
                size_in_bytes: 2,
                time_range: Some(Range { start: 0, end: 100 }),
                generation: 3,
            };

            let split_metadata_2 = SplitMetadata {
                split_id: "two".to_string(),
                split_state: SplitState::Staged,
                num_records: 1,
                size_in_bytes: 2,
                time_range: Some(Range {
                    start: 100,
                    end: 200,
                }),
                generation: 3,
            };

            let split_metadata_3 = SplitMetadata {
                split_id: "three".to_string(),
                split_state: SplitState::Staged,
                num_records: 1,
                size_in_bytes: 2,
                time_range: Some(Range {
                    start: 200,
                    end: 300,
                }),
                generation: 3,
            };

            let split_metadata_4 = SplitMetadata {
                split_id: "four".to_string(),
                split_state: SplitState::Staged,
                num_records: 1,
                size_in_bytes: 2,
                time_range: Some(Range {
                    start: 300,
                    end: 400,
                }),
                generation: 3,
            };
            let split_metadata_5 = SplitMetadata {
                split_id: "five".to_string(),
                split_state: SplitState::Staged,
                num_records: 1,
                size_in_bytes: 2,
                time_range: None,
                generation: 3,
            };

            metastore
                .stage_split(index_id, split_metadata_1)
                .await
                .unwrap();
            metastore
                .stage_split(index_id, split_metadata_2)
                .await
                .unwrap();
            metastore
                .stage_split(index_id, split_metadata_3)
                .await
                .unwrap();
            metastore
                .stage_split(index_id, split_metadata_4)
                .await
                .unwrap();
            metastore
                .stage_split(index_id, split_metadata_5)
                .await
                .unwrap();
        }

        {
            // list
            let time_range_opt = Some(Range {
                start: 0u64,
                end: 99u64,
            });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, time_range_opt)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|split_metadata| split_metadata.split_id)
                .collect();
            assert_eq!(split_ids.contains("one"), true);
            assert_eq!(split_ids.contains("two"), false);
            assert_eq!(split_ids.contains("three"), false);
            assert_eq!(split_ids.contains("four"), false);
            assert_eq!(split_ids.contains("five"), true);
        }

        {
            // list
            let range = Some(Range { start: 0, end: 100 });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), true);
            assert_eq!(split_id_vec.contains(&"two".to_string()), false);
            assert_eq!(split_id_vec.contains(&"three".to_string()), false);
            assert_eq!(split_id_vec.contains(&"four".to_string()), false);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }

        {
            // list
            let range = Some(Range { start: 0, end: 101 });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), true);
            assert_eq!(split_id_vec.contains(&"two".to_string()), true);
            assert_eq!(split_id_vec.contains(&"three".to_string()), false);
            assert_eq!(split_id_vec.contains(&"four".to_string()), false);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }

        {
            // list
            let range = Some(Range { start: 0, end: 199 });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), true);
            assert_eq!(split_id_vec.contains(&"two".to_string()), true);
            assert_eq!(split_id_vec.contains(&"three".to_string()), false);
            assert_eq!(split_id_vec.contains(&"four".to_string()), false);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }

        {
            // list
            let range = Some(Range { start: 0, end: 200 });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), true);
            assert_eq!(split_id_vec.contains(&"two".to_string()), true);
            assert_eq!(split_id_vec.contains(&"three".to_string()), false);
            assert_eq!(split_id_vec.contains(&"four".to_string()), false);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }

        {
            // list
            let range = Some(Range { start: 0, end: 201 });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), true);
            assert_eq!(split_id_vec.contains(&"two".to_string()), true);
            assert_eq!(split_id_vec.contains(&"three".to_string()), true);
            assert_eq!(split_id_vec.contains(&"four".to_string()), false);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }

        {
            // list
            let range = Some(Range { start: 0, end: 299 });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), true);
            assert_eq!(split_id_vec.contains(&"two".to_string()), true);
            assert_eq!(split_id_vec.contains(&"three".to_string()), true);
            assert_eq!(split_id_vec.contains(&"four".to_string()), false);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }

        {
            // list
            let range = Some(Range { start: 0, end: 300 });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), true);
            assert_eq!(split_id_vec.contains(&"two".to_string()), true);
            assert_eq!(split_id_vec.contains(&"three".to_string()), true);
            assert_eq!(split_id_vec.contains(&"four".to_string()), false);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }

        {
            // list
            let range = Some(Range { start: 0, end: 301 });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), true);
            assert_eq!(split_id_vec.contains(&"two".to_string()), true);
            assert_eq!(split_id_vec.contains(&"three".to_string()), true);
            assert_eq!(split_id_vec.contains(&"four".to_string()), true);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }

        {
            // list
            let range = Some(Range {
                start: 301,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), false);
            assert_eq!(split_id_vec.contains(&"two".to_string()), false);
            assert_eq!(split_id_vec.contains(&"three".to_string()), false);
            assert_eq!(split_id_vec.contains(&"four".to_string()), true);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }

        {
            // list
            let range = Some(Range {
                start: 300,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), false);
            assert_eq!(split_id_vec.contains(&"two".to_string()), false);
            assert_eq!(split_id_vec.contains(&"three".to_string()), false);
            assert_eq!(split_id_vec.contains(&"four".to_string()), true);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }

        {
            // list
            let range = Some(Range {
                start: 299,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), false);
            assert_eq!(split_id_vec.contains(&"two".to_string()), false);
            assert_eq!(split_id_vec.contains(&"three".to_string()), true);
            assert_eq!(split_id_vec.contains(&"four".to_string()), true);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }

        {
            // list
            let range = Some(Range {
                start: 201,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), false);
            assert_eq!(split_id_vec.contains(&"two".to_string()), false);
            assert_eq!(split_id_vec.contains(&"three".to_string()), true);
            assert_eq!(split_id_vec.contains(&"four".to_string()), true);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }

        {
            // list
            let range = Some(Range {
                start: 200,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), false);
            assert_eq!(split_id_vec.contains(&"two".to_string()), false);
            assert_eq!(split_id_vec.contains(&"three".to_string()), true);
            assert_eq!(split_id_vec.contains(&"four".to_string()), true);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }

        {
            // list
            let range = Some(Range {
                start: 199,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), false);
            assert_eq!(split_id_vec.contains(&"two".to_string()), true);
            assert_eq!(split_id_vec.contains(&"three".to_string()), true);
            assert_eq!(split_id_vec.contains(&"four".to_string()), true);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }

        {
            // list
            let range = Some(Range {
                start: 101,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), false);
            assert_eq!(split_id_vec.contains(&"two".to_string()), true);
            assert_eq!(split_id_vec.contains(&"three".to_string()), true);
            assert_eq!(split_id_vec.contains(&"four".to_string()), true);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }

        {
            // list
            let range = Some(Range {
                start: 101,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), false);
            assert_eq!(split_id_vec.contains(&"two".to_string()), true);
            assert_eq!(split_id_vec.contains(&"three".to_string()), true);
            assert_eq!(split_id_vec.contains(&"four".to_string()), true);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }

        {
            // list
            let range = Some(Range {
                start: 100,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), false);
            assert_eq!(split_id_vec.contains(&"two".to_string()), true);
            assert_eq!(split_id_vec.contains(&"three".to_string()), true);
            assert_eq!(split_id_vec.contains(&"four".to_string()), true);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }

        {
            // list
            let range = Some(Range {
                start: 99,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), true);
            assert_eq!(split_id_vec.contains(&"two".to_string()), true);
            assert_eq!(split_id_vec.contains(&"three".to_string()), true);
            assert_eq!(split_id_vec.contains(&"four".to_string()), true);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }

        {
            // list
            let range = Some(Range {
                start: 1000,
                end: 1100,
            });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), false);
            assert_eq!(split_id_vec.contains(&"two".to_string()), false);
            assert_eq!(split_id_vec.contains(&"three".to_string()), false);
            assert_eq!(split_id_vec.contains(&"four".to_string()), false);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }

        {
            // list
            let range = None;
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), true);
            assert_eq!(split_id_vec.contains(&"two".to_string()), true);
            assert_eq!(split_id_vec.contains(&"three".to_string()), true);
            assert_eq!(split_id_vec.contains(&"four".to_string()), true);
            assert_eq!(split_id_vec.contains(&"five".to_string()), true);
        }
    }

    #[tokio::test]
    async fn test_single_file_metastore_mark_split_as_deleted() {
        let metastore = SingleFileMetastore::for_test();
        let index_id = "my-index";
        let split_id = "split-one";
        let split_metadata = SplitMetadata {
            split_id: split_id.to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: Some(Range { start: 0, end: 100 }),
            generation: 3,
        };

        {
            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = false;
            assert_eq!(result, expected);

            let index_metadata = IndexMetadata {
                index_id: index_id.to_string(),
                index_uri: "ram://indexes/my-index".to_string(),
            };

            // Create index
            metastore
                .create_index(index_metadata, DocMapping::Dynamic)
                .await
                .unwrap();

            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = true;
            assert_eq!(result, expected);

            // stage split
            metastore
                .stage_split(index_id, split_metadata.clone())
                .await
                .unwrap();

            // publish split
            metastore.publish_split(index_id, split_id).await.unwrap();

            // mark split as deleted
            metastore
                .mark_split_as_deleted(index_id, split_id)
                .await
                .unwrap();
        }

        {
            let cache = metastore.cache.read().await;
            assert_eq!(
                cache
                    .get(index_id)
                    .unwrap()
                    .splits
                    .get(split_id)
                    .unwrap()
                    .split_state,
                SplitState::ScheduledForDeletion
            );
        }

        {
            // mark split as deleted (already marked as deleted)
            metastore
                .mark_split_as_deleted(index_id, split_id)
                .await
                .unwrap();

            // mark split as deleted (non-existent index)
            let result = metastore
                .mark_split_as_deleted("non-existent-index", split_id)
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::IndexDoesNotExist;
            assert_eq!(result, expected);

            // mark split as deleted (non-existent)
            let result = metastore
                .mark_split_as_deleted(index_id, "non-existent-split")
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::SplitDoesNotExist;
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_single_file_metastore_delete_split() {
        let metastore = SingleFileMetastore::for_test();
        let index_id = "my-index";
        let split_id = "split-one";
        let split_metadata = SplitMetadata {
            split_id: split_id.to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: Some(Range { start: 0, end: 100 }),
            generation: 3,
        };

        {
            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = false;
            assert_eq!(result, expected);

            let index_metadata = IndexMetadata {
                index_id: index_id.to_string(),
                index_uri: "ram://indexes/my-index".to_string(),
            };

            // Create index
            metastore
                .create_index(index_metadata, DocMapping::Dynamic)
                .await
                .unwrap();

            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = true;
            assert_eq!(result, expected);

            // stage split
            metastore
                .stage_split(index_id, split_metadata.clone())
                .await
                .unwrap();

            // publish split
            metastore.publish_split(index_id, split_id).await.unwrap();

            // delete split (published split)
            let result = metastore
                .delete_split(index_id, split_id)
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::Forbidden;
            assert_eq!(result, expected);

            // mark split as deleted
            metastore
                .mark_split_as_deleted(index_id, split_id)
                .await
                .unwrap();

            // delete split
            metastore.delete_split(index_id, split_id).await.unwrap();
        }

        {
            let cache = metastore.cache.read().await;
            assert_eq!(
                cache.get(index_id).unwrap().splits.contains_key(split_id),
                false
            );
        }

        {
            // mark split as deleted (non-existent index)
            let result = metastore
                .delete_split("non-existent-index", split_id)
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::IndexDoesNotExist;
            assert_eq!(result, expected);

            // delete split (non-existent split)
            let result = metastore
                .delete_split(index_id, "non-existent-split")
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::SplitDoesNotExist;
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_single_file_metastore_storage_failing() {
        // The single file metastore should not update its internal state if the storage fails.
        let mut mock_storage = MockStorage::default();

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
            split_id: split_id.to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: None,
            generation: 3,
        };

        let index_metadata = IndexMetadata {
            index_id: index_id.to_string(),
            index_uri: "ram://my-indexes/my-index".to_string(),
        };

        // create index
        metastore
            .create_index(index_metadata, DocMapping::Dynamic)
            .await
            .unwrap();

        // stage split
        metastore
            .stage_split(index_id, split_metadata)
            .await
            .unwrap();

        // publish split fails
        let err = metastore.publish_split(index_id, split_id).await;
        assert!(err.is_err());

        // empty
        let split = metastore
            .list_splits(index_id, SplitState::Published, None)
            .await
            .unwrap();
        assert!(split.is_empty());

        // not empty
        let split = metastore
            .list_splits(index_id, SplitState::Staged, None)
            .await
            .unwrap();
        assert!(!split.is_empty());
    }
}
