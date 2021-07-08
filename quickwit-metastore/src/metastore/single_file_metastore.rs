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
use tokio::sync::RwLock;

use quickwit_storage::{PutPayload, Storage, StorageErrorKind};

use crate::{
    IndexMetadata, MetadataSet, Metastore, MetastoreError, MetastoreResult, SplitMetadata,
    SplitState,
};

/// Metadata file managed by [`SingleFileMetastore`].
const META_FILENAME: &str = "quickwit.json";

/// Creates a path to the metadata file from the given index ID.
fn meta_path(index_id: &str) -> PathBuf {
    Path::new(index_id).join(Path::new(META_FILENAME))
}

/// Takes 2 semi-open intervals and returns true iff their intersection is empty
fn is_disjoint(left: &Range<i64>, right: &Range<i64>) -> bool {
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

    /// Checks whether the index exists in storage.
    async fn index_exists(&self, index_id: &str) -> MetastoreResult<bool> {
        let index_path = meta_path(index_id);

        let exists = self.storage.exists(&index_path).await.map_err(|err| {
            MetastoreError::InternalError {
                message: "Failed to check for existence of index file.".to_string(),
                cause: anyhow::anyhow!(err),
            }
        })?;

        Ok(exists)
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
        let index_path = meta_path(index_id);
        let content = self
            .storage
            .get_all(&index_path)
            .await
            .map_err(|storage_err| match storage_err.kind() {
                StorageErrorKind::DoesNotExist => MetastoreError::IndexDoesNotExist {
                    index_id: index_id.to_string(),
                },
                _ => MetastoreError::InternalError {
                    message: "Metastore error".to_string(),
                    cause: anyhow::anyhow!(storage_err),
                },
            })?;

        let metadata_set = serde_json::from_slice::<MetadataSet>(&content[..])
            .map_err(|serde_err| MetastoreError::InvalidManifest { cause: serde_err })?;

        // Finally, update the cache accordingly
        let mut cache = self.cache.write().await;
        cache.insert(index_id.to_string(), metadata_set.clone());

        Ok(metadata_set)
    }

    /// Serializes the metadata set and stores the data on the storage.
    async fn put_index(&self, metadata_set: MetadataSet) -> MetastoreResult<()> {
        // Serialize metadata set.
        let content: Vec<u8> = serde_json::to_vec(&metadata_set).map_err(|serde_err| {
            MetastoreError::InternalError {
                message: "Failed to serialize Metadata set".to_string(),
                cause: anyhow::anyhow!(serde_err),
            }
        })?;

        let index_id = metadata_set.index.index_id.clone();
        let index_path = meta_path(&index_id);

        // Put data back into storage.
        self.storage
            .put(&index_path, PutPayload::from(content))
            .await
            .map_err(|cause| MetastoreError::InternalError {
                message: "Failed to put metadata set back into storage.".to_string(),
                cause: From::from(cause),
            })?;

        // Update the internal data if the storage is successfully updated.
        let mut cache = self.cache.write().await;
        cache.insert(index_id, metadata_set);

        Ok(())
    }
}

#[async_trait]
impl Metastore for SingleFileMetastore {
    async fn create_index(&self, index_metadata: IndexMetadata) -> MetastoreResult<()> {
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

    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()> {
        // Check whether the index exists.
        let exists = self.index_exists(index_id).await?;

        if !exists {
            return Err(MetastoreError::IndexDoesNotExist {
                index_id: index_id.to_string(),
            });
        }

        let index_path = meta_path(index_id);

        // Delete metadata set from storage.
        self.storage
            .delete(&index_path)
            .await
            .map_err(|cause| MetastoreError::InternalError {
                message: "Failed to delete metadata set from storage.".to_string(),
                cause: anyhow::anyhow!(cause),
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

        // Check whether the split exists.
        // If the split exists, return an error to prevent the split from being registered.
        if metadata_set.splits.contains_key(&split_metadata.split_id) {
            return Err(MetastoreError::InternalError {
                message: format!(
                    "Try to stage split that already exists ({})",
                    split_metadata.split_id
                ),
                cause: anyhow::anyhow!(""),
            });
        }

        // Insert a new split metadata as `Staged` state.
        split_metadata.split_state = SplitState::Staged;
        metadata_set
            .splits
            .insert(split_metadata.split_id.to_string(), split_metadata);

        self.put_index(metadata_set).await?;

        Ok(())
    }

    async fn publish_splits<'a>(
        &self,
        index_id: &str,
        split_ids: Vec<&'a str>,
    ) -> MetastoreResult<()> {
        let mut metadata_set = self.get_index(index_id).await?;

        for split_id in split_ids {
            // Check for the existence of split.
            let mut split_metadata = metadata_set.splits.get_mut(split_id).ok_or_else(|| {
                MetastoreError::SplitDoesNotExist {
                    split_id: split_id.to_string(),
                }
            })?;

            match split_metadata.split_state {
                SplitState::Published => {
                    // Split is already published. This is fine, we just skip it.
                    continue;
                }
                SplitState::Staged => {
                    // The split state needs to be updated.
                    split_metadata.split_state = SplitState::Published;
                }
                _ => {
                    return Err(MetastoreError::SplitIsNotStaged {
                        split_id: split_id.to_string(),
                    })
                }
            }
        }
        self.put_index(metadata_set).await?;
        Ok(())
    }

    async fn list_splits(
        &self,
        index_id: &str,
        state: SplitState,
        time_range_opt: Option<Range<i64>>,
    ) -> MetastoreResult<Vec<SplitMetadata>> {
        let time_range_filter = |split_metadata: &SplitMetadata| match (
            time_range_opt.as_ref(),
            split_metadata.time_range.as_ref(),
        ) {
            (Some(filter_time_range), Some(split_time_range)) => {
                !is_disjoint(filter_time_range, split_time_range)
            }
            _ => true, // Return `true` if `time_range` is omitted or the split has no time range.
        };

        let metadata_set = self.get_index(index_id).await?;
        let splits = metadata_set
            .splits
            .into_iter()
            .map(|(_, split_metadata)| split_metadata)
            .filter(|split_metadata| {
                split_metadata.split_state == state && time_range_filter(split_metadata)
            })
            .collect();
        Ok(splits)
    }

    async fn list_all_splits(&self, index_id: &str) -> MetastoreResult<Vec<SplitMetadata>> {
        let metadata_set = self.get_index(index_id).await?;
        let splits = metadata_set
            .splits
            .into_iter()
            .map(|(_, split_metadata)| split_metadata)
            .collect();
        Ok(splits)
    }

    async fn mark_splits_as_deleted<'a>(
        &self,
        index_id: &str,
        split_ids: Vec<&'a str>,
    ) -> MetastoreResult<()> {
        let mut metadata_set = self.get_index(index_id).await?;

        let mut is_modified = false;
        for split_id in split_ids {
            // Check for the existence of split.
            let split_metadata = metadata_set.splits.get_mut(split_id).ok_or_else(|| {
                MetastoreError::SplitDoesNotExist {
                    split_id: split_id.to_string(),
                }
            })?;

            if split_metadata.split_state == SplitState::ScheduledForDeletion {
                // If the split is already scheduled for deletion, this API call returns success.
                continue;
            }

            split_metadata.split_state = SplitState::ScheduledForDeletion;
            is_modified = true;
        }

        if is_modified {
            self.put_index(metadata_set).await?;
        }

        Ok(())
    }

    async fn delete_splits<'a>(
        &self,
        index_id: &str,
        split_ids: Vec<&'a str>,
    ) -> MetastoreResult<()> {
        let mut metadata_set = self.get_index(index_id).await?;

        for split_id in split_ids {
            // Check for the existence of split.
            let split_metadata = metadata_set.splits.get_mut(split_id).ok_or_else(|| {
                MetastoreError::SplitDoesNotExist {
                    split_id: split_id.to_string(),
                }
            })?;

            match split_metadata.split_state {
                SplitState::ScheduledForDeletion | SplitState::Staged => {
                    // Only `ScheduledForDeletion` and `Staged` can be deleted
                    metadata_set.splits.remove(split_id);
                }
                _ => {
                    let message: String = format!(
                        "This split is not a deletable state: {:?}:{:?}",
                        split_id, &split_metadata.split_state
                    );
                    return Err(MetastoreError::Forbidden { message });
                }
            }
        }

        self.put_index(metadata_set).await?;

        Ok(())
    }

    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        let index_metadata = self.get_index(index_id).await?;
        Ok(index_metadata.index)
    }

    fn uri(&self) -> String {
        self.storage.uri()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::ops::Range;
    use std::path::Path;
    use std::sync::Arc;

    use crate::{IndexMetadata, MetastoreError};
    use crate::{Metastore, SingleFileMetastore, SplitMetadata, SplitState};
    use quickwit_doc_mapping::AllFlattenIndexConfig;
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
                index_config: Box::new(AllFlattenIndexConfig::default()),
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
                index_config: Box::new(AllFlattenIndexConfig::default()),
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

            let metastore_error = metastore.create_index(index_metadata).await.unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::IndexAlreadyExists { .. }
            ));
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
                index_config: Box::new(AllFlattenIndexConfig::default()),
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
                "AllFlattenDocMapper".to_string()
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
                index_config: Box::new(AllFlattenIndexConfig::default()),
            };

            // Create index
            metastore.create_index(index_metadata).await.unwrap();

            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = true;
            assert_eq!(result, expected);

            // Delete index
            metastore.delete_index(index_id).await.unwrap();

            // Delete a non-existent index.
            let metastore_error = metastore
                .delete_index("non-existent-index")
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::IndexDoesNotExist { .. }
            ));
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
                index_config: Box::new(AllFlattenIndexConfig::default()),
            };

            // Create index
            metastore.create_index(index_metadata).await.unwrap();

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
            let metastore_error = metastore
                .stage_split(index_id, split_metadata.clone())
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::InternalError { .. }
            ));
        }

        {
            // stage split (non-existent index uri)
            let metastore_error = metastore
                .stage_split("non-existent-index", split_metadata.clone())
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::IndexDoesNotExist { .. }
            ));
        }
    }

    #[tokio::test]
    async fn test_single_file_metastore_publish_split() {
        let metastore = SingleFileMetastore::for_test();
        let index_id = "my-index";
        let split_id_one = "one";
        let split_id_two = "two";
        let split_metadata_one = SplitMetadata {
            split_id: split_id_one.to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: Some(Range { start: 0, end: 100 }),
            generation: 3,
        };
        let split_metadata_two = SplitMetadata {
            split_id: split_id_two.to_string(),
            split_state: SplitState::Staged,
            num_records: 5,
            size_in_bytes: 6,
            time_range: Some(Range {
                start: 30,
                end: 100,
            }),
            generation: 2,
        };

        {
            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = false;
            assert_eq!(result, expected);

            let index_metadata = IndexMetadata {
                index_id: index_id.to_string(),
                index_uri: "ram://indexes/my-index".to_string(),
                index_config: Box::new(AllFlattenIndexConfig::default()),
            };

            // Create index
            metastore.create_index(index_metadata).await.unwrap();

            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = true;
            assert_eq!(result, expected);

            // stage split
            metastore
                .stage_split(index_id, split_metadata_one.clone())
                .await
                .unwrap();

            // publish split
            metastore
                .publish_splits(index_id, vec![split_id_one])
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
                    .get(split_id_one)
                    .unwrap()
                    .split_state,
                SplitState::Published
            );
        }

        {
            // publish published split
            metastore
                .publish_splits(index_id, vec![split_id_one])
                .await
                .unwrap();

            // publish non-staged split
            metastore
                .mark_splits_as_deleted(index_id, vec![split_id_one]) // mark as deleted
                .await
                .unwrap();
            let metastore_error = metastore
                .publish_splits(index_id, vec![split_id_one]) // publish
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::SplitIsNotStaged { .. }
            ));

            // publish non-existent index
            let metastore_error = metastore
                .publish_splits("non-existent-index", vec![split_id_one])
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::IndexDoesNotExist { .. }
            ));

            // publish non-existent split
            let metastore_error = metastore
                .publish_splits(index_id, vec!["non-existent-split"])
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::SplitDoesNotExist { .. }
            ));
        }

        {
            // publish one non-staged split and one non-existent split
            let metastore_error = metastore
                .publish_splits(index_id, vec![split_id_one, split_id_two])
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::SplitIsNotStaged { split_id: _ }
            ));

            // publish two non-existent splits
            metastore
                .delete_splits(index_id, vec![split_id_one])
                .await
                .unwrap();
            let metastore_error = metastore
                .publish_splits(index_id, vec![split_id_one, split_id_two])
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::SplitDoesNotExist { split_id: _ }
            ));

            // publish one staged split and one non-exitent split
            metastore
                .stage_split(index_id, split_metadata_one.clone())
                .await
                .unwrap();
            let metastore_error = metastore
                .publish_splits(index_id, vec![split_id_one, split_id_two])
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::SplitDoesNotExist { .. }
            ));

            // publish two staged splits
            metastore
                .stage_split(index_id, split_metadata_two.clone())
                .await
                .unwrap();
            metastore
                .publish_splits(index_id, vec![split_id_one, split_id_two])
                .await
                .unwrap();

            //publishe two published splits
            metastore
                .publish_splits(index_id, vec![split_id_one, split_id_two])
                .await
                .unwrap();
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
                index_config: Box::new(AllFlattenIndexConfig::default()),
            };

            // create index
            metastore.create_index(index_metadata).await.unwrap();
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
                start: 0i64,
                end: 99i64,
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
            let time_range_opt = Some(Range {
                start: 200,
                end: i64::MAX,
            });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, time_range_opt)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|split_metadata| split_metadata.split_id)
                .collect();
            assert_eq!(split_ids.contains("one"), false);
            assert_eq!(split_ids.contains("two"), false);
            assert_eq!(split_ids.contains("three"), true);
            assert_eq!(split_ids.contains("four"), true);
            assert_eq!(split_ids.contains("five"), true);
        }

        {
            // list
            let time_range_opt = Some(Range {
                start: i64::MIN,
                end: 200,
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
            assert_eq!(split_ids.contains("two"), true);
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
                index_config: Box::new(AllFlattenIndexConfig::default()),
            };

            // Create index
            metastore.create_index(index_metadata).await.unwrap();

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
            metastore
                .publish_splits(index_id, vec![split_id])
                .await
                .unwrap();

            // mark splits as deleted (one non-existent)
            let metastore_error = metastore
                .mark_splits_as_deleted(index_id, vec![split_id, "non-existent-split"])
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::SplitDoesNotExist { .. }
            ));

            // mark split as deleted
            metastore
                .mark_splits_as_deleted(index_id, vec![split_id])
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
                .mark_splits_as_deleted(index_id, vec![split_id])
                .await
                .unwrap();

            // mark split as deleted (non-existent index)
            let metastore_error = metastore
                .mark_splits_as_deleted("non-existent-index", vec![split_id])
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::IndexDoesNotExist { .. }
            ));

            // mark split as deleted (non-existent)
            let metastore_error = metastore
                .mark_splits_as_deleted(index_id, vec!["non-existent-split"])
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::SplitDoesNotExist { .. }
            ));
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
            assert_eq!(result, false);

            let index_metadata = IndexMetadata {
                index_id: index_id.to_string(),
                index_uri: "ram://indexes/my-index".to_string(),
                index_config: Box::new(AllFlattenIndexConfig::default()),
            };

            // Create index
            metastore.create_index(index_metadata).await.unwrap();

            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            assert_eq!(result, true);

            // stage split
            metastore
                .stage_split(index_id, split_metadata.clone())
                .await
                .unwrap();

            // publish split
            metastore
                .publish_splits(index_id, vec![split_id])
                .await
                .unwrap();

            // delete split (published split)
            let metastore_error = metastore
                .delete_splits(index_id, vec![split_id])
                .await
                .unwrap_err();
            assert!(matches!(metastore_error, MetastoreError::Forbidden { .. }));

            // mark split as deleted
            metastore
                .mark_splits_as_deleted(index_id, vec![split_id])
                .await
                .unwrap();

            // mark splits as deleted (one non-existent split)
            let metastore_error = metastore
                .mark_splits_as_deleted(index_id, vec![split_id, "non-existent-split"])
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::SplitDoesNotExist { .. }
            ));

            // delete split
            metastore
                .delete_splits(index_id, vec![split_id])
                .await
                .unwrap();
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
            let metastore_error = metastore
                .delete_splits("non-existent-index", vec![split_id])
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::IndexDoesNotExist { .. }
            ));

            // delete split (non-existent split)
            let metastore_error = metastore
                .delete_splits(index_id, vec!["non-existent-split"])
                .await
                .unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::SplitDoesNotExist { .. }
            ));
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
            index_config: Box::new(AllFlattenIndexConfig::default()),
        };

        // create index
        metastore.create_index(index_metadata).await.unwrap();

        // stage split
        metastore
            .stage_split(index_id, split_metadata)
            .await
            .unwrap();

        // publish split fails
        let err = metastore.publish_splits(index_id, vec![split_id]).await;
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
