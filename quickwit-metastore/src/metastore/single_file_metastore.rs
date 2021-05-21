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
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use quickwit_doc_mapping::DocMapping;
use quickwit_storage::{PutPayload, Storage};

use crate::{
    IndexMetadata, IndexUri, MetadataSet, Metastore, MetastoreErrorKind, MetastoreResult, SplitId,
    SplitMetadata, SplitState, FILE_FORMAT_VERSION,
};

/// Single file meta store implementation.
pub struct SingleFileMetastore {
    storage: Arc<dyn Storage>,
    data: Arc<RwLock<HashMap<IndexUri, MetadataSet>>>,
}

#[allow(dead_code)]
impl SingleFileMetastore {
    /// Creates a meta store given a storage.
    pub async fn new(storage: Arc<dyn Storage>) -> MetastoreResult<Self> {
        Ok(SingleFileMetastore {
            storage,
            data: Arc::new(RwLock::new(HashMap::new())),
        })
    }
}

#[async_trait]
impl Metastore for SingleFileMetastore {
    async fn create_index(
        &self,
        index_uri: IndexUri,
        _doc_mapping: DocMapping,
    ) -> MetastoreResult<()> {
        // Serialized metadata set.
        let contents: Vec<u8>;

        {
            let mut data = self.data.write().await;

            // Check for the existence of index.
            if data.contains_key(&index_uri) {
                return Err(MetastoreErrorKind::ExistingIndexUri
                    .with_error(anyhow::anyhow!("Index already exists: {:?}", &index_uri)));
            }

            // Insert metadata set.
            let metadata_set = MetadataSet {
                index: IndexMetadata {
                    version: FILE_FORMAT_VERSION.to_string(),
                },
                splits: HashMap::new(),
            };
            data.insert(index_uri.clone(), metadata_set.clone());

            // Serialize metadata set.
            contents = match serde_json::to_vec(&metadata_set) {
                Ok(c) => c,
                Err(e) => {
                    return Err(MetastoreErrorKind::InvalidManifest
                        .with_error(anyhow::anyhow!("Failed to serialize meta data: {:?}", e)));
                }
            };
        }

        // Put data back into storage.
        self.storage
            .put(&Path::new(&index_uri), PutPayload::from(contents))
            .await
            .map_err(|e| {
                MetastoreErrorKind::InternalError
                    .with_error(anyhow::anyhow!("Failed to put metadata set: {:?}", e))
            })?;

        Ok(())
    }

    async fn delete_index(&self, index_uri: IndexUri) -> MetastoreResult<()> {
        {
            let mut data = self.data.write().await;

            // Check for the existence of index.
            if !data.contains_key(&index_uri) {
                return Err(MetastoreErrorKind::IndexDoesNotExist
                    .with_error(anyhow::anyhow!("Index does not exist: {:?}", &index_uri)));
            }

            // Remove metadata set.
            data.remove(&index_uri);
        }

        // Delete data from storage.
        self.storage
            .delete(&Path::new(&index_uri))
            .await
            .map_err(|e| {
                MetastoreErrorKind::InternalError
                    .with_error(anyhow::anyhow!("Failed to delete metadata set: {:?}", e))
            })?;

        Ok(())
    }

    async fn stage_split(
        &self,
        index_uri: IndexUri,
        split_id: SplitId,
        mut split_metadata: SplitMetadata,
    ) -> MetastoreResult<SplitId> {
        // Overwrite split state to Staged.
        split_metadata.split_state = SplitState::Staged;

        // Serialized metadata set.
        let contents: Vec<u8>;

        {
            let mut data = self.data.write().await;

            // Check for the existence of index.
            let metadata_set = data.get_mut(&index_uri).ok_or_else(|| {
                MetastoreErrorKind::IndexDoesNotExist
                    .with_error(anyhow::anyhow!("Index does not exist: {:?}", &index_uri))
            })?;

            // Check for the existence of split.
            // If split exists, return an error to prevent the split from being registered.
            if metadata_set.splits.contains_key(&split_id) {
                return Err(MetastoreErrorKind::ExistingSplitId
                    .with_error(anyhow::anyhow!("Split already exists: {:?}", &split_id)));
            }

            // Update split metadata.
            metadata_set.splits.insert(split_id.clone(), split_metadata);

            // Serialize metadata set.
            contents = serde_json::to_vec(&metadata_set).map_err(|e| {
                MetastoreErrorKind::InvalidManifest
                    .with_error(anyhow::anyhow!("Failed to serialize meta data: {:?}", e))
            })?;
        }

        // Put data back into storage.
        self.storage
            .put(&Path::new(&index_uri), PutPayload::from(contents))
            .await
            .map_err(|e| {
                MetastoreErrorKind::InternalError
                    .with_error(anyhow::anyhow!("Failed to put metadata set: {:?}", e))
            })?;

        Ok(split_id)
    }

    async fn publish_split(&self, index_uri: IndexUri, split_id: SplitId) -> MetastoreResult<()> {
        // Serialized metadata set.
        let contents: Vec<u8>;

        {
            let mut data = self.data.write().await;

            // Check for the existence of index.
            let metadata_set = data.get_mut(&index_uri).ok_or_else(|| {
                MetastoreErrorKind::IndexDoesNotExist
                    .with_error(anyhow::anyhow!("Index does not exist: {:?}", &index_uri))
            })?;

            // Check for the existence of split.
            let split_metadata = metadata_set.splits.get_mut(&split_id).ok_or_else(|| {
                MetastoreErrorKind::DoesNotExist
                    .with_error(anyhow::anyhow!("Split does not exist: {:?}", &split_id))
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
                        .with_error(anyhow::anyhow!("Split ID is not staged: {:?}", &split_id)));
                }
            }

            // Serialize metadata set.
            contents = serde_json::to_vec(&metadata_set).map_err(|e| {
                MetastoreErrorKind::InvalidManifest
                    .with_error(anyhow::anyhow!("Failed to serialize meta data: {:?}", e))
            })?;
        }

        // Put data back into storage.
        self.storage
            .put(&Path::new(&index_uri), PutPayload::from(contents))
            .await
            .map_err(|e| {
                MetastoreErrorKind::InternalError
                    .with_error(anyhow::anyhow!("Failed to put metadata set: {:?}", e))
            })?;

        Ok(())
    }

    async fn list_splits(
        &self,
        index_uri: IndexUri,
        state: SplitState,
        time_range: Option<Range<u64>>,
    ) -> MetastoreResult<Vec<SplitMetadata>> {
        // list of split metadata.
        let mut splits: Vec<SplitMetadata> = Vec::new();

        let mut data = self.data.write().await;

        // Check for the existence of index.
        let metadata_set = data.get_mut(&index_uri).ok_or_else(|| {
            MetastoreErrorKind::IndexDoesNotExist
                .with_error(anyhow::anyhow!("Index does not exist: {:?}", &index_uri))
        })?;

        // filter by split state.
        let split_with_meta_matching_state_it = metadata_set
            .splits
            .iter()
            .filter(|&(_split_id, split_metadata)| split_metadata.split_state == state);
        for (_, split_metadata) in split_with_meta_matching_state_it {
            match time_range {
                Some(ref filter_time_range) => {
                    match &split_metadata.time_range {
                        Some(split_time_range) => {
                            // Splits that overlap at least part of the time range of the filter
                            // and the time range of the split are added to the list as search targets.
                            if split_time_range.contains(&filter_time_range.start)
                                || split_time_range.contains(&filter_time_range.end)
                                || filter_time_range.contains(&split_time_range.start)
                                || filter_time_range.contains(&split_time_range.end)
                            {
                                splits.push(split_metadata.clone());
                            } else {
                                // Split's `time_range` is out of range.
                                continue;
                            }
                        }
                        None => {
                            // If the time range of the split that stored in meta store is None,
                            // this split will be ignored.
                            continue;
                        }
                    }
                }
                None => {
                    // if `time_range` is omitted, the metadata is not filtered.
                    splits.push(split_metadata.clone());
                }
            }
        }

        Ok(splits)
    }

    async fn mark_split_as_deleted(
        &self,
        index_uri: IndexUri,
        split_id: SplitId,
    ) -> MetastoreResult<()> {
        // Serialized metadata set.
        let contents: Vec<u8>;

        {
            let mut data = self.data.write().await;

            // Check for the existence of index.
            let metadata_set = data.get_mut(&index_uri).ok_or_else(|| {
                MetastoreErrorKind::IndexDoesNotExist
                    .with_error(anyhow::anyhow!("Index does not exists: {:?}", &index_uri))
            })?;

            // Check for the existence of split.
            let split_metadata = metadata_set.splits.get_mut(&split_id).ok_or_else(|| {
                MetastoreErrorKind::DoesNotExist
                    .with_error(anyhow::anyhow!("Split does not exists: {:?}", &split_id))
            })?;

            match split_metadata.split_state {
                SplitState::ScheduledForDeletion => {
                    // If the split is already scheduled for deleted, this API call returns a success.
                    return Ok(());
                }
                _ => split_metadata.split_state = SplitState::ScheduledForDeletion,
            };

            // Serialize metadata set.
            contents = serde_json::to_vec(&metadata_set).map_err(|e| {
                MetastoreErrorKind::InvalidManifest
                    .with_error(anyhow::anyhow!("Failed to serialize meta data: {:?}", e))
            })?;
        }

        // Put data back into storage.
        self.storage
            .put(&Path::new(&index_uri), PutPayload::from(contents))
            .await
            .map_err(|e| {
                MetastoreErrorKind::InternalError
                    .with_error(anyhow::anyhow!("Failed to put metadata set: {:?}", e))
            })?;

        Ok(())
    }

    async fn delete_split(&self, index_uri: IndexUri, split_id: SplitId) -> MetastoreResult<()> {
        // Serialized metadata set.
        let contents: Vec<u8>;

        {
            let mut data = self.data.write().await;

            // Check for the existence of index.
            let metadata_set = data.get_mut(&index_uri).ok_or_else(|| {
                MetastoreErrorKind::IndexDoesNotExist
                    .with_error(anyhow::anyhow!("Index does not exist: {:?}", &index_uri))
            })?;

            // Check for the existence of split.
            let split_metadata = metadata_set.splits.get_mut(&split_id).ok_or_else(|| {
                MetastoreErrorKind::DoesNotExist
                    .with_error(anyhow::anyhow!("Split does not exist: {:?}", &split_id))
            })?;

            match split_metadata.split_state {
                SplitState::ScheduledForDeletion | SplitState::Staged => {
                    // Only `ScheduledForDeletion` and `Staged` can be deleted
                    metadata_set.splits.remove(&split_id);
                }
                _ => {
                    return Err(MetastoreErrorKind::Forbidden.with_error(anyhow::anyhow!(
                        "This split is not a deletable state: {:?}:{:?}",
                        &split_id,
                        &split_metadata.split_state
                    )));
                }
            };

            // Serialize metadata set.
            contents = serde_json::to_vec(&metadata_set).map_err(|e| {
                MetastoreErrorKind::InvalidManifest
                    .with_error(anyhow::anyhow!("Failed to serialize meta data: {:?}", e))
            })?;
        }

        // Put data back into storage.
        self.storage
            .put(&Path::new(&index_uri), PutPayload::from(contents))
            .await
            .map_err(|e| {
                MetastoreErrorKind::InternalError
                    .with_error(anyhow::anyhow!("Failed to put metadata set: {:?}", e))
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use quickwit_doc_mapping::DocMapping;
    use quickwit_storage::StorageUriResolver;

    use crate::{
        IndexUri, Metastore, MetastoreErrorKind, SingleFileMetastore, SplitMetadata, SplitState,
    };

    #[tokio::test]
    async fn test_single_file_metastore_create_index() {
        let resolver = StorageUriResolver::default();
        let storage = resolver.resolve("ram://").unwrap();
        let metastore = SingleFileMetastore::new(storage).await.unwrap();
        let index_uri = IndexUri::from("ram://test/index");

        {
            // create index
            metastore
                .create_index(index_uri.clone(), DocMapping::Dynamic)
                .await
                .unwrap();
            let data = metastore.data.read().await;
            assert_eq!(data.contains_key(&index_uri), true);
        }

        {
            // create index (already existing)
            let result = metastore
                .create_index(index_uri, DocMapping::Dynamic)
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::ExistingIndexUri;
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_single_file_metastore_delete_index() {
        let resolver = StorageUriResolver::default();
        let storage = resolver.resolve("ram://").unwrap();
        let metastore = SingleFileMetastore::new(storage).await.unwrap();
        let index_uri = IndexUri::from("ram://test/index");

        {
            // create index
            metastore
                .create_index(index_uri.clone(), DocMapping::Dynamic)
                .await
                .unwrap();
        }

        {
            // delete index
            metastore.delete_index(index_uri.clone()).await.unwrap();
            let data = metastore.data.read().await;
            assert_eq!(data.contains_key(&index_uri), false);
        }

        {
            // delete index (non-existent index uri)
            let result = metastore
                .delete_index("ram://test/non-existent-index".to_string())
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::IndexDoesNotExist;
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_single_file_metastore_stage_split() {
        let resolver = StorageUriResolver::default();
        let storage = resolver.resolve("ram://").unwrap();
        let metastore = SingleFileMetastore::new(storage).await.unwrap();
        let index_uri = IndexUri::from("ram://test/index");

        {
            // create index
            metastore
                .create_index(index_uri.clone(), DocMapping::Dynamic)
                .await
                .unwrap();
        }

        {
            // stage split
            let split_id = "one".to_string();
            let split_metadata = SplitMetadata {
                split_id: "one".to_string(),
                split_state: SplitState::Staged,
                num_records: 1,
                size_in_bytes: 2,
                time_range: Some(Range { start: 0, end: 100 }),
                generation: 3,
            };

            metastore
                .stage_split(index_uri.clone(), split_id.clone(), split_metadata)
                .await
                .unwrap();

            let data = metastore.data.read().await;
            assert_eq!(data.get(&index_uri).unwrap().splits.len(), 1);
            assert_eq!(
                data.get(&index_uri)
                    .unwrap()
                    .splits
                    .get(&split_id)
                    .unwrap()
                    .split_id,
                "one".to_string()
            );
            assert_eq!(
                data.get(&index_uri)
                    .unwrap()
                    .splits
                    .get(&split_id)
                    .unwrap()
                    .split_state,
                SplitState::Staged
            );
            assert_eq!(
                data.get(&index_uri)
                    .unwrap()
                    .splits
                    .get(&split_id)
                    .unwrap()
                    .num_records,
                1
            );
            assert_eq!(
                data.get(&index_uri)
                    .unwrap()
                    .splits
                    .get(&split_id)
                    .unwrap()
                    .size_in_bytes,
                2
            );
            assert_eq!(
                data.get(&index_uri)
                    .unwrap()
                    .splits
                    .get(&split_id)
                    .unwrap()
                    .time_range,
                Some(Range { start: 0, end: 100 })
            );
            assert_eq!(
                data.get(&index_uri)
                    .unwrap()
                    .splits
                    .get(&split_id)
                    .unwrap()
                    .generation,
                3
            );
        }

        {
            // stage split (existing split id)
            let split_id = "one".to_string();
            let split_metadata = SplitMetadata {
                split_id: "one".to_string(),
                split_state: SplitState::Staged,
                num_records: 1,
                size_in_bytes: 2,
                time_range: Some(Range { start: 0, end: 100 }),
                generation: 3,
            };

            let result = metastore
                .stage_split(index_uri.clone(), split_id.clone(), split_metadata)
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::ExistingSplitId;
            assert_eq!(result, expected);
        }

        {
            // stage split (non-existent index uri)
            let split_id = "one".to_string();
            let split_metadata = SplitMetadata {
                split_id: "one".to_string(),
                split_state: SplitState::Staged,
                num_records: 1,
                size_in_bytes: 2,
                time_range: Some(Range { start: 0, end: 100 }),
                generation: 3,
            };

            let result = metastore
                .stage_split(
                    "ram://test/non-existent-index".to_string(),
                    split_id.clone(),
                    split_metadata,
                )
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::IndexDoesNotExist;
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_single_file_metastore_publish_split() {
        let resolver = StorageUriResolver::default();
        let storage = resolver.resolve("ram://").unwrap();
        let metastore = SingleFileMetastore::new(storage).await.unwrap();
        let index_uri = IndexUri::from("ram://test/index");

        {
            // create index
            metastore
                .create_index(index_uri.clone(), DocMapping::Dynamic)
                .await
                .unwrap();
        }

        {
            // stage split
            let split_id = "one".to_string();
            let split_metadata = SplitMetadata {
                split_id: "one".to_string(),
                split_state: SplitState::Staged,
                num_records: 1,
                size_in_bytes: 2,
                time_range: Some(Range { start: 0, end: 100 }),
                generation: 3,
            };
            metastore
                .stage_split(index_uri.clone(), split_id.clone(), split_metadata)
                .await
                .unwrap();
        }

        {
            // publish split
            let split_id = "one".to_string();
            metastore
                .publish_split(index_uri.clone(), split_id.clone())
                .await
                .unwrap();
            let data = metastore.data.read().await;
            assert_eq!(
                data.get(&index_uri)
                    .unwrap()
                    .splits
                    .get(&split_id)
                    .unwrap()
                    .split_state,
                SplitState::Published
            );
        }

        {
            // publish published split
            let split_id = "one".to_string();
            metastore
                .publish_split(index_uri.clone(), split_id.clone())
                .await
                .unwrap();
        }

        {
            // publish non-existent split
            let result = metastore
                .publish_split(index_uri.clone(), "non-existant-split".to_string())
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::DoesNotExist;
            assert_eq!(result, expected);
        }

        {
            // publish non-staged split
            let split_id = "one".to_string();
            metastore
                .mark_split_as_deleted(index_uri.clone(), split_id.clone()) // mark as deleted
                .await
                .unwrap();

            let result = metastore
                .publish_split(index_uri.clone(), split_id.clone()) // publish
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::SplitIsNotStaged;
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_single_file_metastore_list_splits() {
        let resolver = StorageUriResolver::default();
        let storage = resolver.resolve("ram://").unwrap();
        let metastore = SingleFileMetastore::new(storage).await.unwrap();
        let index_uri = IndexUri::from("ram://test/index");

        {
            // create index
            metastore
                .create_index(index_uri.clone(), DocMapping::Dynamic)
                .await
                .unwrap();
        }

        {
            // stage split
            let split_id_1 = "one".to_string();
            let split_metadata_1 = SplitMetadata {
                split_id: "one".to_string(),
                split_state: SplitState::Staged,
                num_records: 1,
                size_in_bytes: 2,
                time_range: Some(Range { start: 0, end: 100 }),
                generation: 3,
            };

            let split_id_2 = "two".to_string();
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

            let split_id_3 = "three".to_string();
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

            let split_id_4 = "four".to_string();
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

            metastore
                .stage_split(index_uri.clone(), split_id_1.clone(), split_metadata_1)
                .await
                .unwrap();
            metastore
                .stage_split(index_uri.clone(), split_id_2.clone(), split_metadata_2)
                .await
                .unwrap();
            metastore
                .stage_split(index_uri.clone(), split_id_3.clone(), split_metadata_3)
                .await
                .unwrap();
            metastore
                .stage_split(index_uri.clone(), split_id_4.clone(), split_metadata_4)
                .await
                .unwrap();
        }

        {
            // list
            let range = Some(Range { start: 0, end: 99 });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
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
        }

        {
            // list
            let range = Some(Range { start: 0, end: 100 });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
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
        }

        {
            // list
            let range = Some(Range { start: 0, end: 101 });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
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
        }

        {
            // list
            let range = Some(Range { start: 0, end: 199 });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
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
        }

        {
            // list
            let range = Some(Range { start: 0, end: 200 });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
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
        }

        {
            // list
            let range = Some(Range { start: 0, end: 201 });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
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
        }

        {
            // list
            let range = Some(Range { start: 0, end: 299 });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
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
        }

        {
            // list
            let range = Some(Range { start: 0, end: 300 });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
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
        }

        {
            // list
            let range = Some(Range { start: 0, end: 301 });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
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
        }

        {
            // list
            let range = Some(Range {
                start: 301,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
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
        }

        {
            // list
            let range = Some(Range {
                start: 300,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
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
        }

        {
            // list
            let range = Some(Range {
                start: 299,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
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
        }

        {
            // list
            let range = Some(Range {
                start: 201,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
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
        }

        {
            // list
            let range = Some(Range {
                start: 200,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
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
        }

        {
            // list
            let range = Some(Range {
                start: 199,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
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
        }

        {
            // list
            let range = Some(Range {
                start: 101,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
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
        }

        {
            // list
            let range = Some(Range {
                start: 101,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
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
        }

        {
            // list
            let range = Some(Range {
                start: 100,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
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
        }

        {
            // list
            let range = Some(Range {
                start: 99,
                end: 400,
            });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
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
        }

        {
            // list
            let range = Some(Range {
                start: 1000,
                end: 1100,
            });
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
                .await
                .unwrap();
            assert_eq!(splits.len(), 0);
        }

        {
            // list
            let range = None;
            let splits = metastore
                .list_splits(index_uri.clone(), SplitState::Staged, range)
                .await
                .unwrap();
            let mut split_id_vec = Vec::new();
            for split_metadata in splits {
                split_id_vec.push(split_metadata.split_id);
            }
            assert_eq!(split_id_vec.contains(&"one".to_string()), true);
            assert_eq!(split_id_vec.contains(&"two".to_string()), true);
            assert_eq!(split_id_vec.contains(&"three".to_string()), true);
        }
    }

    #[tokio::test]
    async fn test_single_file_metastore_mark_split_as_deleted() {
        let resolver = StorageUriResolver::default();
        let storage = resolver.resolve("ram://").unwrap();
        let metastore = SingleFileMetastore::new(storage).await.unwrap();
        let index_uri = IndexUri::from("ram://test/index");

        {
            // create index
            metastore
                .create_index(index_uri.clone(), DocMapping::Dynamic)
                .await
                .unwrap();
        }

        {
            // stage split
            let split_id = "one".to_string();
            let split_metadata = SplitMetadata {
                split_id: "one".to_string(),
                split_state: SplitState::Staged,
                num_records: 1,
                size_in_bytes: 2,
                time_range: Some(Range { start: 0, end: 100 }),
                generation: 3,
            };
            metastore
                .stage_split(index_uri.clone(), split_id.clone(), split_metadata)
                .await
                .unwrap();
        }

        {
            // publish split
            let split_id = "one".to_string();
            metastore
                .publish_split(index_uri.clone(), split_id.clone())
                .await
                .unwrap();
        }

        {
            // mark as deleted
            let split_id = "one".to_string();
            metastore
                .mark_split_as_deleted(index_uri.clone(), split_id.clone())
                .await
                .unwrap();
            let data = metastore.data.read().await;
            assert_eq!(
                data.get(&index_uri)
                    .unwrap()
                    .splits
                    .get(&split_id)
                    .unwrap()
                    .split_state,
                SplitState::ScheduledForDeletion
            );
        }

        {
            // mark as deleted (already marked as deleted)
            let split_id = "one".to_string();
            metastore
                .mark_split_as_deleted(index_uri.clone(), split_id.clone())
                .await
                .unwrap();
        }

        {
            // mark as deleted (non-existent)
            let result = metastore
                .mark_split_as_deleted(index_uri.clone(), "non-existant-split".to_string())
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::DoesNotExist;
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_single_file_metastore_delete_split() {
        let resolver = StorageUriResolver::default();
        let storage = resolver.resolve("ram://").unwrap();
        let metastore = SingleFileMetastore::new(storage).await.unwrap();
        let index_uri = IndexUri::from("ram://test/index");

        {
            // create index
            metastore
                .create_index(index_uri.clone(), DocMapping::Dynamic)
                .await
                .unwrap();
        }

        {
            // stage split
            let split_id = "one".to_string();
            let split_metadata = SplitMetadata {
                split_id: "one".to_string(),
                split_state: SplitState::Staged,
                num_records: 1,
                size_in_bytes: 2,
                time_range: Some(Range { start: 0, end: 100 }),
                generation: 3,
            };
            metastore
                .stage_split(index_uri.clone(), split_id.clone(), split_metadata)
                .await
                .unwrap();
        }

        {
            // publish split
            let split_id = "one".to_string();
            metastore
                .publish_split(index_uri.clone(), split_id.clone())
                .await
                .unwrap();
        }

        {
            // delete split (published split)
            let split_id = "one".to_string();
            let result = metastore
                .delete_split(index_uri.clone(), split_id.clone())
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::Forbidden;
            assert_eq!(result, expected);
        }

        {
            // mark as deleted
            let split_id = "one".to_string();
            metastore
                .mark_split_as_deleted(index_uri.clone(), split_id.clone())
                .await
                .unwrap();
        }

        {
            // delete split
            let split_id = "one".to_string();
            metastore
                .delete_split(index_uri.clone(), split_id.clone())
                .await
                .unwrap();
            let data = metastore.data.read().await;
            assert_eq!(
                data.get(&index_uri).unwrap().splits.contains_key(&split_id),
                false
            );
        }

        {
            // delete split (non-existent)
            let result = metastore
                .delete_split(index_uri.clone(), "non-existant-split".to_string())
                .await
                .unwrap_err()
                .kind();
            let expected = MetastoreErrorKind::DoesNotExist;
            assert_eq!(result, expected);
        }
    }
}
