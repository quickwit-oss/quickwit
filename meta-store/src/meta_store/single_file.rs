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
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use quickwit_storage::{PutPayload, Storage, StorageErrorKind};

use crate::meta_store::{
    IndexMetaData, MetaDataSet, MetaStore, MetaStoreErrorKind, MetaStoreResult, SplitId,
    SplitManifest, SplitState,
};

/// Single file meta store implementation.
struct SingleFileMetaStore {
    storage: Box<dyn Storage>,
    path: PathBuf,
    data: Arc<RwLock<MetaDataSet>>,
}

#[allow(dead_code)]
impl SingleFileMetaStore {
    /// Creates a meta store given a storage.
    pub async fn new(storage: Box<dyn Storage>, path: PathBuf) -> MetaStoreResult<Self> {
        let meta_data = match storage.get_all(&path).await {
            Ok(contents) => {
                // Deseliarize meta data.
                let meta_data = match serde_json::from_slice::<MetaDataSet>(contents.as_slice()) {
                    Ok(meta_data) => meta_data,
                    Err(e) => {
                        return Err(MetaStoreErrorKind::InvalidManifest.with_error(
                            anyhow::anyhow!("Failed to deserialize meta data: {:?}", e),
                        ));
                    }
                };

                meta_data
            }
            Err(ref e) if e.kind() == StorageErrorKind::DoesNotExist => {
                // Create a new meta data.
                let meta_data = MetaDataSet {
                    index: IndexMetaData {
                        version: env!("CARGO_PKG_VERSION").to_string(),
                        name: "index_one".to_string(),
                        path: path.to_str().unwrap().to_string(),
                    },
                    splits: HashMap::new(),
                };

                // Serialize meta data.
                let contents = match serde_json::to_vec(&meta_data) {
                    Ok(c) => c,
                    Err(e) => {
                        return Err(MetaStoreErrorKind::InvalidManifest.with_error(
                            anyhow::anyhow!("Failed to serialize meta data: {:?}", e),
                        ));
                    }
                };

                // Put data to storage.
                match storage.put(&path, PutPayload::from(contents)).await {
                    Ok(_) => (),
                    Err(e) => {
                        return Err(MetaStoreErrorKind::InternalError
                            .with_error(anyhow::anyhow!("Failed to put meta data: {:?}", e)));
                    }
                };

                meta_data
            }
            Err(e) => {
                return Err(MetaStoreErrorKind::InternalError
                    .with_error(anyhow::anyhow!("Failed to create meta store: {:?}", e)));
            }
        };

        Ok(SingleFileMetaStore {
            storage,
            data: Arc::new(RwLock::new(meta_data)),
            path,
        })
    }
}

#[async_trait]
impl MetaStore for SingleFileMetaStore {
    async fn stage_split(
        &self,
        split_id: SplitId,
        mut split_manifest: SplitManifest,
    ) -> MetaStoreResult<SplitId> {
        split_manifest.state = SplitState::Staged;
        let contents: Vec<u8>;

        {
            let mut data = self.data.write().unwrap();

            if data.splits.contains_key(&split_id) {
                return Err(
                    MetaStoreErrorKind::ExistingSplitId.with_error(anyhow::anyhow!(
                        "Split ID has already exists: {:?}",
                        &split_id
                    )),
                );
            }

            data.splits.insert(split_id.clone(), split_manifest);

            let meta_data_set = MetaDataSet {
                index: data.index.clone(),
                splits: data.splits.clone(),
            };

            // Serialize meta data.
            contents = match serde_json::to_vec(&meta_data_set) {
                Ok(c) => c,
                Err(e) => {
                    return Err(MetaStoreErrorKind::InvalidManifest
                        .with_error(anyhow::anyhow!("Failed to serialize meta data: {:?}", e)));
                }
            };
        }

        // Put data back into storage.
        match self
            .storage
            .put(&self.path, PutPayload::from(contents))
            .await
        {
            Ok(_) => (),
            Err(e) => {
                return Err(MetaStoreErrorKind::InternalError
                    .with_error(anyhow::anyhow!("Failed to put meta data: {:?}", e)));
            }
        };

        Ok(split_id)
    }

    async fn publish_split(&self, split_id: SplitId) -> MetaStoreResult<()> {
        let contents: Vec<u8>;

        {
            let mut data = self.data.write().unwrap();

            match data.splits.get_mut(&split_id) {
                Some(m) => {
                    match m.state {
                        SplitState::Published => {
                            // If the split is already published, this API call returns a success.
                            return Ok(());
                        }
                        SplitState::Staged => m.state = SplitState::Published,
                        _ => {
                            return Err(MetaStoreErrorKind::SplitIsNotStaged.with_error(
                                anyhow::anyhow!("Split ID is not staged: {:?}", &split_id),
                            ));
                        }
                    }
                }
                None => {
                    return Err(MetaStoreErrorKind::DoesNotExist
                        .with_error(anyhow::anyhow!("Split ID does not exist: {:?}", &split_id)));
                }
            };

            let meta_data_set = MetaDataSet {
                index: data.index.clone(),
                splits: data.splits.clone(),
            };

            // Serialize meta data.
            contents = match serde_json::to_vec(&meta_data_set) {
                Ok(c) => c,
                Err(e) => {
                    return Err(MetaStoreErrorKind::InvalidManifest
                        .with_error(anyhow::anyhow!("Failed to serialize meta data: {:?}", e)));
                }
            };
        }

        // Put data back into storage.
        match self
            .storage
            .put(&self.path, PutPayload::from(contents))
            .await
        {
            Ok(_) => (),
            Err(e) => {
                return Err(MetaStoreErrorKind::InternalError
                    .with_error(anyhow::anyhow!("Failed to put meta data: {:?}", e)));
            }
        };

        Ok(())
    }

    async fn list_splits(
        &self,
        state: SplitState,
        time_range: Option<Range<u64>>,
    ) -> MetaStoreResult<HashMap<SplitId, SplitManifest>> {
        let mut splits = HashMap::new();

        let meta_data_set = self.data.read().unwrap();

        for (split_id, split_manifest) in &meta_data_set.splits {
            if split_manifest.state != state {
                // If the state of the split that stored in meta store is not match with argument,
                // this split will be ignored.
                continue;
            }

            // filter by time range
            match time_range {
                Some(ref filter_time_range) => {
                    match split_manifest.metadata.time_range {
                        Some(ref split_time_range) => {
                            if split_time_range.contains(&filter_time_range.start)
                                || split_time_range.contains(&filter_time_range.end)
                            {
                                splits.insert(split_id.clone(), split_manifest.clone());
                            } else {
                                // Does not match the specified time range.
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
                    // if time_range is omitted, the metadata is not filtered.
                    splits.insert(split_id.clone(), split_manifest.clone());
                }
            }
        }

        if splits.len() == 0 {
            return Err(
                MetaStoreErrorKind::IndexDoesNotExist.with_error(anyhow::anyhow!(
                    "There are no splits that match the criteria."
                )),
            );
        }

        Ok(splits)
    }

    async fn mark_as_deleted(&self, split_id: SplitId) -> MetaStoreResult<()> {
        let contents: Vec<u8>;

        {
            let mut data = self.data.write().unwrap();

            match data.splits.get_mut(&split_id) {
                Some(m) => {
                    match m.state {
                        SplitState::ScheduledForDeleted => {
                            // If the split is already scheduled for deleted, this API call returns a success.
                            return Ok(());
                        }
                        _ => m.state = SplitState::ScheduledForDeleted,
                    }
                }
                None => {
                    return Err(MetaStoreErrorKind::DoesNotExist
                        .with_error(anyhow::anyhow!("Split ID does not exist: {:?}", &split_id)));
                }
            };

            let meta_data_set = MetaDataSet {
                index: data.index.clone(),
                splits: data.splits.clone(),
            };

            // Serialize meta data.
            contents = match serde_json::to_vec(&meta_data_set) {
                Ok(c) => c,
                Err(e) => {
                    return Err(MetaStoreErrorKind::InvalidManifest
                        .with_error(anyhow::anyhow!("Failed to serialize meta data: {:?}", e)));
                }
            };
        }

        // Put data back into storage.
        match self
            .storage
            .put(&self.path, PutPayload::from(contents))
            .await
        {
            Ok(_) => (),
            Err(e) => {
                return Err(MetaStoreErrorKind::InternalError
                    .with_error(anyhow::anyhow!("Failed to put meta data: {:?}", e)));
            }
        };

        Ok(())
    }

    async fn delete_split(&self, split_id: SplitId) -> MetaStoreResult<()> {
        let contents: Vec<u8>;

        {
            let mut data = self.data.write().unwrap();

            match data.splits.get_mut(&split_id) {
                Some(m) => match m.state {
                    SplitState::ScheduledForDeleted | SplitState::Staged => {
                        data.splits.remove(&split_id);
                    }
                    _ => {
                        return Err(MetaStoreErrorKind::Forbidden.with_error(anyhow::anyhow!(
                            "This split is not a deletable state: {:?}:{:?}",
                            &split_id,
                            &m.state
                        )));
                    }
                },
                None => {
                    return Err(MetaStoreErrorKind::DoesNotExist
                        .with_error(anyhow::anyhow!("Split ID does not exist: {:?}", &split_id)));
                }
            };

            let meta_data_set = MetaDataSet {
                index: data.index.clone(),
                splits: data.splits.clone(),
            };

            // Serialize meta data.
            contents = match serde_json::to_vec(&meta_data_set) {
                Ok(c) => c,
                Err(e) => {
                    return Err(MetaStoreErrorKind::InvalidManifest
                        .with_error(anyhow::anyhow!("Failed to serialize meta data: {:?}", e)));
                }
            };
        }

        // Put data back into storage.
        match self
            .storage
            .put(&self.path, PutPayload::from(contents))
            .await
        {
            Ok(_) => (),
            Err(e) => {
                return Err(MetaStoreErrorKind::InternalError
                    .with_error(anyhow::anyhow!("Failed to put meta data: {:?}", e)));
            }
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::path::Path;

    use quickwit_storage::RamStorage;

    use crate::meta_store::single_file::SingleFileMetaStore;
    use crate::meta_store::{
        ManifestEntry, MetaStore, MetaStoreErrorKind, SplitManifest, SplitMetaData, SplitState,
    };

    #[tokio::test]
    async fn test_single_file_meta_store_new() {
        let storage = RamStorage::default();
        let path = Path::new("/meta_store.json").to_path_buf();

        let meta_store = SingleFileMetaStore::new(Box::new(storage), path)
            .await
            .unwrap();

        let data = meta_store.data.read().unwrap();
        assert_eq!(data.index.version, env!("CARGO_PKG_VERSION").to_string());
        assert_eq!(data.index.name, "index_one".to_string());
        assert_eq!(data.index.path, "/meta_store.json".to_string());
        assert_eq!(data.splits.len(), 0);
    }

    #[tokio::test]
    async fn test_single_file_meta_store_stage_split() {
        let storage = RamStorage::default();
        let path = Path::new("/meta_store.json").to_path_buf();

        let meta_store = SingleFileMetaStore::new(Box::new(storage), path)
            .await
            .unwrap();

        let split_id = "split_one".to_string();

        {
            // stage split
            let split_manifest = SplitManifest {
                metadata: SplitMetaData {
                    split_uri: "sprit_one_uri".to_string(),
                    num_records: 1,
                    size_in_bytes: 2,
                    time_range: Some(Range { start: 0, end: 100 }),
                    generation: 3,
                },
                files: vec![ManifestEntry {
                    file_name: "split_one_file_name".to_string(),
                    file_size_in_bytes: 4,
                }],
                state: SplitState::Staged,
            };

            meta_store
                .stage_split(split_id.clone(), split_manifest)
                .await
                .unwrap();

            let data = meta_store.data.read().unwrap();

            assert_eq!(data.splits.len(), 1);
            assert_eq!(
                data.splits.get(&split_id).unwrap().metadata.split_uri,
                "sprit_one_uri".to_string()
            );
            assert_eq!(data.splits.get(&split_id).unwrap().metadata.num_records, 1);
            assert_eq!(
                data.splits.get(&split_id).unwrap().metadata.size_in_bytes,
                2
            );
            assert_eq!(
                data.splits.get(&split_id).unwrap().metadata.time_range,
                Some(Range { start: 0, end: 100 })
            );
            assert_eq!(data.splits.get(&split_id).unwrap().metadata.generation, 3);
            assert_eq!(data.splits.get(&split_id).unwrap().files.len(), 1);
            assert_eq!(
                data.splits
                    .get(&split_id)
                    .unwrap()
                    .files
                    .get(0)
                    .unwrap()
                    .file_name,
                "split_one_file_name".to_string()
            );
            assert_eq!(
                data.splits
                    .get(&split_id)
                    .unwrap()
                    .files
                    .get(0)
                    .unwrap()
                    .file_size_in_bytes,
                4
            );
            assert_eq!(
                data.splits.get(&split_id).unwrap().state,
                SplitState::Staged
            );
        }

        {
            // stage split (existing split id)
            let split_manifest = SplitManifest {
                metadata: SplitMetaData {
                    split_uri: "sprit_one_uri_2".to_string(),
                    num_records: 2,
                    size_in_bytes: 3,
                    time_range: Some(Range {
                        start: 100,
                        end: 200,
                    }),
                    generation: 4,
                },
                files: vec![ManifestEntry {
                    file_name: "split_one_file_name".to_string(),
                    file_size_in_bytes: 4,
                }],
                state: SplitState::Staged,
            };

            let result = meta_store
                .stage_split(split_id.clone(), split_manifest)
                .await
                .unwrap_err()
                .kind();
            let expected = MetaStoreErrorKind::ExistingSplitId;
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_single_file_meta_store_publish_split() {
        let storage = RamStorage::default();
        let path = Path::new("/meta_store.json").to_path_buf();

        let meta_store = SingleFileMetaStore::new(Box::new(storage), path)
            .await
            .unwrap();

        let split_id = "split_one".to_string();

        {
            // stage split
            let split_manifest = SplitManifest {
                metadata: SplitMetaData {
                    split_uri: "sprit_one_uri".to_string(),
                    num_records: 1,
                    size_in_bytes: 2,
                    time_range: Some(Range { start: 0, end: 100 }),
                    generation: 3,
                },
                files: vec![ManifestEntry {
                    file_name: "split_one_file_name".to_string(),
                    file_size_in_bytes: 4,
                }],
                state: SplitState::Staged,
            };

            meta_store
                .stage_split(split_id.clone(), split_manifest)
                .await
                .unwrap();
        }

        {
            // publish split
            let _result = meta_store.publish_split(split_id.clone()).await.unwrap();
            let data = meta_store.data.read().unwrap();

            assert_eq!(data.splits.len(), 1);
            assert_eq!(
                data.splits.get(&split_id).unwrap().metadata.split_uri,
                "sprit_one_uri".to_string()
            );
            assert_eq!(data.splits.get(&split_id).unwrap().metadata.num_records, 1);
            assert_eq!(
                data.splits.get(&split_id).unwrap().metadata.size_in_bytes,
                2
            );
            assert_eq!(
                data.splits.get(&split_id).unwrap().metadata.time_range,
                Some(Range { start: 0, end: 100 })
            );
            assert_eq!(data.splits.get(&split_id).unwrap().metadata.generation, 3);
            assert_eq!(data.splits.get(&split_id).unwrap().files.len(), 1);
            assert_eq!(
                data.splits
                    .get(&split_id)
                    .unwrap()
                    .files
                    .get(0)
                    .unwrap()
                    .file_name,
                "split_one_file_name".to_string()
            );
            assert_eq!(
                data.splits
                    .get(&split_id)
                    .unwrap()
                    .files
                    .get(0)
                    .unwrap()
                    .file_size_in_bytes,
                4
            );
            assert_eq!(
                data.splits.get(&split_id).unwrap().state,
                SplitState::Published
            );
        }

        {
            // publish split (non-existent)
            let result = meta_store
                .publish_split("non-existant".to_string())
                .await
                .unwrap_err()
                .kind();
            let expected = MetaStoreErrorKind::DoesNotExist;
            assert_eq!(result, expected);
        }

        {
            // publish split (non-staged)
            meta_store.mark_as_deleted(split_id.clone()).await.unwrap();
            let result = meta_store
                .publish_split(split_id.clone())
                .await
                .unwrap_err()
                .kind();
            let expected = MetaStoreErrorKind::SplitIsNotStaged;
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_single_file_meta_store_list_splits() {
        let storage = RamStorage::default();
        let path = Path::new("/meta_store.json").to_path_buf();

        let meta_store = SingleFileMetaStore::new(Box::new(storage), path)
            .await
            .unwrap();

        let split_id_1 = "split_one".to_string();
        let split_manifest_1 = SplitManifest {
            metadata: SplitMetaData {
                split_uri: "sprit_one_uri".to_string(),
                num_records: 1,
                size_in_bytes: 2,
                time_range: Some(Range { start: 0, end: 500 }),
                generation: 3,
            },
            files: vec![ManifestEntry {
                file_name: "split_one_file_name".to_string(),
                file_size_in_bytes: 4,
            }],
            state: SplitState::Staged,
        };

        let split_id_2 = "split_two".to_string();
        let split_manifest_2 = SplitManifest {
            metadata: SplitMetaData {
                split_uri: "sprit_two_uri".to_string(),
                num_records: 2,
                size_in_bytes: 3,
                time_range: Some(Range {
                    start: 100,
                    end: 300,
                }),
                generation: 4,
            },
            files: vec![ManifestEntry {
                file_name: "split_two_file_name".to_string(),
                file_size_in_bytes: 5,
            }],
            state: SplitState::Staged,
        };

        let split_id_3 = "split_three".to_string();
        let split_manifest_3 = SplitManifest {
            metadata: SplitMetaData {
                split_uri: "sprit_three_uri".to_string(),
                num_records: 2,
                size_in_bytes: 3,
                time_range: Some(Range {
                    start: 300,
                    end: 700,
                }),
                generation: 4,
            },
            files: vec![ManifestEntry {
                file_name: "split_three_file_name".to_string(),
                file_size_in_bytes: 5,
            }],
            state: SplitState::Staged,
        };

        {
            // stage split
            meta_store
                .stage_split(split_id_1.clone(), split_manifest_1)
                .await
                .unwrap();
            meta_store
                .stage_split(split_id_2.clone(), split_manifest_2)
                .await
                .unwrap();
            meta_store
                .stage_split(split_id_3.clone(), split_manifest_3)
                .await
                .unwrap();
        }

        {
            // list
            let range = Some(Range { start: 0, end: 99 });
            let splits = meta_store
                .list_splits(SplitState::Staged, range)
                .await
                .unwrap();

            assert_eq!(splits.contains_key(&split_id_1), true);
            assert_eq!(splits.contains_key(&split_id_2), false);
            assert_eq!(splits.contains_key(&split_id_3), false);
        }

        {
            // list
            let range = Some(Range {
                start: 99,
                end: 100,
            });
            let splits = meta_store
                .list_splits(SplitState::Staged, range)
                .await
                .unwrap();

            assert_eq!(splits.contains_key(&split_id_1), true);
            assert_eq!(splits.contains_key(&split_id_2), true);
            assert_eq!(splits.contains_key(&split_id_3), false);
        }

        {
            // list
            let range = Some(Range {
                start: 100,
                end: 299,
            });
            let splits = meta_store
                .list_splits(SplitState::Staged, range)
                .await
                .unwrap();

            assert_eq!(splits.contains_key(&split_id_1), true);
            assert_eq!(splits.contains_key(&split_id_2), true);
            assert_eq!(splits.contains_key(&split_id_3), false);
        }

        {
            // list
            let range = Some(Range {
                start: 100,
                end: 300,
            });
            let splits = meta_store
                .list_splits(SplitState::Staged, range)
                .await
                .unwrap();

            assert_eq!(splits.contains_key(&split_id_1), true);
            assert_eq!(splits.contains_key(&split_id_2), true);
            assert_eq!(splits.contains_key(&split_id_3), true);
        }

        {
            // list
            let range = Some(Range {
                start: 299,
                end: 300,
            });
            let splits = meta_store
                .list_splits(SplitState::Staged, range)
                .await
                .unwrap();

            assert_eq!(splits.contains_key(&split_id_1), true);
            assert_eq!(splits.contains_key(&split_id_2), true);
            assert_eq!(splits.contains_key(&split_id_3), true);
        }

        {
            // list
            let range = Some(Range {
                start: 300,
                end: 699,
            });
            let splits = meta_store
                .list_splits(SplitState::Staged, range)
                .await
                .unwrap();

            assert_eq!(splits.contains_key(&split_id_1), true);
            assert_eq!(splits.contains_key(&split_id_2), false);
            assert_eq!(splits.contains_key(&split_id_3), true);
        }

        {
            // list
            let range = Some(Range {
                start: 301,
                end: 699,
            });
            let splits = meta_store
                .list_splits(SplitState::Staged, range)
                .await
                .unwrap();

            assert_eq!(splits.contains_key(&split_id_1), true);
            assert_eq!(splits.contains_key(&split_id_2), false);
            assert_eq!(splits.contains_key(&split_id_3), true);
        }

        {
            // list
            let range = Some(Range {
                start: 499,
                end: 700,
            });
            let splits = meta_store
                .list_splits(SplitState::Staged, range)
                .await
                .unwrap();

            assert_eq!(splits.contains_key(&split_id_1), true);
            assert_eq!(splits.contains_key(&split_id_2), false);
            assert_eq!(splits.contains_key(&split_id_3), true);
        }

        {
            // list
            let range = Some(Range {
                start: 500,
                end: 700,
            });
            let splits = meta_store
                .list_splits(SplitState::Staged, range)
                .await
                .unwrap();

            assert_eq!(splits.contains_key(&split_id_1), false);
            assert_eq!(splits.contains_key(&split_id_2), false);
            assert_eq!(splits.contains_key(&split_id_3), true);
        }

        {
            // list
            let range = Some(Range {
                start: 699,
                end: 800,
            });
            let splits = meta_store
                .list_splits(SplitState::Staged, range)
                .await
                .unwrap();

            assert_eq!(splits.contains_key(&split_id_1), false);
            assert_eq!(splits.contains_key(&split_id_2), false);
            assert_eq!(splits.contains_key(&split_id_3), true);
        }

        {
            // list
            let splits = meta_store
                .list_splits(SplitState::Staged, None)
                .await
                .unwrap();

            assert_eq!(splits.contains_key(&split_id_1), true);
            assert_eq!(splits.contains_key(&split_id_2), true);
            assert_eq!(splits.contains_key(&split_id_3), true);
        }

        {
            // list
            let range = Some(Range {
                start: 700,
                end: 800,
            });
            let result = meta_store
                .list_splits(SplitState::Staged, range)
                .await
                .unwrap_err()
                .kind();
            let expected = MetaStoreErrorKind::IndexDoesNotExist;
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_single_file_meta_store_mark_as_deleted() {
        let storage = RamStorage::default();
        let path = Path::new("/meta_store.json").to_path_buf();

        let meta_store = SingleFileMetaStore::new(Box::new(storage), path)
            .await
            .unwrap();

        let split_id = "split_one".to_string();

        {
            // stage split
            let split_manifest = SplitManifest {
                metadata: SplitMetaData {
                    split_uri: "sprit_one_uri".to_string(),
                    num_records: 1,
                    size_in_bytes: 2,
                    time_range: Some(Range { start: 0, end: 100 }),
                    generation: 3,
                },
                files: vec![ManifestEntry {
                    file_name: "split_one_file_name".to_string(),
                    file_size_in_bytes: 4,
                }],
                state: SplitState::Staged,
            };

            meta_store
                .stage_split(split_id.clone(), split_manifest)
                .await
                .unwrap();
        }

        {
            // mark as deleted
            meta_store.mark_as_deleted(split_id.clone()).await.unwrap();

            let data = meta_store.data.read().unwrap();
            assert_eq!(
                data.splits.get(&split_id).unwrap().state,
                SplitState::ScheduledForDeleted
            );
        }

        {
            // mark as deleted (already marked as deleted)
            meta_store.mark_as_deleted(split_id.clone()).await.unwrap();
        }

        {
            // mark as deleted (non-existent)
            let result = meta_store
                .mark_as_deleted("non-existant".to_string())
                .await
                .unwrap_err()
                .kind();
            let expected = MetaStoreErrorKind::DoesNotExist;
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_single_file_meta_store_delete_split() {
        let storage = RamStorage::default();
        let path = Path::new("/meta_store.json").to_path_buf();

        let meta_store = SingleFileMetaStore::new(Box::new(storage), path)
            .await
            .unwrap();

        let split_id = "split_one".to_string();

        {
            // stage split
            let split_manifest = SplitManifest {
                metadata: SplitMetaData {
                    split_uri: "sprit_one_uri".to_string(),
                    num_records: 1,
                    size_in_bytes: 2,
                    time_range: Some(Range { start: 0, end: 100 }),
                    generation: 3,
                },
                files: vec![ManifestEntry {
                    file_name: "split_one_file_name".to_string(),
                    file_size_in_bytes: 4,
                }],
                state: SplitState::Staged,
            };

            meta_store
                .stage_split(split_id.clone(), split_manifest)
                .await
                .unwrap();
        }

        {
            // publish split
            meta_store.publish_split(split_id.clone()).await.unwrap();
        }

        {
            // delete split (published split)
            let result = meta_store
                .delete_split(split_id.clone())
                .await
                .unwrap_err()
                .kind();
            let expected = MetaStoreErrorKind::Forbidden;
            assert_eq!(result, expected);
        }

        {
            // mark as deleted
            meta_store.mark_as_deleted(split_id.clone()).await.unwrap();
        }

        {
            // delete split
            meta_store.delete_split(split_id.clone()).await.unwrap();

            let data = meta_store.data.read().unwrap();
            assert_eq!(data.splits.contains_key(&split_id), false);
        }

        {
            // delete split (non-existent)
            let result = meta_store
                .delete_split("non-existant".to_string())
                .await
                .unwrap_err()
                .kind();
            let expected = MetaStoreErrorKind::DoesNotExist;
            assert_eq!(result, expected);
        }
    }
}
