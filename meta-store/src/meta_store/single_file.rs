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
        split_manifest: SplitManifest,
    ) -> MetaStoreResult<SplitId> {
        let mut new_split_manifest = split_manifest.clone();
        new_split_manifest.state = SplitState::Staged;

        let mut data = self.data.write().unwrap();

        if data.splits.contains_key(&split_id) {
            return Err(
                MetaStoreErrorKind::ExistingSplitId.with_error(anyhow::anyhow!(
                    "Split ID has already exists: {:?}",
                    &split_id
                )),
            );
        }

        data.splits.insert(split_id.clone(), new_split_manifest);

        // TODO: put it back into storage.

        let new_data = MetaDataSet {
            index: data.index.clone(),
            splits: data.splits.clone(),
        };

        // Serialize meta data.
        let contents = match serde_json::to_vec(&new_data) {
            Ok(c) => c,
            Err(e) => {
                return Err(MetaStoreErrorKind::InvalidManifest
                    .with_error(anyhow::anyhow!("Failed to serialize meta data: {:?}", e)));
            }
        };
        println!("{:?}", String::from_utf8(contents).unwrap());

        // // Put data to storage.
        // match self.storage.put(&self.path, PutPayload::from(contents)).await {
        //     Ok(_) => (),
        //     Err(e) => {
        //         return Err(MetaStoreErrorKind::InternalError
        //             .with_error(anyhow::anyhow!("Failed to put meta data: {:?}", e)));
        //     }
        // };

        Ok(split_id)
    }

    async fn publish_split(&self, split_id: SplitId) -> MetaStoreResult<()> {
        let mut data = self.data.write().unwrap();

        match data.splits.get_mut(&split_id) {
            Some(m) => {
                m.state = SplitState::Published;
            }
            None => {
                return Err(MetaStoreErrorKind::DoesNotExist
                    .with_error(anyhow::anyhow!("Split ID does not exist: {:?}", &split_id)));
            }
        };

        // TODO: put it back into storage.

        Ok(())
    }

    async fn list_splits(
        &self,
        _state: SplitState,
        _time_range: Option<Range<u64>>,
    ) -> MetaStoreResult<()> {
        let _data = self.data.read().unwrap();

        Ok(())
    }

    async fn mark_as_deleted(&self, split_id: SplitId) -> MetaStoreResult<()> {
        let mut data = self.data.write().unwrap();

        match data.splits.get_mut(&split_id) {
            Some(m) => {
                m.state = SplitState::ScheduledForDeleted;
            }
            None => {
                return Err(MetaStoreErrorKind::DoesNotExist
                    .with_error(anyhow::anyhow!("Split ID does not exist: {:?}", &split_id)));
            }
        };

        // TODO: put it back into storage.

        Ok(())
    }

    async fn delete_split(&self, split_id: SplitId) -> MetaStoreResult<()> {
        let mut data = self.data.write().unwrap();

        match data.splits.get_mut(&split_id) {
            Some(m) => {
                match m.state {
                    SplitState::ScheduledForDeleted | SplitState::Staged => {
                        // TODO: delete split form meta data
                    }
                    _ => {
                        return Err(MetaStoreErrorKind::Forbidden.with_error(anyhow::anyhow!(
                            "This split is not a deletable state: {:?}:{:?}",
                            &split_id,
                            &m.state
                        )));
                    }
                }
            }
            None => {
                return Err(MetaStoreErrorKind::DoesNotExist
                    .with_error(anyhow::anyhow!("Split ID does not exist: {:?}", &split_id)));
            }
        };

        // TODO: put it back into storage.

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::path::{Path, PathBuf};

    use quickwit_storage::RamStorage;

    use crate::meta_store::single_file::SingleFileMetaStore;
    use crate::meta_store::{ManifestEntry, MetaStore, SplitManifest, SplitMetaData, SplitState};

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
    }

    #[tokio::test]
    async fn test_single_file_meta_store_stage_split() {
        let storage = RamStorage::default();
        let path = Path::new("/meta_store.json").to_path_buf();

        let meta_store = SingleFileMetaStore::new(Box::new(storage), path)
            .await
            .unwrap();

        let split_id = "split_one".to_string();

        let split_manifest = SplitManifest {
            metadata: SplitMetaData {
                split_uri: "sprit_one_uri".to_string(),
                num_records: 1,
                size_in_bytes: 1,
                time_range: Some(Range { start: 0, end: 1 }),
                generation: 1,
            },
            files: vec![ManifestEntry {
                file_name: "split_one_file_name".to_string(),
                file_size_in_bytes: 1,
            }],
            state: SplitState::Staged,
        };

        let _ = meta_store
            .stage_split(split_id.clone(), split_manifest)
            .await;

        // println!("{:?}", meta_store.data);

        assert_eq!("", "");
    }
}
