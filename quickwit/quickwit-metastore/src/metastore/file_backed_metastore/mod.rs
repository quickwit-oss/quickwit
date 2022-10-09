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

//! Module for [`FileBackedMetastore`]. It is public so that the crate `quickwit-backward-compat`
//! can import [`FileBackedIndex`] and run backward-compatibility tests. You should not have to
//! import anything from here directly.

pub mod file_backed_index;
mod file_backed_metastore_factory;
mod lazy_file_backed_index;
mod store_operations;

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::try_join_all;
use itertools::Itertools;
use quickwit_common::uri::Uri;
use quickwit_config::SourceConfig;
use quickwit_doc_mapper::tag_pruning::TagFilterAst;
use quickwit_proto::metastore_api::{DeleteQuery, DeleteTask};
use quickwit_storage::Storage;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};

use self::file_backed_index::FileBackedIndex;
pub use self::file_backed_metastore_factory::FileBackedMetastoreFactory;
use self::lazy_file_backed_index::LazyFileBackedIndex;
use self::store_operations::{
    delete_index, fetch_and_build_indexes_states, fetch_index, index_exists, put_index,
    put_indexes_states,
};
use crate::checkpoint::IndexCheckpointDelta;
use crate::{
    IndexMetadata, Metastore, MetastoreError, MetastoreResult, Split, SplitMetadata, SplitState,
};

/// State of an index tracked by the metastore.
pub(crate) enum IndexState {
    /// Index is being created but its metadata has not been created on the storage yet.
    Creating,
    /// Index is alive.
    Alive(LazyFileBackedIndex),
    /// Index is being deleted and but its index metadata file has not yet been deleted on the
    /// storage.
    Deleting,
}

/// Metastore that stores all of the metadata associated to each index
/// into as many files and stores a map of indexes
/// (index_id, index_state) in a dedicated file `indexes_states.json`.
/// An `IndexState` describes the lifecyle of an index: `Creating` and
/// `Deleting` are transitioning states that indicates that index is not
/// yet available. On the contrary, `Alive` state indicates the index is ready
/// to be retrieved / updated.
/// Transitioning states are useful to track partial creating/deleting
/// happening when error(s) occur during index creation and deletion:
/// - `Creating` indicates that the metastore updated the `indexes_states.json` file with this state
///   but not yet the index metadata file;
/// - `Deleting` indicates that the metastore updated the `indexes_states.json` file with this state
///   but the index metadata file is not yet deleted.
///
/// !!! Important note: the indexes map `indexes_states.json` does not
/// guarantee exhaustivity: an index metadata file can be on the storage
/// but not present in the states map. As the map is incomplete, the metastore
/// does not rely on it to check index existence, this leads to following
/// implementations:
/// - on creation, the metastore always checks if an index metadata file is already present on the
///   storage even if the index is not in the indexes map;
/// - on get/update of an index, same story, the metastore checks if index is on the storage and if
///   present, the index is loaded in the map and returned /modified;
/// - on deletion, same story, the metastore deletes an index metadata file present on the storage
///   even if the index is not in the map.
///
/// !!! Important note 2: it is strongly advised to restrict the `FileBackedMetastore`
/// usage to the following use cases:
/// - testing;
/// - single-node environment;
/// - multiple-nodes environment with only one writer and readers. In this case, you must be very
///   cautious and ensure that your readers are really readers.
pub struct FileBackedMetastore {
    storage: Arc<dyn Storage>,
    per_index_metastores: Arc<RwLock<HashMap<String, IndexState>>>,
    polling_interval_opt: Option<Duration>,
}

impl FileBackedMetastore {
    /// Creates a [`FileBackedMetastore`] for tests.
    #[doc(hidden)]
    pub fn for_test(storage: Arc<dyn Storage>) -> Self {
        Self {
            storage,
            per_index_metastores: Default::default(),
            polling_interval_opt: None,
        }
    }

    /// Sets the polling interval.
    ///
    /// Only newly accessed indexes will be affected by the change of this setting.
    pub fn set_polling_interval(&mut self, polling_interval_opt: Option<Duration>) {
        self.polling_interval_opt = polling_interval_opt;
    }

    #[cfg(test)]
    pub fn storage(&self) -> Arc<dyn Storage> {
        self.storage.clone()
    }

    /// Creates a [`FileBackedMetastore`] for a specified storage.
    /// Indexes states are immediately fetched from the storage.
    pub async fn try_new(
        storage: Arc<dyn Storage>,
        polling_interval_opt: Option<Duration>,
    ) -> MetastoreResult<Self> {
        let indexes_map =
            fetch_and_build_indexes_states(storage.clone(), polling_interval_opt).await?;
        let per_index_metastores = Arc::new(RwLock::new(indexes_map));
        Ok(Self {
            storage,
            per_index_metastores,
            polling_interval_opt,
        })
    }

    async fn mutate(
        &self,
        index_id: &str,
        mutate_fn: impl FnOnce(&mut FileBackedIndex) -> crate::MetastoreResult<bool>,
    ) -> MetastoreResult<bool> {
        let mut locked_index = self.get_locked_index(index_id).await?;
        let mut index = locked_index.clone();
        let mutation_occurred = mutate_fn(&mut index)?;
        if !mutation_occurred {
            return Ok(false);
        }

        let put_result = put_index(&*self.storage, &index).await;
        match put_result {
            Ok(()) => {
                *locked_index = index;
                Ok(true)
            }
            Err(err) => {
                // For some of the error type here, we cannot know for sure
                // whether the content was written or not.
                //
                // Just to be sure, let's discard the cache.
                let mut per_index_metastores_wlock = self.per_index_metastores.write().await;

                // At this point, we hold both locks.
                per_index_metastores_wlock.insert(
                    index_id.to_string(),
                    IndexState::Alive(LazyFileBackedIndex::new(
                        self.storage.clone(),
                        index_id.to_string(),
                        self.polling_interval_opt,
                        None,
                    )),
                );
                locked_index.discarded = true;

                Err(err)
            }
        }
    }

    async fn read<T, F>(&self, index_id: &str, view: F) -> MetastoreResult<T>
    where F: FnOnce(&FileBackedIndex) -> MetastoreResult<T> {
        let locked_index = self.get_locked_index(index_id).await?;
        view(&*locked_index)
    }

    /// Returns a valid metadataset that is locked.
    ///
    /// This function guarantees that the metadataset has not been
    /// marked as discarded.
    async fn get_locked_index(
        &self,
        index_id: &str,
    ) -> MetastoreResult<OwnedMutexGuard<FileBackedIndex>> {
        loop {
            let index_mutex = self.index(index_id).await?;
            let index_lock = index_mutex.lock_owned().await;
            if !index_lock.discarded {
                return Ok(index_lock);
            }
        }
    }

    /// Returns a FileBackedIndex for the given index_id.
    ///
    /// If `index_id` is in a transitioning state `Creating` or `Deleting`, it will
    /// trigger an error.
    /// If `index_id` is not yet in `per_index_metastores` map,
    /// a fetch to the storage will be initiated and might trigger an error.
    ///
    /// For a given index_id, only copies of the same index_view are returned.
    async fn index(&self, index_id: &str) -> MetastoreResult<Arc<Mutex<FileBackedIndex>>> {
        {
            // Happy path!
            // If the object is already in our cache then we just return a copy
            let per_index_metastores_r = self.per_index_metastores.read().await;
            if let Some(index_state) = per_index_metastores_r.get(index_id) {
                return get_index_mutex(index_id, index_state).await;
            }
        }

        // At this point we do not hold our mutex, so we need to do a little dance
        // to make sure we return the same instance.
        //
        // If there is an error here, note we do not return right away.
        // That's because we want to observe the property that after one success
        // all subsequent calls will succeed.
        let index_result = fetch_index(&*self.storage, index_id).await;

        // Here we retake the lock, still no io ongoing.
        let mut per_index_metastores_wlock = self.per_index_metastores.write().await;

        // At this point, some other client might have added another instance of the Metadataet in
        // the map. We want to avoid two copies to exist in the application, so we keep only
        // one.
        if let Some(index_state) = per_index_metastores_wlock.get(index_id) {
            return get_index_mutex(index_id, index_state).await;
        }

        // We need to instanciate a `LazyFileBackedIndex` that will hold the mutex
        // and take care of spawning the polling if needed.
        let index = index_result?;
        let lazy_index = LazyFileBackedIndex::new(
            self.storage.clone(),
            index_id.to_string(),
            self.polling_interval_opt,
            Some(index),
        );
        let index_mutex = lazy_index.get().await?;
        per_index_metastores_wlock.insert(index_id.to_string(), IndexState::Alive(lazy_index));
        Ok(index_mutex)
    }

    // Helper used for testing to obtain the data associated with the given index.
    #[cfg(test)]
    async fn get_index(&self, index_id: &str) -> MetastoreResult<FileBackedIndex> {
        self.read(index_id, |index| Ok(index.clone())).await
    }

    #[cfg(test)]
    async fn index_exists(&self, index_id: &str) -> MetastoreResult<bool> {
        index_exists(&*self.storage, index_id).await
    }
}

#[async_trait]
impl Metastore for FileBackedMetastore {
    /// -------------------------------------------------------------------------------
    /// Mutations over the high-level index.
    async fn create_index(&self, index_metadata: IndexMetadata) -> MetastoreResult<()> {
        let index_id = index_metadata.index_id.clone();

        // We pick the outer lock here, so that we enter a critical section.
        let mut per_index_metastores_wlock = self.per_index_metastores.write().await;

        // Checking if index already exists is a bit tedious:
        // - first we check the index state: if it's `Alive`, return `IndexAlreadyExists` error, and
        //   if it's `Creating` or `Deleting`, it's ok to override them as these are transitioning
        //   states.
        // - if the index is not in the indexes states map, we still need to check the storage as we
        //   don't want to override an existing metadata file.
        if let Some(index_state) = per_index_metastores_wlock.get(&index_id) {
            if let IndexState::Alive(_) = index_state {
                return Err(MetastoreError::IndexAlreadyExists {
                    index_id: index_metadata.index_id.clone(),
                });
            }
        } else if index_exists(&*self.storage, &index_metadata.index_id).await? {
            return Err(MetastoreError::InternalError {
                message: format!("Index {index_id} cannot be created."),
                cause: format!(
                    "Index {index_id} is not present in the `indexes_states.json` file but its \
                     file `{index_id}/metastore.json` is on the storage."
                ),
            });
        }

        // Set state to Creating` and rollback on metastore error.
        per_index_metastores_wlock.insert(index_id.clone(), IndexState::Creating);
        if let Err(error) = put_indexes_states(&*self.storage, &per_index_metastores_wlock).await {
            per_index_metastores_wlock.remove(&index_id);
            return Err(error);
        }

        // Put index metadata on storage.
        let index = FileBackedIndex::from(index_metadata);
        put_index(&*self.storage, &index).await?;

        per_index_metastores_wlock.insert(
            index_id.clone(),
            IndexState::Alive(LazyFileBackedIndex::new(
                self.storage.clone(),
                index_id.clone(),
                self.polling_interval_opt,
                Some(index),
            )),
        );

        // Set state to `Alive` and rollback on metastore error.
        let put_res = put_indexes_states(&*self.storage, &per_index_metastores_wlock).await;
        if put_res.is_err() {
            per_index_metastores_wlock.insert(index_id.clone(), IndexState::Creating);
        }
        put_res
    }

    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()> {
        // We pick the outer lock here, so that we enter a critical section.
        let mut per_index_metastores_wlock = self.per_index_metastores.write().await;

        // If index is neither in `per_index_metastores_wlock` nor on the storage, it does not
        // exist.
        if !per_index_metastores_wlock.contains_key(index_id)
            && !index_exists(&*self.storage, index_id).await?
        {
            return Err(MetastoreError::IndexDoesNotExist {
                index_id: index_id.to_string(),
            });
        }

        // Set state to `Deleting` and keep the previous state in memory in case we need to insert
        // if an error occurs.
        let index_state_opt =
            per_index_metastores_wlock.insert(index_id.to_string(), IndexState::Deleting);
        // On a put error, reinsert the previous state if any.
        if let Err(error) = put_indexes_states(&*self.storage, &per_index_metastores_wlock).await {
            if let Some(index_state) = index_state_opt {
                per_index_metastores_wlock.insert(index_id.to_string(), index_state);
            } else {
                per_index_metastores_wlock.remove(index_id);
            }
            return Err(error);
        }

        let delete_res = delete_index(&*self.storage, index_id).await;

        match &delete_res {
            Ok(()) |
            // If the index file does not exist, we still need to return an error,
            // but it makes sense to ensure that the index state is removed.
            Err(MetastoreError::IndexDoesNotExist { .. }) => {
                per_index_metastores_wlock.remove(index_id);
                if let Err(error) = put_indexes_states(&*self.storage, &per_index_metastores_wlock).await {
                    per_index_metastores_wlock.insert(index_id.to_string(), IndexState::Deleting);
                    return Err(error);
                }
            },
            _ => {}
        }

        delete_res
    }

    /// -------------------------------------------------------------------------------
    /// Mutations over a single index

    async fn stage_split(
        &self,
        index_id: &str,
        split_metadata: SplitMetadata,
    ) -> MetastoreResult<()> {
        self.mutate(index_id, |index| {
            index.stage_split(split_metadata)?;
            Ok(true)
        })
        .await?;
        Ok(())
    }

    async fn publish_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
        checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    ) -> MetastoreResult<()> {
        self.mutate(index_id, |index| {
            index.publish_splits(split_ids, replaced_split_ids, checkpoint_delta_opt)?;
            Ok(true)
        })
        .await?;
        Ok(())
    }

    async fn mark_splits_for_deletion<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        self.mutate(index_id, |index| {
            index.mark_splits_for_deletion(
                split_ids,
                &[
                    SplitState::Staged,
                    SplitState::Published,
                    SplitState::MarkedForDeletion,
                ],
            )
        })
        .await?;
        Ok(())
    }

    async fn delete_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        self.mutate(index_id, |index| {
            index.delete_splits(split_ids)?;
            Ok(true)
        })
        .await?;
        Ok(())
    }

    async fn add_source(&self, index_id: &str, source: SourceConfig) -> MetastoreResult<()> {
        self.mutate(index_id, |index| index.add_source(source))
            .await?;
        Ok(())
    }

    async fn toggle_source(
        &self,
        index_id: &str,
        source_id: &str,
        enable: bool,
    ) -> MetastoreResult<bool> {
        self.mutate(index_id, |index| index.toggle_source(source_id, enable))
            .await
    }

    async fn delete_source(&self, index_id: &str, source_id: &str) -> MetastoreResult<()> {
        self.mutate(index_id, |index| index.delete_source(source_id))
            .await?;
        Ok(())
    }

    async fn reset_source_checkpoint(
        &self,
        index_id: &str,
        source_id: &str,
    ) -> MetastoreResult<()> {
        self.mutate(index_id, |index| index.reset_source_checkpoint(source_id))
            .await?;
        Ok(())
    }

    /// -------------------------------------------------------------------------------
    /// Read-only accessors

    async fn list_splits(
        &self,
        index_id: &str,
        state: SplitState,
        time_range_opt: Option<Range<i64>>,
        tags: Option<TagFilterAst>,
    ) -> MetastoreResult<Vec<Split>> {
        self.read(index_id, |index| {
            index.list_splits(state, time_range_opt, tags, None)
        })
        .await
    }

    async fn list_all_splits(&self, index_id: &str) -> MetastoreResult<Vec<Split>> {
        self.read(index_id, |index| index.list_all_splits()).await
    }

    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        self.read(index_id, |index| Ok(index.metadata().clone()))
            .await
    }

    async fn list_indexes_metadatas(&self) -> MetastoreResult<Vec<IndexMetadata>> {
        let per_index_metastores_rlock = self.per_index_metastores.read().await;
        try_join_all(
            per_index_metastores_rlock
                .iter()
                .filter_map(|(index_id, index_state)| match index_state {
                    IndexState::Alive(_) => Some(index_id),
                    _ => None,
                })
                .map(|index_id| self.index_metadata(index_id)),
        )
        .await
    }

    async fn list_stale_splits(
        &self,
        index_id: &str,
        delete_opstamp: u64,
        num_splits: usize,
    ) -> MetastoreResult<Vec<Split>> {
        self.read(index_id, |index| {
            let splits = index
                .list_splits(SplitState::Published, None, None, Some(delete_opstamp))?
                .into_iter()
                .sorted_by_key(|split| split.split_metadata.delete_opstamp)
                .take(num_splits)
                .collect_vec();
            Ok(splits)
        })
        .await
    }

    fn uri(&self) -> &Uri {
        self.storage.uri()
    }

    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.storage.check_connectivity().await?;
        Ok(())
    }

    /// -------------------------------------------------------------------------------
    /// Delete tasks

    async fn last_delete_opstamp(&self, index_id: &str) -> MetastoreResult<u64> {
        self.read(index_id, |index| Ok(index.last_delete_opstamp()))
            .await
    }

    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        // Ugly hack to get opstamp and create timestamp as `mutate` callback only returns a boolean
        // and not an actual struct.
        let mut opstamp: u64 = 0;
        let mut create_timestamp: i64 = 0;
        self.mutate(&delete_query.index_id, |index| {
            let delete_task = index.create_delete_task(delete_query.clone())?;
            opstamp = delete_task.opstamp;
            create_timestamp = delete_task.create_timestamp;
            Ok(true)
        })
        .await?;
        Ok(DeleteTask {
            create_timestamp,
            opstamp,
            delete_query: Some(delete_query),
        })
    }

    async fn update_splits_delete_opstamp<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        delete_opstamp: u64,
    ) -> MetastoreResult<()> {
        self.mutate(index_id, |index| {
            index.update_splits_delete_opstamp(split_ids, delete_opstamp)
        })
        .await?;
        Ok(())
    }

    async fn list_delete_tasks(
        &self,
        index_id: &str,
        opstamp_start: u64,
    ) -> MetastoreResult<Vec<DeleteTask>> {
        let delete_tasks = self
            .read(index_id, |index| Ok(index.list_delete_tasks(opstamp_start)))
            .await??;
        Ok(delete_tasks)
    }
}

async fn get_index_mutex(
    index_id: &str,
    index_state: &IndexState,
) -> MetastoreResult<Arc<Mutex<FileBackedIndex>>> {
    match index_state {
        IndexState::Alive(lazy_index) => lazy_index.get().await,
        IndexState::Creating => Err(MetastoreError::InternalError {
            message: format!("Index `{index_id}` cannot be retrieved."),
            cause: "Index `{index_id}` is in transitioning state `Creating` and this should not \
                    happened. Either recreate or delete it."
                .to_string(),
        }),
        IndexState::Deleting => Err(MetastoreError::InternalError {
            message: format!("Index `{index_id}` cannot be retrieved."),
            cause: "Index `{index_id}` is in transitioning state `Deleting` and this should not \
                    happened. Try to delete it again."
                .to_string(),
        }),
    }
}

#[cfg(test)]
#[async_trait]
impl crate::tests::test_suite::DefaultForTest for FileBackedMetastore {
    async fn default_for_test() -> Self {
        use quickwit_storage::RamStorage;
        FileBackedMetastore::try_new(Arc::new(RamStorage::default()), None)
            .await
            .unwrap()
    }
}

metastore_test_suite!(crate::FileBackedMetastore);

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::RangeInclusive;
    use std::path::Path;
    use std::sync::Arc;

    use futures::executor::block_on;
    use quickwit_proto::metastore_api::DeleteQuery;
    use quickwit_storage::{MockStorage, RamStorage, Storage, StorageErrorKind};
    use rand::Rng;
    use time::OffsetDateTime;
    use tokio::time::Duration;

    use super::lazy_file_backed_index::LazyFileBackedIndex;
    use super::store_operations::{
        fetch_and_build_indexes_states, meta_path, put_index_given_index_id, put_indexes_states,
    };
    use super::{FileBackedIndex, FileBackedMetastore, IndexState};
    use crate::tests::test_suite::DefaultForTest;
    use crate::{IndexMetadata, Metastore, MetastoreError, SplitMetadata, SplitState};

    #[tokio::test]
    async fn test_file_backed_metastore_index_exists() {
        let index_id = "test-index";
        let metastore = FileBackedMetastore::default_for_test().await;
        assert!(!metastore.index_exists(index_id).await.unwrap());

        let index_metadata = IndexMetadata::for_test(index_id, "ram:///indexes/test-index");
        metastore.create_index(index_metadata).await.unwrap();

        assert!(metastore.index_exists(index_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_backed_metastore_get_index() {
        let index_id = "test-index";
        let metastore = FileBackedMetastore::default_for_test().await;
        let index_metadata = IndexMetadata::for_test(index_id, "ram:///indexes/test-index");

        // Create index
        metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();

        // Open index and check its metadata
        let created_index = metastore.get_index(index_id).await.unwrap();
        assert_eq!(created_index.index_id(), index_metadata.index_id);
        assert_eq!(created_index.metadata().index_uri, index_metadata.index_uri);

        // Check index is returned by list indexes.
        let indexes = metastore.list_indexes_metadatas().await.unwrap();
        assert_eq!(indexes.len(), 1);

        // Open a non-existent index.
        let metastore_error = metastore
            .get_index("index-does-not-exist")
            .await
            .unwrap_err();
        assert!(matches!(
            metastore_error,
            MetastoreError::IndexDoesNotExist { .. }
        ));
    }

    #[tokio::test]
    async fn test_file_backed_metastore_storage_failing() {
        // The file-backed metastore should not update its internal state if the storage fails.
        let mut mock_storage = MockStorage::default();

        let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();

        let ram_storage = RamStorage::default();
        let ram_storage_clone = ram_storage.clone();

        mock_storage // remove this if we end up changing the semantics of create.
            .expect_exists()
            .returning(|_| Ok(false));
        mock_storage
            .expect_put()
            .times(4)
            .returning(move |path, put_payload| {
                assert!(
                    path == Path::new("indexes_states.json") || path == meta_path("test-index")
                );
                block_on(ram_storage_clone.put(path, put_payload))
            });
        mock_storage
            .expect_get_all()
            .times(1)
            .returning(move |path| block_on(ram_storage.get_all(path)));
        mock_storage.expect_put().times(1).returning(|_uri, _| {
            Err(StorageErrorKind::Io
                .with_error(anyhow::anyhow!("Oops. Some network problem maybe?")))
        });
        let metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));

        let index_id = "test-index";
        let split_id = "split-one";
        let split_metadata = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id.to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(0, 99)),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let index_metadata = IndexMetadata::for_test(index_id, "ram:///indexes/test-index");

        // create index
        metastore.create_index(index_metadata).await.unwrap();

        // stage split
        metastore
            .stage_split(index_id, split_metadata)
            .await
            .unwrap();

        // publish split fails
        let err = metastore
            .publish_splits(index_id, &[split_id], &[], None)
            .await;
        assert!(err.is_err());

        // empty
        let split = metastore
            .list_splits(index_id, SplitState::Published, None, None)
            .await
            .unwrap();
        assert!(split.is_empty());

        // not empty
        let split = metastore
            .list_splits(index_id, SplitState::Staged, None, None)
            .await
            .unwrap();
        assert!(!split.is_empty());
    }

    #[tokio::test]
    async fn test_file_backed_metastore_get_index_checks_for_inconsistent_index_id(
    ) -> crate::MetastoreResult<()> {
        let storage = Arc::new(RamStorage::default());
        let index_id = "test-index";
        let index_metadata =
            IndexMetadata::for_test("my-inconsistent-index", "ram:///indexes/test-index");

        // Put inconsistent index and states into storage.
        let index = FileBackedIndex::from(index_metadata);
        put_index_given_index_id(&*storage, &index, index_id).await?;
        let mut indexes_states = HashMap::new();
        indexes_states.insert(
            index_id.to_string(),
            IndexState::Alive(LazyFileBackedIndex::new(
                storage.clone(),
                index_id.to_string(),
                None,
                None,
            )),
        );
        put_indexes_states(&*storage, &indexes_states)
            .await
            .unwrap();

        let metastore = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();

        // Getting index with inconsistent index ID should raise an error.
        let metastore_error = metastore.get_index(index_id).await.unwrap_err();
        assert!(matches!(
            metastore_error,
            MetastoreError::InternalError { .. }
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_file_backed_metastore_wrt_directly_visible() -> crate::MetastoreResult<()> {
        let metastore = FileBackedMetastore::default_for_test().await;
        let index_id = "test-index";
        let index_metadata = IndexMetadata::for_test(index_id, "ram:///indexes/test-index");
        metastore.create_index(index_metadata).await?;

        assert!(metastore.list_all_splits(index_id).await?.is_empty());
        let split_metadata = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "split1".to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(0..=99),
            ..Default::default()
        };
        assert!(metastore.list_all_splits("test-index").await?.is_empty());
        metastore.stage_split(index_id, split_metadata).await?;
        assert_eq!(metastore.list_all_splits(index_id).await?.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_file_backed_metastore_polling() -> crate::MetastoreResult<()> {
        let storage = Arc::new(RamStorage::default());

        let metastore_wrt = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();
        let polling_interval = Duration::from_millis(20);
        let metastore_read = FileBackedMetastore::try_new(storage.clone(), Some(polling_interval))
            .await
            .unwrap();

        let index_id = "test-index";
        let index_metadata = IndexMetadata::for_test(index_id, "ram:///indexes/test-index");
        metastore_wrt.create_index(index_metadata).await?;

        assert!(metastore_wrt.list_all_splits(index_id).await?.is_empty());
        let split_metadata = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "split1".to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(0..=99),
            ..Default::default()
        };
        assert!(metastore_read
            .list_all_splits("test-index")
            .await?
            .is_empty());
        metastore_wrt.stage_split(index_id, split_metadata).await?;
        assert!(metastore_read
            .list_all_splits("test-index")
            .await?
            .is_empty());
        for _ in 0..10 {
            tokio::time::sleep(polling_interval).await;
            if !metastore_read
                .list_all_splits("test-index")
                .await?
                .is_empty()
            {
                return Ok(());
            }
        }
        panic!("The metastore should have been updated.");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_file_backed_metastore_race_condition() {
        let metastore = Arc::new(FileBackedMetastore::default_for_test().await);
        let index_id = "test-index";

        let index_metadata = IndexMetadata::for_test(index_id, "ram:///indexes/test-index");

        // Create index
        metastore.create_index(index_metadata).await.unwrap();

        // Stage splits in multiple threads
        let mut handles = Vec::new();
        let mut random_generator = rand::thread_rng();
        for i in 1..=20 {
            let sleep_duration = Duration::from_millis(random_generator.gen_range(0..=200));
            let metastore = metastore.clone();
            let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();
            let handle = tokio::spawn(async move {
                let split_metadata = SplitMetadata {
                    footer_offsets: 1000..2000,
                    split_id: format!("split-{}", i),
                    num_docs: 1,
                    uncompressed_docs_size_in_bytes: 2,
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
                    .publish_splits(index_id, &[&split_id], &[], None)
                    .await
                    .unwrap();
            });
            handles.push(handle);
        }

        futures::future::try_join_all(handles).await.unwrap();

        let splits = metastore
            .list_splits(index_id, SplitState::Published, None, None)
            .await
            .unwrap();

        // Make sure that all 20 splits are in `Published` state.
        assert_eq!(splits.len(), 20);
    }

    #[tokio::test]
    async fn test_file_backed_metastore_create_index_when_storage_failing_on_indexes_states_put() {
        let mut mock_storage = MockStorage::default();
        let ram_storage = RamStorage::default();
        let index_id = "test-index";

        mock_storage.expect_exists().returning(|_| Ok(false));
        mock_storage
            .expect_put()
            .times(1)
            .returning(move |path, _| {
                assert!(path == Path::new("indexes_states.json"));
                Err(StorageErrorKind::Io
                    .with_error(anyhow::anyhow!("Oops. Some network problem maybe?")))
            });
        mock_storage
            .expect_get_all()
            .times(1)
            .returning(move |path| block_on(ram_storage.get_all(path)));
        let metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));
        let index_metadata = IndexMetadata::for_test(index_id, "ram:///indexes/test-index");

        // Create index.
        let metastore_error = metastore.create_index(index_metadata).await.unwrap_err();
        assert!(matches!(
            metastore_error,
            MetastoreError::InternalError { .. }
        ));
        // Try fetch the not created index.
        let created_index_error = metastore.get_index(index_id).await.unwrap_err();
        assert!(matches!(
            created_index_error,
            MetastoreError::IndexDoesNotExist { .. }
        ));
    }

    #[tokio::test]
    async fn test_file_backed_metastore_create_index_when_storage_failing_before_metadata_put() {
        let mut mock_storage = MockStorage::default();
        let ram_storage = RamStorage::default();
        let ram_storage_clone = ram_storage.clone();
        let ram_storage_clone_2 = ram_storage.clone();
        let index_id = "test-index";

        mock_storage // remove this if we end up changing the semantics of create.
            .expect_exists()
            .returning(|_| Ok(false));
        mock_storage
            .expect_put()
            .times(4)
            .returning(move |path, put_payload| {
                assert!(
                    path == Path::new("indexes_states.json") || path == meta_path("test-index")
                );
                if path == Path::new("indexes_states.json") {
                    return block_on(ram_storage_clone.put(path, put_payload));
                }
                Err(StorageErrorKind::Io
                    .with_error(anyhow::anyhow!("Oops. Some network problem maybe?")))
            });
        mock_storage
            .expect_get_all()
            .times(1)
            .returning(move |path| block_on(ram_storage.get_all(path)));
        let metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));
        let index_metadata = IndexMetadata::for_test(index_id, "ram:///indexes/test-index");

        // Create index
        let metastore_error = metastore.create_index(index_metadata).await.unwrap_err();
        assert!(matches!(
            metastore_error,
            MetastoreError::InternalError { .. }
        ));
        // Let's fetch the index, we expect an internal error as the index state is in `Creating`
        // state.
        let created_index_error = metastore.get_index(index_id).await.unwrap_err();
        assert!(matches!(
            created_index_error,
            MetastoreError::InternalError { .. }
        ));
        // Check index state is in `Creating` in the states file.
        let index_states =
            fetch_and_build_indexes_states(Arc::new(ram_storage_clone_2.clone()), None)
                .await
                .unwrap();
        assert!(matches!(
            *index_states.get(index_id).unwrap(),
            IndexState::Creating
        ));
        // Let's delete the index to clean states.
        let deleted_index_error = metastore.delete_index(index_id).await.unwrap_err();
        assert!(matches!(
            deleted_index_error,
            MetastoreError::IndexDoesNotExist { .. }
        ));
        let index_states = fetch_and_build_indexes_states(Arc::new(ram_storage_clone_2), None)
            .await
            .unwrap();
        assert!(index_states.get(index_id).is_none());
        // Now we can expect an `IndexDoesNotExist` error.
        let created_index_error = metastore.get_index(index_id).await.unwrap_err();
        assert!(matches!(
            created_index_error,
            MetastoreError::IndexDoesNotExist { .. }
        ));
    }

    #[tokio::test]
    async fn test_file_backed_metastore_create_index_when_storage_failing_before_last_indexes_states_put(
    ) {
        let mut mock_storage = MockStorage::default();
        let ram_storage = RamStorage::default();
        let ram_storage_clone = ram_storage.clone();
        let index_id = "test-index";
        let mut indexes_json_valid_put = 1;
        mock_storage // remove this if we end up changing the semantics of create.
            .expect_exists()
            .returning(|_| Ok(false));
        mock_storage
            .expect_put()
            .times(3)
            .returning(move |path, put_payload| {
                assert!(
                    path == Path::new("indexes_states.json") || path == meta_path("test-index")
                );
                if path == Path::new("indexes_states.json") {
                    if indexes_json_valid_put == 0 {
                        return Err(StorageErrorKind::Io
                            .with_error(anyhow::anyhow!("Oops. Some network problem maybe?")));
                    }
                    indexes_json_valid_put -= 1;
                }
                return block_on(ram_storage_clone.put(path, put_payload));
            });
        let metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));
        let index_metadata = IndexMetadata::for_test(index_id, "ram:///indexes/test-index");

        // Create index
        let metastore_error = metastore.create_index(index_metadata).await.unwrap_err();
        assert!(matches!(
            metastore_error,
            MetastoreError::InternalError { .. }
        ));
        // Let's fetch the index, we expect an internal error as the index state is in `Creating`
        // state.
        let created_index_error = metastore.get_index(index_id).await.unwrap_err();
        assert!(matches!(
            created_index_error,
            MetastoreError::InternalError { .. }
        ));
    }

    #[tokio::test]
    async fn test_file_backed_metastore_delete_index_when_storage_failing_before_metadata_delete() {
        let mut mock_storage = MockStorage::default();
        let ram_storage = RamStorage::default();
        let ram_storage_clone = ram_storage.clone();
        let index_id = "test-index";
        let index_metadata = IndexMetadata::for_test(index_id, "ram:///indexes/test-index");
        let index = FileBackedIndex::from(index_metadata);
        put_index_given_index_id(&ram_storage, &index, index_id)
            .await
            .unwrap();

        mock_storage // remove this if we end up changing the semantics of create.
            .expect_exists()
            .returning(|_| Ok(true));
        mock_storage // remove this if we end up changing the semantics of create.
            .expect_delete()
            .returning(|_| {
                Err(StorageErrorKind::Io
                    .with_error(anyhow::anyhow!("Oops. Some network problem maybe?")))
            });
        mock_storage
            .expect_put()
            .times(1)
            .returning(move |path, put_payload| {
                return block_on(ram_storage_clone.put(path, put_payload));
            });
        let metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));

        // Delete index
        let metastore_error = metastore.delete_index(index_id).await.unwrap_err();
        assert!(matches!(
            metastore_error,
            MetastoreError::InternalError { .. }
        ));
        // Let's fetch the index, we expect an internal error as the index state is in `Deleting`
        // state.
        let created_index_error = metastore.get_index(index_id).await.unwrap_err();
        assert!(matches!(
            created_index_error,
            MetastoreError::InternalError { .. }
        ));
    }

    #[tokio::test]
    async fn test_file_backed_metastore_delete_index_storage_failing_before_last_indexes_states_put(
    ) {
        let mut mock_storage = MockStorage::default();
        let ram_storage = RamStorage::default();
        let ram_storage_clone = ram_storage.clone();
        let index_id = "test-index";
        let index_metadata = IndexMetadata::for_test(index_id, "ram:///indexes/test-index");
        let index = FileBackedIndex::from(index_metadata);
        put_index_given_index_id(&ram_storage, &index, index_id)
            .await
            .unwrap();
        let mut indexes_json_valid_put = 1;
        mock_storage // remove this if we end up changing the semantics of create.
            .expect_exists()
            .returning(|_| Ok(true));
        mock_storage // remove this if we end up changing the semantics of create.
            .expect_delete()
            .returning(|_| Ok(()));
        mock_storage
            .expect_put()
            .times(2)
            .returning(move |path, put_payload| {
                assert!(path == Path::new("indexes_states.json"));
                if path == Path::new("indexes_states.json") {
                    if indexes_json_valid_put == 0 {
                        return Err(StorageErrorKind::Io
                            .with_error(anyhow::anyhow!("Oops. Some network problem maybe?")));
                    }
                    indexes_json_valid_put -= 1;
                }
                return block_on(ram_storage_clone.put(path, put_payload));
            });
        let metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));

        // Delete index
        let metastore_error = metastore.delete_index(index_id).await.unwrap_err();
        assert!(matches!(
            metastore_error,
            MetastoreError::InternalError { .. }
        ));
        // Let's fetch the index, we expect an internal error as the index state is in `Deleting`
        // state.
        let created_index_error = metastore.get_index(index_id).await.unwrap_err();
        assert!(matches!(
            created_index_error,
            MetastoreError::InternalError { .. }
        ));
    }

    #[tokio::test]
    async fn test_file_backed_metastore_get_list_indexes() -> crate::MetastoreResult<()> {
        let index_id_creating = "test-index--creating";
        let index_id_alive = "testing-index--alive";
        let index_id_unregistered = "test-index--unregistered";
        let index_id_deleting = "test-index--deleting";

        let index_metadata_alive =
            IndexMetadata::for_test(index_id_alive, "ram:///indexes/test-index--alive");
        let index_metadata_unregistered = IndexMetadata::for_test(
            index_id_unregistered,
            "ram:///indexes/test-index--unregistered",
        );

        // Put indexes states into storage.
        let ram_storage = Arc::new(RamStorage::default());
        let mut indexes_states = HashMap::new();
        indexes_states.insert(index_id_creating.to_string(), IndexState::Creating);
        indexes_states.insert(
            index_id_alive.to_string(),
            IndexState::Alive(LazyFileBackedIndex::new(
                ram_storage.clone(),
                index_id_alive.to_string(),
                None,
                None,
            )),
        );
        indexes_states.insert(index_id_deleting.to_string(), IndexState::Deleting);
        put_indexes_states(&*ram_storage, &indexes_states)
            .await
            .unwrap();

        let index_alive = FileBackedIndex::from(index_metadata_alive);
        let index_alive_unregistered = FileBackedIndex::from(index_metadata_unregistered);

        // Put indexes metadatas.
        put_index_given_index_id(&*ram_storage, &index_alive, index_id_alive).await?;
        put_index_given_index_id(
            &*ram_storage,
            &index_alive_unregistered,
            index_id_unregistered,
        )
        .await?;

        // Fetch alive indexes metadatas.
        let metastore = FileBackedMetastore::try_new(ram_storage.clone(), None)
            .await
            .unwrap();
        let indexes_metadatas = metastore.list_indexes_metadatas().await.unwrap();
        assert_eq!(indexes_metadatas.len(), 1);

        // Fetch the index metadata not registered in indexes states json.
        metastore.get_index(index_id_unregistered).await.unwrap();

        // Now list indexes return 2 indexes metadatas as the metastore is now aware of
        // 2 alive indexes.
        let indexes_metadatas = metastore.list_indexes_metadatas().await.unwrap();
        assert_eq!(indexes_metadatas.len(), 2);

        // Let's delete indexes.
        metastore.delete_index(index_id_alive).await.unwrap();
        metastore.delete_index(index_id_unregistered).await.unwrap();
        let no_more_indexes = metastore.list_indexes_metadatas().await.unwrap();
        assert!(no_more_indexes.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_monotically_increasing_stamps_by_index() {
        let storage = RamStorage::default();
        let metastore = FileBackedMetastore::try_new(Arc::new(storage.clone()), None)
            .await
            .unwrap();
        let index_id = "test-index-increasing-stamps-by-index";
        let index_metadata = IndexMetadata::for_test(
            index_id,
            "ram:///indexes/test-index-increasing-stamps-by-index",
        );
        metastore.create_index(index_metadata).await.unwrap();
        let delete_query = DeleteQuery {
            start_timestamp: None,
            end_timestamp: None,
            index_id: index_id.to_string(),
            query: "harry potter".to_string(),
            search_fields: Vec::new(),
        };

        let delete_task_1 = metastore
            .create_delete_task(delete_query.clone())
            .await
            .unwrap();
        assert_eq!(delete_task_1.opstamp, 1);
        let delete_task_2 = metastore
            .create_delete_task(delete_query.clone())
            .await
            .unwrap();
        assert_eq!(delete_task_2.opstamp, 2);

        // Create metastore with data already in the storage.
        let new_metastore = FileBackedMetastore::try_new(Arc::new(storage), None)
            .await
            .unwrap();
        let delete_task_3 = new_metastore
            .create_delete_task(delete_query.clone())
            .await
            .unwrap();
        assert_eq!(delete_task_3.opstamp, 3);

        // Create delete tasks on new index.
        let index_id_2 = "test-index-increasing-stamps-by-index-2";
        let index_metadata = IndexMetadata::for_test(
            index_id_2,
            "ram:///indexes/test-index-increasing-stamps-by-index-2",
        );
        metastore.create_index(index_metadata).await.unwrap();
        let delete_query = DeleteQuery {
            start_timestamp: None,
            end_timestamp: None,
            index_id: index_id_2.to_string(),
            query: "harry potter".to_string(),
            search_fields: Vec::new(),
        };
        let delete_task_4 = metastore
            .create_delete_task(delete_query.clone())
            .await
            .unwrap();
        assert_eq!(delete_task_4.opstamp, 1);
    }
}
