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

//! Module for [`FileBackedMetastore`]. It is public so that the crate `quickwit-backward-compat`
//! can import [`FileBackedIndex`] and run backward-compatibility tests. You should not have to
//! import anything from here directly.

pub mod file_backed_index;
mod file_backed_metastore_factory;
mod lazy_file_backed_index;
mod store_operations;

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::try_join_all;
use itertools::{Either, Itertools};
use quickwit_common::uri::Uri;
use quickwit_config::{validate_index_id_pattern, IndexConfig, SourceConfig};
use quickwit_proto::metastore::{
    CloseShardsRequest, CloseShardsResponse, DeleteQuery, DeleteShardsRequest,
    DeleteShardsResponse, DeleteTask, EntityKind, ListShardsRequest, ListShardsResponse,
    MetastoreError, MetastoreResult, OpenShardsRequest, OpenShardsResponse,
};
use quickwit_proto::{IndexUid, PublishToken};
use quickwit_storage::Storage;
use regex::RegexSet;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};

use self::file_backed_index::FileBackedIndex;
pub use self::file_backed_metastore_factory::FileBackedMetastoreFactory;
use self::lazy_file_backed_index::LazyFileBackedIndex;
use self::store_operations::{
    check_indexes_states_exist, delete_index, fetch_index, fetch_or_init_indexes_states,
    index_exists, put_index, put_indexes_states,
};
use crate::checkpoint::IndexCheckpointDelta;
use crate::{
    IndexMetadata, ListIndexesQuery, ListSplitsQuery, Metastore, Split, SplitMetadata, SplitState,
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

pub(crate) enum MutationOccurred<T> {
    Yes(T),
    No(T),
}

impl From<bool> for MutationOccurred<()> {
    fn from(mutation_occurred: bool) -> Self {
        if mutation_occurred {
            Self::Yes(())
        } else {
            Self::No(())
        }
    }
}

/// Metastore that stores all of the metadata associated to each index
/// into as many files and stores a map of indexes
/// (index_id, index_state) in a dedicated file `indexes_states.json`.
/// An `IndexState` describes the lifecycle of an index: `Creating` and
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

impl fmt::Debug for FileBackedMetastore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileBackedMetastore")
            .field("uri", self.storage.uri())
            .field("polling_interval_opt", &self.polling_interval_opt)
            .finish()
    }
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
            fetch_or_init_indexes_states(storage.clone(), polling_interval_opt).await?;
        let per_index_metastores = Arc::new(RwLock::new(indexes_map));
        Ok(Self {
            storage,
            per_index_metastores,
            polling_interval_opt,
        })
    }

    async fn mutate<T>(
        &self,
        index_uid: IndexUid,
        mutate_fn: impl FnOnce(&mut FileBackedIndex) -> MetastoreResult<MutationOccurred<T>>,
    ) -> MetastoreResult<T> {
        let index_id = index_uid.index_id();
        let mut locked_index = self.get_locked_index(index_id).await?;
        if locked_index.index_uid() != index_uid {
            return Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: index_id.to_string(),
            }));
        }
        let mut index = locked_index.clone();
        let value = match mutate_fn(&mut index)? {
            MutationOccurred::Yes(value) => value,
            MutationOccurred::No(value) => {
                return Ok(value);
            }
        };
        locked_index.set_recently_modified();
        let put_result = put_index(&*self.storage, &index).await;
        match put_result {
            Ok(()) => {
                *locked_index = index;
                Ok(value)
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

    async fn read<T, F>(&self, index_uid: IndexUid, view: F) -> MetastoreResult<T>
    where F: FnOnce(&FileBackedIndex) -> MetastoreResult<T> {
        let index_id = index_uid.index_id();
        let locked_index = self.get_locked_index(index_id).await?;
        if locked_index.index_uid() == index_uid {
            view(&locked_index)
        } else {
            Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: index_id.to_string(),
            }))
        }
    }

    async fn read_any<T, F>(&self, index_id: &str, view: F) -> MetastoreResult<T>
    where F: FnOnce(&FileBackedIndex) -> MetastoreResult<T> {
        let locked_index = self.get_locked_index(index_id).await?;
        view(&locked_index)
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

        // We need to instantiate a `LazyFileBackedIndex` that will hold the mutex
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

    /// Helper used for testing to obtain the data associated with the given index.
    #[cfg(test)]
    async fn get_index(&self, index_uid: IndexUid) -> MetastoreResult<FileBackedIndex> {
        self.read(index_uid, |index| Ok(index.clone())).await
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
    async fn create_index(&self, index_config: IndexConfig) -> MetastoreResult<IndexUid> {
        let index_id = index_config.index_id.clone();

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
                return Err(MetastoreError::AlreadyExists(EntityKind::Index {
                    index_id: index_id.clone(),
                }));
            }
        } else if index_exists(&*self.storage, &index_id).await? {
            return Err(MetastoreError::Internal {
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
        let index_metadata = IndexMetadata::new(index_config);
        let index_uid = index_metadata.index_uid.clone();
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
        put_res?;
        Ok(index_uid)
    }

    async fn delete_index(&self, index_uid: IndexUid) -> MetastoreResult<()> {
        // We pick the outer lock here, so that we enter a critical section.
        let mut per_index_metastores_wlock = self.per_index_metastores.write().await;

        let index_id = index_uid.index_id();
        // If index is neither in `per_index_metastores_wlock` nor on the storage, it does not
        // exist.
        if !per_index_metastores_wlock.contains_key(index_id)
            && !index_exists(&*self.storage, index_id).await?
        {
            return Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: index_id.to_string(),
            }));
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
            Err(MetastoreError::NotFound(EntityKind::Index { .. })) => {
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

    async fn stage_splits(
        &self,
        index_uid: IndexUid,
        split_metadata_list: Vec<SplitMetadata>,
    ) -> MetastoreResult<()> {
        self.mutate(index_uid, |index| {
            let mut failed_split_ids = Vec::new();
            for split_metadata in split_metadata_list {
                match index.stage_split(split_metadata) {
                    Ok(()) => {}
                    Err(MetastoreError::FailedPrecondition {
                        entity: EntityKind::Split { split_id },
                        ..
                    }) => {
                        failed_split_ids.push(split_id);
                    }
                    Err(error) => return Err(error),
                };
            }
            if !failed_split_ids.is_empty() {
                let entity = EntityKind::Splits {
                    split_ids: failed_split_ids,
                };
                let message = "splits are not staged".to_string();
                Err(MetastoreError::FailedPrecondition { entity, message })
            } else {
                Ok(MutationOccurred::Yes(()))
            }
        })
        .await?;
        Ok(())
    }

    async fn publish_splits<'a>(
        &self,
        index_uid: IndexUid,
        staged_split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
        checkpoint_delta_opt: Option<IndexCheckpointDelta>,
        publish_token_opt: Option<PublishToken>,
    ) -> MetastoreResult<()> {
        self.mutate(index_uid, |index| {
            index.publish_splits(
                staged_split_ids,
                replaced_split_ids,
                checkpoint_delta_opt,
                publish_token_opt,
            )?;
            Ok(MutationOccurred::Yes(()))
        })
        .await?;
        Ok(())
    }

    async fn mark_splits_for_deletion<'a>(
        &self,
        index_uid: IndexUid,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        self.mutate(index_uid, |index| {
            index
                .mark_splits_for_deletion(
                    split_ids,
                    &[
                        SplitState::Staged,
                        SplitState::Published,
                        SplitState::MarkedForDeletion,
                    ],
                    false,
                )
                .map(MutationOccurred::from)
        })
        .await?;
        Ok(())
    }

    async fn delete_splits<'a>(
        &self,
        index_uid: IndexUid,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        self.mutate(index_uid, |index| {
            index.delete_splits(split_ids)?;
            Ok(MutationOccurred::Yes(()))
        })
        .await?;
        Ok(())
    }

    async fn add_source(&self, index_uid: IndexUid, source: SourceConfig) -> MetastoreResult<()> {
        self.mutate(index_uid, |index| {
            index.add_source(source)?;
            Ok(MutationOccurred::Yes(()))
        })
        .await?;
        Ok(())
    }

    async fn toggle_source(
        &self,
        index_uid: IndexUid,
        source_id: &str,
        enable: bool,
    ) -> MetastoreResult<()> {
        self.mutate(index_uid, |index| {
            index
                .toggle_source(source_id, enable)
                .map(MutationOccurred::from)
        })
        .await?;
        Ok(())
    }

    async fn delete_source(&self, index_uid: IndexUid, source_id: &str) -> MetastoreResult<()> {
        self.mutate(index_uid, |index| {
            index.delete_source(source_id).map(MutationOccurred::from)
        })
        .await?;
        Ok(())
    }

    async fn reset_source_checkpoint(
        &self,
        index_uid: IndexUid,
        source_id: &str,
    ) -> MetastoreResult<()> {
        self.mutate(index_uid, |index| {
            index
                .reset_source_checkpoint(source_id)
                .map(MutationOccurred::from)
        })
        .await?;
        Ok(())
    }

    /// -------------------------------------------------------------------------------
    /// Read-only accessors

    async fn list_splits(&self, query: ListSplitsQuery) -> MetastoreResult<Vec<Split>> {
        let mut all_splits = Vec::new();

        for index_uid in &query.index_uids {
            let splits = self
                .read(index_uid.clone(), |index| index.list_splits(&query))
                .await?;
            all_splits.extend(splits);
        }
        Ok(all_splits)
    }

    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        self.read_any(index_id, |index| Ok(index.metadata().clone()))
            .await
    }

    async fn list_indexes_metadatas(
        &self,
        query: ListIndexesQuery,
    ) -> MetastoreResult<Vec<IndexMetadata>> {
        // Done in two steps:
        // 1) Get index IDs and release the lock on `per_index_metastores`.
        // 2) Get each index metadata. Note that each get will take a read lock on
        // `per_index_metastores`. Lock is released in 1) to let a concurrent task/thread to
        // take a write lock on `per_index_metastores`.
        let index_matcher_result = match query {
            ListIndexesQuery::IndexIdPatterns(patterns) => build_regex_set_from_patterns(patterns),
            ListIndexesQuery::All => build_regex_set_from_patterns(vec!["*".to_string()]),
        };
        let index_matcher = index_matcher_result.map_err(|error| MetastoreError::Internal {
            message: "Failed to build RegexSet from index patterns`".to_string(),
            cause: error.to_string(),
        })?;

        let index_ids: Vec<String> = {
            let per_index_metastores_rlock = self.per_index_metastores.read().await;
            per_index_metastores_rlock
                .iter()
                .filter_map(|(index_id, index_state)| match index_state {
                    IndexState::Alive(_) => Some(index_id),
                    _ => None,
                })
                .filter(|index_id| index_matcher.is_match(index_id))
                .cloned()
                .collect()
        };
        let indexes_metadatas: Vec<IndexMetadata> =
            try_join_all(index_ids.iter().map(|index_id| async move {
                match self.index_metadata(index_id).await {
                    Ok(index_metadata) => Ok(Some(index_metadata)),
                    Err(MetastoreError::NotFound(EntityKind::Index { .. })) => Ok(None),
                    Err(MetastoreError::Internal { message, cause }) => {
                        // Indexes can be in a transition state `Creating` or `Deleting`.
                        // This is fine to ignore them.
                        if cause.contains("is in transitioning state") {
                            Ok(None)
                        } else {
                            Err(MetastoreError::Internal { message, cause })
                        }
                    }
                    Err(error) => Err(error),
                }
            }))
            .await?
            .into_iter()
            .flatten()
            .collect();
        Ok(indexes_metadatas)
    }

    fn uri(&self) -> &Uri {
        self.storage.uri()
    }

    async fn check_connectivity(&self) -> anyhow::Result<()> {
        check_indexes_states_exist(self.storage.clone()).await
    }

    // Shard API

    async fn open_shards(&self, request: OpenShardsRequest) -> MetastoreResult<OpenShardsResponse> {
        let mut subresponses = Vec::with_capacity(request.subrequests.len());

        for subrequest in request.subrequests {
            let index_uid: IndexUid = subrequest.index_uid.clone().into();
            let subresponse = self
                .mutate(index_uid, |index| index.open_shards(subrequest))
                .await?;
            subresponses.push(subresponse);
        }
        let response = OpenShardsResponse { subresponses };
        Ok(response)
    }

    async fn close_shards(
        &self,
        request: CloseShardsRequest,
    ) -> MetastoreResult<CloseShardsResponse> {
        let mut successes = Vec::with_capacity(request.subrequests.len());
        let mut failures = Vec::new();

        for subrequest in request.subrequests {
            let index_uid: IndexUid = subrequest.index_uid.clone().into();
            match self
                .mutate(index_uid, |index| index.close_shards(subrequest))
                .await
            {
                Ok(Either::Left(success)) => {
                    successes.push(success);
                }
                Ok(Either::Right(failure)) => {
                    failures.push(failure);
                }
                Err(error) => return Err(error),
            }
        }
        let response = CloseShardsResponse {
            successes,
            failures,
        };
        Ok(response)
    }

    async fn delete_shards(
        &self,
        request: DeleteShardsRequest,
    ) -> MetastoreResult<DeleteShardsResponse> {
        let mut subresponses = Vec::with_capacity(request.subrequests.len());

        for subrequest in request.subrequests {
            let index_uid: IndexUid = subrequest.index_uid.clone().into();
            let subresponse = self
                .mutate(index_uid, |index| {
                    index.delete_shards(subrequest, request.force)
                })
                .await?;
            subresponses.push(subresponse);
        }
        let response = DeleteShardsResponse {};
        Ok(response)
    }

    async fn list_shards(&self, request: ListShardsRequest) -> MetastoreResult<ListShardsResponse> {
        let mut subresponses = Vec::with_capacity(request.subrequests.len());

        for subrequest in request.subrequests {
            let index_uid: IndexUid = subrequest.index_uid.clone().into();
            let subresponse = self
                .mutate(index_uid, |index| index.list_shards(subrequest))
                .await?;
            subresponses.push(subresponse);
        }
        let response = ListShardsResponse { subresponses };
        Ok(response)
    }

    /// -------------------------------------------------------------------------------
    /// Delete tasks

    async fn last_delete_opstamp(&self, index_uid: IndexUid) -> MetastoreResult<u64> {
        self.read(index_uid, |index| Ok(index.last_delete_opstamp()))
            .await
    }

    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        let index_uid: IndexUid = delete_query.index_uid.clone().into();
        let delete_task = self
            .mutate(index_uid, |index| {
                index
                    .create_delete_task(delete_query)
                    .map(MutationOccurred::Yes)
            })
            .await?;
        Ok(delete_task)
    }

    async fn update_splits_delete_opstamp<'a>(
        &self,
        index_uid: IndexUid,
        split_ids: &[&'a str],
        delete_opstamp: u64,
    ) -> MetastoreResult<()> {
        self.mutate(index_uid, |index| {
            index
                .update_splits_delete_opstamp(split_ids, delete_opstamp)
                .map(MutationOccurred::from)
        })
        .await?;
        Ok(())
    }

    async fn list_delete_tasks(
        &self,
        index_uid: IndexUid,
        opstamp_start: u64,
    ) -> MetastoreResult<Vec<DeleteTask>> {
        let delete_tasks = self
            .read(
                index_uid,
                |index| Ok(index.list_delete_tasks(opstamp_start)),
            )
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
        IndexState::Creating => Err(MetastoreError::Internal {
            message: format!("Index `{index_id}` cannot be retrieved."),
            cause: "Index `{index_id}` is in transitioning state `Creating` and this should not \
                    happened. Either recreate or delete it."
                .to_string(),
        }),
        IndexState::Deleting => Err(MetastoreError::Internal {
            message: format!("Index `{index_id}` cannot be retrieved."),
            cause: "Index `{index_id}` is in transitioning state `Deleting` and this should not \
                    happened. Try to delete it again."
                .to_string(),
        }),
    }
}

/// Returns a [`RegexSet`] built from the following rules:
/// - If the given pattern does not contain a `*` char, it matches the exact pattern.
/// - If the given pattern contains one or more `*`, it matches the regex built from a regex where
///   `*` is replaced by `.*`. All other regular expression meta characters are escaped.
fn build_regex_set_from_patterns(patterns: Vec<String>) -> anyhow::Result<RegexSet> {
    // If there is a match all pattern, no need to go further.
    if patterns.iter().any(|pattern| pattern == "*") {
        return Ok(RegexSet::new([".*".to_string()]).expect("Regex compilation shouldn't fail"));
    }
    let regexes: Vec<String> = patterns
        .iter()
        .map(|index_pattern| build_regex_exprs_from_pattern(index_pattern))
        .try_collect()?;
    let regex_set = RegexSet::new(regexes)?;
    Ok(regex_set)
}

/// Converts the tokens into a valid regex.
fn build_regex_exprs_from_pattern(index_pattern: &str) -> anyhow::Result<String> {
    // Note: consecutive '*' are not allowed in the pattern.
    validate_index_id_pattern(index_pattern)?;
    let mut re: String = String::new();
    re.push('^');
    re.push_str(&index_pattern.split('*').map(regex::escape).join(".*"));
    re.push('$');
    Ok(re)
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
    use quickwit_config::IndexConfig;
    use quickwit_proto::metastore::{DeleteQuery, MetastoreError};
    use quickwit_query::query_ast::qast_json_helper;
    use quickwit_storage::{MockStorage, RamStorage, Storage, StorageErrorKind};
    use rand::Rng;
    use time::OffsetDateTime;
    use tokio::time::Duration;

    use super::lazy_file_backed_index::LazyFileBackedIndex;
    use super::store_operations::{
        fetch_or_init_indexes_states, meta_path, put_index_given_index_id, put_indexes_states,
    };
    use super::*;
    use crate::tests::test_suite::DefaultForTest;
    use crate::{IndexMetadata, ListSplitsQuery, Metastore, SplitMetadata, SplitState};

    #[tokio::test]
    async fn test_file_backed_metastore_connectivity_fails_if_states_file_does_not_exist() {
        let mut mock_storage = MockStorage::default();
        let ram_storage = RamStorage::default();
        let ram_storage_clone = ram_storage.clone();
        mock_storage // remove this if we end up changing the semantics of create.
            .expect_exists()
            .times(2)
            .returning(|_| Ok(false));
        mock_storage
            .expect_put()
            .times(1)
            .returning(move |path, put_payload| {
                assert!(path == Path::new("indexes_states.json"));
                block_on(ram_storage_clone.put(path, put_payload))
            });
        let metastore = FileBackedMetastore::try_new(Arc::new(mock_storage), None)
            .await
            .unwrap();

        metastore.check_connectivity().await.unwrap();
    }

    #[tokio::test]
    async fn test_file_backed_metastore_index_exists() {
        let index_id = "test-index";
        let metastore = FileBackedMetastore::default_for_test().await;
        assert!(!metastore.index_exists(index_id).await.unwrap());

        let index_config = IndexConfig::for_test(index_id, "ram:///indexes/test-index");
        let _index_uid = metastore.create_index(index_config).await.unwrap();

        assert!(metastore.index_exists(index_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_backed_metastore_get_index() {
        let index_id = "test-index";
        let metastore = FileBackedMetastore::default_for_test().await;
        let index_config = IndexConfig::for_test(index_id, "ram:///indexes/test-index");

        // Create index
        let index_uid = metastore.create_index(index_config.clone()).await.unwrap();

        // Open index and check its metadata
        let created_index = metastore.get_index(index_uid).await.unwrap();
        assert_eq!(created_index.index_id(), index_config.index_id);
        assert_eq!(
            created_index.metadata().index_uri(),
            &index_config.index_uri
        );

        // Check index is returned by list indexes.
        let indexes = metastore
            .list_indexes_metadatas(ListIndexesQuery::All)
            .await
            .unwrap();
        assert_eq!(indexes.len(), 1);

        // Open a non-existent index.
        let metastore_error = metastore
            .get_index(IndexUid::new("index-does-not-exist"))
            .await
            .unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::NotFound { .. }));

        // Open a index with a different incarnation_id.
        let metastore_error = metastore
            .get_index(IndexUid::new(index_id))
            .await
            .unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::NotFound { .. }));
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

        let index_uid = IndexUid::new("test-index");
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

        let index_config = IndexConfig::for_test(index_uid.index_id(), "ram:///indexes/test-index");

        // create index
        let index_uid = metastore.create_index(index_config).await.unwrap();

        // stage split
        metastore
            .stage_splits(index_uid.clone(), vec![split_metadata])
            .await
            .unwrap();

        // publish split fails
        let err = metastore
            .publish_splits(index_uid.clone(), &[split_id], &[], None, None)
            .await;
        assert!(err.is_err());

        // empty
        let query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Published);
        let split = metastore.list_splits(query).await.unwrap();
        assert!(split.is_empty());

        // not empty
        let query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Staged);
        let split = metastore.list_splits(query).await.unwrap();
        assert!(!split.is_empty());
    }

    #[tokio::test]
    async fn test_file_backed_metastore_get_index_checks_for_inconsistent_index_id(
    ) -> MetastoreResult<()> {
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
        let metastore_error = metastore
            .get_index(IndexUid::new(index_id))
            .await
            .unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::Internal { .. }));

        Ok(())
    }

    #[tokio::test]
    async fn test_file_backed_metastore_wrt_directly_visible() -> MetastoreResult<()> {
        let metastore = FileBackedMetastore::default_for_test().await;
        let index_config = IndexConfig::for_test("test-index", "ram:///indexes/test-index");
        let index_uid = metastore.create_index(index_config).await?;

        assert!(metastore
            .list_all_splits(index_uid.clone())
            .await?
            .is_empty());
        let split_metadata = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "split1".to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(0..=99),
            ..Default::default()
        };
        assert!(metastore
            .list_all_splits(index_uid.clone())
            .await?
            .is_empty());
        metastore
            .stage_splits(index_uid.clone(), vec![split_metadata])
            .await?;
        assert_eq!(metastore.list_all_splits(index_uid.clone()).await?.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_file_backed_metastore_polling() -> quickwit_proto::metastore::MetastoreResult<()>
    {
        let storage = Arc::new(RamStorage::default());

        let metastore_wrt = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();
        let polling_interval = Duration::from_millis(20);
        let metastore_read = FileBackedMetastore::try_new(storage.clone(), Some(polling_interval))
            .await
            .unwrap();

        let index_id = "test-index";
        let index_config = IndexConfig::for_test(index_id, "ram:///indexes/test-index");
        let index_uid = metastore_wrt.create_index(index_config).await?;

        assert!(metastore_wrt
            .list_all_splits(index_uid.clone())
            .await?
            .is_empty());
        let split_metadata = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "split1".to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(0..=99),
            ..Default::default()
        };
        assert!(metastore_read
            .list_all_splits(index_uid.clone())
            .await?
            .is_empty());
        metastore_wrt
            .stage_splits(index_uid.clone(), vec![split_metadata])
            .await?;
        assert!(metastore_read
            .list_all_splits(index_uid.clone())
            .await?
            .is_empty());
        for _ in 0..10 {
            tokio::time::sleep(polling_interval).await;
            if !metastore_read
                .list_all_splits(index_uid.clone())
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
        let index_uid = IndexUid::new("test-index");

        let index_config = IndexConfig::for_test(index_uid.index_id(), "ram:///indexes/test-index");

        // Create index
        let index_uid = metastore.create_index(index_config).await.unwrap();

        // Stage splits in multiple threads
        let mut handles = Vec::new();
        let mut random_generator = rand::thread_rng();
        for i in 1..=20 {
            let sleep_duration = Duration::from_millis(random_generator.gen_range(0..=200));
            let metastore = metastore.clone();
            let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();
            let handle = tokio::spawn({
                let index_uid = index_uid.clone();
                async move {
                    let split_metadata = SplitMetadata {
                        footer_offsets: 1000..2000,
                        split_id: format!("split-{i}"),
                        num_docs: 1,
                        uncompressed_docs_size_in_bytes: 2,
                        time_range: Some(RangeInclusive::new(0, 99)),
                        create_timestamp: current_timestamp,
                        ..Default::default()
                    };
                    // stage split
                    metastore
                        .stage_splits(index_uid.clone(), vec![split_metadata])
                        .await
                        .unwrap();

                    tokio::time::sleep(sleep_duration).await;

                    // publish split
                    let split_id = format!("split-{i}");
                    metastore
                        .publish_splits(index_uid.clone(), &[&split_id], &[], None, None)
                        .await
                        .unwrap();
                }
            });
            handles.push(handle);
        }

        futures::future::try_join_all(handles).await.unwrap();

        let query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Published);
        let splits = metastore.list_splits(query).await.unwrap();

        // Make sure that all 20 splits are in `Published` state.
        assert_eq!(splits.len(), 20);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_file_backed_metastore_list_indexes_metadata_race_condition() {
        let metastore = Arc::new(FileBackedMetastore::default_for_test().await);
        let mut index_uids = Vec::new();
        for idx in 0..10 {
            let index_uid = IndexUid::new(format!("test-index-{idx}"));
            let index_config =
                IndexConfig::for_test(index_uid.index_id(), "ram:///indexes/test-index");
            let index_uid = metastore.create_index(index_config).await.unwrap();
            index_uids.push(index_uid);
        }
        // Delete indexes + call to list_indexes_metadata.
        let mut handles = Vec::new();
        for index_uid in index_uids {
            {
                let metastore = metastore.clone();
                let handle = tokio::spawn(async move {
                    metastore
                        .list_indexes_metadatas(ListIndexesQuery::All)
                        .await
                        .unwrap();
                });
                handles.push(handle);
            }
            {
                let metastore = metastore.clone();
                let handle = tokio::spawn(async move {
                    metastore.delete_index(index_uid).await.unwrap();
                });
                handles.push(handle);
            }
        }
        tokio::time::timeout(
            Duration::from_secs(2),
            futures::future::try_join_all(handles),
        )
        .await
        .unwrap()
        .unwrap();
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
        let index_config = IndexConfig::for_test(index_id, "ram:///indexes/test-index");

        // Create index.
        let metastore_error = metastore.create_index(index_config).await.unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::Internal { .. }));
        // Try fetch the not created index.
        let created_index_error = metastore
            .get_index(IndexUid::new(index_id))
            .await
            .unwrap_err();
        assert!(matches!(
            created_index_error,
            MetastoreError::NotFound { .. }
        ));
    }

    #[tokio::test]
    async fn test_file_backed_metastore_create_index_when_storage_failing_before_metadata_put() {
        let mut mock_storage = MockStorage::default();
        let ram_storage = RamStorage::default();
        let ram_storage_clone = ram_storage.clone();
        let ram_storage_clone_2 = ram_storage.clone();
        let index_id = "test-index";
        let index_uid = IndexUid::new(index_id);

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
        let index_config = IndexConfig::for_test(index_id, "ram:///indexes/test-index");

        // Create index
        let metastore_error = metastore.create_index(index_config).await.unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::Internal { .. }));
        // Let's fetch the index, we expect an internal error as the index state is in `Creating`
        // state.
        let created_index_error = metastore.get_index(index_uid.clone()).await.unwrap_err();
        assert!(matches!(
            created_index_error,
            MetastoreError::Internal { .. }
        ));
        // Check index state is in `Creating` in the states file.
        let index_states =
            fetch_or_init_indexes_states(Arc::new(ram_storage_clone_2.clone()), None)
                .await
                .unwrap();
        assert!(matches!(
            *index_states.get(index_id).unwrap(),
            IndexState::Creating
        ));
        // Let's delete the index to clean states.
        let deleted_index_error = metastore.delete_index(index_uid.clone()).await.unwrap_err();
        assert!(matches!(
            deleted_index_error,
            MetastoreError::NotFound { .. }
        ));
        let index_states = fetch_or_init_indexes_states(Arc::new(ram_storage_clone_2), None)
            .await
            .unwrap();
        assert!(index_states.get(index_id).is_none());
        // Now we can expect an `IndexDoesNotExist` error.
        let created_index_error = metastore.get_index(index_uid).await.unwrap_err();
        assert!(matches!(
            created_index_error,
            MetastoreError::NotFound { .. }
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
        let index_config = IndexConfig::for_test(index_id, "ram:///indexes/test-index");

        // Create index
        let metastore_error = metastore.create_index(index_config).await.unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::Internal { .. }));
        // Let's fetch the index, we expect an internal error as the index state is in `Creating`
        // state.
        let created_index_error = metastore
            .get_index(IndexUid::new(index_id))
            .await
            .unwrap_err();
        assert!(matches!(
            created_index_error,
            MetastoreError::Internal { .. }
        ));
    }

    #[tokio::test]
    async fn test_file_backed_metastore_delete_index_when_storage_failing_before_metadata_delete() {
        let mut mock_storage = MockStorage::default();
        let ram_storage = RamStorage::default();
        let ram_storage_clone = ram_storage.clone();
        let index_id = "test-index";
        let index_uid = IndexUid::new(index_id);
        let index_metadata =
            IndexMetadata::for_test(index_uid.index_id(), "ram:///indexes/test-index");
        let index = FileBackedIndex::from(index_metadata);
        put_index_given_index_id(&ram_storage, &index, index_uid.index_id())
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
        let metastore_error = metastore.delete_index(index_uid.clone()).await.unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::Internal { .. }));
        // Let's fetch the index, we expect an internal error as the index state is in `Deleting`
        // state.
        let created_index_error = metastore.get_index(index_uid).await.unwrap_err();
        assert!(matches!(
            created_index_error,
            MetastoreError::Internal { .. }
        ));
    }

    #[tokio::test]
    async fn test_file_backed_metastore_delete_index_storage_failing_before_last_indexes_states_put(
    ) {
        let mut mock_storage = MockStorage::default();
        let ram_storage = RamStorage::default();
        let ram_storage_clone = ram_storage.clone();
        let index_id = "test-index";
        let index_uid = IndexUid::new(index_id);
        let index_metadata =
            IndexMetadata::for_test(index_uid.index_id(), "ram:///indexes/test-index");
        let index = FileBackedIndex::from(index_metadata);
        put_index_given_index_id(&ram_storage, &index, index_uid.index_id())
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
        let metastore_error = metastore.delete_index(index_uid.clone()).await.unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::Internal { .. }));
        // Let's fetch the index, we expect an internal error as the index state is in `Deleting`
        // state.
        let created_index_error = metastore.get_index(index_uid).await.unwrap_err();
        assert!(matches!(
            created_index_error,
            MetastoreError::Internal { .. }
        ));
    }

    #[tokio::test]
    async fn test_file_backed_metastore_get_list_indexes() -> MetastoreResult<()> {
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
        let index_uid_alive = index_alive.index_uid();
        let index_uid_unregistered = index_alive_unregistered.index_uid();

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
        let indexes_metadatas = metastore
            .list_indexes_metadatas(ListIndexesQuery::All)
            .await
            .unwrap();
        assert_eq!(indexes_metadatas.len(), 1);

        // Fetch the index metadata not registered in indexes states json.
        metastore
            .get_index(index_uid_unregistered.clone())
            .await
            .unwrap();

        // Now list indexes return 2 indexes metadatas as the metastore is now aware of
        // 2 alive indexes.
        let indexes_metadatas = metastore
            .list_indexes_metadatas(ListIndexesQuery::All)
            .await
            .unwrap();
        assert_eq!(indexes_metadatas.len(), 2);

        // Let's delete indexes.
        metastore.delete_index(index_uid_alive).await.unwrap();
        metastore
            .delete_index(index_uid_unregistered)
            .await
            .unwrap();
        let no_more_indexes = metastore
            .list_indexes_metadatas(ListIndexesQuery::All)
            .await
            .unwrap();
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
        let index_config = IndexConfig::for_test(
            index_id,
            "ram:///indexes/test-index-increasing-stamps-by-index",
        );
        let index_uid = metastore.create_index(index_config).await.unwrap();
        let delete_query = DeleteQuery {
            start_timestamp: None,
            end_timestamp: None,
            index_uid: index_uid.to_string(),
            query_ast: qast_json_helper("harry potter", &["body"]),
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
        let index_config = IndexConfig::for_test(
            index_id_2,
            "ram:///indexes/test-index-increasing-stamps-by-index-2",
        );
        let index_uid = metastore.create_index(index_config).await.unwrap();
        let delete_query = DeleteQuery {
            start_timestamp: None,
            end_timestamp: None,
            index_uid: index_uid.to_string(),
            query_ast: qast_json_helper("harry potter", &["body"]),
        };
        let delete_task_4 = metastore
            .create_delete_task(delete_query.clone())
            .await
            .unwrap();
        assert_eq!(delete_task_4.opstamp, 1);
    }
    #[test]
    fn test_build_regexes_from_pattern() {
        assert_eq!(build_regex_exprs_from_pattern("*").unwrap(), r"^.*$",);
        assert_eq!(
            build_regex_exprs_from_pattern("index-1").unwrap(),
            r"^index\-1$",
        );
        assert_eq!(
            build_regex_exprs_from_pattern("*-index-*-1").unwrap(),
            r"^.*\-index\-.*\-1$",
        );
        assert_eq!(
            build_regex_exprs_from_pattern("INDEX.2*-1").unwrap(),
            r"^INDEX\.2.*\-1$",
        );
        // Tests with invalid pattern.
        assert_eq!(
            &build_regex_exprs_from_pattern("index-**-1")
                .unwrap_err()
                .to_string(),
            "Index ID pattern `index-**-1` is invalid. Patterns must not contain multiple \
             consecutive `*`.",
        );
        assert!(build_regex_exprs_from_pattern("-index-1").is_err());
    }

    #[test]
    fn test_index_ids_patterns_matcher() {
        {
            let matcher = build_regex_set_from_patterns(vec![
                "index-1".to_string(),
                "index-2".to_string(),
                "*-index-pattern-1-*".to_string(),
                "*.index.pattern.*.2-*".to_string(),
            ])
            .unwrap();

            assert!(matcher.is_match("index-1"));
            assert!(matcher.is_match("index-2"));
            assert!(matcher.is_match("abc-index-pattern-1-1"));
            assert!(matcher.is_match("def-index-pattern-1-2"));
            assert!(matcher.is_match("ghi.index.pattern.1.2-1"));
            assert!(matcher.is_match("jkl.index.pattern.1.2-bignumber"));
            assert!(!matcher.is_match("index-3"));
            assert!(!matcher.is_match("index.pattern.1.2-1"));
        }
        {
            let matcher =
                build_regex_set_from_patterns(vec!["index-1".to_string(), "*".to_string()])
                    .unwrap();

            assert!(matcher.is_match("index-1"));
            assert!(matcher.is_match("index-2"));
            assert!(matcher.is_match("abc-index-pattern-1-1"));
        }
    }
}
