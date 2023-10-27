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

use core::fmt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::try_join_all;
use itertools::Itertools;
use quickwit_config::validate_index_id_pattern;
use quickwit_proto::metastore::{
    AcquireShardsRequest, AcquireShardsResponse, AcquireShardsSubrequest, AddSourceRequest,
    CreateIndexRequest, CreateIndexResponse, DeleteIndexRequest, DeleteQuery, DeleteShardsRequest,
    DeleteShardsResponse, DeleteShardsSubrequest, DeleteSourceRequest, DeleteSplitsRequest,
    DeleteTask, EmptyResponse, EntityKind, IndexMetadataRequest, IndexMetadataResponse,
    LastDeleteOpstampRequest, LastDeleteOpstampResponse, ListDeleteTasksRequest,
    ListDeleteTasksResponse, ListIndexesMetadataRequest, ListIndexesMetadataResponse,
    ListShardsRequest, ListShardsResponse, ListSplitsRequest, ListSplitsResponse,
    ListStaleSplitsRequest, MarkSplitsForDeletionRequest, MetastoreError, MetastoreResult,
    MetastoreService, OpenShardsRequest, OpenShardsResponse, OpenShardsSubrequest,
    PublishSplitsRequest, ResetSourceCheckpointRequest, StageSplitsRequest, ToggleSourceRequest,
    UpdateSplitsDeleteOpstampRequest, UpdateSplitsDeleteOpstampResponse,
};
use quickwit_proto::types::IndexUid;
use quickwit_storage::Storage;
use regex::RegexSet;
use time::OffsetDateTime;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};

use self::file_backed_index::FileBackedIndex;
pub use self::file_backed_metastore_factory::FileBackedMetastoreFactory;
use self::lazy_file_backed_index::LazyFileBackedIndex;
use self::store_operations::{
    check_indexes_states_exist, delete_index, fetch_index, fetch_or_init_indexes_states,
    index_exists, put_index, put_indexes_states,
};
use super::{
    AddSourceRequestExt, CreateIndexRequestExt, IndexMetadataResponseExt,
    ListIndexesMetadataResponseExt, ListSplitsRequestExt, ListSplitsResponseExt,
    PublishSplitsRequestExt, StageSplitsRequestExt,
};
use crate::checkpoint::IndexCheckpointDelta;
use crate::{
    IndexMetadata, ListIndexesMetadataRequestExt, ListIndexesQuery, ListSplitsQuery,
    MetastoreServiceExt, SplitState,
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

#[derive(Debug)]
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
#[derive(Clone)]
pub struct FileBackedMetastore {
    storage: Arc<dyn Storage>,
    per_index_metastores: Arc<RwLock<HashMap<String, IndexState>>>,
    polling_interval_opt: Option<Duration>,
}

impl fmt::Debug for FileBackedMetastore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileBackedMetastore")
            .field("storage_uri", &self.storage.uri())
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
        if *locked_index.index_uid() != index_uid {
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
        if *locked_index.index_uid() == index_uid {
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
}

#[async_trait]
impl MetastoreService for FileBackedMetastore {
    async fn check_connectivity(&mut self) -> anyhow::Result<()> {
        check_indexes_states_exist(self.storage.clone()).await
    }

    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
        vec![self.storage.uri().clone()]
    }

    /// -------------------------------------------------------------------------------
    /// Mutations over the high-level index.
    async fn create_index(
        &mut self,
        request: CreateIndexRequest,
    ) -> MetastoreResult<CreateIndexResponse> {
        let index_config = request.deserialize_index_config()?;
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
                message: format!("index {index_id} cannot be created"),
                cause: format!(
                    "index {index_id} is not present in the `indexes_states.json` file but its \
                     file `{index_id}/metastore.json` is on the storage"
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
        let response = CreateIndexResponse {
            index_uid: index_uid.into(),
        };
        Ok(response)
    }

    async fn delete_index(
        &mut self,
        request: DeleteIndexRequest,
    ) -> MetastoreResult<EmptyResponse> {
        // We pick the outer lock here, so that we enter a critical section.
        let mut per_index_metastores_wlock = self.per_index_metastores.write().await;

        let index_uid: IndexUid = request.index_uid.into();
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
        delete_res.map(|_| EmptyResponse {})
    }

    /// -------------------------------------------------------------------------------
    /// Mutations over a single index

    async fn stage_splits(
        &mut self,
        request: StageSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let splits_metadata = request.deserialize_splits_metadata()?;
        let index_uid: IndexUid = request.index_uid.into();

        self.mutate(index_uid, |index| {
            let mut failed_split_ids = Vec::new();

            for split_metadata in splits_metadata {
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
        Ok(EmptyResponse {})
    }

    async fn publish_splits(
        &mut self,
        request: PublishSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_checkpoint_delta: Option<IndexCheckpointDelta> =
            request.deserialize_index_checkpoint()?;
        self.mutate(request.index_uid.into(), |index| {
            index.publish_splits(
                request.staged_split_ids,
                request.replaced_split_ids,
                index_checkpoint_delta,
                request.publish_token_opt,
            )?;
            Ok(MutationOccurred::Yes(()))
        })
        .await?;
        Ok(EmptyResponse {})
    }

    async fn mark_splits_for_deletion(
        &mut self,
        request: MarkSplitsForDeletionRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid.into();

        self.mutate(index_uid, |index| {
            index
                .mark_splits_for_deletion(
                    request.split_ids,
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
        Ok(EmptyResponse {})
    }

    async fn delete_splits(
        &mut self,
        request: DeleteSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid.into();

        self.mutate(index_uid, |index| {
            index.delete_splits(request.split_ids)?;
            Ok(MutationOccurred::Yes(EmptyResponse {}))
        })
        .await?;
        Ok(EmptyResponse {})
    }

    async fn add_source(&mut self, request: AddSourceRequest) -> MetastoreResult<EmptyResponse> {
        let source_config = request.deserialize_source_config()?;
        let index_uid = request.index_uid.into();

        self.mutate(index_uid, |index| {
            index.add_source(source_config)?;
            Ok(MutationOccurred::Yes(()))
        })
        .await?;
        Ok(EmptyResponse {})
    }

    async fn toggle_source(
        &mut self,
        request: ToggleSourceRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid.into();

        self.mutate(index_uid, |index| {
            index
                .toggle_source(&request.source_id, request.enable)
                .map(MutationOccurred::from)
        })
        .await?;
        Ok(EmptyResponse {})
    }

    async fn delete_source(
        &mut self,
        request: DeleteSourceRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid.into();

        self.mutate(index_uid, |index| {
            index
                .delete_source(&request.source_id)
                .map(MutationOccurred::from)
        })
        .await?;
        Ok(EmptyResponse {})
    }

    async fn reset_source_checkpoint(
        &mut self,
        request: ResetSourceCheckpointRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid.into();

        self.mutate(index_uid, |index| {
            index
                .reset_source_checkpoint(&request.source_id)
                .map(MutationOccurred::from)
        })
        .await?;
        Ok(EmptyResponse {})
    }

    /// -------------------------------------------------------------------------------
    /// Read-only accessors

    async fn list_splits(
        &mut self,
        request: ListSplitsRequest,
    ) -> MetastoreResult<ListSplitsResponse> {
        let list_splits_query = request.deserialize_list_splits_query()?;
        let mut all_splits = Vec::new();

        for index_uid in &list_splits_query.index_uids {
            let splits = self
                .read(index_uid.clone(), |index| {
                    index.list_splits(&list_splits_query)
                })
                .await?;
            all_splits.extend(splits);
        }
        let response = ListSplitsResponse::try_from_splits(all_splits)?;
        Ok(response)
    }

    async fn list_stale_splits(
        &mut self,
        request: ListStaleSplitsRequest,
    ) -> MetastoreResult<ListSplitsResponse> {
        let list_splits_query = ListSplitsQuery::for_index(request.index_uid.into())
            .with_delete_opstamp_lt(request.delete_opstamp)
            .with_split_state(SplitState::Published)
            .retain_mature(OffsetDateTime::now_utc())
            .sort_by_staleness()
            .with_limit(request.num_splits as usize);
        let list_splits_request = ListSplitsRequest::try_from_list_splits_query(list_splits_query)?;
        self.list_splits(list_splits_request).await
    }

    async fn index_metadata(
        &mut self,
        request: IndexMetadataRequest,
    ) -> MetastoreResult<IndexMetadataResponse> {
        let index_id = request.get_index_id()?;
        let index_metadata = self
            .read_any(&index_id, |index| Ok(index.metadata().clone()))
            .await?;
        if let Some(index_uid) = &request.index_uid {
            if index_metadata.index_uid != *index_uid {
                return Err(MetastoreError::NotFound(EntityKind::Index {
                    index_id: index_id.to_string(),
                }));
            }
        }
        let response = IndexMetadataResponse::try_from_index_metadata(index_metadata)?;
        Ok(response)
    }

    async fn list_indexes_metadata(
        &mut self,
        request: ListIndexesMetadataRequest,
    ) -> MetastoreResult<ListIndexesMetadataResponse> {
        // Done in two steps:
        // 1) Get index IDs and release the lock on `per_index_metastores`.
        // 2) Get each index metadata. Note that each get will take a read lock on
        // `per_index_metastores`. Lock is released in 1) to let a concurrent task/thread to
        // take a write lock on `per_index_metastores`.
        let query = request.deserialize_list_indexes_query()?;
        let index_matcher_result = match query {
            ListIndexesQuery::IndexIdPatterns(patterns) => build_regex_set_from_patterns(patterns),
            ListIndexesQuery::All => build_regex_set_from_patterns(vec!["*".to_string()]),
        };
        let index_matcher = index_matcher_result.map_err(|error| MetastoreError::Internal {
            message: "failed to build RegexSet from index patterns`".to_string(),
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
        let metastore = self.clone();
        let indexes_metadata: Vec<IndexMetadata> = try_join_all(
            index_ids
                .into_iter()
                .map(|index_id| get_index_metadata(metastore.clone(), index_id)),
        )
        .await?
        .into_iter()
        .flatten()
        .collect();
        let response = ListIndexesMetadataResponse::try_from_indexes_metadata(indexes_metadata)?;
        Ok(response)
    }

    // Shard API

    async fn open_shards(
        &mut self,
        request: OpenShardsRequest,
    ) -> MetastoreResult<OpenShardsResponse> {
        let mut response = OpenShardsResponse {
            subresponses: Vec::with_capacity(request.subrequests.len()),
        };
        // We must group the subrequests by `index_uid` to mutate each index only once, since each
        // mutation triggers an IO.
        let grouped_subrequests: HashMap<IndexUid, Vec<OpenShardsSubrequest>> = request
            .subrequests
            .into_iter()
            .into_group_map_by(|subrequest| IndexUid::from(subrequest.index_uid.clone()));

        for (index_uid, subrequests) in grouped_subrequests {
            let subresponses = self
                .mutate(index_uid, |index| index.open_shards(subrequests))
                .await?;
            response.subresponses.extend(subresponses);
        }
        Ok(response)
    }

    async fn acquire_shards(
        &mut self,
        request: AcquireShardsRequest,
    ) -> MetastoreResult<AcquireShardsResponse> {
        let mut response = AcquireShardsResponse {
            subresponses: Vec::with_capacity(request.subrequests.len()),
        };
        // We must group the subrequests by `index_uid` to mutate each index only once, since each
        // mutation triggers an IO.
        let grouped_subrequests: HashMap<IndexUid, Vec<AcquireShardsSubrequest>> = request
            .subrequests
            .into_iter()
            .into_group_map_by(|subrequest| IndexUid::from(subrequest.index_uid.clone()));

        for (index_uid, subrequests) in grouped_subrequests {
            let subresponses = self
                .mutate(index_uid, |index| index.acquire_shards(subrequests))
                .await?;
            response.subresponses.extend(subresponses);
        }
        Ok(response)
    }

    async fn delete_shards(
        &mut self,
        request: DeleteShardsRequest,
    ) -> MetastoreResult<DeleteShardsResponse> {
        let mut subresponses = Vec::with_capacity(request.subrequests.len());

        // We must group the subrequests by `index_uid` to mutate each index only once, since each
        // mutation triggers an IO.
        let grouped_subrequests: HashMap<IndexUid, Vec<DeleteShardsSubrequest>> = request
            .subrequests
            .into_iter()
            .into_group_map_by(|subrequest| IndexUid::from(subrequest.index_uid.clone()));

        for (index_uid, subrequests) in grouped_subrequests {
            let subresponse = self
                .mutate(index_uid, |index| {
                    index.delete_shards(subrequests, request.force)
                })
                .await?;
            subresponses.push(subresponse);
        }
        let response = DeleteShardsResponse {};
        Ok(response)
    }

    async fn list_shards(
        &mut self,
        request: ListShardsRequest,
    ) -> MetastoreResult<ListShardsResponse> {
        let mut subresponses = Vec::with_capacity(request.subrequests.len());

        for subrequest in request.subrequests {
            let index_uid: IndexUid = subrequest.index_uid.clone().into();
            let subresponse = self
                .read(index_uid, |index| index.list_shards(subrequest))
                .await?;
            subresponses.push(subresponse);
        }
        let response = ListShardsResponse { subresponses };
        Ok(response)
    }

    /// -------------------------------------------------------------------------------
    /// Delete tasks

    async fn last_delete_opstamp(
        &mut self,
        request: LastDeleteOpstampRequest,
    ) -> MetastoreResult<LastDeleteOpstampResponse> {
        let last_delete_opstamp = self
            .read(request.index_uid.into(), |index| {
                Ok(index.last_delete_opstamp())
            })
            .await?;
        Ok(LastDeleteOpstampResponse::new(last_delete_opstamp))
    }

    async fn create_delete_task(
        &mut self,
        delete_query: DeleteQuery,
    ) -> MetastoreResult<DeleteTask> {
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

    async fn update_splits_delete_opstamp(
        &mut self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> MetastoreResult<UpdateSplitsDeleteOpstampResponse> {
        let index_uid: IndexUid = request.index_uid.into();

        self.mutate(index_uid, |index| {
            let split_ids_str = request
                .split_ids
                .iter()
                .map(|split_id| split_id.as_str())
                .collect::<Vec<_>>();
            index
                .update_splits_delete_opstamp(&split_ids_str, request.delete_opstamp)
                .map(MutationOccurred::from)
        })
        .await?;
        Ok(UpdateSplitsDeleteOpstampResponse {})
    }

    async fn list_delete_tasks(
        &mut self,
        request: ListDeleteTasksRequest,
    ) -> MetastoreResult<ListDeleteTasksResponse> {
        let index_uid: IndexUid = request.index_uid.into();

        let delete_tasks = self
            .read(index_uid, |index| {
                Ok(index.list_delete_tasks(request.opstamp_start))
            })
            .await??;
        let response = ListDeleteTasksResponse { delete_tasks };
        Ok(response)
    }
}

impl MetastoreServiceExt for FileBackedMetastore {}

async fn get_index_mutex(
    index_id: &str,
    index_state: &IndexState,
) -> MetastoreResult<Arc<Mutex<FileBackedIndex>>> {
    match index_state {
        IndexState::Alive(lazy_index) => lazy_index.get().await,
        IndexState::Creating => Err(MetastoreError::Internal {
            message: format!("index `{index_id}` cannot be retrieved"),
            cause: "index `{index_id}` is in transitioning state `creating` and this should not \
                    happened. either recreate or delete it"
                .to_string(),
        }),
        IndexState::Deleting => Err(MetastoreError::Internal {
            message: format!("index `{index_id}` cannot be retrieved"),
            cause: "index `{index_id}` is in transitioning state `deleting` and this should not \
                    happened. try to delete it again"
                .to_string(),
        }),
    }
}

async fn get_index_metadata(
    mut metastore: FileBackedMetastore,
    index_id: String,
) -> MetastoreResult<Option<IndexMetadata>> {
    let request = IndexMetadataRequest::for_index_id(index_id);
    let index_metadata_result = metastore
        .index_metadata(request)
        .await
        .and_then(|response| response.deserialize_index_metadata());
    match index_metadata_result {
        Ok(index_metadata) => Ok(Some(index_metadata)),
        Err(MetastoreError::NotFound { .. }) => Ok(None),
        Err(MetastoreError::Internal { message, cause }) => {
            // Indexes can be in transient states `Creating` or `Deleting`.
            // It is fine to ignore those errors.
            if message.contains("transient state") {
                Ok(None)
            } else {
                Err(MetastoreError::Internal { message, cause })
            }
        }
        Err(error) => Err(error),
    }
}

/// Returns a [`RegexSet`] built from the following rules:
/// - If the given pattern does not contain a `*` char, it matches the exact pattern.
/// - If the given pattern contains one or more `*`, it matches the regex built from a regex where
///   `*` is replaced by `.*`. All other regular expression meta characters are escaped.
fn build_regex_set_from_patterns(patterns: Vec<String>) -> anyhow::Result<RegexSet> {
    // If there is a match all pattern, no need to go further.
    if patterns.iter().any(|pattern| pattern == "*") {
        return Ok(RegexSet::new([".*".to_string()]).expect("regex compilation shouldn't fail"));
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
impl crate::tests::DefaultForTest for FileBackedMetastore {
    async fn default_for_test() -> Self {
        use quickwit_storage::RamStorage;
        FileBackedMetastore::try_new(Arc::new(RamStorage::default()), None)
            .await
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::RangeInclusive;
    use std::path::Path;
    use std::sync::Arc;

    use futures::executor::block_on;
    use quickwit_config::IndexConfig;
    use quickwit_proto::metastore::{DeleteQuery, MetastoreError};
    use quickwit_query::query_ast::qast_helper;
    use quickwit_storage::{MockStorage, RamStorage, Storage, StorageErrorKind};
    use rand::Rng;
    use time::OffsetDateTime;
    use tokio::time::Duration;

    use super::lazy_file_backed_index::LazyFileBackedIndex;
    use super::store_operations::{
        fetch_or_init_indexes_states, meta_path, put_index_given_index_id, put_indexes_states,
    };
    use super::*;
    use crate::tests::DefaultForTest;
    use crate::{metastore_test_suite, IndexMetadata, ListSplitsQuery, SplitMetadata, SplitState};

    metastore_test_suite!(crate::FileBackedMetastore);

    #[tokio::test]
    async fn test_metastore_connectivity_and_endpoints() {
        let mut metastore = FileBackedMetastore::default_for_test().await;
        metastore.check_connectivity().await.unwrap();
        assert!(metastore.endpoints()[0].protocol().is_ram());
    }

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
        let mut metastore = FileBackedMetastore::try_new(Arc::new(mock_storage), None)
            .await
            .unwrap();

        metastore.check_connectivity().await.unwrap();
    }

    #[tokio::test]
    async fn test_file_backed_metastore_index_exists() {
        let index_id = "test-index";
        let mut metastore = FileBackedMetastore::default_for_test().await;
        assert!(!metastore.index_exists(index_id).await.unwrap());

        let index_config = IndexConfig::for_test(index_id, "ram:///indexes/test-index");
        let create_index_request = CreateIndexRequest::try_from_index_config(index_config).unwrap();
        metastore.create_index(create_index_request).await.unwrap();

        assert!(metastore.index_exists(index_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_backed_metastore_get_index() {
        let mut metastore = FileBackedMetastore::default_for_test().await;

        // Create index
        let index_id = "test-index";
        let index_config = IndexConfig::for_test(index_id, "ram:///indexes/test-index");
        let create_index_request =
            CreateIndexRequest::try_from_index_config(index_config.clone()).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

        // Open index and check its metadata
        let created_index = metastore.get_index(index_uid).await.unwrap();
        assert_eq!(created_index.index_id(), index_config.index_id);
        assert_eq!(
            created_index.metadata().index_uri(),
            &index_config.index_uri
        );

        // Check index is returned by list indexes.
        let indexes_metadata = metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .unwrap()
            .deserialize_indexes_metadata()
            .unwrap();
        assert_eq!(indexes_metadata.len(), 1);

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
        let mut metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));

        let index_config = IndexConfig::for_test("test-index", "ram:///indexes/test-index");
        let create_index_request = CreateIndexRequest::try_from_index_config(index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid
            .into();

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
        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), split_metadata).unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        // publish split fails
        let publish_splits_request = PublishSplitsRequest {
            index_uid: index_uid.to_string(),
            staged_split_ids: vec![split_id.to_string()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap_err();

        let query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Published);
        let list_splits_request = ListSplitsRequest::try_from_list_splits_query(query).unwrap();
        let list_splits_response = metastore.list_splits(list_splits_request).await.unwrap();
        let splits = list_splits_response.deserialize_splits().unwrap();
        assert!(splits.is_empty());

        let list_splits_query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Staged);
        let list_splits_request =
            ListSplitsRequest::try_from_list_splits_query(list_splits_query).unwrap();
        let list_splits_response = metastore.list_splits(list_splits_request).await.unwrap();
        let splits = list_splits_response.deserialize_splits().unwrap();
        assert!(!splits.is_empty());
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
    async fn test_file_backed_metastore_write_directly_visible() -> MetastoreResult<()> {
        let mut metastore = FileBackedMetastore::default_for_test().await;

        let index_config = IndexConfig::for_test("test-index", "ram:///indexes/test-index");
        let create_index_request = CreateIndexRequest::try_from_index_config(index_config).unwrap();
        let create_index_response = metastore.create_index(create_index_request).await.unwrap();
        let index_uid: IndexUid = create_index_response.index_uid.into();

        let list_splits_response = metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await
            .unwrap();
        let splits = list_splits_response.deserialize_splits().unwrap();
        assert!(splits.is_empty());

        let split_metadata = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "split1".to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(0..=99),
            ..Default::default()
        };
        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), split_metadata).unwrap();
        metastore.stage_splits(stage_splits_request).await?;

        let list_splits_response = metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid).unwrap())
            .await
            .unwrap();
        let splits = list_splits_response.deserialize_splits().unwrap();
        assert_eq!(splits.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_file_backed_metastore_polling() -> MetastoreResult<()> {
        let storage = Arc::new(RamStorage::default());

        let mut metastore_write = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();
        let polling_interval = Duration::from_millis(20);
        let mut metastore_read = FileBackedMetastore::try_new(storage, Some(polling_interval))
            .await
            .unwrap();

        let index_config = IndexConfig::for_test("test-index", "ram:///indexes/test-index");
        let create_index_request = CreateIndexRequest::try_from_index_config(index_config).unwrap();
        let create_index_response = metastore_write
            .create_index(create_index_request)
            .await
            .unwrap();
        let index_uid: IndexUid = create_index_response.index_uid.into();

        let list_splits_response = metastore_write
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await
            .unwrap();
        let splits = list_splits_response.deserialize_splits().unwrap();
        assert!(splits.is_empty());

        let list_splits_response = metastore_read
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await
            .unwrap();
        let splits = list_splits_response.deserialize_splits().unwrap();
        assert!(splits.is_empty());

        let split_metadata = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "split1".to_string(),
            num_docs: 1,
            uncompressed_docs_size_in_bytes: 2,
            time_range: Some(0..=99),
            ..Default::default()
        };
        let stage_splits_request =
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), split_metadata).unwrap();
        metastore_write.stage_splits(stage_splits_request).await?;

        let list_splits_response = metastore_read
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await
            .unwrap();
        let splits = list_splits_response.deserialize_splits().unwrap();
        assert!(splits.is_empty());

        for _ in 0..10 {
            tokio::time::sleep(polling_interval).await;

            let list_splits_response = metastore_read
                .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
                .await
                .unwrap();
            let splits = list_splits_response.deserialize_splits().unwrap();

            if !splits.is_empty() {
                return Ok(());
            }
        }
        panic!("The metastore should have been updated.");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_file_backed_metastore_race_condition() {
        let mut metastore = FileBackedMetastore::default_for_test().await;

        let index_config = IndexConfig::for_test("test-index", "ram:///indexes/test-index");
        let create_index_request = CreateIndexRequest::try_from_index_config(index_config).unwrap();
        let create_index_response = metastore.create_index(create_index_request).await.unwrap();
        let index_uid: IndexUid = create_index_response.index_uid.into();

        // Stage splits in multiple threads
        let mut handles = Vec::new();
        let mut random_generator = rand::thread_rng();
        for i in 1..=20 {
            let sleep_duration = Duration::from_millis(random_generator.gen_range(0..=200));
            let mut metastore = metastore.clone();
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
                    let stage_splits_request = StageSplitsRequest::try_from_split_metadata(
                        index_uid.clone(),
                        split_metadata,
                    )
                    .unwrap();
                    metastore.stage_splits(stage_splits_request).await.unwrap();

                    tokio::time::sleep(sleep_duration).await;

                    // publish split
                    let split_id = format!("split-{i}");
                    let publish_splits_request = PublishSplitsRequest {
                        index_uid: index_uid.to_string(),
                        staged_split_ids: vec![split_id.to_string()],
                        ..Default::default()
                    };
                    metastore
                        .publish_splits(publish_splits_request)
                        .await
                        .unwrap();
                }
            });
            handles.push(handle);
        }

        futures::future::try_join_all(handles).await.unwrap();

        let list_splits_query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Published);
        let list_splits_request =
            ListSplitsRequest::try_from_list_splits_query(list_splits_query).unwrap();
        let list_splits_response = metastore.list_splits(list_splits_request).await.unwrap();
        let splits = list_splits_response.deserialize_splits().unwrap();

        // Make sure that all 20 splits are in `Published` state.
        assert_eq!(splits.len(), 20);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_file_backed_metastore_list_indexes_race_condition() {
        let mut metastore = FileBackedMetastore::default_for_test().await;
        let mut index_uids = Vec::new();
        for idx in 0..10 {
            let index_uid = IndexUid::new(format!("test-index-{idx}"));
            let index_config =
                IndexConfig::for_test(index_uid.index_id(), "ram:///indexes/test-index");
            let create_index_request =
                CreateIndexRequest::try_from_index_config(index_config).unwrap();
            let index_uid: IndexUid = metastore
                .create_index(create_index_request)
                .await
                .unwrap()
                .index_uid
                .into();
            index_uids.push(index_uid);
        }
        // Delete indexes + call to list_indexes_metadata.
        let mut handles = Vec::new();
        for index_uid in index_uids {
            let delete_request = DeleteIndexRequest {
                index_uid: index_uid.to_string(),
            };
            {
                let mut metastore = metastore.clone();
                let handle = tokio::spawn(async move {
                    metastore
                        .list_indexes_metadata(ListIndexesMetadataRequest::all())
                        .await
                        .unwrap();
                });
                handles.push(handle);
            }
            {
                let mut metastore = metastore.clone();
                let handle = tokio::spawn(async move {
                    metastore.delete_index(delete_request).await.unwrap();
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
        let mut metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));
        let index_config = IndexConfig::for_test(index_id, "ram:///indexes/test-index");

        // Create index.
        let create_index_request = CreateIndexRequest::try_from_index_config(index_config).unwrap();
        let metastore_error = metastore
            .create_index(create_index_request)
            .await
            .unwrap_err();
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
        let mut metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));
        let index_config = IndexConfig::for_test(index_id, "ram:///indexes/test-index");

        // Create index
        let create_index_request = CreateIndexRequest::try_from_index_config(index_config).unwrap();
        let metastore_error = metastore
            .create_index(create_index_request)
            .await
            .unwrap_err();
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
        let delete_request = DeleteIndexRequest {
            index_uid: index_uid.to_string(),
        };
        let deleted_index_error = metastore.delete_index(delete_request).await.unwrap_err();
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
                        return Err(StorageErrorKind::Io.with_error(anyhow::anyhow!(
                            "oops. perhaps there are some network problems"
                        )));
                    }
                    indexes_json_valid_put -= 1;
                }
                return block_on(ram_storage_clone.put(path, put_payload));
            });
        let mut metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));
        let index_config = IndexConfig::for_test(index_id, "ram:///indexes/test-index");

        // Create index
        let metastore_error = metastore
            .create_index(CreateIndexRequest::try_from_index_config(index_config).unwrap())
            .await
            .unwrap_err();
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
        let mut metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));

        // Delete index
        let delete_request = DeleteIndexRequest {
            index_uid: index_uid.to_string(),
        };
        let metastore_error = metastore.delete_index(delete_request).await.unwrap_err();
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
                        return Err(StorageErrorKind::Io.with_error(anyhow::anyhow!(
                            "oops. perhaps there are some network problems"
                        )));
                    }
                    indexes_json_valid_put -= 1;
                }
                return block_on(ram_storage_clone.put(path, put_payload));
            });
        let mut metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));

        // Delete index
        let delete_request = DeleteIndexRequest {
            index_uid: index_uid.to_string(),
        };
        let metastore_error = metastore.delete_index(delete_request).await.unwrap_err();
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
        let mut metastore = FileBackedMetastore::try_new(ram_storage.clone(), None)
            .await
            .unwrap();
        let indexes_metadata = metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .unwrap()
            .deserialize_indexes_metadata()
            .unwrap();
        assert_eq!(indexes_metadata.len(), 1);

        // Fetch the index metadata not registered in indexes states json.
        metastore
            .get_index(index_uid_unregistered.clone())
            .await
            .unwrap();

        // Now list indexes return 2 indexes metadatas as the metastore is now aware of
        // 2 alive indexes.
        let indexes_metadata = metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .unwrap()
            .deserialize_indexes_metadata()
            .unwrap();
        assert_eq!(indexes_metadata.len(), 2);

        // Let's delete indexes.
        let delete_request = DeleteIndexRequest {
            index_uid: index_uid_alive.to_string(),
        };
        metastore.delete_index(delete_request).await.unwrap();

        let delete_request = DeleteIndexRequest {
            index_uid: index_uid_unregistered.to_string(),
        };
        metastore.delete_index(delete_request).await.unwrap();
        let indexes_metadata = metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .unwrap()
            .deserialize_indexes_metadata()
            .unwrap();
        assert!(indexes_metadata.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_monotically_increasing_stamps_by_index() {
        let storage = RamStorage::default();
        let mut metastore = FileBackedMetastore::try_new(Arc::new(storage.clone()), None)
            .await
            .unwrap();
        let index_id = "test-index-increasing-stamps-by-index";
        let index_config = IndexConfig::for_test(
            index_id,
            "ram:///indexes/test-index-increasing-stamps-by-index",
        );
        let create_index_request = CreateIndexRequest::try_from_index_config(index_config).unwrap();
        let create_index_response = metastore.create_index(create_index_request).await.unwrap();
        let index_uid = create_index_response.index_uid;

        let delete_query = DeleteQuery {
            start_timestamp: None,
            end_timestamp: None,
            index_uid,
            query_ast: serde_json::to_string(&qast_helper("harry potter", &["body"])).unwrap(),
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
        let mut new_metastore = FileBackedMetastore::try_new(Arc::new(storage), None)
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
        let create_index_request = CreateIndexRequest::try_from_index_config(index_config).unwrap();
        let create_index_response = metastore.create_index(create_index_request).await.unwrap();
        let index_uid = create_index_response.index_uid;

        let delete_query = DeleteQuery {
            start_timestamp: None,
            end_timestamp: None,
            index_uid,
            query_ast: serde_json::to_string(&qast_helper("harry potter", &["body"])).unwrap(),
        };
        let delete_task_4 = metastore.create_delete_task(delete_query).await.unwrap();
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
            "index ID pattern `index-**-1` is invalid. patterns must not contain multiple \
             consecutive `*`",
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
