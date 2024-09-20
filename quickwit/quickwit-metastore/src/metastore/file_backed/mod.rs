// Copyright (C) 2024 Quickwit, Inc.
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
mod index_id_matcher;
mod index_template_matcher;
mod lazy_file_backed_index;
pub(crate) mod manifest;
mod state;
mod store_operations;

use core::fmt;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::try_join_all;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use itertools::Itertools;
use quickwit_common::ServiceStream;
use quickwit_config::IndexTemplate;
use quickwit_proto::metastore::{
    serde_utils, AcquireShardsRequest, AcquireShardsResponse, AddSourceRequest, CreateIndexRequest,
    CreateIndexResponse, CreateIndexTemplateRequest, DeleteIndexRequest,
    DeleteIndexTemplatesRequest, DeleteQuery, DeleteShardsRequest, DeleteShardsResponse,
    DeleteSourceRequest, DeleteSplitsRequest, DeleteTask, EmptyResponse, EntityKind,
    FindIndexTemplateMatchesRequest, FindIndexTemplateMatchesResponse, GetIndexTemplateRequest,
    GetIndexTemplateResponse, IndexMetadataFailure, IndexMetadataFailureReason,
    IndexMetadataRequest, IndexMetadataResponse, IndexTemplateMatch, IndexesMetadataRequest,
    IndexesMetadataResponse, LastDeleteOpstampRequest, LastDeleteOpstampResponse,
    ListDeleteTasksRequest, ListDeleteTasksResponse, ListIndexTemplatesRequest,
    ListIndexTemplatesResponse, ListIndexesMetadataRequest, ListIndexesMetadataResponse,
    ListShardsRequest, ListShardsResponse, ListSplitsRequest, ListSplitsResponse,
    ListStaleSplitsRequest, MarkSplitsForDeletionRequest, MetastoreError, MetastoreResult,
    MetastoreService, MetastoreServiceStream, OpenShardSubrequest, OpenShardsRequest,
    OpenShardsResponse, PruneShardsRequest, PublishSplitsRequest, ResetSourceCheckpointRequest,
    StageSplitsRequest, ToggleSourceRequest, UpdateIndexRequest, UpdateSplitsDeleteOpstampRequest,
    UpdateSplitsDeleteOpstampResponse,
};
use quickwit_proto::types::{IndexId, IndexUid};
use quickwit_storage::Storage;
use time::OffsetDateTime;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};

use self::file_backed_index::FileBackedIndex;
pub use self::file_backed_metastore_factory::FileBackedMetastoreFactory;
use self::index_id_matcher::IndexIdMatcher;
use self::lazy_file_backed_index::LazyFileBackedIndex;
use self::manifest::{load_or_create_manifest, save_manifest, MANIFEST_FILE_NAME};
use self::state::MetastoreState;
use self::store_operations::{delete_index, index_exists, load_index, put_index};
use super::{
    AddSourceRequestExt, CreateIndexRequestExt, IndexMetadataResponseExt,
    IndexesMetadataResponseExt, ListIndexesMetadataResponseExt, ListSplitsRequestExt,
    ListSplitsResponseExt, PublishSplitsRequestExt, StageSplitsRequestExt, UpdateIndexRequestExt,
    STREAM_SPLITS_CHUNK_SIZE,
};
use crate::checkpoint::IndexCheckpointDelta;
use crate::{IndexMetadata, ListSplitsQuery, MetastoreServiceExt, Split, SplitState};

/// Status of an index tracked by the metastore.
pub(crate) enum LazyIndexStatus {
    /// The index is being created but its metadata have yet to be written on the storage.
    Creating,
    /// The index is created and available.
    Active(LazyFileBackedIndex),
    /// The index is being deleted and but its index metadata file has not yet been removed from
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

/// A metastore implementation that stores all the metadata associated to each index
/// into as many files and stores a map of indexes
/// (index_id, index_status) in a dedicated file `manifest.json`.
///
/// A [`LazyIndexStatus`] describes the lifecycle of an index: [`LazyIndexStatus::Creating`] and
/// [`LazyIndexStatus::Deleting`] are transitioning states that indicates that the index is not
/// yet available. On the contrary, the [`LazyIndexStatus::Active`] status indicates the index is
/// ready to be fetched and updated.
///
/// Transitioning states are useful to track inconsistencies between the in-memory and on-disk data
/// structures when error(s) occur during index creations and deletions:
/// - `Creating` indicates that the metastore updated the manifest file with this state but not yet
///   the index metadata file;
/// - `Deleting` indicates that the metastore updated the manifest file with this state but the
///   index metadata file is not yet deleted.
///
/// !!! Important note: the indexes map manifest does not
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
    state: Arc<RwLock<MetastoreState>>,
    storage: Arc<dyn Storage>,
    polling_interval_opt: Option<Duration>,
}

impl fmt::Debug for FileBackedMetastore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileBackedMetastore")
            .field("storage_uri", self.storage.uri())
            .field("polling_interval_opt", &self.polling_interval_opt)
            .finish()
    }
}

impl FileBackedMetastore {
    /// Creates a [`FileBackedMetastore`] for tests.
    #[doc(hidden)]
    pub fn for_test(storage: Arc<dyn Storage>) -> Self {
        Self {
            state: Default::default(),
            storage,
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

    /// Creates a [`FileBackedMetastore`] for a specified storage, immediately loading the manifest
    /// file.
    pub async fn try_new(
        storage: Arc<dyn Storage>,
        polling_interval_opt: Option<Duration>,
    ) -> MetastoreResult<Self> {
        let manifest = load_or_create_manifest(&*storage).await?;
        let state =
            MetastoreState::try_from_manifest(storage.clone(), manifest, polling_interval_opt)?;
        let metastore = Self {
            state: Arc::new(RwLock::new(state)),
            storage,
            polling_interval_opt,
        };
        Ok(metastore)
    }

    async fn mutate<T>(
        &self,
        index_uid: &IndexUid,
        mutate_fn: impl FnOnce(&mut FileBackedIndex) -> MetastoreResult<MutationOccurred<T>>,
    ) -> MetastoreResult<T> {
        let index_id = &index_uid.index_id;
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
            Err(error) => {
                // For some of the error type here, we cannot know for sure
                // whether the content was written or not.
                //
                // Just to be sure, let's discard the cache.
                let mut state_wlock_guard = self.state.write().await;

                // At this point, we hold both locks.
                state_wlock_guard.indexes.insert(
                    index_id.to_string(),
                    LazyIndexStatus::Active(LazyFileBackedIndex::new(
                        self.storage.clone(),
                        index_id.to_string(),
                        self.polling_interval_opt,
                        None,
                    )),
                );
                locked_index.discarded = true;
                Err(error)
            }
        }
    }

    async fn read<T, F>(&self, index_uid: &IndexUid, view: F) -> MetastoreResult<T>
    where F: FnOnce(&FileBackedIndex) -> MetastoreResult<T> {
        let index_id = &index_uid.index_id;
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

    /// Returns a valid locked index.
    ///
    /// This function guarantees that it has not been
    /// marked as discarded.
    async fn get_locked_index(
        &self,
        index_id: &str,
    ) -> MetastoreResult<OwnedMutexGuard<FileBackedIndex>> {
        loop {
            let index = self.index(index_id).await?;
            let locked_index = index.lock_owned().await;

            if !locked_index.discarded {
                return Ok(locked_index);
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
            let inner_rlock_guard = self.state.read().await;
            if let Some(index_state) = inner_rlock_guard.indexes.get(index_id) {
                return get_index_mutex(index_id, index_state).await;
            }
        }
        // At this point we do not hold our mutex, so we need to do a little dance
        // to make sure we return the same instance.
        //
        // If there is an error here, note we do not return right away.
        // That's because we want to observe the property that after one success
        // all subsequent calls will succeed.
        let index_result = load_index(&*self.storage, index_id).await;

        // Here we retake the lock, still no io ongoing.
        let mut state_wlock_guard = self.state.write().await;

        // At this point, some other client might have added another instance of the Metadataet in
        // the map. We want to avoid two copies to exist in the application, so we keep only
        // one.
        if let Some(index_state) = state_wlock_guard.indexes.get(index_id) {
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
        state_wlock_guard
            .indexes
            .insert(index_id.to_string(), LazyIndexStatus::Active(lazy_index));
        Ok(index_mutex)
    }

    async fn index_metadata_inner(
        &self,
        index_id_opt: Option<IndexId>,
        index_uid_opt: Option<IndexUid>,
    ) -> Result<IndexMetadata, (MetastoreError, Option<IndexId>, Option<IndexUid>)> {
        let index_id = if let Some(index_id) = &index_id_opt {
            index_id
        } else if let Some(index_uid) = &index_uid_opt {
            &index_uid.index_id
        } else {
            let message = "invalid request: neither `index_id` nor `index_uid` is set".to_string();
            let metastore_error = MetastoreError::Internal {
                message,
                cause: "".to_string(),
            };
            return Err((metastore_error, index_id_opt, index_uid_opt));
        };
        let index_metadata = match self
            .read_any(index_id, |index| Ok(index.metadata().clone()))
            .await
        {
            Ok(index_metadata) => index_metadata,
            Err(metastore_error) => {
                return Err((metastore_error, index_id_opt, index_uid_opt));
            }
        };
        if let Some(index_uid) = &index_uid_opt {
            if index_metadata.index_uid != *index_uid {
                let metastore_error = MetastoreError::NotFound(EntityKind::Index {
                    index_id: index_id.to_string(),
                });
                return Err((metastore_error, index_id_opt, index_uid_opt));
            }
        }
        Ok(index_metadata)
    }

    /// Returns the list of splits for the given request.
    /// No error is returned if any of the requested `index_uid` does not exist.
    async fn list_splits_inner(&self, request: ListSplitsRequest) -> MetastoreResult<Vec<Split>> {
        let list_splits_query = request.deserialize_list_splits_query()?;
        let mut all_splits = Vec::new();
        for index_uid in &list_splits_query.index_uids {
            let splits = match self
                .read(index_uid, |index| index.list_splits(&list_splits_query))
                .await
            {
                Ok(splits) => splits,
                Err(MetastoreError::NotFound(_)) => {
                    // If the index does not exist, we just skip it.
                    continue;
                }
                Err(error) => return Err(error),
            };
            all_splits.extend(splits);
        }
        Ok(all_splits)
    }

    /// Helper used for testing to obtain the data associated with the given index.
    #[cfg(test)]
    async fn get_index(&self, index_uid: &IndexUid) -> MetastoreResult<FileBackedIndex> {
        self.read(index_uid, |index| Ok(index.clone())).await
    }
}

#[async_trait]
impl MetastoreService for FileBackedMetastore {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.storage.exists(Path::new(MANIFEST_FILE_NAME)).await?;
        Ok(())
    }

    fn endpoints(&self) -> Vec<quickwit_common::uri::Uri> {
        vec![self.storage.uri().clone()]
    }

    /// -------------------------------------------------------------------------------
    /// Mutations over the high-level index.
    async fn create_index(
        &self,
        request: CreateIndexRequest,
    ) -> MetastoreResult<CreateIndexResponse> {
        let index_config = request.deserialize_index_config()?;
        let source_configs = request.deserialize_source_configs()?;

        let mut index_metadata = IndexMetadata::new(index_config);

        for source_config in source_configs {
            index_metadata.add_source(source_config)?;
        }
        let index_uid = index_metadata.index_uid.clone();
        let index_id = &index_uid.index_id;

        let index_metadata_json = serde_utils::to_json_str(&index_metadata)?;
        let index = FileBackedIndex::from(index_metadata);

        let mut state_wlock_guard = self.state.write().await;

        // Checking if index already exists is a bit tedious:
        // - first we check the index state: if it's `Active`, return `IndexAlreadyExists` error,
        //   and if it's `Creating` or `Deleting`, it's ok to override them as these are
        //   transitioning states.
        // - if the index is not in the index states map, we still need to check the storage as we
        //   don't want to override an existing metadata file.
        if let Some(index_status) = state_wlock_guard.indexes.get(index_id) {
            if let LazyIndexStatus::Active(_) = index_status {
                return Err(MetastoreError::AlreadyExists(EntityKind::Index {
                    index_id: index_id.to_string(),
                }));
            }
        } else if index_exists(&*self.storage, index_id).await? {
            return Err(MetastoreError::Internal {
                message: format!("index {index_id} cannot be created"),
                cause: format!(
                    "index {index_id} is not present in the manifest file but its file \
                     `{index_id}/metastore.json` is on the storage"
                ),
            });
        }
        // Set state to `Creating` and rollback on metastore error.
        state_wlock_guard
            .indexes
            .insert(index_id.clone(), LazyIndexStatus::Creating);

        let manifest = state_wlock_guard.as_manifest();

        if let Err(error) = save_manifest(&*self.storage, &manifest).await {
            state_wlock_guard.indexes.remove(index_id);
            return Err(error);
        }
        put_index(&*self.storage, &index).await?;

        state_wlock_guard.indexes.insert(
            index_id.clone(),
            LazyIndexStatus::Active(LazyFileBackedIndex::new(
                self.storage.clone(),
                index_id.clone(),
                self.polling_interval_opt,
                Some(index),
            )),
        );
        // Set state to `Active` and rollback on metastore error.
        let manifest = state_wlock_guard.as_manifest();

        if let Err(error) = save_manifest(&*self.storage, &manifest).await {
            state_wlock_guard
                .indexes
                .insert(index_id.clone(), LazyIndexStatus::Creating);
            return Err(error);
        }

        let response = CreateIndexResponse {
            index_uid: index_uid.into(),
            index_metadata_json,
        };
        Ok(response)
    }

    async fn update_index(
        &self,
        request: UpdateIndexRequest,
    ) -> MetastoreResult<IndexMetadataResponse> {
        let retention_policy_opt = request.deserialize_retention_policy()?;
        let search_settings = request.deserialize_search_settings()?;
        let indexing_settings = request.deserialize_indexing_settings()?;
        let doc_mapping = request.deserialize_doc_mapping()?;
        let index_uid = request.index_uid();

        let index_metadata = self
            .mutate(index_uid, |index| {
                let mut mutation_occurred = index.set_retention_policy(retention_policy_opt);
                mutation_occurred |= index.set_search_settings(search_settings);
                mutation_occurred |= index.set_indexing_settings(indexing_settings);
                mutation_occurred |= index.set_doc_mapping(doc_mapping);

                let index_metadata = index.metadata().clone();

                if mutation_occurred {
                    Ok(MutationOccurred::Yes(index_metadata))
                } else {
                    Ok(MutationOccurred::No(index_metadata))
                }
            })
            .await?;
        IndexMetadataResponse::try_from_index_metadata(&index_metadata)
    }

    async fn delete_index(&self, request: DeleteIndexRequest) -> MetastoreResult<EmptyResponse> {
        // We pick the outer lock here, so that we enter a critical section.
        let mut state_wlock_guard = self.state.write().await;

        let index_id = &request.index_uid().index_id;
        // If index is neither in `per_index_metastores_wlock` nor on the storage, it does not
        // exist.
        if !state_wlock_guard.indexes.contains_key(index_id)
            && !index_exists(&*self.storage, index_id).await?
        {
            return Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: index_id.to_string(),
            }));
        }
        // Set state to `Deleting` and keep the previous state in memory in case we need to insert
        // if an error occurs.
        let index_state_opt = state_wlock_guard
            .indexes
            .insert(index_id.to_string(), LazyIndexStatus::Deleting);
        let manifest = state_wlock_guard.as_manifest();
        // On a put error, reinsert the previous state if any.
        if let Err(error) = save_manifest(&*self.storage, &manifest).await {
            if let Some(index_state) = index_state_opt {
                state_wlock_guard
                    .indexes
                    .insert(index_id.to_string(), index_state);
            } else {
                state_wlock_guard.indexes.remove(index_id);
            }
            return Err(error);
        }

        let delete_result = delete_index(&*self.storage, index_id).await;

        if matches!(
            &delete_result,
            Ok(()) | Err(MetastoreError::NotFound(EntityKind::Index { .. }))
        ) {
            state_wlock_guard.indexes.remove(index_id);
            let manifest = state_wlock_guard.as_manifest();

            if let Err(error) = save_manifest(&*self.storage, &manifest).await {
                state_wlock_guard
                    .indexes
                    .insert(index_id.to_string(), LazyIndexStatus::Deleting);
                return Err(error);
            }
        }
        delete_result.map(|_| EmptyResponse {})
    }

    /// -------------------------------------------------------------------------------
    /// Mutations over a single index

    async fn stage_splits(&self, request: StageSplitsRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid().clone();
        let splits_metadata = request.deserialize_splits_metadata()?;

        self.mutate(&index_uid, |index| {
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
        &self,
        request: PublishSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_checkpoint_delta: Option<IndexCheckpointDelta> =
            request.deserialize_index_checkpoint()?;
        let index_uid = request.index_uid().clone();
        self.mutate(&index_uid, |index| {
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
        &self,
        request: MarkSplitsForDeletionRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid().clone();

        self.mutate(&index_uid, |index| {
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

    async fn delete_splits(&self, request: DeleteSplitsRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid().clone();

        self.mutate(&index_uid, |index| {
            index.delete_splits(request.split_ids)?;
            Ok(MutationOccurred::Yes(EmptyResponse {}))
        })
        .await?;
        Ok(EmptyResponse {})
    }

    async fn add_source(&self, request: AddSourceRequest) -> MetastoreResult<EmptyResponse> {
        let source_config = request.deserialize_source_config()?;
        let index_uid = request.index_uid();

        self.mutate(index_uid, |index| {
            index.add_source(source_config)?;
            Ok(MutationOccurred::Yes(()))
        })
        .await?;
        Ok(EmptyResponse {})
    }

    async fn toggle_source(&self, request: ToggleSourceRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid();

        self.mutate(index_uid, |index| {
            index
                .toggle_source(&request.source_id, request.enable)
                .map(MutationOccurred::from)
        })
        .await?;
        Ok(EmptyResponse {})
    }

    async fn delete_source(&self, request: DeleteSourceRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid();

        self.mutate(index_uid, |index| {
            index.delete_source(&request.source_id)?;
            Ok(MutationOccurred::Yes(()))
        })
        .await?;
        Ok(EmptyResponse {})
    }

    async fn reset_source_checkpoint(
        &self,
        request: ResetSourceCheckpointRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid();

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

    /// Streams of splits for the given request.
    /// No error is returned if any of the requested `index_uid` does not exist.
    async fn list_splits(
        &self,
        request: ListSplitsRequest,
    ) -> MetastoreResult<MetastoreServiceStream<ListSplitsResponse>> {
        let splits = self.list_splits_inner(request).await?;
        let splits_responses: Vec<MetastoreResult<ListSplitsResponse>> = splits
            .chunks(STREAM_SPLITS_CHUNK_SIZE)
            .map(|chunk| ListSplitsResponse::try_from_splits(chunk.to_vec()))
            .collect();
        let splits_responses_stream = Box::pin(futures::stream::iter(splits_responses));
        Ok(ServiceStream::new(splits_responses_stream))
    }

    async fn list_stale_splits(
        &self,
        request: ListStaleSplitsRequest,
    ) -> MetastoreResult<ListSplitsResponse> {
        let list_splits_query = ListSplitsQuery::for_index(request.index_uid().clone())
            .with_delete_opstamp_lt(request.delete_opstamp)
            .with_split_state(SplitState::Published)
            .retain_mature(OffsetDateTime::now_utc())
            .sort_by_staleness()
            .with_limit(request.num_splits as usize);
        let list_splits_request =
            ListSplitsRequest::try_from_list_splits_query(&list_splits_query)?;
        let splits = self.list_splits_inner(list_splits_request).await?;
        ListSplitsResponse::try_from_splits(splits)
    }

    async fn index_metadata(
        &self,
        request: IndexMetadataRequest,
    ) -> MetastoreResult<IndexMetadataResponse> {
        let index_metadata = self
            .index_metadata_inner(request.index_id, request.index_uid)
            .await
            .map_err(|(metastore_error, _index_id_opt, _index_uid_opt)| metastore_error)?;
        let response = IndexMetadataResponse::try_from_index_metadata(&index_metadata)?;
        Ok(response)
    }

    async fn indexes_metadata(
        &self,
        request: IndexesMetadataRequest,
    ) -> MetastoreResult<IndexesMetadataResponse> {
        let mut indexes_metadata: Vec<IndexMetadata> =
            Vec::with_capacity(request.subrequests.len());
        let mut failures: Vec<IndexMetadataFailure> = Vec::new();

        let mut index_metadata_futures = FuturesUnordered::new();

        for subrequest in request.subrequests {
            let metastore = self.clone();
            let index_metadata_future = async move {
                metastore
                    .index_metadata_inner(subrequest.index_id, subrequest.index_uid)
                    .await
            };
            index_metadata_futures.push(index_metadata_future);
        }
        while let Some(index_metadata_result) = index_metadata_futures.next().await {
            match index_metadata_result {
                Ok(index_metadata) => indexes_metadata.push(index_metadata),
                Err((MetastoreError::NotFound(_), index_id, index_uid)) => {
                    let failure = IndexMetadataFailure {
                        index_id,
                        index_uid,
                        reason: IndexMetadataFailureReason::NotFound as i32,
                    };
                    failures.push(failure)
                }
                // All other errors are considered internal errors.
                Err((_metastore_error, index_id, index_uid)) => {
                    let failure = IndexMetadataFailure {
                        index_id,
                        index_uid,
                        reason: IndexMetadataFailureReason::Internal as i32,
                    };
                    failures.push(failure)
                }
            }
        }
        let response =
            IndexesMetadataResponse::try_from_indexes_metadata(indexes_metadata, failures).await?;
        Ok(response)
    }

    async fn list_indexes_metadata(
        &self,
        request: ListIndexesMetadataRequest,
    ) -> MetastoreResult<ListIndexesMetadataResponse> {
        // Done in two steps:
        // 1) Get index IDs and release the lock on `per_index_metastores`.
        // 2) Get each index metadata. Note that each get will take a read lock on
        // `per_index_metastores`. Lock is released in 1) to let a concurrent task/thread to
        // take a write lock on `per_index_metastores`.
        let index_id_matcher =
            IndexIdMatcher::try_from_index_id_patterns(&request.index_id_patterns)?;
        let inner_rlock_guard = self.state.read().await;
        let index_ids: Vec<IndexId> = inner_rlock_guard
            .indexes
            .iter()
            .filter_map(|(index_id, index_state)| match index_state {
                LazyIndexStatus::Active(_) if index_id_matcher.is_match(index_id) => Some(index_id),
                _ => None,
            })
            .cloned()
            .collect();
        drop(inner_rlock_guard);

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
        let response =
            ListIndexesMetadataResponse::try_from_indexes_metadata(indexes_metadata).await?;
        Ok(response)
    }

    // Shard API

    async fn open_shards(&self, request: OpenShardsRequest) -> MetastoreResult<OpenShardsResponse> {
        let mut response = OpenShardsResponse {
            subresponses: Vec::with_capacity(request.subrequests.len()),
        };
        // We must group the subrequests by `index_uid` to mutate each index only once, since each
        // mutation triggers an IO.
        let per_index_uid_subrequests: HashMap<IndexUid, Vec<OpenShardSubrequest>> = request
            .subrequests
            .into_iter()
            .into_group_map_by(|subrequest| subrequest.index_uid().clone());

        for (index_uid, subrequests) in per_index_uid_subrequests {
            let subresponses = self
                .mutate(&index_uid, |index| index.open_shards(subrequests))
                .await?;
            response.subresponses.extend(subresponses);
        }
        Ok(response)
    }

    async fn acquire_shards(
        &self,
        request: AcquireShardsRequest,
    ) -> MetastoreResult<AcquireShardsResponse> {
        let index_uid = request.index_uid().clone();
        let response = self
            .mutate(&index_uid, |index| index.acquire_shards(request))
            .await?;
        Ok(response)
    }

    async fn delete_shards(
        &self,
        request: DeleteShardsRequest,
    ) -> MetastoreResult<DeleteShardsResponse> {
        let index_uid = request.index_uid().clone();
        let response = self
            .mutate(&index_uid, |index| index.delete_shards(request))
            .await?;
        Ok(response)
    }

    async fn prune_shards(&self, request: PruneShardsRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid = request.index_uid().clone();
        self.mutate(&index_uid, |index| index.prune_shards(request))
            .await?;
        Ok(EmptyResponse {})
    }

    async fn list_shards(&self, request: ListShardsRequest) -> MetastoreResult<ListShardsResponse> {
        let mut subresponses = Vec::with_capacity(request.subrequests.len());

        for subrequest in request.subrequests {
            let index_uid = subrequest.index_uid().clone();
            let subresponse = self
                .read(&index_uid, |index| index.list_shards(subrequest))
                .await?;
            subresponses.push(subresponse);
        }
        let response = ListShardsResponse { subresponses };
        Ok(response)
    }

    /// -------------------------------------------------------------------------------
    /// Delete tasks

    async fn last_delete_opstamp(
        &self,
        request: LastDeleteOpstampRequest,
    ) -> MetastoreResult<LastDeleteOpstampResponse> {
        let last_delete_opstamp = self
            .read(request.index_uid(), |index| Ok(index.last_delete_opstamp()))
            .await?;
        Ok(LastDeleteOpstampResponse::new(last_delete_opstamp))
    }

    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        let index_uid = delete_query.index_uid().clone();
        let delete_task = self
            .mutate(&index_uid, |index| {
                index
                    .create_delete_task(delete_query)
                    .map(MutationOccurred::Yes)
            })
            .await?;
        Ok(delete_task)
    }

    async fn update_splits_delete_opstamp(
        &self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> MetastoreResult<UpdateSplitsDeleteOpstampResponse> {
        let index_uid = request.index_uid();

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
        &self,
        request: ListDeleteTasksRequest,
    ) -> MetastoreResult<ListDeleteTasksResponse> {
        let index_uid = request.index_uid();

        let delete_tasks = self
            .read(index_uid, |index| {
                Ok(index.list_delete_tasks(request.opstamp_start))
            })
            .await??;
        let response = ListDeleteTasksResponse { delete_tasks };
        Ok(response)
    }

    // Index Template API

    async fn create_index_template(
        &self,
        request: CreateIndexTemplateRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_template: IndexTemplate =
            serde_utils::from_json_str(&request.index_template_json)?;
        let template_id = index_template.template_id.clone();

        let mut state_wlock_guard = self.state.write().await;

        let evicted_template_opt = match state_wlock_guard.templates.entry(template_id.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(index_template.clone());
                None
            }
            Entry::Occupied(mut entry) if request.overwrite => {
                let evicted_template = entry.insert(index_template.clone());
                Some(evicted_template)
            }
            Entry::Occupied(_) => {
                return Err(MetastoreError::AlreadyExists(EntityKind::IndexTemplate {
                    template_id,
                }));
            }
        };
        if let Err(error) = state_wlock_guard.template_matcher.insert(&index_template) {
            if let Some(evicted_template) = evicted_template_opt {
                state_wlock_guard
                    .templates
                    .insert(evicted_template.template_id.clone(), evicted_template);
            } else {
                state_wlock_guard.templates.remove(&template_id);
            }
            return Err(error);
        }
        let manifest = state_wlock_guard.as_manifest();
        let save_result = save_manifest(&*self.storage, &manifest).await;

        // Rollback on error.
        if let Err(error) = save_result {
            if let Some(evicted_template) = evicted_template_opt {
                state_wlock_guard
                    .template_matcher
                    .insert(&evicted_template)
                    .expect("evicted template should be valid");
                state_wlock_guard
                    .templates
                    .insert(evicted_template.template_id.clone(), evicted_template);
            } else {
                state_wlock_guard.templates.remove(&template_id);
                state_wlock_guard.template_matcher.remove(&template_id);
            }
            return Err(error);
        }
        Ok(EmptyResponse {})
    }

    async fn get_index_template(
        &self,
        request: GetIndexTemplateRequest,
    ) -> MetastoreResult<GetIndexTemplateResponse> {
        let inner_rlock_guard = self.state.read().await;
        let index_template = inner_rlock_guard
            .templates
            .get(&request.template_id)
            .ok_or({
                MetastoreError::NotFound(EntityKind::IndexTemplate {
                    template_id: request.template_id,
                })
            })?;
        let index_template_json = serde_utils::to_json_str(index_template)?;
        let response = GetIndexTemplateResponse {
            index_template_json,
        };
        Ok(response)
    }

    async fn find_index_template_matches(
        &self,
        request: FindIndexTemplateMatchesRequest,
    ) -> MetastoreResult<FindIndexTemplateMatchesResponse> {
        let inner_rlock_guard = self.state.read().await;

        let mut matches = Vec::new();

        for index_id in request.index_ids {
            if let Some(template_id) = inner_rlock_guard
                .template_matcher
                .find_match(&index_id)
                .clone()
            {
                let index_template = inner_rlock_guard
                    .templates
                    .get(&template_id)
                    .expect("template should exist");
                let index_template_json = serde_utils::to_json_str(index_template)?;
                let index_template_match = IndexTemplateMatch {
                    index_id,
                    template_id,
                    index_template_json,
                };
                matches.push(index_template_match);
            };
        }
        let response = FindIndexTemplateMatchesResponse { matches };
        Ok(response)
    }

    async fn list_index_templates(
        &self,
        _request: ListIndexTemplatesRequest,
    ) -> MetastoreResult<ListIndexTemplatesResponse> {
        let inner_rlock_guard = self.state.read().await;

        let index_templates_json: Vec<String> = inner_rlock_guard
            .templates
            .values()
            .map(serde_utils::to_json_str)
            .collect::<MetastoreResult<_>>()?;
        let response = ListIndexTemplatesResponse {
            index_templates_json,
        };
        Ok(response)
    }

    async fn delete_index_templates(
        &self,
        request: DeleteIndexTemplatesRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let mut evicted_templates = Vec::with_capacity(request.template_ids.len());
        let mut state_wlock_guard = self.state.write().await;

        for template_id in &request.template_ids {
            if let Some(evicted_template) = state_wlock_guard.templates.remove(template_id) {
                evicted_templates.push(evicted_template);
                state_wlock_guard.template_matcher.remove(template_id);
            }
        }
        let manifest = state_wlock_guard.as_manifest();
        let save_result = save_manifest(&*self.storage, &manifest).await;

        // Rollback on error.
        if let Err(error) = save_result {
            for evicted_template in evicted_templates {
                state_wlock_guard
                    .template_matcher
                    .insert(&evicted_template)
                    .expect("evicted template should be valid");
                state_wlock_guard
                    .templates
                    .insert(evicted_template.template_id.clone(), evicted_template);
            }
            return Err(error);
        }
        Ok(EmptyResponse {})
    }
}

impl MetastoreServiceExt for FileBackedMetastore {}

async fn get_index_mutex(
    index_id: &str,
    lazy_index_status: &LazyIndexStatus,
) -> MetastoreResult<Arc<Mutex<FileBackedIndex>>> {
    match lazy_index_status {
        LazyIndexStatus::Active(lazy_index) => lazy_index.get().await,
        LazyIndexStatus::Creating => Err(MetastoreError::Internal {
            message: format!("index `{index_id}` cannot be retrieved"),
            cause: "index `{index_id}` is in transitioning state `creating` and this should not \
                    happened. either recreate or delete it"
                .to_string(),
        }),
        LazyIndexStatus::Deleting => Err(MetastoreError::Internal {
            message: format!("index `{index_id}` cannot be retrieved"),
            cause: "index `{index_id}` is in transitioning state `deleting` and this should not \
                    happened. try to delete it again"
                .to_string(),
        }),
    }
}

async fn get_index_metadata(
    metastore: FileBackedMetastore,
    index_id: IndexId,
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

    use std::ops::RangeInclusive;
    use std::path::Path;
    use std::sync::Arc;

    use futures::executor::block_on;
    use quickwit_common::uri::{Protocol, Uri};
    use quickwit_config::IndexConfig;
    use quickwit_proto::ingest::Shard;
    use quickwit_proto::metastore::{DeleteQuery, MetastoreError};
    use quickwit_proto::types::SourceId;
    use quickwit_query::query_ast::qast_helper;
    use quickwit_storage::{MockStorage, RamStorage, Storage, StorageErrorKind};
    use rand::Rng;
    use tests::manifest::{IndexStatus, Manifest};
    use time::OffsetDateTime;
    use tokio::time::Duration;

    use super::store_operations::{metastore_filepath, put_index_given_index_id};
    use super::*;
    use crate::metastore::MetastoreServiceStreamSplitsExt;
    use crate::tests::shard::ReadWriteShardsForTest;
    use crate::tests::DefaultForTest;
    use crate::{metastore_test_suite, IndexMetadata, ListSplitsQuery, SplitMetadata, SplitState};

    #[async_trait]
    impl ReadWriteShardsForTest for FileBackedMetastore {
        async fn insert_shards(
            &self,
            index_uid: &IndexUid,
            source_id: &SourceId,
            shards: Vec<Shard>,
        ) {
            self.mutate(index_uid, |index| {
                index.insert_shards(source_id, shards);
                Ok(MutationOccurred::Yes(()))
            })
            .await
            .unwrap();
        }

        async fn list_all_shards(&self, index_uid: &IndexUid, source_id: &SourceId) -> Vec<Shard> {
            self.read(index_uid, |index| {
                let shards = index.list_all_shards(source_id);
                Ok(shards)
            })
            .await
            .unwrap()
        }
    }

    metastore_test_suite!(crate::FileBackedMetastore);

    #[tokio::test]
    async fn test_metastore_connectivity_and_endpoints() {
        let metastore = FileBackedMetastore::default_for_test().await;
        metastore.check_connectivity().await.unwrap();
        assert_eq!(metastore.endpoints()[0].protocol(), Protocol::Ram);
    }

    #[tokio::test]
    async fn test_file_backed_metastore_connectivity_fails_if_states_file_does_not_exist() {
        let mut mock_storage = MockStorage::default();
        let ram_storage = RamStorage::default();
        let ram_storage_clone = ram_storage.clone();
        mock_storage // remove this if we end up changing the semantics of create.
            .expect_exists()
            .times(3)
            .returning(|_| Ok(false));
        mock_storage
            .expect_put()
            .times(1)
            .returning(move |path, put_payload| {
                assert!(path == Path::new("manifest.json"));
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
        let mut metastore = FileBackedMetastore::default_for_test().await;
        assert!(!metastore.index_exists(index_id).await.unwrap());

        let index_config = IndexConfig::for_test(index_id, "ram:///indexes/test-index");
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        metastore.create_index(create_index_request).await.unwrap();

        assert!(metastore.index_exists(index_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_backed_metastore_get_index() {
        let metastore = FileBackedMetastore::default_for_test().await;

        // Create index
        let index_id = "test-index";
        let index_config = IndexConfig::for_test(index_id, "ram:///indexes/test-index");
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

        // Open index and check its metadata
        let created_index = metastore.get_index(&index_uid).await.unwrap();
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
            .await
            .unwrap();
        assert_eq!(indexes_metadata.len(), 1);

        // Open a non-existent index.
        let metastore_error = metastore
            .get_index(&IndexUid::new_with_random_ulid("index-does-not-exist"))
            .await
            .unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::NotFound { .. }));

        // Open a index with a different incarnation_id.
        let metastore_error = metastore
            .get_index(&IndexUid::new_with_random_ulid(index_id))
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
                    path == Path::new("manifest.json") || path == metastore_filepath("test-index")
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

        let index_config = IndexConfig::for_test("test-index", "ram:///indexes/test-index");
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await
            .unwrap()
            .index_uid()
            .clone();

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
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await.unwrap();

        // publish split fails
        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split_id.to_string()],
            ..Default::default()
        };
        metastore
            .publish_splits(publish_splits_request)
            .await
            .unwrap_err();

        let list_splits_query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Published);
        let list_splits_request =
            ListSplitsRequest::try_from_list_splits_query(&list_splits_query).unwrap();
        let splits = metastore
            .list_splits(list_splits_request)
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert!(splits.is_empty());

        let list_splits_query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Staged);
        let list_splits_request =
            ListSplitsRequest::try_from_list_splits_query(&list_splits_query).unwrap();
        let splits = metastore
            .list_splits(list_splits_request)
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert!(!splits.is_empty());
    }

    #[tokio::test]
    async fn test_file_backed_metastore_get_index_checks_for_inconsistent_index_id(
    ) -> MetastoreResult<()> {
        let storage = Arc::new(RamStorage::default());
        let index_id = "test-index";
        let index_metadata =
            IndexMetadata::for_test("my-inconsistent-index", "ram:///indexes/test-index");

        // Put inconsistent index and manifest into storage.
        let index = FileBackedIndex::from(index_metadata);
        put_index_given_index_id(&*storage, &index, index_id).await?;
        let mut manifest = Manifest::default();
        manifest
            .indexes
            .insert(index_id.to_string(), IndexStatus::Active);
        save_manifest(&*storage, &manifest).await.unwrap();

        let metastore = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();

        // Getting index with inconsistent index ID should raise an error.
        let metastore_error = metastore
            .get_index(&IndexUid::new_with_random_ulid(index_id))
            .await
            .unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::Internal { .. }));

        Ok(())
    }

    #[tokio::test]
    async fn test_file_backed_metastore_write_directly_visible() -> MetastoreResult<()> {
        let metastore = FileBackedMetastore::default_for_test().await;

        let index_config = IndexConfig::for_test("test-index", "ram:///indexes/test-index");
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let create_index_response = metastore.create_index(create_index_request).await.unwrap();
        let index_uid: IndexUid = create_index_response.index_uid().clone();

        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
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
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                .unwrap();
        metastore.stage_splits(stage_splits_request).await?;

        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert_eq!(splits.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_file_backed_metastore_polling() -> MetastoreResult<()> {
        let storage = Arc::new(RamStorage::default());

        let metastore_write = FileBackedMetastore::try_new(storage.clone(), None)
            .await
            .unwrap();
        let polling_interval = Duration::from_millis(20);
        let metastore_read = FileBackedMetastore::try_new(storage, Some(polling_interval))
            .await
            .unwrap();

        let index_config = IndexConfig::for_test("test-index", "ram:///indexes/test-index");
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let create_index_response = metastore_write
            .create_index(create_index_request)
            .await
            .unwrap();
        let index_uid: IndexUid = create_index_response.index_uid().clone();

        let splits = metastore_write
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert!(splits.is_empty());

        let splits = metastore_read
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
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
            StageSplitsRequest::try_from_split_metadata(index_uid.clone(), &split_metadata)
                .unwrap();
        metastore_write.stage_splits(stage_splits_request).await?;

        let splits = metastore_read
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert!(splits.is_empty());

        for _ in 0..10 {
            tokio::time::sleep(polling_interval).await;

            let splits = metastore_read
                .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
                .await
                .unwrap()
                .collect_splits()
                .await
                .unwrap();
            if !splits.is_empty() {
                return Ok(());
            }
        }
        panic!("The metastore should have been updated.");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_file_backed_metastore_race_condition() {
        let metastore = FileBackedMetastore::default_for_test().await;

        let index_config = IndexConfig::for_test("test-index", "ram:///indexes/test-index");
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let create_index_response = metastore.create_index(create_index_request).await.unwrap();
        let index_uid: IndexUid = create_index_response.index_uid().clone();

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
                    let stage_splits_request = StageSplitsRequest::try_from_split_metadata(
                        index_uid.clone(),
                        &split_metadata,
                    )
                    .unwrap();
                    metastore.stage_splits(stage_splits_request).await.unwrap();

                    tokio::time::sleep(sleep_duration).await;

                    // publish split
                    let split_id = format!("split-{i}");
                    let publish_splits_request = PublishSplitsRequest {
                        index_uid: Some(index_uid.clone()),
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
            ListSplitsRequest::try_from_list_splits_query(&list_splits_query).unwrap();
        let splits = metastore
            .list_splits(list_splits_request)
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();

        // Make sure that all 20 splits are in `Published` state.
        assert_eq!(splits.len(), 20);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_file_backed_metastore_list_indexes_race_condition() {
        let metastore = FileBackedMetastore::default_for_test().await;
        let mut index_uids = Vec::new();
        for idx in 0..10 {
            let index_uid = IndexUid::new_with_random_ulid(&format!("test-index-{idx}"));
            let index_config =
                IndexConfig::for_test(&index_uid.index_id, "ram:///indexes/test-index");
            let create_index_request =
                CreateIndexRequest::try_from_index_config(&index_config).unwrap();
            let index_uid: IndexUid = metastore
                .create_index(create_index_request)
                .await
                .unwrap()
                .index_uid()
                .clone();
            index_uids.push(index_uid);
        }
        // Delete indexes + call to list_indexes_metadata.
        let mut handles = Vec::new();
        for index_uid in index_uids {
            let delete_request = DeleteIndexRequest {
                index_uid: Some(index_uid.clone()),
            };
            {
                let metastore = metastore.clone();
                let handle = tokio::spawn(async move {
                    metastore
                        .list_indexes_metadata(ListIndexesMetadataRequest::all())
                        .await
                        .unwrap();
                });
                handles.push(handle);
            }
            {
                let metastore = metastore.clone();
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

        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("ram:///indexes"));
        mock_storage.expect_exists().returning(|_| Ok(false));
        mock_storage
            .expect_put()
            .times(1)
            .returning(move |path, _| {
                assert!(path == Path::new("manifest.json"));
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
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let metastore_error = metastore
            .create_index(create_index_request)
            .await
            .unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::Internal { .. }));
        // Try fetch the not created index.
        let created_index_error = metastore
            .get_index(&IndexUid::new_with_random_ulid(index_id))
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
        let index_uid = IndexUid::new_with_random_ulid(index_id);

        mock_storage // remove this if we end up changing the semantics of create.
            .expect_exists()
            .returning(|_| Ok(false));
        mock_storage
            .expect_put()
            .times(4)
            .returning(move |path, put_payload| {
                assert!(
                    path == Path::new("manifest.json") || path == metastore_filepath("test-index")
                );
                if path == Path::new("manifest.json") {
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
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        let metastore_error = metastore
            .create_index(create_index_request)
            .await
            .unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::Internal { .. }));
        // Let's fetch the index, we expect an internal error as the index state is in `Creating`
        // state.
        let created_index_error = metastore.get_index(&index_uid.clone()).await.unwrap_err();
        assert!(matches!(
            created_index_error,
            MetastoreError::Internal { .. }
        ));
        // Check index state is in `Creating` in the manifest file.
        let storage = Arc::new(ram_storage_clone_2.clone());
        let manifest = load_or_create_manifest(&*storage).await.unwrap();
        assert!(matches!(
            *manifest.indexes.get(index_id).unwrap(),
            IndexStatus::Creating
        ));
        // Let's delete the index to clean states.
        let delete_request = DeleteIndexRequest {
            index_uid: Some(index_uid.clone()),
        };
        let deleted_index_error = metastore.delete_index(delete_request).await.unwrap_err();
        assert!(matches!(
            deleted_index_error,
            MetastoreError::NotFound { .. }
        ));
        let manifest = load_or_create_manifest(&*storage).await.unwrap();
        assert!(!manifest.indexes.contains_key(index_id));
        // Now we can expect an `IndexDoesNotExist` error.
        let created_index_error = metastore.get_index(&index_uid).await.unwrap_err();
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

        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("ram:///indexes"));
        mock_storage // remove this if we end up changing the semantics of create.
            .expect_exists()
            .returning(|_| Ok(false));
        mock_storage
            .expect_put()
            .times(3)
            .returning(move |path, put_payload| {
                assert!(
                    path == Path::new("manifest.json") || path == metastore_filepath("test-index")
                );
                if path == Path::new("manifest.json") {
                    if indexes_json_valid_put == 0 {
                        return Err(StorageErrorKind::Io.with_error(anyhow::anyhow!(
                            "oops. perhaps there are some network problems"
                        )));
                    }
                    indexes_json_valid_put -= 1;
                }
                return block_on(ram_storage_clone.put(path, put_payload));
            });
        let metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));
        let index_config = IndexConfig::for_test(index_id, "ram:///indexes/test-index");

        // Create index
        let metastore_error = metastore
            .create_index(CreateIndexRequest::try_from_index_config(&index_config).unwrap())
            .await
            .unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::Internal { .. }));
        // Let's fetch the index, we expect an internal error as the index state is in `Creating`
        // state.
        let created_index_error = metastore
            .get_index(&IndexUid::new_with_random_ulid(index_id))
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
        let index_uid = IndexUid::new_with_random_ulid(index_id);
        let index_metadata =
            IndexMetadata::for_test(&index_uid.index_id, "ram:///indexes/test-index");
        let index = FileBackedIndex::from(index_metadata);
        put_index_given_index_id(&ram_storage, &index, &index_uid.index_id)
            .await
            .unwrap();

        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("ram:///indexes"));
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
        let delete_request = DeleteIndexRequest {
            index_uid: Some(index_uid.clone()),
        };
        let metastore_error = metastore.delete_index(delete_request).await.unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::Internal { .. }));
        // Let's fetch the index, we expect an internal error as the index state is in `Deleting`
        // state.
        let created_index_error = metastore.get_index(&index_uid).await.unwrap_err();
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
        let index_uid = IndexUid::new_with_random_ulid(index_id);
        let index_metadata =
            IndexMetadata::for_test(&index_uid.index_id, "ram:///indexes/test-index");
        let index = FileBackedIndex::from(index_metadata);
        put_index_given_index_id(&ram_storage, &index, &index_uid.index_id)
            .await
            .unwrap();
        let mut indexes_json_valid_put = 1;
        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("ram:///indexes"));
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
                assert!(path == Path::new("manifest.json"));
                if path == Path::new("manifest.json") {
                    if indexes_json_valid_put == 0 {
                        return Err(StorageErrorKind::Io.with_error(anyhow::anyhow!(
                            "oops. perhaps there are some network problems"
                        )));
                    }
                    indexes_json_valid_put -= 1;
                }
                return block_on(ram_storage_clone.put(path, put_payload));
            });
        let metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));

        // Delete index
        let delete_request = DeleteIndexRequest {
            index_uid: Some(index_uid.clone()),
        };
        let metastore_error = metastore.delete_index(delete_request).await.unwrap_err();
        assert!(matches!(metastore_error, MetastoreError::Internal { .. }));
        // Let's fetch the index, we expect an internal error as the index state is in `Deleting`
        // state.
        let created_index_error = metastore.get_index(&index_uid).await.unwrap_err();
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

        // Put index states into storage.
        let ram_storage = Arc::new(RamStorage::default());
        let mut manifest = Manifest::default();
        manifest
            .indexes
            .insert(index_id_creating.to_string(), IndexStatus::Creating);
        manifest
            .indexes
            .insert(index_id_alive.to_string(), IndexStatus::Active);
        manifest
            .indexes
            .insert(index_id_deleting.to_string(), IndexStatus::Deleting);
        save_manifest(&*ram_storage, &manifest).await.unwrap();

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
        let indexes_metadata = metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .unwrap()
            .deserialize_indexes_metadata()
            .await
            .unwrap();
        assert_eq!(indexes_metadata.len(), 1);

        // Fetch the index metadata not registered in index states json.
        metastore
            .get_index(&index_uid_unregistered.clone())
            .await
            .unwrap();

        // Now list indexes return 2 indexes metadatas as the metastore is now aware of
        // 2 alive indexes.
        let indexes_metadata = metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .unwrap()
            .deserialize_indexes_metadata()
            .await
            .unwrap();
        assert_eq!(indexes_metadata.len(), 2);

        // Let's delete indexes.
        let delete_request = DeleteIndexRequest {
            index_uid: Some(index_uid_alive.clone()),
        };
        metastore.delete_index(delete_request).await.unwrap();

        let delete_request = DeleteIndexRequest {
            index_uid: Some(index_uid_unregistered.clone()),
        };
        metastore.delete_index(delete_request).await.unwrap();
        let indexes_metadata = metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .unwrap()
            .deserialize_indexes_metadata()
            .await
            .unwrap();
        assert!(indexes_metadata.is_empty());

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
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
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
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
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

    #[tokio::test]
    async fn test_create_index_template_rollback() {
        let mut mock_storage = MockStorage::default();

        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("ram:///indexes"));

        mock_storage
            .expect_put()
            .once()
            .returning(|path, _payload| {
                assert_eq!(path, Path::new(MANIFEST_FILE_NAME));
                Ok(())
            });

        mock_storage
            .expect_put()
            .once()
            .returning(|path, _payload| {
                assert_eq!(path, Path::new(MANIFEST_FILE_NAME));
                let io_error = StorageErrorKind::Io.with_error(anyhow::anyhow!("IO error"));
                Err(io_error)
            });

        let metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));

        let index_template = IndexTemplate::for_test("test-template", &["test-index-foo*"], 100);
        let index_template_json = serde_json::to_string(&index_template).unwrap();
        let create_index_template_request = CreateIndexTemplateRequest {
            index_template_json,
            overwrite: false,
        };
        metastore
            .create_index_template(create_index_template_request)
            .await
            .unwrap();
        {
            let state = metastore.state.read().await;
            assert_eq!(state.templates.len(), 1);
            state.template_matcher.find_match("test-index-foo").unwrap();
        }
        let index_template = IndexTemplate::for_test("test-template", &["test-index-bar*"], 100);
        let index_template_json = serde_json::to_string(&index_template).unwrap();
        let create_index_template_request = CreateIndexTemplateRequest {
            index_template_json,
            overwrite: true,
        };
        metastore
            .create_index_template(create_index_template_request)
            .await
            .unwrap_err();
        {
            let state = metastore.state.read().await;
            assert_eq!(state.templates.len(), 1);
            state.template_matcher.find_match("test-index-foo").unwrap();
        }
    }

    #[tokio::test]
    async fn test_delete_index_templates_rollback() {
        let mut mock_storage = MockStorage::default();

        mock_storage
            .expect_uri()
            .return_const(Uri::for_test("ram:///indexes"));

        mock_storage
            .expect_put()
            .once()
            .returning(|path, _payload| {
                assert_eq!(path, Path::new(MANIFEST_FILE_NAME));
                Ok(())
            });

        mock_storage
            .expect_put()
            .once()
            .returning(|path, _payload| {
                assert_eq!(path, Path::new(MANIFEST_FILE_NAME));
                let io_error = StorageErrorKind::Io.with_error(anyhow::anyhow!("IO error"));
                Err(io_error)
            });

        let metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));

        let index_template = IndexTemplate::for_test("test-template", &["test-index-foo*"], 100);
        let index_template_json = serde_json::to_string(&index_template).unwrap();
        let create_index_template_request = CreateIndexTemplateRequest {
            index_template_json,
            overwrite: false,
        };
        metastore
            .create_index_template(create_index_template_request)
            .await
            .unwrap();
        {
            let state = metastore.state.read().await;
            assert_eq!(state.templates.len(), 1);
            state.template_matcher.find_match("test-index-foo").unwrap();
        }
        let delete_index_templates_request = DeleteIndexTemplatesRequest {
            template_ids: vec![index_template.template_id],
        };
        metastore
            .delete_index_templates(delete_index_templates_request)
            .await
            .unwrap_err();
        {
            let state = metastore.state.read().await;
            assert_eq!(state.templates.len(), 1);
            state.template_matcher.find_match("test-index-foo").unwrap();

            assert!(state
                .template_matcher
                .find_match("test-index-bar")
                .is_none());
        }
    }
}
