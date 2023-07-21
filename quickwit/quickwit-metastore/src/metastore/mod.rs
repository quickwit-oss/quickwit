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

pub mod file_backed_metastore;
pub mod grpc_metastore;
pub(crate) mod index_metadata;
mod instrumented_metastore;
pub mod metastore_event_publisher;
#[cfg(feature = "postgres")]
pub mod postgresql_metastore;
#[cfg(feature = "postgres")]
mod postgresql_model;
pub mod retrying_metastore;

use std::ops::{Bound, RangeInclusive};

use async_trait::async_trait;
pub use index_metadata::IndexMetadata;
use quickwit_common::uri::Uri;
use quickwit_config::{IndexConfig, SourceConfig};
use quickwit_doc_mapper::tag_pruning::TagFilterAst;
use quickwit_proto::metastore::{
    AddSourceRequest, CreateIndexRequest, CreateIndexResponse, DeleteIndexRequest, DeleteQuery,
    DeleteSourceRequest, DeleteSplitsRequest, DeleteTask, EmptyResponse, IndexMetadataRequest,
    IndexMetadataResponse, ListDeleteTasksRequest, ListDeleteTasksResponse, ListIndexesRequest,
    ListIndexesResponse, ListSplitsRequest, ListSplitsResponse, MarkSplitsForDeletionRequest,
    PublishSplitsRequest, ResetSourceCheckpointRequest, StageSplitsRequest, ToggleSourceRequest,
    UpdateSplitsDeleteOpstampRequest,
};
use quickwit_proto::IndexUid;
use time::OffsetDateTime;

use crate::{MetastoreError, MetastoreResult, SplitMetadata, SplitState};

/// Metastore meant to manage Quickwit's indexes, their splits and delete tasks.
///
/// I. Index and splits management.
///
/// Quickwit needs a way to ensure that we can cleanup unused files,
/// and this process needs to be resilient to any fail-stop failures.
/// We rely on atomically transitioning the status of splits.
///
/// The split state goes through the following life cycle:
/// 1. `Staged`
///   - Start uploading the split files.
/// 2. `Published`
///   - Uploading the split files is complete and the split is searchable.
/// 3. `MarkedForDeletion`
///   - Mark the split for deletion.
///
/// If a split has a file in the storage, it MUST be registered in the metastore,
/// and its state can be as follows:
/// - `Staged`: The split is almost ready. Some of its files may have been uploaded in the storage.
/// - `Published`: The split is ready and published.
/// - `MarkedForDeletion`: The split is marked for deletion.
///
/// Before creating any file, we need to stage the split. If there is a failure, upon recovery, we
/// schedule for deletion all the staged splits. A client may not necessarily remove files from
/// storage right after marking it for deletion. A CLI client may delete files right away, but a
/// more serious deployment should probably only delete those files after a grace period so that the
/// running search queries can complete.
///
/// II. Delete tasks management.
///
/// A delete task is defined on a given index and by a search query. It can be
/// applied to all the splits of the index.
///
/// Quickwit needs a way to track that a delete task has been applied to a split. This is ensured
/// by two mechanisms:
/// - On creation of a delete task, we give to the task a monotically increasing opstamp (uniqueness
///   and monotonically increasing must be true at the index level).
/// - When a delete task is executed on a split, that is when the documents matched by the search
///   query are removed from the splits, we update the split's `delete_opstamp` to the value of the
///   task's opstamp. This marks the split as "up-to-date" regarding this delete task. If new delete
///   tasks are added, we will know that we need to run these delete tasks on the splits as its
///   `delete_optstamp` will be inferior to the `opstamp` of the new tasks.
///
/// For splits created after a given delete task, Quickwit's indexing ensures that these splits
/// are created with a `delete_optstamp` equal the latest opstamp of the tasks of the
/// corresponding index.
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait Metastore: Send + Sync + 'static {
    /// Returns the metastore's uri.
    fn uri(&self) -> &Uri;

    /// Checks whether the metastore is available.
    async fn check_connectivity(&self) -> anyhow::Result<()>;

    // Index API

    /// Creates an index.
    ///
    /// This API creates a new index in the metastore.
    /// An error will occur if an index that already exists in the storage is specified.
    async fn create_index(
        &self,
        request: CreateIndexRequest,
    ) -> MetastoreResult<CreateIndexResponse>;

    /// Returns the [`IndexMetadata`] of an index identified by its ID.
    async fn index_metadata(
        &self,
        request: IndexMetadataRequest,
    ) -> MetastoreResult<IndexMetadataResponse>;

    /// Lists the indexes.
    ///
    /// This API lists the indexes stored in the metastore and returns a collection of
    /// [`IndexMetadata`].
    async fn list_indexes(
        &self,
        request: ListIndexesRequest,
    ) -> MetastoreResult<ListIndexesResponse>;

    /// Deletes an index.
    ///
    /// This API removes the specified  from the metastore, but does not remove the index from the
    /// storage. An error will occur if an index that does not exist in the storage is
    /// specified.
    async fn delete_index(&self, request: DeleteIndexRequest) -> MetastoreResult<EmptyResponse>;

    // Split API

    /// Stages multiple splits.
    ///
    /// If a split already exists and is not in the `Staged` state, a `SplitsNotStaged` error
    /// will be returned.
    /// Attempting to re-stage any split which is not currently `Staged` is incorrect and should not
    /// be attempted.
    ///
    /// A split needs to be staged before uploading any of its files to the storage.
    /// An error will occur if an index that does not exist in the storage is specified.
    async fn stage_splits(&self, request: StageSplitsRequest) -> MetastoreResult<EmptyResponse>;

    /// Publishes a set of staged splits while optionally marking another set of published splits
    /// for deletion.
    ///
    /// This API merely updates the state of the staged splits from [`SplitState::Staged`] to
    /// [`SplitState::Published`]. At this point, the split files are assumed to have already
    /// been uploaded.
    /// An error will occur if you specify an index or split that does not exist in the storage.
    ///
    /// This method can be used to advance the checkpoint, by supplying an empty array for
    /// `staged_split_ids`.
    async fn publish_splits(&self, request: PublishSplitsRequest)
        -> MetastoreResult<EmptyResponse>;

    /// Lists the splits.
    ///
    /// Returns a list of splits that intersects the given `time_range`, `split_state`, and `tag`.
    /// Regardless of the time range filter, if a split has no timestamp it is always returned.
    /// An error will occur if an index that does not exist in the storage is specified.
    async fn list_splits(&self, request: ListSplitsRequest) -> MetastoreResult<ListSplitsResponse>;

    /// Marks a list of splits for deletion.
    ///
    /// This API will change the state to [`SplitState::MarkedForDeletion`] so that it is not
    /// referenced by the client anymore. It actually does not remove the split from storage. An
    /// error will occur if you specify an index or split that does not exist in the storage.
    async fn mark_splits_for_deletion(
        &self,
        request: MarkSplitsForDeletionRequest,
    ) -> MetastoreResult<EmptyResponse>;

    /// Deletes a list of splits.
    ///
    /// This API only accepts splits that are in [`SplitState::Staged`] or
    /// [`SplitState::MarkedForDeletion`] state. This removes the split metadata from the
    /// metastore, but does not remove the split from storage. An error will occur if you
    /// specify an index or split that does not exist in the storage.
    async fn delete_splits(&self, request: DeleteSplitsRequest) -> MetastoreResult<EmptyResponse>;

    // Source API

    /// Adds a new source. Fails with
    /// [`SourceAlreadyExists`](crate::MetastoreError::SourceAlreadyExists) if a source with the
    /// same ID is already defined for the index.
    ///
    /// If a checkpoint is already registered for the source, it is kept.
    async fn add_source(&self, request: AddSourceRequest) -> MetastoreResult<EmptyResponse>;

    /// Enables or Disables a source.
    /// Fails with `SourceDoesNotExist` error if the specified source doesn't exist.
    async fn toggle_source(&self, request: ToggleSourceRequest) -> MetastoreResult<EmptyResponse>;

    /// Resets the checkpoint of a source identified by `index_uid` and `source_id`.
    async fn reset_source_checkpoint(
        &self,
        request: ResetSourceCheckpointRequest,
    ) -> MetastoreResult<EmptyResponse>;

    /// Deletes a source. Fails with
    /// [`SourceDoesNotExist`](crate::MetastoreError::SourceDoesNotExist) if the specified source
    /// does not exist.
    ///
    /// The checkpoint associated to the source is deleted as well.
    /// If the checkpoint is missing, this does not trigger an error.
    async fn delete_source(&self, request: DeleteSourceRequest) -> MetastoreResult<EmptyResponse>;

    // Delete tasks API

    /// Creates a new [`DeleteTask`] from a [`DeleteQuery`].
    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask>;

    /// Retrieves the last delete opstamp for a given `index_uid`.
    async fn last_delete_opstamp(&self, index_uid: IndexUid) -> MetastoreResult<u64>;

    /// Updates splits `split_metadata.delete_opstamp` to the value `delete_opstamp`.
    async fn update_splits_delete_opstamp(
        &self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> MetastoreResult<EmptyResponse>;

    /// Lists [`DeleteTask`] with `delete_task.opstamp` > `opstamp_start` for a given `index_id`.
    async fn list_delete_tasks(
        &self,
        request: ListDeleteTasksRequest,
    ) -> MetastoreResult<ListDeleteTasksResponse>;
}

pub trait CreateIndexRequestExt {
    fn try_from_index_config(index_config: IndexConfig) -> MetastoreResult<CreateIndexRequest>;

    fn deserialize_index_config(&self) -> MetastoreResult<IndexConfig>;
}

impl CreateIndexRequestExt for CreateIndexRequest {
    fn try_from_index_config(index_config: IndexConfig) -> MetastoreResult<CreateIndexRequest> {
        let index_config_json = serde_json::to_string(&index_config).map_err(|error| {
            MetastoreError::JsonSerializeError {
                struct_name: "IndexConfig".to_string(),
                message: format!("Failed to serialize index config: {error:?}"),
            }
        })?;
        let index_id = index_config.index_id;
        let request = Self {
            index_id,
            index_config_json,
        };
        Ok(request)
    }

    fn deserialize_index_config(&self) -> MetastoreResult<IndexConfig> {
        serde_json::from_str(&self.index_config_json).map_err(|error| {
            MetastoreError::JsonDeserializeError {
                struct_name: "IndexConfig".to_string(),
                message: format!("Failed to deserialize index config: {error:?}"),
            }
        })
    }
}

pub trait IndexMetadataResponseExt {
    fn try_from_index_metadata(
        index_metadata: IndexMetadata,
    ) -> MetastoreResult<IndexMetadataResponse>;

    fn deserialize_index_metadata(&self) -> MetastoreResult<IndexMetadata>;
}

impl IndexMetadataResponseExt for IndexMetadataResponse {
    fn try_from_index_metadata(index_metadata: IndexMetadata) -> MetastoreResult<Self> {
        let index_metadata_json = serde_json::to_string(&index_metadata).map_err(|error| {
            MetastoreError::JsonSerializeError {
                struct_name: "IndexMetadata".to_string(),
                message: format!("Failed to serialize index metadata: {error:?}"),
            }
        })?;
        let request = Self {
            index_metadata_json,
        };
        Ok(request)
    }

    fn deserialize_index_metadata(&self) -> MetastoreResult<IndexMetadata> {
        serde_json::from_str(&self.index_metadata_json).map_err(|error| {
            MetastoreError::JsonDeserializeError {
                struct_name: "IndexMetadata".to_string(),
                message: format!("Failed to deserialize index metadata: {error:?}"),
            }
        })
    }
}

pub trait ListIndexesResponseExt {
    fn try_from_indexes_metadata(
        indexes_metadata: impl IntoIterator<Item = IndexMetadata>,
    ) -> MetastoreResult<ListIndexesResponse>;

    fn deserialize_indexes_metadata(&self) -> MetastoreResult<Vec<IndexMetadata>>;
}

impl ListIndexesResponseExt for ListIndexesResponse {
    fn try_from_indexes_metadata(
        indexes_metadata: impl IntoIterator<Item = IndexMetadata>,
    ) -> MetastoreResult<Self> {
        let indexes_metadata: Vec<IndexMetadata> = indexes_metadata.into_iter().collect();
        let indexes_metadata_json = serde_json::to_string(&indexes_metadata).map_err(|error| {
            MetastoreError::JsonSerializeError {
                struct_name: "Vec<IndexMetadata>".to_string(),
                message: format!("Failed to serialize indexes metadata: {error:?}"),
            }
        })?;
        let request = Self {
            indexes_metadata_json,
        };
        Ok(request)
    }

    fn deserialize_indexes_metadata(&self) -> MetastoreResult<Vec<IndexMetadata>> {
        serde_json::from_str(&self.indexes_metadata_json).map_err(|error| {
            MetastoreError::JsonDeserializeError {
                struct_name: "Vec<IndexMetadata>".to_string(),
                message: format!("Failed to deserialize indexes metadata: {error:?}"),
            }
        })
    }
}

pub trait AddSourceRequestExt {
    fn try_from_source_config(
        index_uid: impl Into<IndexUid>,
        source_config: SourceConfig,
    ) -> MetastoreResult<AddSourceRequest>;

    fn deserialize_source_config(&self) -> MetastoreResult<SourceConfig>;
}

impl AddSourceRequestExt for AddSourceRequest {
    fn try_from_source_config(
        index_uid: impl Into<IndexUid>,
        source_config: SourceConfig,
    ) -> MetastoreResult<AddSourceRequest> {
        let source_config_json = serde_json::to_string(&source_config).map_err(|error| {
            MetastoreError::JsonSerializeError {
                struct_name: "SourceConfig".to_string(),
                message: format!("Failed to serialize source config: {error:?}"),
            }
        })?;
        let request = Self {
            index_uid: index_uid.into().into(),
            source_config_json,
        };
        Ok(request)
    }

    fn deserialize_source_config(&self) -> MetastoreResult<SourceConfig> {
        serde_json::from_str(&self.source_config_json).map_err(|error| {
            MetastoreError::JsonDeserializeError {
                struct_name: "SourceConfig".to_string(),
                message: format!("Failed to deserialize source config: {error:?}"),
            }
        })
    }
}

pub trait StageSplitsRequestExt {
    fn try_from_split_metadata(
        index_uid: impl Into<IndexUid>,
        split_metadata: SplitMetadata,
    ) -> MetastoreResult<StageSplitsRequest>;

    fn try_from_splits_metadata(
        index_uid: impl Into<IndexUid>,
        splits_metadata: impl IntoIterator<Item = SplitMetadata>,
    ) -> MetastoreResult<StageSplitsRequest>;

    fn deserialize_splits_metadata(&self) -> MetastoreResult<Vec<SplitMetadata>>;
}

impl StageSplitsRequestExt for StageSplitsRequest {
    fn try_from_split_metadata(
        index_uid: impl Into<IndexUid>,
        split_metadata: SplitMetadata,
    ) -> MetastoreResult<StageSplitsRequest> {
        let splits_metadata_json = serde_json::to_string(&[split_metadata]).map_err(|error| {
            MetastoreError::JsonSerializeError {
                struct_name: "SplitMetadata".to_string(),
                message: format!("Failed to serialize split metadata: {error:?}"),
            }
        })?;
        let request = Self {
            index_uid: index_uid.into().into(),
            splits_metadata_json,
        };
        Ok(request)
    }

    fn try_from_splits_metadata(
        index_uid: impl Into<IndexUid>,
        splits_metadata: impl IntoIterator<Item = SplitMetadata>,
    ) -> MetastoreResult<StageSplitsRequest> {
        let splits_metadata: Vec<SplitMetadata> = splits_metadata.into_iter().collect();
        let splits_metadata_json = serde_json::to_string(&splits_metadata).map_err(|error| {
            MetastoreError::JsonSerializeError {
                struct_name: "Vec<SplitMetadata>".to_string(),
                message: format!("Failed to serialize splits metadata: {error:?}"),
            }
        })?;
        let request = Self {
            index_uid: index_uid.into().into(),
            splits_metadata_json,
        };
        Ok(request)
    }

    fn deserialize_splits_metadata(&self) -> MetastoreResult<Vec<SplitMetadata>> {
        serde_json::from_str(&self.splits_metadata_json).map_err(|error| {
            MetastoreError::JsonDeserializeError {
                struct_name: "Vec<SplitMetadata>".to_string(),
                message: format!("Failed to deserialize splits metadata: {error:?}"),
            }
        })
    }
}

pub trait ListSplitsRequestExt {
    fn try_from_list_splits_query(
        index_uid: impl Into<IndexUid>,
        list_splits_query: ListSplitsQuery,
    ) -> MetastoreResult<ListSplitsRequest>;

    fn deserialize_list_splits_query(&self) -> MetastoreResult<ListSplitsQuery>;
}

impl ListSplitsRequestExt for ListSplitsRequest {
    fn try_from_list_splits_query(
        index_uid: impl Into<IndexUid>,
        list_splits_query: ListSplitsQuery,
    ) -> MetastoreResult<ListSplitsRequest> {
        let list_splits_query_json =
            serde_json::to_string(&list_splits_query).map_err(|error| {
                MetastoreError::JsonSerializeError {
                    struct_name: "ListSplitsQuery".to_string(),
                    message: format!("Failed to serialize list splits query: {error:?}"),
                }
            })?;
        let request = Self {
            index_uid: index_uid.into().into(),
            list_splits_query_json: Some(list_splits_query_json),
        };
        Ok(request)
    }

    fn deserialize_list_splits_query(&self) -> MetastoreResult<ListSplitsQuery> {
        let list_splits_query = self
            .list_splits_query_json
            .as_ref()
            .map(|list_splits_query_json| serde_json::from_str(&list_splits_query_json))
            .transpose()
            .map_err(|error| MetastoreError::JsonDeserializeError {
                struct_name: "ListSplitsQuery".to_string(),
                message: format!("Failed to deserialize list splits query: {error:?}"),
            })?
            .unwrap_or_default();
        Ok(list_splits_query)
    }
}

pub trait ListSplitsResponseExt {
    fn try_from_splits_metadata(
        splits_metadata: impl IntoIterator<Item = SplitMetadata>,
    ) -> MetastoreResult<ListSplitsResponse>;

    fn deserialize_splits_metadata(&self) -> MetastoreResult<Vec<SplitMetadata>>;
}

impl ListSplitsResponseExt for ListSplitsResponse {
    fn try_from_splits_metadata(
        splits_metadata: impl IntoIterator<Item = SplitMetadata>,
    ) -> MetastoreResult<Self> {
        let splits_metadata: Vec<SplitMetadata> = splits_metadata.into_iter().collect();
        let splits_metadata_json = serde_json::to_string(&splits_metadata).map_err(|error| {
            MetastoreError::JsonSerializeError {
                struct_name: "Vec<SplitMetadata>".to_string(),
                message: format!("Failed to serialize splits metadata: {error:?}"),
            }
        })?;
        let request = Self {
            splits_metadata_json,
        };
        Ok(request)
    }

    fn deserialize_splits_metadata(&self) -> MetastoreResult<Vec<SplitMetadata>> {
        serde_json::from_str(&self.splits_metadata_json).map_err(|error| {
            MetastoreError::JsonDeserializeError {
                struct_name: "Vec<SplitMetadata>".to_string(),
                message: format!("Failed to deserialize splits metadata: {error:?}"),
            }
        })
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// A query builder for listing splits within the metastore.
pub struct ListSplitsQuery {
    /// The maximum number of splits to retrieve.
    pub limit: Option<usize>,

    /// The number of splits to skip.
    pub offset: Option<usize>,

    /// A specific split state(s) to filter by.
    pub split_states: Vec<SplitState>,

    /// A specific set of tag(s) to filter by.
    pub tags: Option<TagFilterAst>,

    /// The time range to filter by.
    pub time_range: FilterRange<i64>,

    /// The delete opstamp range to filter by.
    pub delete_opstamp: FilterRange<u64>,

    /// The update timestamp range to filter by.
    pub update_timestamp: FilterRange<i64>,

    /// The create timestamp range to filter by.
    pub create_timestamp: FilterRange<i64>,

    /// The datetime at which you include or exclude mature splits.
    pub mature: Bound<OffsetDateTime>,

    /// Sorts the splits by staleness, i.e. by delete opstamp and publish timestamp in ascending
    /// order.
    pub sort_by_staleness: bool,
}

impl Default for ListSplitsQuery {
    fn default() -> Self {
        Self {
            limit: None,
            offset: None,
            split_states: Vec::new(),
            tags: None,
            time_range: Default::default(),
            delete_opstamp: Default::default(),
            update_timestamp: Default::default(),
            create_timestamp: Default::default(),
            mature: Bound::Unbounded,
            sort_by_staleness: false,
        }
    }
}

#[allow(unused_attributes)]
impl ListSplitsQuery {
    /// Sets the maximum number of splits to retrieve.
    pub fn with_limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Sets the number of splits to skip.
    pub fn with_offset(mut self, n: usize) -> Self {
        self.offset = Some(n);
        self
    }

    /// Select splits which have the given split state.
    pub fn with_split_state(mut self, state: SplitState) -> Self {
        self.split_states.push(state);
        self
    }

    /// Select splits which have the any of the following split state.
    pub fn with_split_states(mut self, states: impl AsRef<[SplitState]>) -> Self {
        self.split_states.extend_from_slice(states.as_ref());
        self
    }

    /// Select splits which match the given tag filter.
    pub fn with_tags_filter(mut self, tags: TagFilterAst) -> Self {
        self.tags = Some(tags);
        self
    }

    /// Set the field's lower bound to match values that are
    /// *less than or equal to* the provided value.
    pub fn with_time_range_end_lte(mut self, v: i64) -> Self {
        self.time_range.end = Bound::Included(v);
        self
    }

    /// Set the field's lower bound to match values that are
    /// *less than* the provided value.
    pub fn with_time_range_end_lt(mut self, v: i64) -> Self {
        self.time_range.end = Bound::Excluded(v);
        self
    }

    /// Set the field's upper bound to match values that are
    /// *greater than or equal to* the provided value.
    pub fn with_time_range_start_gte(mut self, v: i64) -> Self {
        self.time_range.start = Bound::Included(v);
        self
    }

    /// Set the field's upper bound to match values that are
    /// *greater than* the provided value.
    pub fn with_time_range_start_gt(mut self, v: i64) -> Self {
        self.time_range.start = Bound::Excluded(v);
        self
    }

    /// Set the field's lower bound to match values that are
    /// *less than or equal to* the provided value.
    pub fn with_delete_opstamp_lte(mut self, v: u64) -> Self {
        self.delete_opstamp.end = Bound::Included(v);
        self
    }

    /// Set the field's lower bound to match values that are
    /// *less than* the provided value.
    pub fn with_delete_opstamp_lt(mut self, v: u64) -> Self {
        self.delete_opstamp.end = Bound::Excluded(v);
        self
    }

    /// Set the field's upper bound to match values that are
    /// *greater than or equal to* the provided value.
    pub fn with_delete_opstamp_gte(mut self, v: u64) -> Self {
        self.delete_opstamp.start = Bound::Included(v);
        self
    }

    /// Set the field's upper bound to match values that are
    /// *greater than* the provided value.
    pub fn with_delete_opstamp_gt(mut self, v: u64) -> Self {
        self.delete_opstamp.start = Bound::Excluded(v);
        self
    }

    /// Set the field's lower bound to match values that are
    /// *less than or equal to* the provided value.
    pub fn with_update_timestamp_lte(mut self, v: i64) -> Self {
        self.update_timestamp.end = Bound::Included(v);
        self
    }

    /// Set the field's lower bound to match values that are
    /// *less than* the provided value.
    pub fn with_update_timestamp_lt(mut self, v: i64) -> Self {
        self.update_timestamp.end = Bound::Excluded(v);
        self
    }

    /// Set the field's upper bound to match values that are
    /// *greater than or equal to* the provided value.
    pub fn with_update_timestamp_gte(mut self, v: i64) -> Self {
        self.update_timestamp.start = Bound::Included(v);
        self
    }

    /// Set the field's upper bound to match values that are
    /// *greater than* the provided value.
    pub fn with_update_timestamp_gt(mut self, v: i64) -> Self {
        self.update_timestamp.start = Bound::Excluded(v);
        self
    }

    /// Set the field's lower bound to match values that are
    /// *less than or equal to* the provided value.
    pub fn with_create_timestamp_lte(mut self, v: i64) -> Self {
        self.create_timestamp.end = Bound::Included(v);
        self
    }

    /// Set the field's lower bound to match values that are
    /// *less than* the provided value.
    pub fn with_create_timestamp_lt(mut self, v: i64) -> Self {
        self.create_timestamp.end = Bound::Excluded(v);
        self
    }

    /// Set the field's upper bound to match values that are
    /// *greater than or equal to* the provided value.
    pub fn with_create_timestamp_gte(mut self, v: i64) -> Self {
        self.create_timestamp.start = Bound::Included(v);
        self
    }

    /// Set the field's upper bound to match values that are
    /// *greater than* the provided value.
    pub fn with_create_timestamp_gt(mut self, v: i64) -> Self {
        self.create_timestamp.start = Bound::Excluded(v);
        self
    }

    /// Retains splits that are mature at the given datetime.
    pub fn retain_mature(mut self, now: OffsetDateTime) -> Self {
        self.mature = Bound::Included(now);
        self
    }

    /// Retains splits that are immature at the given datetime.
    pub fn retain_immature(mut self, now: OffsetDateTime) -> Self {
        self.mature = Bound::Excluded(now);
        self
    }

    /// Sorts the splits by staleness, i.e. by delete opstamp and publish timestamp in ascending
    /// order.
    pub fn sort_by_staleness(mut self) -> Self {
        self.sort_by_staleness = true;
        self
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// A range containing the upper and lower bounds to filter documents by.
pub struct FilterRange<T> {
    /// The lower bound of the filter.
    pub start: Bound<T>,
    /// The upper bound of the filter.
    pub end: Bound<T>,
}

impl<T: PartialEq + PartialOrd> FilterRange<T> {
    /// Checks if both the upper and lower bound are `Bound::Unbounded`.
    pub fn is_unbounded(&self) -> bool {
        self.start == Bound::Unbounded && self.end == Bound::Unbounded
    }

    /// Checks if the provided value lies within the upper and lower bounds
    /// of the range.
    pub fn contains(&self, value: &T) -> bool {
        if self.is_unbounded() {
            return true;
        }

        let lower_check = match &self.start {
            Bound::Unbounded => true,
            Bound::Included(left) => left <= value,
            Bound::Excluded(left) => left < value,
        };

        let upper_check = match &self.end {
            Bound::Unbounded => true,
            Bound::Included(left) => left >= value,
            Bound::Excluded(left) => left > value,
        };

        lower_check && upper_check
    }

    /// Checks if the provided range overlaps with the range.
    pub fn overlaps_with(&self, range: RangeInclusive<T>) -> bool {
        if self.is_unbounded() {
            return true;
        }

        let lower_check = match &self.start {
            Bound::Unbounded => true,
            Bound::Included(left) => left <= range.end(),
            Bound::Excluded(left) => left < range.end(),
        };

        let upper_check = match &self.end {
            Bound::Unbounded => true,
            Bound::Included(left) => left >= range.start(),
            Bound::Excluded(left) => left > range.start(),
        };

        lower_check && upper_check
    }
}

// The `Default` derive implementation imposes a restriction
// for `T` to also implement Default when this is not required.
impl<T> Default for FilterRange<T> {
    fn default() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }
}

#[cfg(test)]
mod list_splits_query_tests {
    use super::*;

    #[test]
    fn test_filter_contains() {
        let filter = FilterRange {
            start: Bound::Unbounded,
            end: Bound::Excluded(50),
        };
        assert!(!filter.contains(&50));
        assert!(filter.contains(&0));
        assert!(filter.contains(&49));

        let filter = FilterRange {
            start: Bound::Included(50),
            end: Bound::Unbounded,
        };
        assert!(filter.contains(&50));
        assert!(filter.contains(&51));
        assert!(!filter.contains(&0));

        let filter = FilterRange {
            start: Bound::Included(50),
            end: Bound::Excluded(75),
        };
        assert!(filter.contains(&50));
        assert!(filter.contains(&51));
        assert!(!filter.contains(&0));
        assert!(!filter.contains(&75));
        assert!(filter.contains(&74));
    }

    #[test]
    fn test_overlaps_with() {
        let filter = FilterRange {
            start: Bound::Unbounded,
            end: Bound::Excluded(50),
        };
        assert!(filter.overlaps_with(0..=50));
        assert!(filter.overlaps_with(0..=51));
        assert!(filter.overlaps_with(32..=63));
        assert!(filter.overlaps_with(32..=32));
        assert!(!filter.overlaps_with(51..=76));
        assert!(!filter.overlaps_with(50..=76));

        let filter = FilterRange {
            start: Bound::Unbounded,
            end: Bound::Included(50),
        };
        assert!(filter.overlaps_with(0..=50));
        assert!(filter.overlaps_with(0..=51));
        assert!(filter.overlaps_with(50..=76));
        assert!(!filter.overlaps_with(51..=76));

        let filter = FilterRange {
            start: Bound::Excluded(50),
            end: Bound::Unbounded,
        };
        assert!(filter.overlaps_with(51..=75));
        assert!(filter.overlaps_with(0..=51));
        assert!(filter.overlaps_with(51..=76));
        assert!(filter.overlaps_with(50..=76));
        assert!(!filter.overlaps_with(0..=49));
        assert!(!filter.overlaps_with(0..=50));

        let filter = FilterRange {
            start: Bound::Included(50),
            end: Bound::Unbounded,
        };
        assert!(filter.overlaps_with(51..=75));
        assert!(filter.overlaps_with(0..=51));
        assert!(filter.overlaps_with(51..=76));
        assert!(filter.overlaps_with(50..=76));
        assert!(filter.overlaps_with(0..=50));
        assert!(!filter.overlaps_with(0..=49));

        let filter = FilterRange {
            start: Bound::Included(50),
            end: Bound::Excluded(75),
        };
        assert!(filter.overlaps_with(51..=75));
        assert!(filter.overlaps_with(0..=51));
        assert!(filter.overlaps_with(45..=76));
        assert!(filter.overlaps_with(50..=76));
        assert!(filter.overlaps_with(0..=50));
        assert!(filter.overlaps_with(74..=124));
        assert!(!filter.overlaps_with(0..=49));
        assert!(!filter.overlaps_with(75..=124));
    }
}
