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

pub mod file_backed_metastore;
pub mod grpc_metastore;
mod index_metadata;
#[cfg(feature = "postgres")]
pub mod postgresql_metastore;
#[cfg(feature = "postgres")]
mod postgresql_model;

use std::ops::Bound;

use async_trait::async_trait;
pub use index_metadata::IndexMetadata;
use quickwit_common::uri::Uri;
use quickwit_config::SourceConfig;
use quickwit_doc_mapper::tag_pruning::TagFilterAst;
use quickwit_proto::metastore_api::{DeleteQuery, DeleteTask};

use crate::checkpoint::IndexCheckpointDelta;
use crate::{MetastoreError, MetastoreResult, Split, SplitMetadata, SplitState};

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
/// by two mecanisms:
/// - On creation of a delete task, we give to the task a monotically increasing opstamp (uniqueness
///   and monotonically increasing must be true at the index level).
/// - When a delete task is executed on a split, that is when the documents matched by the search
///   query are removed from the splits, we update the split's `delete_opstamp` to the value of the
///   task's opstamp. This marks the split as "up-to-date" regarding this delete task. If new delete
///   tasks are added, we will know that we need to run these delete tasks on the splits as its
///   `delete_optstamp` will be inferior to the `opstamp` of the new tasks.
///
/// For splits created after a given delete task, Quickwit's indexing ensures that these splits
/// are created with a `delete_optstamp` equal the lastest opstamp of the tasks of the
/// corresponding index.
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait Metastore: Send + Sync + 'static {
    /// Checks whether the metastore is available.
    async fn check_connectivity(&self) -> anyhow::Result<()>;

    /// Returns whether an index exists in the metastore.
    async fn index_exists(&self, index_id: &str) -> MetastoreResult<bool> {
        match self.index_metadata(index_id).await {
            Ok(_) => Ok(true),
            Err(MetastoreError::IndexDoesNotExist { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Creates an index.
    ///
    /// This API creates a new index in the metastore.
    /// An error will occur if an index that already exists in the storage is specified.
    async fn create_index(&self, index_metadata: IndexMetadata) -> MetastoreResult<()>;

    /// List indexes.
    ///
    /// This API lists the indexes stored in the metastore and returns a collection of
    /// [`IndexMetadata`].
    async fn list_indexes_metadatas(&self) -> MetastoreResult<Vec<IndexMetadata>>;

    /// Returns the [`IndexMetadata`] for a given index.
    /// TODO consider merging with list_splits to remove one round-trip
    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata>;

    /// Deletes an index.
    ///
    /// This API removes the specified  from the metastore, but does not remove the index from the
    /// storage. An error will occur if an index that does not exist in the storage is
    /// specified.
    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()>;

    /// Stages a split.
    ///
    /// A split needs to be staged before uploading any of its files to the storage.
    /// An error will occur if an index that does not exist in the storage is specified, or if you
    /// specify a split that already exists.
    async fn stage_split(
        &self,
        index_id: &str,
        split_metadata: SplitMetadata,
    ) -> MetastoreResult<()>;

    /// Publishes a list of splits.
    ///
    /// This API only updates the state of the split from [`SplitState::Staged`] to
    /// [`SplitState::Published`]. At this point, the split files are assumed to have already
    /// been uploaded. If the split is already published, this API call returns a success.
    /// An error will occur if you specify an index or split that does not exist in the storage.
    ///
    /// This method can be used to advance the checkpoint, by supplying an empty array for
    /// `split_ids`.
    async fn publish_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
        checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    ) -> MetastoreResult<()>;

    /// Lists the splits.
    ///
    /// Returns a list of splits that intersects the given `time_range`, `split_state`, and `tag`.
    /// Regardless of the time range filter, if a split has no timestamp it is always returned.
    /// An error will occur if an index that does not exist in the storage is specified.
    async fn list_splits<'a>(&self, filter: ListSplitsQuery<'a>) -> MetastoreResult<Vec<Split>>;

    /// Lists all the splits without filtering.
    ///
    /// Returns a list of all splits currently known to the metastore regardless of their state.
    async fn list_all_splits(&self, index_id: &str) -> MetastoreResult<Vec<Split>> {
        let filter = ListSplitsQuery::for_index(index_id);
        self.list_splits(filter).await
    }

    /// Lists splits with `split.delete_opstamp` < `delete_opstamp` for a given `index_id`.
    /// These splits are called "stale" as they have an `delete_opstamp` strictly inferior
    /// to the given `delete_opstamp`.
    async fn list_stale_splits(
        &self,
        index_id: &str,
        delete_opstamp: u64,
        num_splits: usize,
    ) -> MetastoreResult<Vec<Split>> {
        let filter = ListSplitsQuery::for_index(index_id)
            .with_opstamp_older_than(delete_opstamp)
            .with_limit(num_splits);
        self.list_splits(filter).await
    }

    /// Marks a list of splits for deletion.
    ///
    /// This API will change the state to [`SplitState::MarkedForDeletion`] so that it is not
    /// referenced by the client anymore. It actually does not remove the split from storage. An
    /// error will occur if you specify an index or split that does not exist in the storage.
    async fn mark_splits_for_deletion<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()>;

    /// Deletes a list of splits.
    ///
    /// This API only accepts splits that are in [`SplitState::Staged`] or
    /// [`SplitState::MarkedForDeletion`] state. This removes the split metadata from the
    /// metastore, but does not remove the split from storage. An error will occur if you
    /// specify an index or split that does not exist in the storage.
    async fn delete_splits<'a>(&self, index_id: &str, split_ids: &[&'a str])
        -> MetastoreResult<()>;

    /// Adds a new source. Fails with
    /// [`SourceAlreadyExists`](crate::MetastoreError::SourceAlreadyExists) if a source with the
    /// same ID is already defined for the index.
    ///
    /// If a checkpoint is already registered for the source, it is kept.
    async fn add_source(&self, index_id: &str, source: SourceConfig) -> MetastoreResult<()>;

    /// Deletes a source. Fails with
    /// [`SourceDoesNotExist`](crate::MetastoreError::SourceDoesNotExist) if the specified source
    /// does not exist.
    ///
    /// The checkpoint associated to the source is deleted as well.
    /// If the checkpoint is missing, this does not trigger an error.
    async fn delete_source(&self, index_id: &str, source_id: &str) -> MetastoreResult<()>;

    /// Resets the checkpoint of a source identified by `index_id` and `source_id`.
    async fn reset_source_checkpoint(&self, index_id: &str, source_id: &str)
        -> MetastoreResult<()>;

    /// Returns the metastore uri.
    fn uri(&self) -> &Uri;

    /// Gets the last delete opstamp for a given `index_id`.
    async fn last_delete_opstamp(&self, index_id: &str) -> MetastoreResult<u64>;

    /// Creates a [`DeleteTask`] from a [`DeleteQuery`].
    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask>;

    /// Updates splits `split_metadata.delete_opstamp` to the value `delete_opstamp`.
    async fn update_splits_delete_opstamp<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        delete_opstamp: u64,
    ) -> MetastoreResult<()>;

    /// Lists [`DeleteTask`] with `delete_task.opstamp` > `opstamp_start` for a given `index_id`.
    async fn list_delete_tasks(
        &self,
        index_id: &str,
        opstamp_start: u64,
    ) -> MetastoreResult<Vec<DeleteTask>>;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// A filter builder for filtering splits within the metastore.
pub struct ListSplitsQuery<'a> {
    /// The index to get splits from.
    pub index: &'a str,

    /// The maximum number of splits to retrieve.
    pub limit: Option<usize>,

    /// The number of splits to skip.
    pub offset: Option<usize>,

    /// The timestamp which splits must be newer than.
    pub time_range_start: Option<i64>,

    /// The timestamp which splits must be older than.
    pub time_range_end: Option<i64>,

    /// The timestamp which splits must be updated after.
    pub updated_after: Bound<i64>,

    /// The timestamp which splits must be updated before.
    pub updated_before: Bound<i64>,

    /// Filter splits with `split.delete_opstamp` < `delete_opstamp`.
    pub delete_opstamp: Option<u64>,

    /// A specific split state to filter by.
    pub split_state: Option<SplitState>,

    /// A specific set of tag(s) to filter by.
    pub tags: Option<TagFilterAst>,
}

#[allow(unused_attributes)]
impl<'a> ListSplitsQuery<'a> {
    /// Create a new index for a specific index.
    pub fn for_index(index: &'a str) -> Self {
        Self {
            index,
            limit: None,
            offset: None,
            time_range_start: None,
            time_range_end: None,
            updated_after: Bound::Unbounded,
            updated_before: Bound::Unbounded,
            delete_opstamp: None,
            split_state: None,
            tags: None,
        }
    }

    /// Set the maximum number of splits to retrieve.
    pub fn with_limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Set the number of splits to skip.
    pub fn with_offset(mut self, n: usize) -> Self {
        self.offset = Some(n);
        self
    }

    /// Select splits which are newer than or equal to this timestamp.
    pub fn with_time_range_from(mut self, ts: i64) -> Self {
        self.time_range_start = Some(ts);
        self
    }

    /// Select splits which are older than this timestamp.
    pub fn with_time_range_to(mut self, ts: i64) -> Self {
        self.time_range_end = Some(ts);
        self
    }

    /// Select splits which are newer than or equal to this timestamp.
    pub fn updated_after(mut self, ts: i64) -> Self {
        self.updated_after = Bound::Excluded(ts);
        self
    }

    /// Select splits which are newer than or equal to this timestamp.
    pub fn updated_after_or_at(mut self, ts: i64) -> Self {
        self.updated_after = Bound::Included(ts);
        self
    }

    /// Select splits which are older than this timestamp.
    pub fn updated_before(mut self, ts: i64) -> Self {
        self.updated_before = Bound::Excluded(ts);
        self
    }

    /// Select splits which are older than this timestamp.
    pub fn updated_before_or_at(mut self, ts: i64) -> Self {
        self.updated_before = Bound::Included(ts);
        self
    }

    /// Filter splits with `split.delete_opstamp` < `delete_opstamp`.
    pub fn with_opstamp_older_than(mut self, opstamp: u64) -> Self {
        self.delete_opstamp = Some(opstamp);
        self
    }

    /// Select splits which have the are in the given split state.
    pub fn with_split_state(mut self, state: SplitState) -> Self {
        self.split_state = Some(state);
        self
    }

    /// Select splits which match the given tag filter.
    pub fn with_tags_filter(mut self, tags: TagFilterAst) -> Self {
        self.tags = Some(tags);
        self
    }

    /// Returns if the filter has no additional constraints set and is
    /// just a filter by `index_id`.
    pub fn is_default(&self) -> bool {
        self.limit.is_none()
            | self.offset.is_none()
            | self.time_range_start.is_none()
            | self.time_range_end.is_none()
            | (self.updated_after == Bound::Unbounded)
            | (self.updated_before == Bound::Unbounded)
            | self.delete_opstamp.is_none()
            | self.split_state.is_none()
            | self.tags.is_none()
    }
}
