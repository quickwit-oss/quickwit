// Copyright (C) 2021 Quickwit, Inc.
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
mod index_metadata;
#[cfg(feature = "postgres")]
pub mod postgresql_metastore;

use std::ops::Range;

use async_trait::async_trait;
pub use index_metadata::IndexMetadata;
use quickwit_index_config::tag_pruning::TagFilterAst;

use crate::checkpoint::CheckpointDelta;
use crate::{MetastoreResult, Split, SplitMetadata, SplitState};

/// Metastore meant to manage Quickwit's indexes and their splits.
///
/// Quickwit needs a way to ensure that we can cleanup unused files,
/// and this process needs to be resilient to any fail-stop failures.
/// We rely on atomically transitioning the status of splits.
///
/// The split state goes through the following life cycle:
/// 1. `New`
///   - Create new split and start indexing.
/// 2. `Staged`
///   - Start uploading the split files.
/// 3. `Published`
///   - Uploading the split files is complete and the split is searchable.
/// 4. `MarkedForDeletion`
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
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait Metastore: Send + Sync + 'static {
    /// Checks if the metastore is available.
    async fn check_connectivity(&self) -> anyhow::Result<()>;

    /// Checks if the given index is in this metastore.
    async fn check_index_available(&self, index_id: &str) -> anyhow::Result<()> {
        self.index_metadata(index_id).await?;
        Ok(())
    }

    /// Creates an index.
    /// This API creates  in the metastore.
    /// An error will occur if an index that already exists in the storage is specified.
    async fn create_index(&self, index_metadata: IndexMetadata) -> MetastoreResult<()>;

    /// Returns the index_metadata for a given index.
    ///
    /// TODO consider merging with list_splits to remove one round-trip
    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata>;

    /// Deletes an index.
    /// This API removes the specified  from the metastore,
    /// but does not remove the index from the storage.
    /// An error will occur if an index that does not exist in the storage is specified.
    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()>;

    /// Stages a split.
    /// A split needs to be staged before uploading any of its files to the storage.
    /// An error will occur if an index that does not exist in the storage is specified.
    /// Also, an error will occur if you specify a split that already exists.
    async fn stage_split(
        &self,
        index_id: &str,
        split_metadata: SplitMetadata,
    ) -> MetastoreResult<()>;

    /// Publishes a list splits.
    /// This API only updates the state of the split from `Staged` to `Published`.
    /// At this point, the split files are assumed to have already been uploaded.
    /// If the split is already published, this API call returns a success.
    /// An error will occur if you specify an index or split that does not exist in the storage.
    async fn publish_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        checkpoint_delta: CheckpointDelta,
    ) -> MetastoreResult<()>;

    /// Replaces a list of splits with another list.
    /// This API is useful during merge and demux operations.
    /// The new splits should be staged, and the replaced splits should exist.
    async fn replace_splits<'a>(
        &self,
        index_id: &str,
        new_split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
    ) -> MetastoreResult<()>;

    /// Lists the splits.
    /// Returns a list of splits that intersects the given `time_range`, `split_state` and `tag`.
    /// Regardless of the time range filter, if a split has no timestamp it is always returned.
    /// An error will occur if an index that does not exist in the storage is specified.
    async fn list_splits(
        &self,
        index_id: &str,
        split_state: SplitState,
        time_range: Option<Range<i64>>,
        tags: Option<TagFilterAst>,
    ) -> MetastoreResult<Vec<Split>>;

    /// Lists the splits without filtering.
    /// Returns a list of all splits currently known to the metastore regardless of their state.
    async fn list_all_splits(&self, index_id: &str) -> MetastoreResult<Vec<Split>>;

    /// Marks a list of splits for deletion.
    /// This API will change the state to `MarkedForDeletion` so that it is not referenced by the
    /// client. It actually does not remove the split from storage.
    /// An error will occur if you specify an index or split that does not exist in the storage.
    async fn mark_splits_for_deletion<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()>;

    /// Deletes a list of splits.
    /// This API only takes splits that are in `Staged` or `MarkedForDeletion` state.
    /// This removes the split metadata from the metastore, but does not remove the split from
    /// storage. An error will occur if you specify an index or split that does not exist in the
    /// storage.
    async fn delete_splits<'a>(&self, index_id: &str, split_ids: &[&'a str])
        -> MetastoreResult<()>;

    /// Returns the Metastore uri.
    fn uri(&self) -> String;
}
