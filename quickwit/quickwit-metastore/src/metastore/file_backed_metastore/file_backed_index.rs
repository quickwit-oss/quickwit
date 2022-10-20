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

//! [`FileBackedIndex`] module. It is public so that the crate `quickwit-backward-compat` can
//! import [`FileBackedIndex`] and run backward-compatibility tests. You should not have to import
//! anything from here directly.

use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Bound;

use itertools::Itertools;
use quickwit_config::SourceConfig;
use quickwit_proto::metastore_api::{DeleteQuery, DeleteTask};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::checkpoint::IndexCheckpointDelta;
use crate::{
    split_tag_filter, IndexMetadata, MetastoreError, MetastoreResult, Split, ListSplitsQuery,
    SplitMetadata, SplitState,
};

/// A `FileBackedIndex` object carries an index metadata and its split metadata.
// This struct is meant to be used only within the [`FileBackedMetastore`]. The public visibility is
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(into = "VersionedFileBackedIndex")]
#[serde(from = "VersionedFileBackedIndex")]
pub struct FileBackedIndex {
    /// Metadata specific to the index.
    metadata: IndexMetadata,
    /// List of splits belonging to the index.
    splits: HashMap<String, Split>,
    /// Delete tasks.
    delete_tasks: Vec<DeleteTask>,
    /// Stamper.
    stamper: Stamper,
    /// Has been discarded. This field exists to make
    /// it possible to discard this entry if there is an error
    /// while mutating the Index.
    pub discarded: bool,
}

impl From<IndexMetadata> for FileBackedIndex {
    fn from(index_metadata: IndexMetadata) -> Self {
        Self {
            metadata: index_metadata,
            splits: Default::default(),
            delete_tasks: Default::default(),
            stamper: Default::default(),
            discarded: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "version")]
pub(crate) enum VersionedFileBackedIndex {
    #[serde(rename = "0")]
    V0(FileBackedIndexV0),
}

impl From<FileBackedIndex> for VersionedFileBackedIndex {
    fn from(index: FileBackedIndex) -> Self {
        VersionedFileBackedIndex::V0(index.into())
    }
}

impl From<VersionedFileBackedIndex> for FileBackedIndex {
    fn from(index: VersionedFileBackedIndex) -> Self {
        match index {
            VersionedFileBackedIndex::V0(index) => index.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FileBackedIndexV0 {
    #[serde(rename = "index")]
    metadata: IndexMetadata,
    splits: Vec<Split>,
    #[serde(default)]
    delete_tasks: Vec<DeleteTask>,
}

impl From<FileBackedIndex> for FileBackedIndexV0 {
    fn from(index: FileBackedIndex) -> Self {
        Self {
            metadata: index.metadata,
            splits: index
                .splits
                .into_values()
                .sorted_by_key(|split| split.update_timestamp)
                .collect(),
            delete_tasks: index
                .delete_tasks
                .into_iter()
                .sorted_by_key(|delete_task| delete_task.opstamp)
                .collect(),
        }
    }
}

impl From<FileBackedIndexV0> for FileBackedIndex {
    fn from(mut index: FileBackedIndexV0) -> Self {
        // Override split index_id to support old SplitMetadata format.
        for split in index.splits.iter_mut() {
            if split.split_metadata.index_id.is_empty() {
                split.split_metadata.index_id = index.metadata.index_id.clone();
            }
        }
        Self::new(index.metadata, index.splits, index.delete_tasks)
    }
}

enum DeleteSplitOutcome {
    Success,
    SplitNotFound,
    ForbiddenBecausePublished,
}

impl FileBackedIndex {
    /// Constructor.
    pub fn new(metadata: IndexMetadata, splits: Vec<Split>, delete_tasks: Vec<DeleteTask>) -> Self {
        let last_opstamp = delete_tasks
            .iter()
            .map(|delete_task| delete_task.opstamp)
            .max()
            .unwrap_or(0) as usize;
        Self {
            metadata,
            splits: splits
                .into_iter()
                .map(|split| (split.split_id().to_string(), split))
                .collect(),
            delete_tasks,
            stamper: Stamper::new(last_opstamp),
            discarded: false,
        }
    }

    /// Index ID accessor.
    pub fn index_id(&self) -> &str {
        &self.metadata.index_id
    }

    /// Index metadata accessor.
    pub fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    /// Splits accessor.
    pub fn splits(&self) -> &HashMap<String, Split> {
        &self.splits
    }

    pub(crate) fn stage_split(
        &mut self,
        split_metadata: SplitMetadata,
    ) -> crate::MetastoreResult<()> {
        // Check whether the split exists.
        // If the split exists, return an error to prevent the split from being registered.
        if self.splits.contains_key(split_metadata.split_id()) {
            return Err(MetastoreError::InternalError {
                message: format!(
                    "Failed to stage split  `{}`: split already exists.",
                    split_metadata.split_id()
                ),
                cause: "".to_string(),
            });
        }

        let now_timestamp = OffsetDateTime::now_utc().unix_timestamp();
        let metadata = Split {
            split_state: SplitState::Staged,
            update_timestamp: now_timestamp,
            publish_timestamp: None,
            split_metadata,
        };

        self.splits
            .insert(metadata.split_id().to_string(), metadata);
        self.metadata.update_timestamp = now_timestamp;
        Ok(())
    }

    /// Marks the splits for deletion. Returns whether a mutation occurred.
    pub(crate) fn mark_splits_for_deletion(
        &mut self,
        split_ids: &[&str],
        deletable_states: &[SplitState],
    ) -> MetastoreResult<bool> {
        let mut is_modified = false;
        let mut split_not_found_ids = Vec::new();
        let mut non_deletable_split_ids = Vec::new();
        let now_timestamp = OffsetDateTime::now_utc().unix_timestamp();

        for &split_id in split_ids {
            // Check for the existence of split.
            let metadata = match self.splits.get_mut(split_id) {
                Some(metadata) => metadata,
                None => {
                    split_not_found_ids.push(split_id.to_string());
                    continue;
                }
            };
            if !deletable_states.contains(&metadata.split_state) {
                non_deletable_split_ids.push(split_id.to_string());
                continue;
            };
            if metadata.split_state == SplitState::MarkedForDeletion {
                // If the split is already marked for deletion, This is fine, we just skip it.
                continue;
            }

            metadata.split_state = SplitState::MarkedForDeletion;
            metadata.update_timestamp = now_timestamp;
            is_modified = true;
        }
        if !split_not_found_ids.is_empty() {
            return Err(MetastoreError::SplitsDoNotExist {
                split_ids: split_not_found_ids,
            });
        }
        if !non_deletable_split_ids.is_empty() {
            return Err(MetastoreError::SplitsNotDeletable {
                split_ids: non_deletable_split_ids,
            });
        }
        if is_modified {
            self.metadata.update_timestamp = now_timestamp;
        }
        Ok(is_modified)
    }

    /// Helper to mark a list of splits as published.
    /// This function however does not update the checkpoint.
    fn mark_splits_as_published_helper<'a>(
        &mut self,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        let mut split_not_found_ids = vec![];
        let mut split_not_staged_ids = vec![];
        let now_timestamp = OffsetDateTime::now_utc().unix_timestamp();
        for &split_id in split_ids {
            // Check for the existence of split.
            let metadata = match self.splits.get_mut(split_id) {
                Some(metadata) => metadata,
                _ => {
                    split_not_found_ids.push(split_id.to_string());
                    continue;
                }
            };

            match metadata.split_state {
                SplitState::Published => {
                    // Split is already published. This is fine, we just skip it.
                    continue;
                }
                SplitState::Staged => {
                    // The split state needs to be updated.
                    metadata.split_state = SplitState::Published;
                    metadata.update_timestamp = now_timestamp;
                    metadata.publish_timestamp = Some(now_timestamp);
                }
                _ => {
                    split_not_staged_ids.push(split_id.to_string());
                }
            }
        }

        if !split_not_found_ids.is_empty() {
            return Err(MetastoreError::SplitsDoNotExist {
                split_ids: split_not_found_ids,
            });
        }

        if !split_not_staged_ids.is_empty() {
            return Err(MetastoreError::SplitsNotStaged {
                split_ids: split_not_staged_ids,
            });
        }

        self.metadata.update_timestamp = now_timestamp;
        Ok(())
    }

    /// Publishes splits.
    pub(crate) fn publish_splits<'a>(
        &mut self,
        split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
        checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    ) -> MetastoreResult<()> {
        if let Some(checkpoint_delta) = checkpoint_delta_opt {
            self.metadata.checkpoint.try_apply_delta(checkpoint_delta)?;
        }
        self.mark_splits_as_published_helper(split_ids)?;
        self.mark_splits_for_deletion(replaced_split_ids, &[SplitState::Published])?;
        Ok(())
    }

    /// Lists splits.
    pub(crate) fn list_splits(&self, filter: ListSplitsQuery<'_>) -> MetastoreResult<Vec<Split>> {
        let split_state_filter = |split: &&Split| {
            filter
                .split_state
                .map(|state| split.split_state == state)
                .unwrap_or(true)
        };
        let time_range_start_filter = |split: &&Split| {
            filter
                .time_range_start
                .and_then(|ts| Some((ts, split.split_metadata.time_range.as_ref()?)))
                .map(|(ts, range)| range.start() >= &ts)
                .unwrap_or(true)
        };
        let time_range_end_filter = |split: &&Split| {
            filter
                .time_range_end
                .and_then(|ts| Some((ts, split.split_metadata.time_range.as_ref()?)))
                .map(|(ts, range)| range.end() < &ts)
                .unwrap_or(true)
        };
        let update_after_filter = |split: &&Split| match filter.updated_after {
            Bound::Excluded(ts) => split.update_timestamp > ts,
            Bound::Included(ts) => split.update_timestamp >= ts,
            Bound::Unbounded => true,
        };
        let update_before_filter = |split: &&Split| match filter.updated_before {
            Bound::Excluded(ts) => split.update_timestamp < ts,
            Bound::Included(ts) => split.update_timestamp <= ts,
            Bound::Unbounded => true,
        };
        let delete_opstamp_filter = |split: &&Split| {
            filter
                .delete_opstamp
                .map(|delete_opstamp| split.split_metadata.delete_opstamp < delete_opstamp)
                .unwrap_or(true)
        };

        let splits = self
            .splits
            .values()
            .filter(split_state_filter)
            .filter(time_range_start_filter)
            .filter(time_range_end_filter)
            .filter(update_after_filter)
            .filter(update_before_filter)
            .filter(|split| split_tag_filter(split, filter.tags.as_ref()))
            .filter(delete_opstamp_filter)
            .skip(filter.offset.unwrap_or_default())
            .take(filter.limit.unwrap_or(usize::MAX))
            .cloned()
            .collect();

        Ok(splits)
    }

    /// Lists all splits.
    pub(crate) fn list_all_splits(&self) -> MetastoreResult<Vec<Split>> {
        let splits = self.splits.values().cloned().collect();
        Ok(splits)
    }

    /// Deletes a split.
    fn delete_split(&mut self, split_id: &str) -> DeleteSplitOutcome {
        match self.splits.get(split_id).map(|split| split.split_state) {
            // Only `Staged` and `MarkedForDeletion` splits can be deleted
            Some(SplitState::Staged | SplitState::MarkedForDeletion) => {
                self.splits.remove(split_id);
                DeleteSplitOutcome::Success
            }
            Some(SplitState::Published) => DeleteSplitOutcome::ForbiddenBecausePublished,
            None => DeleteSplitOutcome::SplitNotFound,
        }
    }

    /// Deletes multiple splits.
    pub(crate) fn delete_splits(&mut self, split_ids: &[&str]) -> MetastoreResult<()> {
        let mut split_not_found_ids = Vec::new();
        let mut split_not_deletable_ids = Vec::new();

        for &split_id in split_ids {
            match self.delete_split(split_id) {
                DeleteSplitOutcome::Success => {}
                DeleteSplitOutcome::SplitNotFound => {
                    split_not_found_ids.push(split_id.to_string());
                }
                DeleteSplitOutcome::ForbiddenBecausePublished => {
                    split_not_deletable_ids.push(split_id.to_string());
                }
            }
        }
        if !split_not_found_ids.is_empty() {
            return Err(MetastoreError::SplitsDoNotExist {
                split_ids: split_not_found_ids,
            });
        }
        if !split_not_deletable_ids.is_empty() {
            return Err(MetastoreError::SplitsNotDeletable {
                split_ids: split_not_deletable_ids,
            });
        }
        self.metadata.update_timestamp = OffsetDateTime::now_utc().unix_timestamp();
        Ok(())
    }

    /// Adds a source. Returns whether a mutation occurred.
    pub(crate) fn add_source(&mut self, source: SourceConfig) -> MetastoreResult<bool> {
        self.metadata.add_source(source)
    }

    /// Deletes the source. Returns whether a mutation occurred.
    pub(crate) fn delete_source(&mut self, source_id: &str) -> MetastoreResult<bool> {
        self.metadata.delete_source(source_id)
    }

    /// Resets the checkpoint of a source. Returns whether a mutation occurred.
    pub(crate) fn reset_source_checkpoint(&mut self, source_id: &str) -> MetastoreResult<bool> {
        Ok(self.metadata.checkpoint.reset_source(source_id))
    }

    /// Creates [`DeleteTask`] from a [`DeleteQuery`].
    pub(crate) fn create_delete_task(
        &mut self,
        delete_query: DeleteQuery,
    ) -> MetastoreResult<DeleteTask> {
        let now_timestamp = OffsetDateTime::now_utc().unix_timestamp();
        let delete_task = DeleteTask {
            create_timestamp: now_timestamp,
            opstamp: self.stamper.stamp() as u64,
            delete_query: Some(delete_query),
        };
        self.delete_tasks.push(delete_task.clone());
        Ok(delete_task)
    }

    /// Returns index last delete opstamp.
    pub(crate) fn last_delete_opstamp(&self) -> u64 {
        self.delete_tasks
            .iter()
            .map(|delete_task| delete_task.opstamp)
            .max()
            .unwrap_or(0)
    }

    /// Updates splits delete opstamp. Returns that a mutation occurred (true).
    pub(crate) fn update_splits_delete_opstamp(
        &mut self,
        split_ids: &[&str],
        delete_opstamp: u64,
    ) -> MetastoreResult<bool> {
        for split_id in split_ids {
            let split =
                self.splits
                    .get_mut(*split_id)
                    .ok_or_else(|| MetastoreError::SplitsDoNotExist {
                        split_ids: vec![split_id.to_string()],
                    })?;
            split.split_metadata.delete_opstamp = delete_opstamp;
        }
        Ok(true)
    }

    /// Lists delete tasks with opstamp > `opstamp_start`.
    pub(crate) fn list_delete_tasks(&self, opstamp_start: u64) -> MetastoreResult<Vec<DeleteTask>> {
        let delete_tasks = self
            .delete_tasks
            .iter()
            .filter(|delete_task| delete_task.opstamp > opstamp_start)
            .cloned()
            .collect_vec();
        Ok(delete_tasks)
    }
}

/// Stamper provides Opstamps, which is just an auto-increment id to label
/// a delete operation.
#[derive(Clone, Default)]
struct Stamper(usize);

impl Stamper {
    /// Creates a [`Stamper`].
    pub fn new(first_opstamp: usize) -> Self {
        Self(first_opstamp)
    }

    /// Increments the stamper by 1 and returns the incremented value.
    pub fn stamp(&mut self) -> usize {
        self.0 += 1;
        self.0
    }
}

impl Debug for Stamper {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt.debug_struct("Stamper").field("stamp", &self.0).finish()
    }
}
