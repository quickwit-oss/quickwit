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
use std::ops::{Range, RangeInclusive};

use itertools::Itertools;
use quickwit_config::SourceConfig;
use quickwit_doc_mapper::tag_pruning::TagFilterAst;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::checkpoint::IndexCheckpointDelta;
use crate::{IndexMetadata, MetastoreError, MetastoreResult, Split, SplitMetadata, SplitState};

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
        }
    }
}

impl From<FileBackedIndexV0> for FileBackedIndex {
    fn from(index: FileBackedIndexV0) -> Self {
        let index_id = index.metadata.index_id.clone();
        let splits = index.splits
            .into_iter()
            .map(|mut split| {
                split.split_metadata.index_id = index_id.clone();
                split
            })
            .collect();
        Self::new(index.metadata, splits)
    }
}

enum DeleteSplitOutcome {
    Success,
    SplitNotFound,
    ForbiddenBecausePublished,
}

/// Takes 2 intervals and returns true iff their intersection is empty
fn is_disjoint(left: &Range<i64>, right: &RangeInclusive<i64>) -> bool {
    left.end <= *right.start() || *right.end() < left.start
}

impl FileBackedIndex {
    /// Constructor.
    pub fn new(metadata: IndexMetadata, splits: Vec<Split>) -> Self {
        Self {
            metadata,
            splits: splits
                .into_iter()
                .map(|split| (split.split_id().to_string(), split))
                .collect(),
            discarded: false,
        }
    }

    /// Used to update Split from old metastore format.
    pub fn into_new_split_format(self) -> Self {
        let index_id = self.metadata.index_id.clone();
        let splits = self.splits
            .into_iter()
            .map(|(split_id, mut split)|{
                split.split_metadata.index_id = index_id.clone();
                (split_id, split)
            }).collect();
        FileBackedIndex { 
            metadata: self.metadata,
            splits,
            discarded: self.discarded
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

    pub(crate) fn list_splits(
        &self,
        state: SplitState,
        time_range_opt: Option<Range<i64>>,
        tags_filter: Option<TagFilterAst>,
    ) -> MetastoreResult<Vec<Split>> {
        let time_range_filter = |split: &&Split| match (
            time_range_opt.as_ref(),
            split.split_metadata.time_range.as_ref(),
        ) {
            (Some(filter_time_range), Some(split_time_range)) => {
                !is_disjoint(filter_time_range, split_time_range)
            }
            _ => true, // Return `true` if `time_range` is omitted or the split has no time range.
        };

        let tag_filter = |split: &&Split| {
            tags_filter
                .as_ref()
                .map(|tags_filter_ast| tags_filter_ast.evaluate(&split.split_metadata.tags))
                .unwrap_or(true)
        };
        let splits = self
            .splits
            .values()
            .filter(|&split| split.split_state == state)
            .filter(time_range_filter)
            .filter(tag_filter)
            .cloned()
            .collect();
        Ok(splits)
    }

    pub(crate) fn list_all_splits(&self) -> MetastoreResult<Vec<Split>> {
        let splits = self.splits.values().cloned().collect();
        Ok(splits)
    }

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

    pub(crate) fn add_source(&mut self, source: SourceConfig) -> MetastoreResult<bool> {
        self.metadata.add_source(source)?;
        Ok(true)
    }

    pub(crate) fn delete_source(&mut self, source_id: &str) -> MetastoreResult<bool> {
        self.metadata.delete_source(source_id)?;
        Ok(true)
    }

    /// Resets the checkpoint of a source. Returns whether a mutation occurred.
    pub(crate) fn reset_source_checkpoint(&mut self, source_id: &str) -> MetastoreResult<bool> {
        Ok(self.metadata.checkpoint.reset_source(source_id))
    }
}
