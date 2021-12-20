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

use std::collections::HashMap;
use std::ops::{Range, RangeInclusive};

use chrono::Utc;
use quickwit_index_config::tag_pruning::TagFilterAst;
use serde::{Deserialize, Serialize};

use crate::checkpoint::CheckpointDelta;
use crate::{IndexMetadata, MetastoreError, MetastoreResult, Split, SplitMetadata, SplitState};

/// A MetadataSet carries an index metadata and its split metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct MetadataSet {
    /// Metadata specific to the index.
    index: IndexMetadata,
    /// List of splits belonging to the index.
    splits: HashMap<String, Split>,
    /// Has been discarded. This field exists to make
    /// it possible to discard this entry if there is an error
    /// while mutating the metadata set.
    #[serde(skip)]
    pub discarded: bool,
}

impl From<IndexMetadata> for MetadataSet {
    fn from(index: IndexMetadata) -> Self {
        MetadataSet {
            index,
            splits: Default::default(),
            discarded: false,
        }
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

impl MetadataSet {
    pub fn index_id(&self) -> &str {
        &self.index.index_id
    }

    pub fn index_metadata(&self) -> &IndexMetadata {
        &self.index
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
                cause: anyhow::anyhow!(""),
            });
        }

        let now_timestamp = Utc::now().timestamp();
        let metadata = Split {
            split_state: SplitState::Staged,
            update_timestamp: now_timestamp,
            split_metadata,
        };

        self.splits
            .insert(metadata.split_id().to_string(), metadata);

        self.index.update_timestamp = now_timestamp;
        Ok(())
    }

    pub(crate) fn replace_splits<'a>(
        &mut self,
        new_split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        // Try to publish the new splits.
        // We do not want to update the delta, which is why we use an empty
        // checkpoint delta.
        self.publish_splits(new_split_ids, CheckpointDelta::default())?;

        // Mark splits for deletion.
        self.mark_splits_for_deletion(replaced_split_ids)?;

        Ok(())
    }

    /// Returns true if modified
    pub(crate) fn mark_splits_for_deletion(
        &mut self,
        split_ids: &[&'_ str],
    ) -> MetastoreResult<bool> {
        let mut is_modified = false;
        let mut split_not_found_ids = vec![];
        let now_timestamp = Utc::now().timestamp();
        for &split_id in split_ids {
            // Check for the existence of split.
            let metadata = match self.splits.get_mut(split_id) {
                Some(metadata) => metadata,
                None => {
                    split_not_found_ids.push(split_id.to_string());
                    continue;
                }
            };

            if metadata.split_state == SplitState::ScheduledForDeletion {
                // If the split is already scheduled for deletion, This is fine, we just skip it.
                continue;
            }

            metadata.split_state = SplitState::ScheduledForDeletion;
            metadata.update_timestamp = now_timestamp;
            is_modified = true;
        }

        if !split_not_found_ids.is_empty() {
            return Err(MetastoreError::SplitsDoNotExist {
                split_ids: split_not_found_ids,
            });
        }

        if is_modified {
            self.index.update_timestamp = now_timestamp;
        }
        Ok(is_modified)
    }

    /// Helper to publish a list of splits.
    pub(crate) fn publish_splits<'a>(
        &mut self,
        split_ids: &[&'a str],
        checkpoint_delta: CheckpointDelta,
    ) -> MetastoreResult<()> {
        self.index.checkpoint.try_apply_delta(checkpoint_delta)?;
        let mut split_not_found_ids = vec![];
        let mut split_not_staged_ids = vec![];
        let now_timestamp = Utc::now().timestamp();
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

        self.index.update_timestamp = now_timestamp;
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
        let metadata = match self.splits.get_mut(split_id) {
            Some(metadata) => metadata,
            None => {
                return DeleteSplitOutcome::SplitNotFound;
            }
        };
        match metadata.split_state {
            SplitState::ScheduledForDeletion | SplitState::Staged => {
                // Only `ScheduledForDeletion` and `Staged` can be deleted
                self.splits.remove(split_id);
                DeleteSplitOutcome::Success
            }
            SplitState::Published => DeleteSplitOutcome::ForbiddenBecausePublished,
        }
    }

    // Delete several splits.
    pub(crate) fn delete_splits(&mut self, split_ids: &[&'_ str]) -> MetastoreResult<()> {
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

        self.index.update_timestamp = Utc::now().timestamp();
        Ok(())
    }
}
