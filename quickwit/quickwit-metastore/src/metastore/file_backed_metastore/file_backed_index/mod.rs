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

mod serialize;

use std::collections::HashMap;
use std::fmt::Debug;

use itertools::Itertools;
use quickwit_common::PrettySample;
use quickwit_config::{SourceConfig, TestableForRegression};
use quickwit_proto::metastore_api::{DeleteQuery, DeleteTask};
use serde::{Deserialize, Serialize};
use serialize::VersionedFileBackedIndex;
use time::OffsetDateTime;
use tracing::{info, warn};

use crate::checkpoint::IndexCheckpointDelta;
use crate::{
    split_tag_filter, IndexMetadata, ListSplitsQuery, MetastoreError, MetastoreResult, Split,
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
    /// Flag used to avoid polling the metastore if
    /// the process is actually writing the metastore.
    ///
    /// The logic is "soft". We avoid the polling step
    /// if the metastore wrote some value since the last
    /// polling loop.
    recently_modified: bool,
    /// Has been discarded. This field exists to make
    /// it possible to discard this entry if there is an error
    /// while mutating the Index.
    pub discarded: bool,
}

impl TestableForRegression for FileBackedIndex {
    fn sample_for_regression() -> Self {
        let index_metadata = IndexMetadata::sample_for_regression();
        let split_metadata = SplitMetadata::sample_for_regression();
        let split = Split {
            split_state: SplitState::Published,
            split_metadata,
            update_timestamp: 1789,
            publish_timestamp: Some(1789),
        };
        let splits = vec![split];
        let delete_task = DeleteTask {
            create_timestamp: 0,
            opstamp: 10,
            delete_query: Some(DeleteQuery {
                index_id: "index".to_string(),
                start_timestamp: None,
                end_timestamp: None,
                query: "Harry Potter".to_string(),
                search_fields: Vec::new(),
            }),
        };
        FileBackedIndex::new(index_metadata, splits, vec![delete_task])
    }

    fn test_equality(&self, other: &Self) {
        self.metadata().test_equality(other.metadata());
        assert_eq!(self.splits(), other.splits());
    }
}

impl From<IndexMetadata> for FileBackedIndex {
    fn from(index_metadata: IndexMetadata) -> Self {
        Self {
            metadata: index_metadata,
            splits: Default::default(),
            delete_tasks: Default::default(),
            stamper: Default::default(),
            recently_modified: false,
            discarded: false,
        }
    }
}

enum DeleteSplitOutcome {
    Success,
    SplitNotFound,
    // The split is in another state than marked for deletion.
    Forbidden,
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
            recently_modified: false,
            discarded: false,
        }
    }

    /// Sets the `recently_modified` flag to false and returns the previous value.
    pub fn flip_recently_modified_down(&mut self) -> bool {
        std::mem::replace(&mut self.recently_modified, false)
    }

    /// Marks the file as `recently_modified`.
    pub fn set_recently_modified(&mut self) {
        self.recently_modified = true;
    }

    /// Index ID accessor.
    pub fn index_id(&self) -> &str {
        self.metadata.index_id()
    }

    /// Index metadata accessor.
    pub fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    /// Splits accessor.
    pub fn splits(&self) -> &HashMap<String, Split> {
        &self.splits
    }

    /// Stages a single split.
    ///
    /// If a split already exists and is in the [SplitState::Staged] state,
    /// it is simply updated/overwritten.
    ///
    /// If a split already exists and is *not* in the [SplitState::Staged] state, a
    /// [MetastoreError::SplitsNotStaged] error is returned providing the split ID to go with
    /// it.
    pub(crate) fn stage_split(
        &mut self,
        split_metadata: SplitMetadata,
    ) -> Result<(), MetastoreError> {
        // Check whether the split exists.
        // If the split exists, we check what state it is in. If it's anything other than `Staged`
        // something has gone very wrong and we should abort the operation.
        if let Some(existing_split) = self.splits.get(split_metadata.split_id()) {
            if existing_split.split_state != SplitState::Staged {
                return Err(MetastoreError::SplitsNotStaged {
                    split_ids: vec![existing_split.split_id().to_string()],
                });
            }
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
        Ok(())
    }

    /// Marks the splits for deletion. Returns whether a mutation occurred.
    pub(crate) fn mark_splits_for_deletion(
        &mut self,
        split_ids: &[&str],
        deletable_states: &[SplitState],
        return_error_on_splits_not_found: bool,
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
            if return_error_on_splits_not_found {
                return Err(MetastoreError::SplitsDoNotExist {
                    split_ids: split_not_found_ids,
                });
            } else {
                warn!(
                    index_id=%self.index_id(),
                    split_ids=?PrettySample::new(&split_not_found_ids, 5),
                    "{} splits were not found and could not be marked for deletion.",
                    split_not_found_ids.len()
                );
            }
        }
        if !non_deletable_split_ids.is_empty() {
            return Err(MetastoreError::SplitsNotDeletable {
                split_ids: non_deletable_split_ids,
            });
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
            let Some(metadata) = self.splits.get_mut(split_id) else {
                    split_not_found_ids.push(split_id.to_string());
                    continue;
                };
            if metadata.split_state == SplitState::Staged {
                metadata.split_state = SplitState::Published;
                metadata.update_timestamp = now_timestamp;
                metadata.publish_timestamp = Some(now_timestamp);
            } else {
                split_not_staged_ids.push(split_id.to_string());
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
        self.mark_splits_for_deletion(replaced_split_ids, &[SplitState::Published], true)?;
        Ok(())
    }

    /// Lists splits.
    pub(crate) fn list_splits(&self, query: ListSplitsQuery<'_>) -> MetastoreResult<Vec<Split>> {
        let limit = query.limit.unwrap_or(usize::MAX);
        let offset = query.offset.unwrap_or_default();

        let splits: Vec<Split> = self
            .splits
            .values()
            .filter(|split| split_query_predicate(split, &query))
            .skip(offset)
            .take(limit)
            .cloned()
            .collect();

        Ok(splits)
    }

    /// Deletes a split.
    fn delete_split(&mut self, split_id: &str) -> DeleteSplitOutcome {
        match self.splits.get(split_id).map(|split| split.split_state) {
            Some(SplitState::MarkedForDeletion) => {
                self.splits.remove(split_id);
                DeleteSplitOutcome::Success
            }
            Some(SplitState::Staged | SplitState::Published) => DeleteSplitOutcome::Forbidden,
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
                    split_not_found_ids.push(split_id);
                }
                DeleteSplitOutcome::Forbidden => {
                    split_not_deletable_ids.push(split_id.to_string());
                }
            }
        }
        if !split_not_deletable_ids.is_empty() {
            return Err(MetastoreError::SplitsNotDeletable {
                split_ids: split_not_deletable_ids,
            });
        }
        info!(index_id=%self.index_id(), "Deleted {} splits from index.", split_ids.len());

        if !split_not_found_ids.is_empty() {
            warn!(
                index_id=%self.index_id(),
                split_ids=?PrettySample::new(&split_not_found_ids, 5),
                "{} splits were not found and could not be deleted.",
                split_not_found_ids.len()
            );
        }
        Ok(())
    }

    /// Adds a source.
    pub(crate) fn add_source(&mut self, source: SourceConfig) -> MetastoreResult<()> {
        self.metadata.add_source(source)
    }

    pub(crate) fn toggle_source(&mut self, source_id: &str, enable: bool) -> MetastoreResult<bool> {
        self.metadata.toggle_source(source_id, enable)
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

fn split_query_predicate(split: &&Split, query: &ListSplitsQuery<'_>) -> bool {
    if !split_tag_filter(split, query.tags.as_ref()) {
        return false;
    }

    if !query.split_states.is_empty() && !query.split_states.contains(&split.split_state) {
        return false;
    }

    if !query
        .delete_opstamp
        .contains(&split.split_metadata.delete_opstamp)
    {
        return false;
    }

    if !query.update_timestamp.contains(&split.update_timestamp) {
        return false;
    }

    if !query
        .create_timestamp
        .contains(&split.split_metadata.create_timestamp)
    {
        return false;
    }

    if let Some(range) = split.split_metadata.time_range.as_ref() {
        if !query.time_range.overlaps_with(range.clone()) {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use quickwit_doc_mapper::tag_pruning::TagFilterAst;

    use crate::file_backed_metastore::file_backed_index::split_query_predicate;
    use crate::{ListSplitsQuery, Split, SplitMetadata, SplitState};

    fn make_splits() -> [Split; 3] {
        [
            Split {
                split_metadata: SplitMetadata {
                    split_id: "split-1".to_string(),
                    delete_opstamp: 9,
                    time_range: Some(32..=40),
                    tags: BTreeSet::from(["tag-1".to_string()]),
                    create_timestamp: 12,
                    ..Default::default()
                },
                split_state: SplitState::Staged,
                update_timestamp: 70i64,
                publish_timestamp: None,
            },
            Split {
                split_metadata: SplitMetadata {
                    split_id: "split-2".to_string(),
                    delete_opstamp: 4,
                    time_range: None,
                    tags: BTreeSet::from(["tag-2".to_string(), "tag-3".to_string()]),
                    create_timestamp: 5,
                    ..Default::default()
                },
                split_state: SplitState::MarkedForDeletion,
                update_timestamp: 50i64,
                publish_timestamp: None,
            },
            Split {
                split_metadata: SplitMetadata {
                    split_id: "split-3".to_string(),
                    delete_opstamp: 0,
                    time_range: Some(0..=90),
                    tags: BTreeSet::from(["tag-2".to_string(), "tag-4".to_string()]),
                    create_timestamp: 64,
                    ..Default::default()
                },
                split_state: SplitState::Published,
                update_timestamp: 0i64,
                publish_timestamp: None,
            },
        ]
    }

    #[test]
    fn test_single_filter_behaviour() {
        let [split_1, split_2, split_3] = make_splits();

        let query = ListSplitsQuery::for_index("test-index").with_split_state(SplitState::Staged);
        assert!(split_query_predicate(&&split_1, &query));

        let query =
            ListSplitsQuery::for_index("test-index").with_split_state(SplitState::Published);
        assert!(!split_query_predicate(&&split_2, &query));

        let query = ListSplitsQuery::for_index("test-index")
            .with_split_states([SplitState::Published, SplitState::MarkedForDeletion]);
        assert!(!split_query_predicate(&&split_1, &query));
        assert!(split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index("test-index").with_update_timestamp_lt(51);
        assert!(!split_query_predicate(&&split_1, &query));
        assert!(split_query_predicate(&&split_2, &query));
        assert!(split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index("test-index").with_create_timestamp_gte(51);
        assert!(!split_query_predicate(&&split_1, &query));
        assert!(!split_query_predicate(&&split_2, &query));
        assert!(split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index("test-index").with_delete_opstamp_gte(4);
        assert!(split_query_predicate(&&split_1, &query));
        assert!(split_query_predicate(&&split_2, &query));
        assert!(!split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index("test-index").with_time_range_start_gt(45);
        assert!(!split_query_predicate(&&split_1, &query));
        assert!(split_query_predicate(&&split_2, &query));
        assert!(split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index("test-index").with_time_range_end_lt(45);
        assert!(split_query_predicate(&&split_1, &query));
        assert!(split_query_predicate(&&split_2, &query));
        assert!(split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index("test-index").with_tags_filter(TagFilterAst::Tag {
            is_present: false,
            tag: "tag-2".to_string(),
        });
        assert!(split_query_predicate(&&split_1, &query));
        assert!(!split_query_predicate(&&split_2, &query));
        assert!(!split_query_predicate(&&split_3, &query));
    }

    #[test]
    fn test_combination_filter() {
        let [split_1, split_2, split_3] = make_splits();

        let query = ListSplitsQuery::for_index("test-index")
            .with_time_range_start_gt(0)
            .with_time_range_end_lt(40);
        assert!(split_query_predicate(&&split_1, &query));
        assert!(split_query_predicate(&&split_2, &query));
        assert!(split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index("test-index")
            .with_time_range_start_gt(45)
            .with_delete_opstamp_gt(0);
        assert!(!split_query_predicate(&&split_1, &query));
        assert!(split_query_predicate(&&split_2, &query));
        assert!(!split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index("test-index")
            .with_update_timestamp_lt(51)
            .with_split_states([SplitState::Published, SplitState::MarkedForDeletion]);
        assert!(!split_query_predicate(&&split_1, &query));
        assert!(split_query_predicate(&&split_2, &query));
        assert!(split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index("test-index")
            .with_update_timestamp_lt(51)
            .with_create_timestamp_lte(63);
        assert!(!split_query_predicate(&&split_1, &query));
        assert!(split_query_predicate(&&split_2, &query));
        assert!(!split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index("test-index")
            .with_time_range_start_gt(90)
            .with_tags_filter(TagFilterAst::Tag {
                is_present: true,
                tag: "tag-1".to_string(),
            });
        assert!(!split_query_predicate(&&split_1, &query));
        assert!(!split_query_predicate(&&split_2, &query));
        assert!(!split_query_predicate(&&split_3, &query));
    }
}
