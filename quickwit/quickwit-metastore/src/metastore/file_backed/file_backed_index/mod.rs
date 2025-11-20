// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! [`FileBackedIndex`] module. It is public so that the crate `quickwit-backward-compat` can
//! import [`FileBackedIndex`] and run backward-compatibility tests. You should not have to import
//! anything from here directly.

mod serialize;
mod shards;

use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Bound;

use itertools::Itertools;
use quickwit_common::pretty::PrettySample;
use quickwit_config::{
    DocMapping, IndexingSettings, IngestSettings, RetentionPolicy, SearchSettings, SourceConfig,
};
use quickwit_proto::metastore::{
    AcquireShardsRequest, AcquireShardsResponse, DeleteQuery, DeleteShardsRequest,
    DeleteShardsResponse, DeleteTask, EntityKind, ListShardsSubrequest, ListShardsSubresponse,
    MetastoreError, MetastoreResult, OpenShardSubrequest, OpenShardSubresponse, PruneShardsRequest,
};
use quickwit_proto::types::{IndexUid, PublishToken, SourceId, SplitId};
use serde::{Deserialize, Serialize};
use serialize::VersionedFileBackedIndex;
use shards::Shards;
use time::OffsetDateTime;
use tracing::{info, warn};

use super::MutationOccurred;
use crate::checkpoint::IndexCheckpointDelta;
use crate::metastore::{SortBy, use_shard_api};
use crate::{IndexMetadata, ListSplitsQuery, Split, SplitMetadata, SplitState, split_tag_filter};

/// A `FileBackedIndex` object carries an index metadata and its split metadata.
// This struct is meant to be used only within the [`FileBackedMetastore`]. The public visibility is
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(into = "VersionedFileBackedIndex")]
#[serde(from = "VersionedFileBackedIndex")]
pub(crate) struct FileBackedIndex {
    /// Metadata specific to the index.
    metadata: IndexMetadata,
    /// List of splits belonging to the index.
    splits: HashMap<SplitId, Split>,
    /// Shards of each source.
    per_source_shards: HashMap<SourceId, Shards>,
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

#[cfg(any(test, feature = "testsuite"))]
impl quickwit_config::TestableForRegression for FileBackedIndex {
    fn sample_for_regression() -> Self {
        use quickwit_config::INGEST_V2_SOURCE_ID;
        use quickwit_proto::ingest::{Shard, ShardState};
        use quickwit_proto::types::{DocMappingUid, Position, ShardId};

        let index_metadata = IndexMetadata::sample_for_regression();
        let index_uid = index_metadata.index_uid.clone();
        let source_id = INGEST_V2_SOURCE_ID.to_string();

        let split_metadata = SplitMetadata::sample_for_regression();
        let split = Split {
            split_state: SplitState::Published,
            split_metadata,
            update_timestamp: 1789,
            publish_timestamp: Some(1789),
        };
        let splits = vec![split];

        let shard = Shard {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            leader_id: "leader-ingester".to_string(),
            follower_id: Some("follower-ingester".to_string()),
            doc_mapping_uid: Some(DocMappingUid::for_test(1)),
            publish_position_inclusive: Some(Position::Beginning),
            update_timestamp: 1724240908,
            ..Default::default()
        };
        let shards = Shards::from_shards_vec(index_uid.clone(), source_id.clone(), vec![shard]);
        let per_source_shards = HashMap::from_iter([(source_id, shards)]);

        let delete_task = DeleteTask {
            create_timestamp: 0,
            opstamp: 10,
            delete_query: Some(DeleteQuery {
                index_uid: index_uid.into(),
                start_timestamp: None,
                end_timestamp: None,
                query_ast: quickwit_query::query_ast::qast_json_helper("Harry Potter", &["body"]),
            }),
        };
        let delete_tasks = vec![delete_task];
        FileBackedIndex::new(index_metadata, splits, per_source_shards, delete_tasks)
    }

    fn assert_equality(&self, other: &Self) {
        self.metadata().assert_equality(other.metadata());
        assert_eq!(self.splits, other.splits);
        assert_eq!(self.per_source_shards, other.per_source_shards);
        assert_eq!(self.delete_tasks, other.delete_tasks);
    }
}

impl From<IndexMetadata> for FileBackedIndex {
    fn from(index_metadata: IndexMetadata) -> Self {
        let per_source_shards = index_metadata
            .sources
            .keys()
            .map(|source_id| {
                let shards = Shards::empty(index_metadata.index_uid.clone(), source_id.clone());
                (source_id.clone(), shards)
            })
            .collect();

        Self {
            metadata: index_metadata,
            splits: Default::default(),
            per_source_shards,
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
    pub fn new(
        metadata: IndexMetadata,
        splits: Vec<Split>,
        per_source_shards: HashMap<SourceId, Shards>,
        delete_tasks: Vec<DeleteTask>,
    ) -> Self {
        let last_opstamp = delete_tasks
            .iter()
            .map(|delete_task| delete_task.opstamp)
            .max()
            .unwrap_or(0) as usize;
        let splits = splits
            .into_iter()
            .map(|split| (split.split_id().to_string(), split))
            .collect();
        Self {
            metadata,
            splits,
            per_source_shards,
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

    /// Index UID accessor.
    pub fn index_uid(&self) -> &IndexUid {
        &self.metadata.index_uid
    }

    /// Index metadata accessor.
    pub fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    pub fn update_index_config(
        &mut self,
        doc_mapping: DocMapping,
        indexing_settings: IndexingSettings,
        ingest_settings: IngestSettings,
        search_settings: SearchSettings,
        retention_policy_opt: Option<RetentionPolicy>,
    ) -> MetastoreResult<bool> {
        self.metadata.update_index_config(
            doc_mapping,
            indexing_settings,
            ingest_settings,
            search_settings,
            retention_policy_opt,
        )
    }

    /// Stages a single split.
    ///
    /// If a split already exists and is in the [SplitState::Staged] state,
    /// it is simply updated/overwritten.
    ///
    /// If a split already exists and is *not* in the [SplitState::Staged] state, a
    /// [MetastoreError::NotFound] error is returned providing the split ID to go with
    /// it.
    pub(crate) fn stage_split(
        &mut self,
        split_metadata: SplitMetadata,
    ) -> Result<(), MetastoreError> {
        // Check whether the split exists.
        // If the split exists, we check what state it is in. If it's anything other than `Staged`
        // something has gone very wrong and we should abort the operation.
        if let Some(split) = self.splits.get(split_metadata.split_id())
            && split.split_state != SplitState::Staged
        {
            let entity = EntityKind::Split {
                split_id: split.split_id().to_string(),
            };
            let message = "split is not staged".to_string();
            return Err(MetastoreError::FailedPrecondition { entity, message });
        }
        let now_timestamp = OffsetDateTime::now_utc().unix_timestamp();
        let split = Split {
            split_state: SplitState::Staged,
            update_timestamp: now_timestamp,
            publish_timestamp: None,
            split_metadata,
        };
        self.splits.insert(split.split_id().to_string(), split);
        Ok(())
    }

    /// Marks the splits for deletion. Returns whether a mutation occurred.
    pub(crate) fn mark_splits_for_deletion(
        &mut self,
        split_ids: impl IntoIterator<Item = impl AsRef<str>>,
        deletable_split_states: &[SplitState],
        return_error_on_splits_not_found: bool,
    ) -> MetastoreResult<bool> {
        let mut mutation_occurred = false;
        let mut split_not_found_ids = Vec::new();
        let mut non_deletable_split_ids = Vec::new();
        let now_timestamp = OffsetDateTime::now_utc().unix_timestamp();

        for split_id in split_ids {
            let split_id_ref = split_id.as_ref();
            // Check for the existence of split.
            let metadata = match self.splits.get_mut(split_id_ref) {
                Some(metadata) => metadata,
                None => {
                    split_not_found_ids.push(split_id_ref.to_string());
                    continue;
                }
            };
            if !deletable_split_states.contains(&metadata.split_state) {
                non_deletable_split_ids.push(split_id_ref.to_string());
                continue;
            };
            if metadata.split_state == SplitState::MarkedForDeletion {
                // If the split is already marked for deletion, This is fine, we just skip it.
                continue;
            }
            metadata.split_state = SplitState::MarkedForDeletion;
            metadata.update_timestamp = now_timestamp;
            mutation_occurred = true;
        }
        if !split_not_found_ids.is_empty() {
            if return_error_on_splits_not_found {
                return Err(MetastoreError::NotFound(EntityKind::Splits {
                    split_ids: split_not_found_ids,
                }));
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
            let entity = EntityKind::Splits {
                split_ids: non_deletable_split_ids,
            };
            let message = "splits are not deletable".to_string();
            return Err(MetastoreError::FailedPrecondition { entity, message });
        }
        Ok(mutation_occurred)
    }

    /// Helper to mark a list of splits as published.
    /// This function however does not update the checkpoint.
    fn mark_splits_as_published_helper(
        &mut self,
        staged_split_ids: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> MetastoreResult<()> {
        let mut split_not_found_ids = Vec::new();
        let mut split_not_staged_ids = Vec::new();

        let now_timestamp = OffsetDateTime::now_utc().unix_timestamp();

        for staged_plit_id in staged_split_ids {
            let staged_split_id_ref = staged_plit_id.as_ref();
            // Check for the existence of split.
            let Some(metadata) = self.splits.get_mut(staged_split_id_ref) else {
                split_not_found_ids.push(staged_split_id_ref.to_string());
                continue;
            };
            if metadata.split_state == SplitState::Staged {
                metadata.split_state = SplitState::Published;
                metadata.update_timestamp = now_timestamp;
                metadata.publish_timestamp = Some(now_timestamp);
            } else {
                split_not_staged_ids.push(staged_split_id_ref.to_string());
            }
        }
        if !split_not_found_ids.is_empty() {
            return Err(MetastoreError::NotFound(EntityKind::Splits {
                split_ids: split_not_found_ids,
            }));
        }
        if !split_not_staged_ids.is_empty() {
            let entity = EntityKind::Splits {
                split_ids: split_not_staged_ids,
            };
            let message = "splits are not staged".to_string();
            return Err(MetastoreError::FailedPrecondition { entity, message });
        }
        Ok(())
    }

    /// Publishes splits.
    pub(crate) fn publish_splits(
        &mut self,
        staged_split_ids: impl IntoIterator<Item = impl AsRef<str>>,
        replaced_split_ids: impl IntoIterator<Item = impl AsRef<str>>,
        checkpoint_delta_opt: Option<IndexCheckpointDelta>,
        publish_token_opt: Option<PublishToken>,
    ) -> MetastoreResult<()> {
        if let Some(checkpoint_delta) = checkpoint_delta_opt {
            let source_id = checkpoint_delta.source_id.clone();
            let source = self.metadata.sources.get(&source_id).ok_or_else(|| {
                MetastoreError::NotFound(EntityKind::Source {
                    index_id: self.index_id().to_string(),
                    source_id: source_id.clone(),
                })
            })?;

            if use_shard_api(&source.source_params) {
                let publish_token = publish_token_opt.ok_or_else(|| {
                    let message = format!(
                        "publish token is required for publishing splits for source `{source_id}`"
                    );
                    MetastoreError::InvalidArgument { message }
                })?;
                self.try_apply_delta_v2(checkpoint_delta, publish_token)?;
            } else {
                self.metadata
                    .checkpoint
                    .try_apply_delta(checkpoint_delta)
                    .map_err(|error| {
                        quickwit_common::rate_limited_error!(
                            limit_per_min = 6,
                            index = self.index_id(),
                            "failed to apply checkpoint delta"
                        );
                        let entity = EntityKind::CheckpointDelta {
                            index_id: self.index_id().to_string(),
                            source_id,
                        };
                        let message = error.to_string();
                        MetastoreError::FailedPrecondition { entity, message }
                    })?;
            }
        }
        self.mark_splits_as_published_helper(staged_split_ids)?;
        self.mark_splits_for_deletion(replaced_split_ids, &[SplitState::Published], true)?;
        Ok(())
    }

    /// Lists splits.
    pub(crate) fn list_splits(&self, query: &ListSplitsQuery) -> MetastoreResult<Vec<Split>> {
        let limit = query
            .limit
            .map(|limit| limit + query.offset.unwrap_or_default())
            .unwrap_or(usize::MAX);
        // skip is done at a higher layer in case other indexes give spltis that would go before
        // ours

        let results = if query.sort_by == SortBy::None {
            // internally sorted_unstable_by collect everything to an intermediary vec. When not
            // sorting at all, skip that.
            self.splits
                .values()
                .filter(|split| split_query_predicate(split, query))
                .take(limit)
                .cloned()
                .collect()
        } else {
            self.splits
                .values()
                .filter(|split| split_query_predicate(split, query))
                .sorted_unstable_by(|lhs, rhs| query.sort_by.compare(lhs, rhs))
                .take(limit)
                .cloned()
                .collect()
        };
        Ok(results)
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
    pub(crate) fn delete_splits(
        &mut self,
        split_ids: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> MetastoreResult<()> {
        let num_deleted_splits = 0;
        let mut split_not_found_ids = Vec::new();
        let mut split_not_deletable_ids = Vec::new();

        for split_id in split_ids {
            let split_id_ref = split_id.as_ref();
            match self.delete_split(split_id_ref) {
                DeleteSplitOutcome::Success => {}
                DeleteSplitOutcome::SplitNotFound => {
                    split_not_found_ids.push(split_id_ref.to_string());
                }
                DeleteSplitOutcome::Forbidden => {
                    split_not_deletable_ids.push(split_id_ref.to_string());
                }
            }
        }
        if !split_not_deletable_ids.is_empty() {
            let entity = EntityKind::Splits {
                split_ids: split_not_deletable_ids,
            };
            let message = "splits are not deletable".to_string();
            return Err(MetastoreError::FailedPrecondition { entity, message });
        }
        info!(index_id=%self.index_id(), "deleted {num_deleted_splits} splits from index");

        if !split_not_found_ids.is_empty() {
            warn!(
                index_id=self.index_id().to_string(),
                split_ids=?PrettySample::new(&split_not_found_ids, 5),
                "{} splits were not found and could not be deleted",
                split_not_found_ids.len()
            );
        }
        Ok(())
    }

    /// Adds a source.
    pub(crate) fn add_source(&mut self, source_config: SourceConfig) -> MetastoreResult<()> {
        let index_uid = self.index_uid().clone();
        let source_id = source_config.source_id.clone();

        self.metadata.add_source(source_config)?;

        let shards = Shards::empty(index_uid, source_id.clone());
        self.per_source_shards.insert(source_id, shards);
        Ok(())
    }

    /// Updates a source. Returns whether a mutation occurred.
    pub(crate) fn update_source(&mut self, source_config: SourceConfig) -> MetastoreResult<bool> {
        self.metadata.update_source(source_config)
    }

    /// Enables or disables a source. Returns whether a mutation occurred.
    pub(crate) fn toggle_source(&mut self, source_id: &str, enable: bool) -> MetastoreResult<bool> {
        self.metadata.toggle_source(source_id, enable)
    }

    /// Deletes the source. Returns whether a mutation occurred.
    pub(crate) fn delete_source(&mut self, source_id: &str) -> MetastoreResult<()> {
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
            let split = self.splits.get_mut(*split_id).ok_or_else(|| {
                MetastoreError::NotFound(EntityKind::Splits {
                    split_ids: vec![split_id.to_string()],
                })
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
            .collect();
        Ok(delete_tasks)
    }

    // Shard API

    fn get_shards_for_source(&self, source_id: &str) -> MetastoreResult<&Shards> {
        self.per_source_shards.get(source_id).ok_or_else(|| {
            MetastoreError::NotFound(EntityKind::Source {
                index_id: self.index_id().to_string(),
                source_id: source_id.to_string(),
            })
        })
    }

    fn get_shards_for_source_mut(&mut self, source_id: &str) -> MetastoreResult<&mut Shards> {
        self.per_source_shards.get_mut(source_id).ok_or_else(|| {
            MetastoreError::NotFound(EntityKind::Source {
                index_id: self.metadata.index_id().to_string(),
                source_id: source_id.to_string(),
            })
        })
    }

    pub(crate) fn open_shards(
        &mut self,
        subrequests: Vec<OpenShardSubrequest>,
    ) -> MetastoreResult<MutationOccurred<Vec<OpenShardSubresponse>>> {
        let mut mutation_occurred = false;
        let mut subresponses = Vec::with_capacity(subrequests.len());

        for subrequest in subrequests {
            let subresponse = match self
                .get_shards_for_source_mut(&subrequest.source_id)?
                .open_shard(subrequest)?
            {
                MutationOccurred::Yes(subresponse) => {
                    mutation_occurred = true;
                    subresponse
                }
                MutationOccurred::No(subresponse) => subresponse,
            };
            subresponses.push(subresponse);
        }
        if mutation_occurred {
            Ok(MutationOccurred::Yes(subresponses))
        } else {
            Ok(MutationOccurred::No(subresponses))
        }
    }

    pub(crate) fn acquire_shards(
        &mut self,
        request: AcquireShardsRequest,
    ) -> MetastoreResult<MutationOccurred<AcquireShardsResponse>> {
        self.get_shards_for_source_mut(&request.source_id)?
            .acquire_shards(request)
    }

    pub(crate) fn delete_shards(
        &mut self,
        request: DeleteShardsRequest,
    ) -> MetastoreResult<MutationOccurred<DeleteShardsResponse>> {
        self.get_shards_for_source_mut(&request.source_id)?
            .delete_shards(request)
    }

    pub(crate) fn prune_shards(
        &mut self,
        request: PruneShardsRequest,
    ) -> MetastoreResult<MutationOccurred<()>> {
        self.get_shards_for_source_mut(&request.source_id)?
            .prune_shards(request)
    }

    pub(crate) fn list_shards(
        &self,
        subrequest: ListShardsSubrequest,
    ) -> MetastoreResult<ListShardsSubresponse> {
        self.get_shards_for_source(&subrequest.source_id)?
            .list_shards(subrequest)
    }

    pub(crate) fn try_apply_delta_v2(
        &mut self,
        checkpoint_delta: IndexCheckpointDelta,
        publish_token: PublishToken,
    ) -> MetastoreResult<MutationOccurred<()>> {
        self.get_shards_for_source_mut(&checkpoint_delta.source_id)?
            .try_apply_delta(checkpoint_delta.source_delta, publish_token)
    }
}

/// Stamper provides Opstamps, which is just an auto-increment id to label
/// a delete operation.
#[derive(Clone, Default)]
struct Stamper(usize);

impl Stamper {
    /// Creates a new [`Stamper`].
    pub fn new(initial_opstamp: usize) -> Self {
        Self(initial_opstamp)
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

fn split_query_predicate(split: &&Split, query: &ListSplitsQuery) -> bool {
    if !split_tag_filter(&split.split_metadata, query.tags.as_ref()) {
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

    match &query.mature {
        Bound::Included(evaluation_datetime) => {
            return split.split_metadata.is_mature(*evaluation_datetime);
        }
        Bound::Excluded(evaluation_datetime) => {
            return !split.split_metadata.is_mature(*evaluation_datetime);
        }
        Bound::Unbounded => {}
    }

    if let Some(range) = &split.split_metadata.time_range {
        if !query.time_range.overlaps_with(range.clone()) {
            return false;
        }
        if let Some(v) = query.max_time_range_end
            && range.end() > &v
        {
            return false;
        }
    }

    if let Some(node_id) = &query.node_id
        && split.split_metadata.node_id != *node_id
    {
        return false;
    }

    if let Some((index_uid, split_id)) = &query.after_split {
        if *index_uid > split.split_metadata.index_uid {
            return false;
        }
        if *index_uid == split.split_metadata.index_uid
            && *split_id >= split.split_metadata.split_id
        {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use quickwit_doc_mapper::tag_pruning::TagFilterAst;
    use quickwit_proto::ingest::Shard;
    use quickwit_proto::metastore::ListShardsSubrequest;
    use quickwit_proto::types::{IndexUid, SourceId};

    use super::FileBackedIndex;
    use crate::file_backed::file_backed_index::split_query_predicate;
    use crate::{ListSplitsQuery, Split, SplitMetadata, SplitState};

    impl FileBackedIndex {
        pub(crate) fn insert_shards(&mut self, source_id: &SourceId, shards: Vec<Shard>) {
            self.per_source_shards
                .get_mut(source_id)
                .unwrap()
                .insert_shards(shards)
        }

        pub(crate) fn list_all_shards(&self, source_id: &SourceId) -> Vec<Shard> {
            self.per_source_shards
                .get(source_id)
                .unwrap()
                .list_shards(ListShardsSubrequest {
                    ..Default::default()
                })
                .unwrap()
                .shards
        }
    }

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
                publish_timestamp: Some(10i64),
            },
        ]
    }

    #[test]
    fn test_single_filter_behaviour() {
        let [split_1, split_2, split_3] = make_splits();

        let query = ListSplitsQuery::for_index(IndexUid::new_with_random_ulid("test-index"))
            .with_split_state(SplitState::Staged);
        assert!(split_query_predicate(&&split_1, &query));

        let query = ListSplitsQuery::for_index(IndexUid::new_with_random_ulid("test-index"))
            .with_split_state(SplitState::Published);
        assert!(!split_query_predicate(&&split_2, &query));

        let query = ListSplitsQuery::for_index(IndexUid::new_with_random_ulid("test-index"))
            .with_split_states([SplitState::Published, SplitState::MarkedForDeletion]);
        assert!(!split_query_predicate(&&split_1, &query));
        assert!(split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index(IndexUid::new_with_random_ulid("test-index"))
            .with_update_timestamp_lt(51);
        assert!(!split_query_predicate(&&split_1, &query));
        assert!(split_query_predicate(&&split_2, &query));
        assert!(split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index(IndexUid::new_with_random_ulid("test-index"))
            .with_create_timestamp_gte(51);
        assert!(!split_query_predicate(&&split_1, &query));
        assert!(!split_query_predicate(&&split_2, &query));
        assert!(split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index(IndexUid::new_with_random_ulid("test-index"))
            .with_delete_opstamp_gte(4);
        assert!(split_query_predicate(&&split_1, &query));
        assert!(split_query_predicate(&&split_2, &query));
        assert!(!split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index(IndexUid::new_with_random_ulid("test-index"))
            .with_time_range_start_gt(45);
        assert!(!split_query_predicate(&&split_1, &query));
        assert!(split_query_predicate(&&split_2, &query));
        assert!(split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index(IndexUid::new_with_random_ulid("test-index"))
            .with_time_range_end_lt(45);
        assert!(split_query_predicate(&&split_1, &query));
        assert!(split_query_predicate(&&split_2, &query));
        assert!(split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index(IndexUid::new_with_random_ulid("test-index"))
            .with_tags_filter(TagFilterAst::Tag {
                is_present: false,
                tag: "tag-2".to_string(),
            });
        assert!(split_query_predicate(&&split_1, &query));
        assert!(!split_query_predicate(&&split_2, &query));
        assert!(!split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index(IndexUid::new_with_random_ulid("test-index"))
            .with_max_time_range_end(50);
        assert!(split_query_predicate(&&split_1, &query));
        assert!(split_query_predicate(&&split_2, &query));
        assert!(!split_query_predicate(&&split_3, &query));
    }

    #[test]
    fn test_combination_filter() {
        let [split_1, split_2, split_3] = make_splits();

        let query = ListSplitsQuery::for_index(IndexUid::new_with_random_ulid("test-index"))
            .with_time_range_start_gt(0)
            .with_time_range_end_lt(40);
        assert!(split_query_predicate(&&split_1, &query));
        assert!(split_query_predicate(&&split_2, &query));
        assert!(split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index(IndexUid::new_with_random_ulid("test-index"))
            .with_time_range_start_gt(45)
            .with_delete_opstamp_gt(0);
        assert!(!split_query_predicate(&&split_1, &query));
        assert!(split_query_predicate(&&split_2, &query));
        assert!(!split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index(IndexUid::new_with_random_ulid("test-index"))
            .with_update_timestamp_lt(51)
            .with_split_states([SplitState::Published, SplitState::MarkedForDeletion]);
        assert!(!split_query_predicate(&&split_1, &query));
        assert!(split_query_predicate(&&split_2, &query));
        assert!(split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index(IndexUid::new_with_random_ulid("test-index"))
            .with_update_timestamp_lt(51)
            .with_create_timestamp_lte(63);
        assert!(!split_query_predicate(&&split_1, &query));
        assert!(split_query_predicate(&&split_2, &query));
        assert!(!split_query_predicate(&&split_3, &query));

        let query = ListSplitsQuery::for_index(IndexUid::new_with_random_ulid("test-index"))
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
