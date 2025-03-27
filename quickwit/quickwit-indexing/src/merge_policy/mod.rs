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

mod const_write_amplification;
mod nop_merge_policy;
mod stable_log_merge_policy;

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

pub(crate) use const_write_amplification::ConstWriteAmplificationMergePolicy;
use itertools::Itertools;
pub use nop_merge_policy::NopMergePolicy;
use quickwit_common::tracker::TrackedObject;
use quickwit_config::merge_policy_config::MergePolicyConfig;
use quickwit_config::IndexingSettings;
use quickwit_metastore::{SplitMaturity, SplitMetadata};
use quickwit_proto::types::SplitId;
use serde::Serialize;
pub(crate) use stable_log_merge_policy::StableLogMergePolicy;
use tracing::{info_span, Span};

use crate::actors::MergePermit;
use crate::new_split_id;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum MergeOperationType {
    Merge,
    DeleteAndMerge,
}

impl fmt::Display for MergeOperationType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

pub struct MergeTask {
    pub merge_operation: TrackedObject<MergeOperation>,
    pub(crate) _merge_permit: MergePermit,
}

impl MergeTask {
    #[cfg(any(test, feature = "testsuite"))]
    pub fn from_merge_operation_for_test(merge_operation: MergeOperation) -> MergeTask {
        let tracker = quickwit_common::tracker::Tracker::new();
        let tracked_merge_operation = tracker.track(merge_operation);
        MergeTask {
            merge_operation: tracked_merge_operation,
            _merge_permit: MergePermit::for_test(),
        }
    }
}

impl fmt::Debug for MergeTask {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.merge_operation.as_ref().fmt(f)
    }
}

impl Deref for MergeTask {
    type Target = MergeOperation;

    fn deref(&self) -> &Self::Target {
        self.merge_operation.as_ref()
    }
}

#[derive(Clone, Serialize)]
pub struct MergeOperation {
    #[serde(skip_serializing)]
    pub merge_parent_span: Span,
    pub merge_split_id: SplitId,
    pub splits: Vec<SplitMetadata>,
    pub operation_type: MergeOperationType,
}

impl MergeOperation {
    pub fn new_merge_operation(splits: Vec<SplitMetadata>) -> Self {
        let merge_split_id = new_split_id();
        let split_ids = splits.iter().map(|split| split.split_id()).collect_vec();
        let merge_parent_span = info_span!("merge", merge_split_id=%merge_split_id, split_ids=?split_ids, typ=%MergeOperationType::Merge);
        Self {
            merge_parent_span,
            merge_split_id,
            splits,
            operation_type: MergeOperationType::Merge,
        }
    }

    pub fn total_num_bytes(&self) -> u64 {
        self.splits
            .iter()
            .map(|split: &SplitMetadata| split.footer_offsets.end)
            .sum()
    }

    pub fn new_delete_and_merge_operation(split: SplitMetadata) -> Self {
        let merge_split_id = new_split_id();
        let merge_parent_span = info_span!("delete", merge_split_id=%merge_split_id, split_ids=?split.split_id(), typ=%MergeOperationType::DeleteAndMerge);
        Self {
            merge_parent_span,
            merge_split_id,
            splits: vec![split],
            operation_type: MergeOperationType::DeleteAndMerge,
        }
    }

    pub fn splits_as_slice(&self) -> &[SplitMetadata] {
        self.splits.as_slice()
    }
}

impl fmt::Debug for MergeOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Merge(operation_type={}, merged_split_id={},splits=[",
            self.operation_type, self.merge_split_id
        )?;
        for split in &self.splits {
            write!(f, "{},", split.split_id())?;
        }
        write!(f, "])")?;
        Ok(())
    }
}

/// A merge policy wraps the logic that decides what should be merged.
/// The SplitMetadata must be extracted from the splits `Vec`.
///
/// It is called by the merge planner whenever a new split is added.
pub trait MergePolicy: Send + Sync + fmt::Debug {
    /// Returns the list of merge operations that should be performed.
    fn operations(&self, splits: &mut Vec<SplitMetadata>) -> Vec<MergeOperation>;

    /// After the last indexing pipeline has been shutdown, quickwit
    /// finishes the ongoing merge operations, and eventually needs to shut it down.
    ///
    /// This method makes it possible to offer a last list of merge operations before
    /// really shutting down the merge policy.
    ///
    /// This is especially useful for users relying on a one-index-per-day scheme.
    fn finalize_operations(&self, _splits: &mut Vec<SplitMetadata>) -> Vec<MergeOperation> {
        Vec::new()
    }

    /// Returns split maturity.
    /// A split is either:
    /// - `Mature` if it does not undergo new merge operations.
    /// - or `Immature` with a `maturation_period` after which it becomes mature.
    fn split_maturity(&self, split_num_docs: usize, split_num_merge_ops: usize) -> SplitMaturity;

    /// Checks a bunch of properties specific to the given merge policy.
    /// This method is used in proptesting.
    ///
    /// - `merge_op` is a merge operation emitted by this merge policy.
    /// - `remaining_splits` is the list of remaining splits.
    #[cfg(test)]
    fn check_is_valid(&self, _merge_op: &MergeOperation, _remaining_splits: &[SplitMetadata]) {}
}

pub fn merge_policy_from_settings(settings: &IndexingSettings) -> Arc<dyn MergePolicy> {
    match settings.merge_policy.clone() {
        MergePolicyConfig::Nop => Arc::new(NopMergePolicy),
        MergePolicyConfig::ConstWriteAmplification(config) => {
            let merge_policy =
                ConstWriteAmplificationMergePolicy::new(config, settings.split_num_docs_target);
            Arc::new(merge_policy)
        }
        MergePolicyConfig::StableLog(config) => {
            let merge_policy = StableLogMergePolicy::new(config, settings.split_num_docs_target);
            Arc::new(merge_policy)
        }
    }
}

pub fn default_merge_policy() -> Arc<dyn MergePolicy> {
    let indexing_settings = IndexingSettings::default();
    merge_policy_from_settings(&indexing_settings)
}

pub fn nop_merge_policy() -> Arc<dyn MergePolicy> {
    Arc::new(NopMergePolicy)
}

struct SplitShortDebug<'a>(&'a SplitMetadata);

impl fmt::Debug for SplitShortDebug<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Split")
            .field("split_id", &self.0.split_id())
            .field("num_docs", &self.0.num_docs)
            .finish()
    }
}

fn splits_short_debug(splits: &[SplitMetadata]) -> Vec<SplitShortDebug> {
    splits.iter().map(SplitShortDebug).collect()
}

#[cfg(test)]
pub mod tests {

    use std::collections::hash_map::DefaultHasher;
    use std::collections::{BTreeSet, HashMap};
    use std::hash::Hasher;
    use std::ops::RangeInclusive;

    use proptest::prelude::*;
    use quickwit_actors::Universe;
    use quickwit_proto::indexing::{IndexingPipelineId, MergePipelineId};
    use quickwit_proto::types::{IndexUid, NodeId, PipelineUid};
    use rand::seq::SliceRandom;
    use time::OffsetDateTime;

    use super::*;
    use crate::actors::{
        merge_split_attrs, MergePlanner, MergeSchedulerService, MergeSplitDownloader,
        RunFinalizeMergePolicyAndQuit,
    };
    use crate::models::{create_split_metadata, NewSplits};

    fn pow_of_10(n: usize) -> usize {
        10usize.pow(n as u32)
    }

    prop_compose! {
        fn num_docs_around_power_of_ten()(
            pow_ten in 1usize..5usize,
            diff in -2isize..2isize
        ) -> usize {
            (pow_of_10(pow_ten) as isize + diff).max(1isize) as usize
        }
    }

    fn num_docs_strategy() -> impl Strategy<Value = usize> {
        prop_oneof![1usize..10_000_000usize, num_docs_around_power_of_ten()]
    }

    prop_compose! {
      fn split_strategy()
        (num_merge_ops in 0usize..5usize, start_timestamp in 1_664_000_000i64..1_665_000_000i64, average_time_delta in 100i64..120i64, delta_creation_date in 0u64..100_000u64, num_docs in num_docs_strategy()) -> SplitMetadata {
        let split_id = crate::new_split_id();
        let end_timestamp = start_timestamp + average_time_delta * pow_of_10(num_merge_ops) as i64;
        let create_timestamp: i64 = (end_timestamp as u64 + delta_creation_date) as i64;
        SplitMetadata {
            split_id,
            time_range: Some(start_timestamp..=end_timestamp),
            num_docs,
            create_timestamp,
            num_merge_ops,
            .. Default::default()
        }
      }
    }

    pub(crate) fn create_splits(
        merge_policy: &dyn MergePolicy,
        num_docs_vec: Vec<usize>,
    ) -> Vec<SplitMetadata> {
        let num_docs_with_timestamp = num_docs_vec
            .into_iter()
            // we give the same timestamp to all of them and rely on stable sort to keep the split
            // order.
            .map(|num_docs| (num_docs, (1630563067..=1630564067)))
            .collect();
        create_splits_with_timestamps(merge_policy, num_docs_with_timestamp)
    }

    fn create_splits_with_timestamps(
        merge_policy: &dyn MergePolicy,
        num_docs_vec: Vec<(usize, RangeInclusive<i64>)>,
    ) -> Vec<SplitMetadata> {
        num_docs_vec
            .into_iter()
            .enumerate()
            .map(|(split_ord, (num_docs, time_range))| {
                let create_timestamp = OffsetDateTime::now_utc().unix_timestamp();
                let time_to_maturity = merge_policy.split_maturity(num_docs, 0);
                SplitMetadata {
                    split_id: format!("split_{split_ord:02}"),
                    num_docs,
                    time_range: Some(time_range),
                    create_timestamp,
                    maturity: time_to_maturity,
                    ..Default::default()
                }
            })
            .collect()
    }

    // Creates a checksum for a given merge operation.
    // This does not take in account the merge split id,
    // and is split order independent.
    fn compute_checksum_op(op: &MergeOperation) -> u64 {
        let mut checksum = 0u64;
        for split in op.splits_as_slice() {
            let mut hasher = DefaultHasher::default();
            hasher.write(split.split_id.as_bytes());
            checksum ^= hasher.finish();
        }
        checksum
    }

    // Creates a checksum for a set of operations.
    // This checksum does not depend on the order of the merrge operations,
    // nor the merge split ids.
    fn compute_checksum_ops(ops: &[MergeOperation]) -> u64 {
        let mut checksum = 0u64;
        for op in ops {
            let op_checksum = compute_checksum_op(op);
            let mut hasher = DefaultHasher::default();
            hasher.write_u64(op_checksum);
            checksum ^= hasher.finish();
        }
        checksum
    }

    fn compare_merge_operations(left_ops: &[MergeOperation], right_ops: &[MergeOperation]) -> bool {
        compute_checksum_ops(left_ops) == compute_checksum_ops(right_ops)
    }

    pub(crate) fn proptest_merge_policy(merge_policy: &dyn MergePolicy) {
        proptest!(|(mut splits in prop::collection::vec(split_strategy(), 0..100))| {
            let mut cloned_splits = splits.clone();
            cloned_splits.shuffle(&mut rand::thread_rng());

            let original_num_splits = splits.len();

            let mut operations: Vec<MergeOperation> = merge_policy.operations(&mut splits);
            let operations_after_shuffle = merge_policy.operations(&mut cloned_splits);
            assert!(compare_merge_operations(&operations[..],
                &operations_after_shuffle[..]),
                "Merge policy result should be independent from the original order.");

            let num_splits_in_merge: usize = operations.iter().map(|op| op.splits_as_slice().len()).sum();

            assert_eq!(
                num_splits_in_merge + splits.len(), original_num_splits,
                "Splits should not be lost."
            );

            // This property is not uninteresting but is currently not observed
            // in the stable log merge policy.
            // assert!(
            //     merge_policy.operations(&mut splits).is_empty(),
            //     "Merge policy are expected to return all available merge operations."
            // );
            let now_utc = OffsetDateTime::now_utc();
            for merge_op in &mut operations {
                assert_eq!(merge_op.operation_type, MergeOperationType::Merge,
                    "A merge policy should only emit Merge operations."
                );
                assert!(merge_op.splits_as_slice().len() >= 2,
            "Merge policies should not suggest merging a single split.");
                for split in merge_op.splits_as_slice() {
                    assert!(!split.is_mature(now_utc), "Merges should not contain mature splits.");
                }
                merge_policy.check_is_valid(merge_op, &splits[..]);
            }
        });
    }

    fn merge_tags(splits: &[SplitMetadata]) -> BTreeSet<String> {
        splits
            .iter()
            .flat_map(|split| split.tags.iter().cloned())
            .collect()
    }

    fn fake_merge(merge_policy: &Arc<dyn MergePolicy>, splits: &[SplitMetadata]) -> SplitMetadata {
        assert!(!splits.is_empty(), "Split list should not be empty.");
        let merged_split_id = new_split_id();
        let tags = merge_tags(splits);
        let pipeline_id = MergePipelineId {
            node_id: NodeId::from("test_node"),
            index_uid: IndexUid::new_with_random_ulid("test_index"),
            source_id: "test_source".to_string(),
        };
        let split_attrs = merge_split_attrs(pipeline_id, merged_split_id, splits).unwrap();
        create_split_metadata(merge_policy, None, &split_attrs, tags, 0..0)
    }

    fn apply_merge(
        merge_policy: &Arc<dyn MergePolicy>,
        split_index: &mut HashMap<String, SplitMetadata>,
        merge_op: TrackedObject<MergeOperation>,
    ) -> SplitMetadata {
        for split in merge_op.splits_as_slice() {
            assert!(split_index.remove(split.split_id()).is_some());
        }
        let merged_split = fake_merge(merge_policy, merge_op.splits_as_slice());
        split_index.insert(merged_split.split_id().to_string(), merged_split.clone());
        merge_op.acknowledge();
        merged_split
    }

    async fn aux_test_simulate_merge_planner(
        merge_policy: Arc<dyn MergePolicy>,
        incoming_splits: Vec<SplitMetadata>,
        check_final_configuration: &dyn Fn(&[SplitMetadata]),
    ) -> anyhow::Result<Vec<SplitMetadata>> {
        let universe = Universe::new();
        let (merge_task_mailbox, merge_task_inbox) =
            universe.create_test_mailbox::<MergeSplitDownloader>();
        let pipeline_id = IndexingPipelineId {
            index_uid: IndexUid::new_with_random_ulid("test-index"),
            source_id: "test-source".to_string(),
            node_id: NodeId::from("test-node"),
            pipeline_uid: PipelineUid::default(),
        };
        let merge_planner = MergePlanner::new(
            &pipeline_id.merge_pipeline_id(),
            Vec::new(),
            merge_policy.clone(),
            merge_task_mailbox,
            universe.get_or_spawn_one::<MergeSchedulerService>(),
        )
        .await?;
        let mut split_index: HashMap<String, SplitMetadata> = HashMap::default();
        let (merge_planner_mailbox, merge_planner_handler) =
            universe.spawn_builder().spawn(merge_planner);

        for split in incoming_splits {
            split_index.insert(split.split_id().to_string(), split.clone());
            merge_planner_mailbox
                .send_message(NewSplits {
                    new_splits: vec![split],
                })
                .await?;
            loop {
                let obs = merge_planner_handler.process_pending_and_observe().await;
                assert_eq!(obs.obs_type, quickwit_actors::ObservationType::Alive);
                let merge_tasks = merge_task_inbox.drain_for_test_typed::<MergeTask>();
                if merge_tasks.is_empty() {
                    break;
                }
                let new_splits: Vec<SplitMetadata> = merge_tasks
                    .into_iter()
                    .map(|merge_op| {
                        apply_merge(&merge_policy, &mut split_index, merge_op.merge_operation)
                    })
                    .collect();
                merge_planner_mailbox
                    .send_message(NewSplits { new_splits })
                    .await?;
            }
            let split_metadatas: Vec<SplitMetadata> = split_index.values().cloned().collect();
            check_final_configuration(&split_metadatas);
        }

        merge_planner_mailbox
            .send_message(RunFinalizeMergePolicyAndQuit)
            .await
            .unwrap();

        let obs = merge_planner_handler.process_pending_and_observe().await;
        assert_eq!(obs.obs_type, quickwit_actors::ObservationType::PostMortem);

        let merge_tasks = merge_task_inbox.drain_for_test_typed::<MergeTask>();
        for merge_task in merge_tasks {
            apply_merge(&merge_policy, &mut split_index, merge_task.merge_operation);
        }

        let split_metadatas: Vec<SplitMetadata> = split_index.values().cloned().collect();

        universe.assert_quit().await;
        Ok(split_metadatas)
    }

    /// Mock split meta helper.
    fn mock_split_meta_from_num_docs(
        time_range: RangeInclusive<i64>,
        num_docs: u64,
        maturity: SplitMaturity,
    ) -> SplitMetadata {
        SplitMetadata {
            split_id: crate::new_split_id(),
            partition_id: 3u64,
            num_docs: num_docs as usize,
            uncompressed_docs_size_in_bytes: 256u64 * num_docs,
            time_range: Some(time_range),
            create_timestamp: OffsetDateTime::now_utc().unix_timestamp(),
            maturity,
            tags: BTreeSet::from_iter(vec!["tenant_id:1".to_string(), "tenant_id:2".to_string()]),
            footer_offsets: 0..100,
            index_uid: IndexUid::new_with_random_ulid("test-index"),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            ..Default::default()
        }
    }

    pub async fn aux_test_simulate_merge_planner_num_docs(
        merge_policy: Arc<dyn MergePolicy>,
        batch_num_docs: &[usize],
        check_final_configuration: &dyn Fn(&[SplitMetadata]),
    ) -> anyhow::Result<Vec<SplitMetadata>> {
        let split_metadatas: Vec<SplitMetadata> = batch_num_docs
            .iter()
            .cloned()
            .enumerate()
            .map(|(split_ord, num_docs)| {
                let time_first = split_ord as i64 * 1_000;
                let time_last = time_first + 999;
                let time_range = time_first..=time_last;
                let time_to_maturity = merge_policy.split_maturity(num_docs, 0);
                mock_split_meta_from_num_docs(time_range, num_docs as u64, time_to_maturity)
            })
            .collect();
        aux_test_simulate_merge_planner(merge_policy, split_metadatas, check_final_configuration)
            .await
    }
}
