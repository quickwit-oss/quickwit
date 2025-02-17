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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::tracker::Tracker;
use quickwit_metastore::SplitMetadata;
use quickwit_proto::indexing::MergePipelineId;
use quickwit_proto::types::DocMappingUid;
use serde::Serialize;
use time::OffsetDateTime;
use tracing::{info, warn};

use super::MergeSchedulerService;
use crate::actors::merge_scheduler_service::{schedule_merge, GetOperationTracker};
use crate::actors::MergeSplitDownloader;
use crate::merge_policy::MergeOperation;
use crate::models::NewSplits;
use crate::MergePolicy;

#[derive(Debug)]
pub(crate) struct RunFinalizeMergePolicyAndQuit;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MergePartition {
    partition_id: u64,
    doc_mapping_uid: DocMappingUid,
}

impl MergePartition {
    fn from_split_meta(split_meta: &SplitMetadata) -> MergePartition {
        MergePartition {
            partition_id: split_meta.partition_id,
            doc_mapping_uid: split_meta.doc_mapping_uid,
        }
    }
}

/// The merge planner decides when to start a merge task.
pub struct MergePlanner {
    /// A young split is a split that has not reached maturity
    /// yet and can be candidate to merge operations.
    partitioned_young_splits: HashMap<MergePartition, Vec<SplitMetadata>>,

    /// This set contains all of the split ids that we "acknowledged".
    /// The point of this set is to rapidly dismiss redundant `NewSplit` message.
    ///
    /// Complex scenarii that can result in the reemission of
    /// such messages are described in #3627.
    ///
    /// At any given point in time, the set must contains at least:
    /// - young splits (non-mature)
    /// - splits that are currently in merge.
    ///
    /// It also contain other splits, that have gone through a successful
    /// merge and have been deleted for instance.
    ///
    /// We incrementally build this set, by adding new splits to it.
    /// When it becomes too large, we entirely rebuild it.
    known_split_ids: HashSet<String>,
    known_split_ids_recompute_attempt_id: usize,

    merge_policy: Arc<dyn MergePolicy>,

    merge_split_downloader_mailbox: Mailbox<MergeSplitDownloader>,
    merge_scheduler_service: Mailbox<MergeSchedulerService>,

    /// Track ongoing and failed merge operations for this index
    ///
    /// We don't want to emit a new merge for splits already in the process of
    /// being merged, but we want to keep track of failed merges so we can
    /// reschedule them.
    // TODO currently the MergePlanner is teared down when a merge fails, so this
    // mechanism is only useful when there are some merges left from a previous
    // pipeline. We could only tear down the rest of the pipeline on error.
    ongoing_merge_operations_tracker: Tracker<MergeOperation>,

    /// We use the actor start_time as a way to identify incarnations.
    ///
    /// Since we recycle the mailbox of the merge planner, this incarnation
    /// makes it possible to ignore messages that where emitted from the previous
    /// instantiation.
    incarnation_started_at: Instant,
}

#[async_trait]
impl Actor for MergePlanner {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}

    fn name(&self) -> String {
        "MergePlanner".to_string()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        MergePlanner::queue_capacity()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        // We do not call the handle method directly and instead queue the message in order to drain
        // the recycled mailbox and get a consolidated vision of the set of published
        // splits, before scheduling any merge operation. See #3847 for more details.

        // If the mailbox is full, this send message might fail (the capacity is very low).
        // This is however not much of a problem: it probably contains a NewSplit message.
        // If it does not, we will be losing an opportunity to plan merge right away, but it will
        // happen on the next split publication.
        let _ = ctx.try_send_self_message(PlanMerge {
            incarnation_started_at: self.incarnation_started_at,
        });

        Ok(())
    }
}

#[async_trait]
impl Handler<RunFinalizeMergePolicyAndQuit> for MergePlanner {
    type Reply = ();

    async fn handle(
        &mut self,
        _plan_merge: RunFinalizeMergePolicyAndQuit,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        // consume failed merges so that we may try to reschedule them one last time
        for failed_merge in self.ongoing_merge_operations_tracker.take_dead() {
            for split in failed_merge.splits {
                // if they were from a dead merge, we always record them, they are likely
                // already part of our known splits, and we don't want to rebuild the known
                // split list as it's likely to log about not halving its size.
                self.record_split(split);
            }
        }
        self.send_merge_ops(true, ctx).await?;
        Err(ActorExitStatus::Success)
    }
}

#[async_trait]
impl Handler<PlanMerge> for MergePlanner {
    type Reply = ();

    async fn handle(
        &mut self,
        plan_merge: PlanMerge,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if plan_merge.incarnation_started_at == self.incarnation_started_at {
            // Note we ignore messages that could be coming from a different incarnation.
            // (See comment on `Self::incarnation_start_at`.)
            self.send_merge_ops(false, ctx).await?;
        }
        self.recompute_known_splits_if_necessary();
        Ok(())
    }
}

#[async_trait]
impl Handler<NewSplits> for MergePlanner {
    type Reply = ();

    async fn handle(
        &mut self,
        new_splits: NewSplits,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.record_splits_if_necessary(new_splits.new_splits);
        self.send_merge_ops(false, ctx).await?;
        self.recompute_known_splits_if_necessary();
        Ok(())
    }
}

impl MergePlanner {
    pub fn queue_capacity() -> QueueCapacity {
        // We cannot have a Queue capacity of 0 here because `try_send_self`
        // would never succeed.
        QueueCapacity::Bounded(1)
    }

    pub async fn new(
        pipeline_id: &MergePipelineId,
        immature_splits: Vec<SplitMetadata>,
        merge_policy: Arc<dyn MergePolicy>,
        merge_split_downloader_mailbox: Mailbox<MergeSplitDownloader>,
        merge_scheduler_service: Mailbox<MergeSchedulerService>,
    ) -> anyhow::Result<MergePlanner> {
        let immature_splits: Vec<SplitMetadata> = immature_splits
            .into_iter()
            .filter(|split_metadata| belongs_to_pipeline(pipeline_id, split_metadata))
            .collect();
        let ongoing_merge_operations_tracker = merge_scheduler_service
            .ask(GetOperationTracker(pipeline_id.index_uid.clone()))
            .await?;

        let mut known_split_ids: HashSet<String> = HashSet::new();
        let ongoing_merge_operations = ongoing_merge_operations_tracker.list_ongoing();
        for merge_op in ongoing_merge_operations {
            for split in &merge_op.splits {
                known_split_ids.insert(split.split_id().to_string());
            }
        }

        let mut merge_planner = MergePlanner {
            known_split_ids,
            known_split_ids_recompute_attempt_id: 0,
            partitioned_young_splits: Default::default(),
            merge_policy,
            merge_split_downloader_mailbox,
            merge_scheduler_service,
            ongoing_merge_operations_tracker,

            incarnation_started_at: Instant::now(),
        };
        merge_planner.record_splits_if_necessary(immature_splits);
        Ok(merge_planner)
    }

    fn rebuild_known_split_ids(&self) -> HashSet<String> {
        let mut known_split_ids: HashSet<String> = HashSet::default();
        // Add splits that in `partitioned_young_splits`.
        for young_split_partition in self.partitioned_young_splits.values() {
            for split in young_split_partition {
                known_split_ids.insert(split.split_id().to_string());
            }
        }
        let ongoing_merge_operations = self.ongoing_merge_operations_tracker.list_ongoing();
        // Add splits that are known as in merge.
        for merge_op in ongoing_merge_operations {
            for split in &merge_op.splits {
                known_split_ids.insert(split.split_id().to_string());
            }
        }
        if known_split_ids.len() * 2 >= self.known_split_ids.len() {
            warn!(
                known_split_ids_len_after = known_split_ids.len(),
                known_split_ids_len_before = self.known_split_ids.len(),
                "Rebuilding the known split ids set ended up not halving its size. Please report. \
                 This is likely a bug, please report."
            );
        }
        known_split_ids
    }

    /// Updates `known_split_ids` and return true if the split was not
    /// previously known and should be recorded.
    fn acknowledge_split(&mut self, split_id: &str) -> bool {
        if self.known_split_ids.contains(split_id) {
            return false;
        }
        self.known_split_ids.insert(split_id.to_string());
        true
    }

    // No need to rebuild every time, we do once out of 100 times.
    fn recompute_known_splits_if_necessary(&mut self) {
        self.known_split_ids_recompute_attempt_id += 1;
        if self.known_split_ids_recompute_attempt_id % 100 == 0 {
            self.known_split_ids = self.rebuild_known_split_ids();
            self.known_split_ids_recompute_attempt_id = 0;

            for failed_merge in self.ongoing_merge_operations_tracker.take_dead() {
                self.record_splits_if_necessary(failed_merge.splits);
            }
        }
    }

    // Record a split. This function does NOT check if the split is mature or not, or if the split
    // is known or not.
    fn record_split(&mut self, new_split: SplitMetadata) {
        let splits_for_partition: &mut Vec<SplitMetadata> = self
            .partitioned_young_splits
            .entry(MergePartition::from_split_meta(&new_split))
            .or_default();
        splits_for_partition.push(new_split);
    }

    // Records a list of splits.
    //
    // Internally this function will detect and avoid adding the split
    // that are:
    // - already known
    // - mature
    // - do not belong to the current timeline.
    fn record_splits_if_necessary(&mut self, split_metadatas: Vec<SplitMetadata>) {
        for new_split in split_metadatas {
            if new_split.is_mature(OffsetDateTime::now_utc()) {
                continue;
            }
            // Due to the recycling of the mailbox of the merge planner, it is possible for
            // a split already in store to be received.
            //
            // See `known_split_ids`.
            if !self.acknowledge_split(new_split.split_id()) {
                continue;
            }
            self.record_split(new_split);
        }
    }

    async fn compute_merge_ops(
        &mut self,
        is_finalize: bool,
        ctx: &ActorContext<Self>,
    ) -> Result<Vec<MergeOperation>, ActorExitStatus> {
        let mut merge_operations = Vec::new();
        for young_splits in self.partitioned_young_splits.values_mut() {
            if !young_splits.is_empty() {
                let operations = if is_finalize {
                    self.merge_policy.finalize_operations(young_splits)
                } else {
                    self.merge_policy.operations(young_splits)
                };
                merge_operations.extend(operations);
            }
            ctx.record_progress();
            ctx.yield_now().await;
        }
        self.partitioned_young_splits
            .retain(|_, splits| !splits.is_empty());
        // We recompute the number of young splits.
        Ok(merge_operations)
    }

    async fn send_merge_ops(
        &mut self,
        is_finalize: bool,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        // We identify all of the merge operations we want to run and leave it
        // to the merge scheduler to decide in which order these should be scheduled.
        //
        // The merge scheduler has the merit of knowing about merge operations from other
        // index as well.
        let merge_ops = self.compute_merge_ops(is_finalize, ctx).await?;
        for merge_operation in merge_ops {
            info!(merge_operation=?merge_operation, "schedule merge operation");
            let tracked_merge_operation =
                self.ongoing_merge_operations_tracker.track(merge_operation);
            schedule_merge(
                &self.merge_scheduler_service,
                tracked_merge_operation,
                self.merge_split_downloader_mailbox.clone(),
            )
            .await?
        }
        Ok(())
    }
}

/// We can only merge splits with the same (node_id, index_id, source_id).
fn belongs_to_pipeline(pipeline_id: &MergePipelineId, split: &SplitMetadata) -> bool {
    pipeline_id.node_id == split.node_id
        && pipeline_id.index_uid == split.index_uid
        && pipeline_id.source_id == split.source_id
}

#[derive(Debug)]
struct PlanMerge {
    incarnation_started_at: Instant,
}

#[derive(Clone, Debug, Serialize)]
pub struct MergePlannerState {
    pub(crate) ongoing_merge_operations: Vec<MergeOperation>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use itertools::Itertools;
    use quickwit_actors::{ActorExitStatus, Command, QueueCapacity, Universe};
    use quickwit_config::merge_policy_config::{
        ConstWriteAmplificationMergePolicyConfig, MergePolicyConfig, StableLogMergePolicyConfig,
    };
    use quickwit_config::IndexingSettings;
    use quickwit_metastore::{SplitMaturity, SplitMetadata};
    use quickwit_proto::indexing::MergePipelineId;
    use quickwit_proto::types::{DocMappingUid, IndexUid, NodeId};
    use time::OffsetDateTime;

    use crate::actors::MergePlanner;
    use crate::merge_policy::{
        merge_policy_from_settings, MergePolicy, MergeTask, StableLogMergePolicy,
    };
    use crate::models::NewSplits;

    fn split_metadata_for_test(
        index_uid: &IndexUid,
        split_id: &str,
        partition_id: u64,
        doc_mapping_uid: DocMappingUid,
        num_docs: usize,
        num_merge_ops: usize,
    ) -> SplitMetadata {
        SplitMetadata {
            split_id: split_id.to_string(),
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            num_docs,
            partition_id,
            num_merge_ops,
            create_timestamp: OffsetDateTime::now_utc().unix_timestamp(),
            maturity: SplitMaturity::Immature {
                maturation_period: Duration::from_secs(3600),
            },
            doc_mapping_uid,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_merge_planner_with_stable_custom_merge_policy() -> anyhow::Result<()> {
        let node_id = NodeId::from("test-node");
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_id = "test-source".to_string();
        let [doc_mapping_uid1, doc_mapping_uid2] = {
            let mut doc_mappings = [DocMappingUid::random(), DocMappingUid::random()];
            doc_mappings.sort();
            doc_mappings
        };
        let pipeline_id = MergePipelineId {
            node_id,
            index_uid: index_uid.clone(),
            source_id,
        };
        let merge_policy = Arc::new(StableLogMergePolicy::new(
            StableLogMergePolicyConfig {
                min_level_num_docs: 10_000,
                merge_factor: 3,
                max_merge_factor: 5,
                maturation_period: Duration::from_secs(3600),
            },
            50_000,
        ));
        let universe = Universe::with_accelerated_time();
        let (merge_split_downloader_mailbox, merge_split_downloader_inbox) =
            universe.create_test_mailbox();

        let merge_planner = MergePlanner::new(
            &pipeline_id,
            Vec::new(),
            merge_policy,
            merge_split_downloader_mailbox,
            universe.get_or_spawn_one(),
        )
        .await?;
        let (merge_planner_mailbox, merge_planner_handle) =
            universe.spawn_builder().spawn(merge_planner);
        {
            // send one split
            let message = NewSplits {
                new_splits: vec![
                    split_metadata_for_test(&index_uid, "1_1", 1, doc_mapping_uid1, 2500, 0),
                    split_metadata_for_test(&index_uid, "1v2_1", 1, doc_mapping_uid2, 2500, 0),
                    split_metadata_for_test(&index_uid, "1_2", 2, doc_mapping_uid1, 3000, 0),
                ],
            };
            merge_planner_mailbox.send_message(message).await?;
            let merge_ops = merge_split_downloader_inbox.drain_for_test();
            assert_eq!(merge_ops.len(), 0);
        }
        {
            // send two splits with a duplicate
            let message = NewSplits {
                new_splits: vec![
                    split_metadata_for_test(&index_uid, "2_1", 1, doc_mapping_uid1, 2000, 0),
                    split_metadata_for_test(&index_uid, "2v2_1", 1, doc_mapping_uid2, 2500, 0),
                    split_metadata_for_test(&index_uid, "1_2", 2, doc_mapping_uid1, 3000, 0),
                ],
            };
            merge_planner_mailbox.send_message(message).await?;
            let merge_ops = merge_split_downloader_inbox.drain_for_test();
            assert_eq!(merge_ops.len(), 0);
        }
        {
            // send four more splits to generate merge
            let message = NewSplits {
                new_splits: vec![
                    split_metadata_for_test(&index_uid, "3_1", 1, doc_mapping_uid1, 1500, 0),
                    split_metadata_for_test(&index_uid, "4_1", 1, doc_mapping_uid1, 1000, 0),
                    split_metadata_for_test(&index_uid, "3v2_1", 1, doc_mapping_uid2, 1500, 0),
                    split_metadata_for_test(&index_uid, "2_2", 2, doc_mapping_uid1, 2000, 0),
                    split_metadata_for_test(&index_uid, "3_2", 2, doc_mapping_uid1, 4000, 0),
                ],
            };
            merge_planner_mailbox.send_message(message).await?;
            merge_planner_handle.process_pending_and_observe().await;
            let operations = merge_split_downloader_inbox.drain_for_test_typed::<MergeTask>();
            assert_eq!(operations.len(), 3);
            let mut merge_operations = operations
                .into_iter()
                .sorted_by_key(|op| (op.splits[0].partition_id, op.splits[0].doc_mapping_uid));

            let first_merge_operation = merge_operations.next().unwrap();
            assert_eq!(first_merge_operation.splits.len(), 4);
            assert!(first_merge_operation
                .splits
                .iter()
                .all(|split| split.partition_id == 1 && split.doc_mapping_uid == doc_mapping_uid1));

            let second_merge_operation = merge_operations.next().unwrap();
            assert_eq!(second_merge_operation.splits.len(), 3);
            assert!(second_merge_operation
                .splits
                .iter()
                .all(|split| split.partition_id == 1 && split.doc_mapping_uid == doc_mapping_uid2));

            let third_merge_operation = merge_operations.next().unwrap();
            assert_eq!(third_merge_operation.splits.len(), 3);
            assert!(third_merge_operation
                .splits
                .iter()
                .all(|split| split.partition_id == 2 && split.doc_mapping_uid == doc_mapping_uid1));
        }
        universe.assert_quit().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_merge_planner_spawns_merge_over_existing_splits_on_startup() -> anyhow::Result<()>
    {
        let node_id = NodeId::from("test-node");
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_id = "test-source".to_string();
        let doc_mapping_uid = DocMappingUid::random();
        let pipeline_id = MergePipelineId {
            node_id,
            index_uid: index_uid.clone(),
            source_id,
        };
        let universe = Universe::with_accelerated_time();
        let (merge_split_downloader_mailbox, merge_split_downloader_inbox) = universe
            .spawn_ctx()
            .create_mailbox("MergeSplitDownloader", QueueCapacity::Bounded(2));
        let merge_policy_config = ConstWriteAmplificationMergePolicyConfig {
            merge_factor: 2,
            max_merge_factor: 2,
            max_merge_ops: 3,
            ..Default::default()
        };
        let indexing_settings = IndexingSettings {
            merge_policy: MergePolicyConfig::ConstWriteAmplification(merge_policy_config),
            ..Default::default()
        };
        let immature_splits = vec![
            split_metadata_for_test(
                &index_uid,
                "a_small",
                0, // partition_id
                doc_mapping_uid,
                1_000_000,
                2,
            ),
            split_metadata_for_test(
                &index_uid,
                "b_small",
                0, // partition_id
                doc_mapping_uid,
                1_000_000,
                2,
            ),
        ];
        let merge_policy: Arc<dyn MergePolicy> = merge_policy_from_settings(&indexing_settings);
        let merge_planner = MergePlanner::new(
            &pipeline_id,
            immature_splits.clone(),
            merge_policy,
            merge_split_downloader_mailbox,
            universe.get_or_spawn_one(),
        )
        .await?;
        let (merge_planner_mailbox, merge_planner_handle) =
            universe.spawn_builder().spawn(merge_planner);

        // We wait for the first merge ops. If we sent the Quit message right away, it would have
        // been queue before first `PlanMerge` message.
        let merge_task_res = merge_split_downloader_inbox
            .recv_typed_message::<MergeTask>()
            .await;
        assert!(merge_task_res.is_ok());

        // We make sure that the known splits filtering set filters out splits are currently in
        // merge.
        merge_planner_mailbox
            .ask(NewSplits {
                new_splits: immature_splits,
            })
            .await?;

        let _ = merge_planner_handle.process_pending_and_observe().await;

        let merge_ops = merge_split_downloader_inbox.drain_for_test_typed::<MergeTask>();

        assert!(merge_ops.is_empty());

        merge_planner_mailbox.send_message(Command::Quit).await?;

        let (exit_status, _last_state) = merge_planner_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Quit));
        let merge_ops = merge_split_downloader_inbox.drain_for_test_typed::<MergeTask>();
        assert!(merge_ops.is_empty());
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_merge_planner_dismiss_splits_from_different_pipeline_id() -> anyhow::Result<()> {
        let node_id = NodeId::from("test-node");
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_id = "test-source".to_string();
        let doc_mapping_uid = DocMappingUid::random();
        let pipeline_id = MergePipelineId {
            node_id,
            index_uid,
            source_id,
        };
        // This test makes sure that the merge planner ignores the splits that do not belong
        // to the same pipeline
        let universe = Universe::with_accelerated_time();
        let (merge_split_downloader_mailbox, merge_split_downloader_inbox) = universe
            .spawn_ctx()
            .create_mailbox("MergeSplitDownloader", QueueCapacity::Bounded(2));

        let merge_policy_config = ConstWriteAmplificationMergePolicyConfig {
            merge_factor: 2,
            max_merge_factor: 2,
            max_merge_ops: 3,
            ..Default::default()
        };
        let indexing_settings = IndexingSettings {
            merge_policy: MergePolicyConfig::ConstWriteAmplification(merge_policy_config),
            ..Default::default()
        };

        // It is different from the index_uid because the index uid has a unique suffix.
        let other_index_uid = IndexUid::new_with_random_ulid("test-index");

        let immature_splits = vec![
            split_metadata_for_test(
                &other_index_uid,
                "a_small",
                0, // partition_id
                doc_mapping_uid,
                1_000_000,
                2,
            ),
            split_metadata_for_test(
                &other_index_uid,
                "b_small",
                0, // partition_id
                doc_mapping_uid,
                1_000_000,
                2,
            ),
        ];
        let merge_policy: Arc<dyn MergePolicy> = merge_policy_from_settings(&indexing_settings);
        let merge_planner = MergePlanner::new(
            &pipeline_id,
            immature_splits.clone(),
            merge_policy,
            merge_split_downloader_mailbox,
            universe.get_or_spawn_one(),
        )
        .await?;
        let (merge_planner_mailbox, merge_planner_handle) =
            universe.spawn_builder().spawn(merge_planner);
        universe.sleep(Duration::from_secs(10)).await;
        merge_planner_mailbox.send_message(Command::Quit).await?;
        let (exit_status, _last_state) = merge_planner_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Quit));
        let merge_tasks = merge_split_downloader_inbox.drain_for_test_typed::<MergeTask>();

        assert!(merge_tasks.is_empty());
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_merge_planner_inherit_mailbox_with_splits_bug_3847() -> anyhow::Result<()> {
        let node_id = NodeId::from("test-node");
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_id = "test-source".to_string();
        let doc_mapping_uid = DocMappingUid::random();
        let pipeline_id = MergePipelineId {
            node_id,
            index_uid: index_uid.clone(),
            source_id,
        };
        let universe = Universe::with_accelerated_time();
        let (merge_split_downloader_mailbox, merge_split_downloader_inbox) = universe
            .spawn_ctx()
            .create_mailbox("MergeSplitDownloader", QueueCapacity::Bounded(2));

        let merge_policy_config = ConstWriteAmplificationMergePolicyConfig {
            merge_factor: 2,
            max_merge_factor: 2,
            max_merge_ops: 3,
            ..Default::default()
        };
        let indexing_settings = IndexingSettings {
            merge_policy: MergePolicyConfig::ConstWriteAmplification(merge_policy_config),
            ..Default::default()
        };
        let immature_splits = vec![
            split_metadata_for_test(
                &index_uid,
                "a_small",
                0, // partition_id
                doc_mapping_uid,
                1_000_000,
                2,
            ),
            split_metadata_for_test(
                &index_uid,
                "b_small",
                0, // partition_id
                doc_mapping_uid,
                1_000_000,
                2,
            ),
        ];
        let merge_policy: Arc<dyn MergePolicy> = merge_policy_from_settings(&indexing_settings);
        let merge_planner = MergePlanner::new(
            &pipeline_id,
            immature_splits.clone(),
            merge_policy,
            merge_split_downloader_mailbox,
            universe.get_or_spawn_one(),
        )
        .await?;
        // We create a fake old mailbox that contains two new splits and a PlanMerge message from an
        // old incarnation. This could happen in real life if the merge pipeline failed
        // right after a `PlanMerge` was pushed to the pipeline. Note that #3847 did not
        // even require the `PlanMerge` to be in the pipeline
        let (merge_planner_mailbox, merge_planner_inbox) =
            universe.create_test_mailbox::<MergePlanner>();

        // We spawn our merge planner with this recycled mailbox.
        let (merge_planner_mailbox, merge_planner_handle) = universe
            .spawn_builder()
            .set_mailboxes(merge_planner_mailbox, merge_planner_inbox)
            .spawn(merge_planner);

        // The low capacity of the queue of the merge planner prevents us from sending a Command in
        // the low priority queue. It would take the single slot and prevent the message
        // sent in the initialize method.

        // Instead, we wait for the first merge ops.
        let merge_task_res = merge_split_downloader_inbox
            .recv_typed_message::<MergeTask>()
            .await;
        assert!(merge_task_res.is_ok());

        // At this point, our merge has been initialized.
        merge_planner_mailbox.send_message(Command::Quit).await?;
        let (exit_status, _last_state) = merge_planner_handle.join().await;

        assert!(matches!(exit_status, ActorExitStatus::Quit));
        let merge_tasks = merge_split_downloader_inbox.drain_for_test_typed::<MergeTask>();
        assert!(merge_tasks.is_empty());

        universe.assert_quit().await;
        Ok(())
    }
}
