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

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use itertools::Itertools;
use quickwit_actors::channel_with_priority::TrySendError;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_metastore::SplitMetadata;
use quickwit_proto::indexing::IndexingPipelineId;
use serde::Serialize;
use tantivy::Inventory;
use time::OffsetDateTime;
use tracing::{info, warn};

use crate::actors::MergeSplitDownloader;
use crate::merge_policy::MergeOperation;
use crate::metrics::INDEXER_METRICS;
use crate::models::NewSplits;
use crate::MergePolicy;

/// The merge planner decides when to start a merge task.
pub struct MergePlanner {
    pipeline_id: IndexingPipelineId,

    /// A young split is a split that has not reached maturity
    /// yet and can be candidate to merge operations.
    partitioned_young_splits: HashMap<u64, Vec<SplitMetadata>>,

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

    merge_policy: Arc<dyn MergePolicy>,
    merge_split_downloader_mailbox: Mailbox<MergeSplitDownloader>,

    /// Inventory of ongoing merge operations. If everything goes well,
    /// a merge operation is dropped after the publish of the merged split.
    /// Used for observability.
    ongoing_merge_operations_inventory: Inventory<MergeOperation>,

    /// We use the actor start_time as a way to identify incarnations.
    ///
    /// Since we recycle the mailbox of the merge planner, this incarnation
    /// makes it possible to ignore messages that where emitted from the previous
    /// instantiation.
    ///
    /// In particular, it is necessary to avoid ever increasing the number
    /// `RefreshMetrics` loop, every time the `MergePlanner` is respawned.
    incarnation_started_at: Instant,
}

#[async_trait]
impl Actor for MergePlanner {
    type ObservableState = MergePlannerState;

    fn observable_state(&self) -> Self::ObservableState {
        let ongoing_merge_operations = self
            .ongoing_merge_operations_inventory
            .list()
            .iter()
            .map(|tracked_operation| tracked_operation.as_ref().clone())
            .collect_vec();
        MergePlannerState {
            ongoing_merge_operations,
        }
    }

    fn name(&self) -> String {
        "MergePlanner".to_string()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        MergePlanner::queue_capacity()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.handle(
            RefreshMetrics {
                incarnation_started_at: self.incarnation_started_at,
            },
            ctx,
        )
        .await?;
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
            self.send_merge_ops(ctx).await?;
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
        self.send_merge_ops(ctx).await?;
        if self.known_split_ids.len() >= self.num_known_splits_rebuild_threshold() {
            self.known_split_ids = self.rebuild_known_split_ids();
        }
        self.recompute_known_splits_if_necessary();
        Ok(())
    }
}

fn max_merge_ops(merge_op: &MergeOperation) -> usize {
    merge_op
        .splits_as_slice()
        .iter()
        .map(|split_metadata| split_metadata.num_merge_ops)
        .max()
        .unwrap_or(0)
}

impl MergePlanner {
    pub fn queue_capacity() -> QueueCapacity {
        // We cannot have a Queue capacity of 0 here because `try_send_self`
        // would never succeed.
        QueueCapacity::Bounded(1)
    }

    pub fn new(
        pipeline_id: IndexingPipelineId,
        published_splits: Vec<SplitMetadata>,
        merge_policy: Arc<dyn MergePolicy>,
        merge_split_downloader_mailbox: Mailbox<MergeSplitDownloader>,
    ) -> MergePlanner {
        let published_splits: Vec<SplitMetadata> = published_splits
            .into_iter()
            .filter(|split_metadata| belongs_to_pipeline(&pipeline_id, split_metadata))
            .collect();
        let mut merge_planner = MergePlanner {
            pipeline_id,
            known_split_ids: Default::default(),
            partitioned_young_splits: Default::default(),
            merge_policy,
            merge_split_downloader_mailbox,
            ongoing_merge_operations_inventory: Inventory::default(),
            incarnation_started_at: Instant::now(),
        };
        merge_planner.record_splits_if_necessary(published_splits);
        merge_planner
    }

    fn rebuild_known_split_ids(&self) -> HashSet<String> {
        let mut known_split_ids: HashSet<String> =
            HashSet::with_capacity(self.num_known_splits_rebuild_threshold());
        // Add splits that in `partitioned_young_splits`.
        for young_split_partition in self.partitioned_young_splits.values() {
            for split in young_split_partition {
                known_split_ids.insert(split.split_id().to_string());
            }
        }
        let ongoing_merge_operations = self.ongoing_merge_operations_inventory.list();
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
    fn acknownledge_split(&mut self, split_id: &str) -> bool {
        if self.known_split_ids.contains(split_id) {
            return false;
        }
        self.known_split_ids.insert(split_id.to_string());
        true
    }

    fn recompute_known_splits_if_necessary(&mut self) {
        if self.known_split_ids.len() >= self.num_known_splits_rebuild_threshold() {
            self.known_split_ids = self.rebuild_known_split_ids();
        }
        if cfg!(test) {
            let merge_operation = self.ongoing_merge_operations_inventory.list();
            let mut young_splits = HashSet::new();
            for (&partition_id, young_splits_in_partition) in &self.partitioned_young_splits {
                for split_metadata in young_splits_in_partition {
                    assert_eq!(split_metadata.partition_id, partition_id);
                    young_splits.insert(split_metadata.split_id());
                }
            }
            for merge_op in merge_operation {
                assert!(!self.known_split_ids.contains(&merge_op.merge_split_id));
                for split_in_merge in merge_op.splits_as_slice() {
                    assert!(self.known_split_ids.contains(split_in_merge.split_id()));
                }
            }
            assert!(self.known_split_ids.len() <= self.num_known_splits_rebuild_threshold() + 1);
        }
    }

    /// Whenever the number of known splits exceeds this threshold, we rebuild the `known_split_ids`
    /// set.
    ///
    /// We have this function to return a number that is higher than 2 times the len of
    /// `known_split_ids` after a rebuild to get amortization.
    fn num_known_splits_rebuild_threshold(&self) -> usize {
        // The idea behind this formula is that we expect the max legitimate of splits after a
        // rebuild to be  `num_young_splits` + `num_splits_merge`.
        // The capacity of `partioned_young_splits` is a good upper bound for the number of
        // partition.
        //
        // We can expect a maximum of 100 ongoing splits in merge per partition. (We oversize this
        // because it actually depends on the merge factor.
        1 + self.num_young_splits() + (1 + self.partitioned_young_splits.capacity()) * 20
    }

    // Record a split. This function does NOT check if the split is mature or not, or if the split
    // is known or not.
    fn record_split(&mut self, new_split: SplitMetadata) {
        let splits_for_partition: &mut Vec<SplitMetadata> = self
            .partitioned_young_splits
            .entry(new_split.partition_id)
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
            if !self.acknownledge_split(new_split.split_id()) {
                continue;
            }
            self.record_split(new_split);
        }
    }
    async fn compute_merge_ops(
        &mut self,
        ctx: &ActorContext<Self>,
    ) -> Result<Vec<MergeOperation>, ActorExitStatus> {
        let mut merge_operations = Vec::new();
        for young_splits in self.partitioned_young_splits.values_mut() {
            if !young_splits.is_empty() {
                merge_operations.extend(self.merge_policy.operations(young_splits));
            }
            ctx.record_progress();
            ctx.yield_now().await;
        }
        self.partitioned_young_splits
            .retain(|_, splits| !splits.is_empty());
        // We recompute the number of young splits.
        Ok(merge_operations)
    }

    fn num_young_splits(&self) -> usize {
        self.partitioned_young_splits
            .values()
            .map(|splits| splits.len())
            .sum()
    }

    async fn send_merge_ops(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        // We do not want to simply schedule all available merge operations here.
        //
        // The reason is that in presence of partitioning, it is very possible
        // to receive a set of splits opening the opportunity to run a lot "large" merge
        // operations at the same time.
        //
        // These large merge operation will in turn prevent small merge operations
        // from being executed, when in fact small merge operations should be executed
        // in priority.
        //
        // As an alternative approach, this function push merge operations until it starts
        // experience some push back, and then just "loops".
        let mut merge_ops = self.compute_merge_ops(ctx).await?;
        // We run smaller merges in priority.
        merge_ops.sort_by_cached_key(|merge_op| Reverse(max_merge_ops(merge_op)));
        while let Some(merge_operation) = merge_ops.pop() {
            info!(merge_operation=?merge_operation, "Planned merge operation.");
            let tracked_merge_operation = self
                .ongoing_merge_operations_inventory
                .track(merge_operation);
            if let Err(try_send_err) = self
                .merge_split_downloader_mailbox
                .try_send_message(tracked_merge_operation)
            {
                match try_send_err {
                    TrySendError::Disconnected => {
                        return Err(ActorExitStatus::DownstreamClosed);
                    }
                    TrySendError::Full(merge_op) => {
                        ctx.send_message(&self.merge_split_downloader_mailbox, merge_op)
                            .await?;
                        break;
                    }
                }
            }
        }
        if !merge_ops.is_empty() {
            // We experienced some push back and decided to stop queueing too
            // many merge operation. (For more detail see #2348)
            //
            // We need to re-record the related split, so that we
            // perform a merge in the future.
            for merge_op in merge_ops {
                for split in merge_op.splits {
                    self.record_split(split);
                }
            }
            // We try_self_send a `PlanMerge` message in order to ensure that
            // progress on our merges.
            //
            // If `try_send_self_message` returns an error, it means that the
            // the self queue is full, which means that the `PlanMerge`
            // message is not really needed anyway.
            let _ignored_result = ctx.try_send_self_message(PlanMerge {
                incarnation_started_at: self.incarnation_started_at,
            });
        }
        Ok(())
    }
}

/// We can merge splits from the same (index_id, source_id, node_id).
fn belongs_to_pipeline(pipeline_id: &IndexingPipelineId, split: &SplitMetadata) -> bool {
    pipeline_id.index_uid == split.index_uid
        && pipeline_id.source_id == split.source_id
        && pipeline_id.node_id == split.node_id
}

#[derive(Debug)]
struct RefreshMetrics {
    incarnation_started_at: Instant,
}

#[derive(Debug)]
struct PlanMerge {
    incarnation_started_at: Instant,
}

#[async_trait]
impl Handler<RefreshMetrics> for MergePlanner {
    type Reply = ();

    async fn handle(
        &mut self,
        refresh_metric: RefreshMetrics,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if self.incarnation_started_at != refresh_metric.incarnation_started_at {
            // This message was emitted by a different incarnation.
            // (See `Self::incarnation_started_at`)
            return Ok(());
        }
        INDEXER_METRICS
            .ongoing_merge_operations
            .with_label_values([
                self.pipeline_id.index_uid.index_id(),
                self.pipeline_id.source_id.as_str(),
            ])
            .set(self.ongoing_merge_operations_inventory.list().len() as i64);
        ctx.schedule_self_msg(
            *quickwit_actors::HEARTBEAT,
            RefreshMetrics {
                incarnation_started_at: self.incarnation_started_at,
            },
        )
        .await;
        Ok(())
    }
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
    use quickwit_proto::indexing::IndexingPipelineId;
    use quickwit_proto::types::IndexUid;
    use tantivy::TrackedObject;
    use time::OffsetDateTime;

    use crate::actors::MergePlanner;
    use crate::merge_policy::{
        merge_policy_from_settings, MergeOperation, MergePolicy, StableLogMergePolicy,
    };
    use crate::models::NewSplits;

    fn split_metadata_for_test(
        index_uid: &IndexUid,
        split_id: &str,
        partition_id: u64,
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
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_merge_planner_with_stable_custom_merge_policy() -> anyhow::Result<()> {
        let universe = Universe::with_accelerated_time();
        let index_uid = IndexUid::new("test-index");
        let (merge_split_downloader_mailbox, merge_split_downloader_inbox) =
            universe.create_test_mailbox();
        let pipeline_id = IndexingPipelineId {
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
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
        let merge_planner = MergePlanner::new(
            pipeline_id,
            Vec::new(),
            merge_policy,
            merge_split_downloader_mailbox,
        );

        let (merge_planner_mailbox, merge_planner_handle) =
            universe.spawn_builder().spawn(merge_planner);
        {
            // send one split
            let message = NewSplits {
                new_splits: vec![
                    split_metadata_for_test(&index_uid, "1_1", 1, 2500, 0),
                    split_metadata_for_test(&index_uid, "1_2", 2, 3000, 0),
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
                    split_metadata_for_test(&index_uid, "2_1", 1, 2000, 0),
                    split_metadata_for_test(&index_uid, "1_2", 2, 3000, 0),
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
                    split_metadata_for_test(&index_uid, "3_1", 1, 1500, 0),
                    split_metadata_for_test(&index_uid, "4_1", 1, 1000, 0),
                    split_metadata_for_test(&index_uid, "2_2", 2, 2000, 0),
                    split_metadata_for_test(&index_uid, "3_2", 2, 4000, 0),
                ],
            };
            merge_planner_mailbox.send_message(message).await?;
            merge_planner_handle.process_pending_and_observe().await;
            let operations = merge_split_downloader_inbox
                .drain_for_test_typed::<TrackedObject<MergeOperation>>();
            assert_eq!(operations.len(), 2);
            let mut merge_operations = operations.into_iter().sorted_by(|left_op, right_op| {
                left_op.splits[0]
                    .partition_id
                    .cmp(&right_op.splits[0].partition_id)
            });

            let first_merge_operation = merge_operations.next().unwrap();
            assert_eq!(first_merge_operation.splits.len(), 4);

            let second_merge_operation = merge_operations.next().unwrap();
            assert_eq!(second_merge_operation.splits.len(), 3);
        }
        universe.assert_quit().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_merge_planner_priority() -> anyhow::Result<()> {
        let universe = Universe::with_accelerated_time();
        let (merge_split_downloader_mailbox, merge_split_downloader_inbox) =
            universe.create_test_mailbox();
        let index_uid = IndexUid::new("test-index");
        let pipeline_id = IndexingPipelineId {
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };
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
        let merge_policy: Arc<dyn MergePolicy> = merge_policy_from_settings(&indexing_settings);
        let merge_planner = MergePlanner::new(
            pipeline_id,
            Vec::new(),
            merge_policy,
            merge_split_downloader_mailbox,
        );
        let (merge_planner_mailbox, merge_planner_handle) =
            universe.spawn_builder().spawn(merge_planner);
        // send 4 splits, offering 2 merge opportunities.
        let message = NewSplits {
            new_splits: vec![
                split_metadata_for_test(&index_uid, "2_a", 2, 100, 2),
                split_metadata_for_test(&index_uid, "2_b", 2, 100, 2),
                split_metadata_for_test(&index_uid, "1_a", 1, 10, 1),
                split_metadata_for_test(&index_uid, "1_b", 1, 10, 1),
            ],
        };
        merge_planner_mailbox.send_message(message).await?;
        merge_planner_handle.process_pending_and_observe().await;
        let merge_ops: Vec<TrackedObject<MergeOperation>> =
            merge_split_downloader_inbox.drain_for_test_typed();
        assert_eq!(merge_ops.len(), 2);
        assert_eq!(merge_ops[0].splits_as_slice()[0].num_merge_ops, 1);
        assert_eq!(merge_ops[1].splits_as_slice()[0].num_merge_ops, 2);
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_merge_planner_priority_only_queue_up_to_capacity() {
        let universe = Universe::with_accelerated_time();
        let (merge_split_downloader_mailbox, merge_split_downloader_inbox) = universe
            .spawn_ctx()
            .create_mailbox("MergeSplitDownloader", QueueCapacity::Bounded(2));
        let index_uid = IndexUid::new("test-index");
        let pipeline_id = IndexingPipelineId {
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };
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
        let merge_policy: Arc<dyn MergePolicy> = merge_policy_from_settings(&indexing_settings);
        let merge_planner = MergePlanner::new(
            pipeline_id,
            Vec::new(),
            merge_policy,
            merge_split_downloader_mailbox,
        );
        let universe = Universe::with_accelerated_time();
        let (merge_planner_mailbox, _) = universe.spawn_builder().spawn(merge_planner);
        tokio::task::spawn(async move {
            // Sending 20 splits offering 10 split opportunities
            let messages_with_merge_ops2 = NewSplits {
                new_splits: (0..10)
                    .flat_map(|partition_id| {
                        [
                            split_metadata_for_test(
                                &index_uid,
                                &format!("{partition_id}_a_large"),
                                partition_id,
                                1_000_000,
                                2,
                            ),
                            split_metadata_for_test(
                                &index_uid,
                                &format!("{partition_id}_b_large"),
                                partition_id,
                                1_000_000,
                                2,
                            ),
                        ]
                    })
                    .collect(),
            };
            merge_planner_mailbox
                .send_message(messages_with_merge_ops2)
                .await
                .unwrap();
            let messages_with_merge_ops1 = NewSplits {
                new_splits: (0..10)
                    .flat_map(|partition_id| {
                        [
                            split_metadata_for_test(
                                &index_uid,
                                &format!("{partition_id}_a_small"),
                                partition_id,
                                100_000,
                                1,
                            ),
                            split_metadata_for_test(
                                &index_uid,
                                &format!("{partition_id}_b_small"),
                                partition_id,
                                100_000,
                                1,
                            ),
                        ]
                    })
                    .collect(),
            };
            merge_planner_mailbox
                .send_message(messages_with_merge_ops1)
                .await
                .unwrap();
        });
        tokio::task::spawn_blocking(move || {
            let mut merge_ops: Vec<TrackedObject<MergeOperation>> = Vec::new();
            while merge_ops.len() < 20 {
                merge_ops.extend(merge_split_downloader_inbox.drain_for_test_typed());
            }
        })
        .await
        .unwrap();
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_merge_planner_spawns_merge_over_existing_splits_on_startup() -> anyhow::Result<()>
    {
        let universe = Universe::with_accelerated_time();
        let (merge_split_downloader_mailbox, merge_split_downloader_inbox) = universe
            .spawn_ctx()
            .create_mailbox("MergeSplitDownloader", QueueCapacity::Bounded(2));
        let index_uid = IndexUid::new("test-index");
        let pipeline_id = IndexingPipelineId {
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };
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
        let pre_existing_splits = vec![
            split_metadata_for_test(
                &index_uid, "a_small", 0, // partition_id
                1_000_000, 2,
            ),
            split_metadata_for_test(
                &index_uid, "b_small", 0, // partition_id
                1_000_000, 2,
            ),
        ];
        let merge_policy: Arc<dyn MergePolicy> = merge_policy_from_settings(&indexing_settings);
        let merge_planner = MergePlanner::new(
            pipeline_id,
            pre_existing_splits.clone(),
            merge_policy,
            merge_split_downloader_mailbox,
        );
        let (merge_planner_mailbox, merge_planner_handle) =
            universe.spawn_builder().spawn(merge_planner);

        // We wait for the first merge ops. If we sent the Quit message right away, it would have
        // been queue before first `PlanMerge` message.
        let merge_op = merge_split_downloader_inbox
            .recv_typed_message::<TrackedObject<MergeOperation>>()
            .await;
        assert!(merge_op.is_some());

        // We make sure that the known splits filtering set filters out splits are currently in
        // merge.
        merge_planner_mailbox
            .ask(NewSplits {
                new_splits: pre_existing_splits,
            })
            .await?;

        let _ = merge_planner_handle.process_pending_and_observe().await;

        let merge_ops =
            merge_split_downloader_inbox.drain_for_test_typed::<TrackedObject<MergeOperation>>();

        assert!(merge_ops.is_empty());

        merge_planner_mailbox.send_message(Command::Quit).await?;

        let (exit_status, _last_state) = merge_planner_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Quit));
        let merge_ops =
            merge_split_downloader_inbox.drain_for_test_typed::<TrackedObject<MergeOperation>>();
        assert!(merge_ops.is_empty());
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_merge_planner_dismiss_splits_from_different_pipeline_id() -> anyhow::Result<()> {
        // This test makes sure that the merge planner ignores the splits that do not belong
        // to the same pipeline
        let universe = Universe::with_accelerated_time();
        let (merge_split_downloader_mailbox, merge_split_downloader_inbox) = universe
            .spawn_ctx()
            .create_mailbox("MergeSplitDownloader", QueueCapacity::Bounded(2));
        let index_uid = IndexUid::new("test-index");
        let pipeline_id = IndexingPipelineId {
            index_uid,
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };
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
        let other_index_uid = IndexUid::new("test-index");

        let pre_existing_splits = vec![
            split_metadata_for_test(
                &other_index_uid,
                "a_small",
                0, // partition_id
                1_000_000,
                2,
            ),
            split_metadata_for_test(
                &other_index_uid,
                "b_small",
                0, // partition_id
                1_000_000,
                2,
            ),
        ];
        let merge_policy: Arc<dyn MergePolicy> = merge_policy_from_settings(&indexing_settings);
        let merge_planner = MergePlanner::new(
            pipeline_id,
            pre_existing_splits.clone(),
            merge_policy,
            merge_split_downloader_mailbox,
        );
        let (merge_planner_mailbox, merge_planner_handle) =
            universe.spawn_builder().spawn(merge_planner);
        universe.sleep(Duration::from_secs(10)).await;
        merge_planner_mailbox.send_message(Command::Quit).await?;
        let (exit_status, _last_state) = merge_planner_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Quit));
        let merge_ops =
            merge_split_downloader_inbox.drain_for_test_typed::<TrackedObject<MergeOperation>>();

        assert!(merge_ops.is_empty());
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_merge_planner_inherit_mailbox_with_splits_bug_3847() -> anyhow::Result<()> {
        let universe = Universe::with_accelerated_time();
        let (merge_split_downloader_mailbox, merge_split_downloader_inbox) = universe
            .spawn_ctx()
            .create_mailbox("MergeSplitDownloader", QueueCapacity::Bounded(2));
        let index_uid = IndexUid::new("test-index");
        let pipeline_id = IndexingPipelineId {
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };
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

        let pre_existing_splits = vec![
            split_metadata_for_test(
                &index_uid, "a_small", 0, // partition_id
                1_000_000, 2,
            ),
            split_metadata_for_test(
                &index_uid, "b_small", 0, // partition_id
                1_000_000, 2,
            ),
        ];

        let merge_policy: Arc<dyn MergePolicy> = merge_policy_from_settings(&indexing_settings);
        let merge_planner = MergePlanner::new(
            pipeline_id,
            pre_existing_splits.clone(),
            merge_policy,
            merge_split_downloader_mailbox,
        );
        let universe = Universe::with_accelerated_time();

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
        let merge_ops = merge_split_downloader_inbox
            .recv_typed_message::<TrackedObject<MergeOperation>>()
            .await;
        assert!(merge_ops.is_some());

        // At this point, our merge has been initialized.
        merge_planner_mailbox.send_message(Command::Quit).await?;
        let (exit_status, _last_state) = merge_planner_handle.join().await;

        assert!(matches!(exit_status, ActorExitStatus::Quit));
        let merge_ops =
            merge_split_downloader_inbox.drain_for_test_typed::<TrackedObject<MergeOperation>>();
        assert!(merge_ops.is_empty());

        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_merge_planner_known_splits_set_size_stays_bounded() -> anyhow::Result<()> {
        let universe = Universe::with_accelerated_time();
        let (merge_split_downloader_mailbox, merge_split_downloader_inbox) = universe
            .spawn_ctx()
            .create_mailbox("MergeSplitDownloader", QueueCapacity::Unbounded);
        let index_uid = IndexUid::new("test-index");
        let pipeline_id = IndexingPipelineId {
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };
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
        let merge_policy: Arc<dyn MergePolicy> = merge_policy_from_settings(&indexing_settings);
        let merge_planner = MergePlanner::new(
            pipeline_id,
            Vec::new(),
            merge_policy,
            merge_split_downloader_mailbox,
        );
        let universe = Universe::with_accelerated_time();

        // We spawn our merge planner with this recycled mailbox.
        let (merge_planner_mailbox, merge_planner_handle) =
            universe.spawn_builder().spawn(merge_planner);

        for j in 0..100 {
            for i in 0..10 {
                merge_planner_mailbox
                    .ask(NewSplits {
                        new_splits: vec![split_metadata_for_test(
                            &index_uid,
                            &format!("split_{}", j * 10 + i),
                            0,
                            1_000_000,
                            1,
                        )],
                    })
                    .await
                    .unwrap();
            }
            // We drain the merge_ops to make sure merge ops are dropped (as if merges where
            // successful) and that we are properly testing that the known_splits_set is
            // bounded.
            let merge_ops = merge_split_downloader_inbox
                .drain_for_test_typed::<TrackedObject<MergeOperation>>();
            assert_eq!(merge_ops.len(), 5);
        }

        // At this point, our merge has been initialized.
        merge_planner_mailbox.send_message(Command::Quit).await?;
        let (exit_status, _last_state) = merge_planner_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Quit));

        universe.assert_quit().await;
        Ok(())
    }
}
