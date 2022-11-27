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

use std::cmp::Reverse;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use quickwit_actors::channel_with_priority::TrySendError;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_metastore::SplitMetadata;
use serde::Serialize;
use tantivy::Inventory;
use tracing::info;

use crate::actors::MergeSplitDownloader;
use crate::merge_policy::MergeOperation;
use crate::metrics::INDEXER_METRICS;
use crate::models::{IndexingPipelineId, NewSplits};
use crate::MergePolicy;

/// The merge planner decides when to start a merge task.
pub struct MergePlanner {
    pipeline_id: IndexingPipelineId,
    /// A young split is a split that has not reached maturity
    /// yet and can be candidate to merge operations.
    partitioned_young_splits: HashMap<u64, Vec<SplitMetadata>>,
    merge_policy: Arc<dyn MergePolicy>,
    merge_split_downloader_mailbox: Mailbox<MergeSplitDownloader>,
    /// Inventory of ongoing merge operations. If everything goes well,
    /// a merge operation is dropped after the publish of the merged split.
    /// Used for observability.
    ongoing_merge_operations_inventory: Inventory<MergeOperation>,
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
        self.handle(RefreshMetric, ctx).await?;
        self.send_merge_ops(ctx).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<PlanMerge> for MergePlanner {
    type Reply = ();

    async fn handle(
        &mut self,
        _: PlanMerge,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.send_merge_ops(ctx).await?;
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
        self.record_splits(new_splits.new_splits);
        self.send_merge_ops(ctx).await?;
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
            partitioned_young_splits: Default::default(),
            merge_policy,
            merge_split_downloader_mailbox,
            ongoing_merge_operations_inventory: Inventory::default(),
        };
        merge_planner.record_splits(published_splits);
        merge_planner
    }

    fn record_split(&mut self, new_split: SplitMetadata) {
        if self.merge_policy.is_mature(&new_split) {
            return;
        }
        let splits_for_partition: &mut Vec<SplitMetadata> = self
            .partitioned_young_splits
            .entry(new_split.partition_id)
            .or_default();
        // Due to the recycling of the mailbox of the merge planner, it is possible for
        // a split already in store to be received.
        if splits_for_partition
            .iter()
            .any(|split| split.split_id() == new_split.split_id())
        {
            return;
        }
        splits_for_partition.push(new_split);
    }

    // Records a list of splits.
    //
    // Internally this function will detect and avoid adding the split
    // that are:
    // - already known
    // - mature
    // - do not belong to the current timeline.
    fn record_splits(&mut self, split_metadatas: Vec<SplitMetadata>) {
        for new_split in split_metadatas {
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
        Ok(merge_operations)
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
                self.record_splits(merge_op.splits);
            }
            // We try_self_send a `PlanMerge` message in order to ensure that
            // progress on our merges.
            //
            // If `try_send_self_message` returns an error, it means that the
            // the self queue is full, which means that the `PlanMerge`
            // message is not really needed anyway.
            let _ignored_result = ctx.try_send_self_message(PlanMerge);
        }
        Ok(())
    }
}

/// We can merge splits from the same (index_id, source_id, node_id).
fn belongs_to_pipeline(pipeline_id: &IndexingPipelineId, split: &SplitMetadata) -> bool {
    pipeline_id.index_id == split.index_id
        && pipeline_id.source_id == split.source_id
        && pipeline_id.node_id == split.node_id
}

#[derive(Debug)]
struct RefreshMetric;

#[derive(Debug)]
struct PlanMerge;

#[async_trait]
impl Handler<RefreshMetric> for MergePlanner {
    type Reply = ();

    async fn handle(
        &mut self,
        _: RefreshMetric,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        INDEXER_METRICS
            .ongoing_merge_operations
            .with_label_values([
                self.pipeline_id.index_id.as_str(),
                self.pipeline_id.source_id.as_str(),
            ])
            .set(self.ongoing_merge_operations_inventory.list().len() as i64);
        ctx.schedule_self_msg(quickwit_actors::HEARTBEAT, RefreshMetric)
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
    use quickwit_actors::{create_mailbox, QueueCapacity, Universe};
    use quickwit_config::merge_policy_config::{
        ConstWriteAmplificationMergePolicyConfig, MergePolicyConfig, StableLogMergePolicyConfig,
    };
    use quickwit_config::IndexingSettings;
    use quickwit_metastore::SplitMetadata;
    use tantivy::TrackedObject;
    use time::OffsetDateTime;

    use crate::actors::MergePlanner;
    use crate::merge_policy::{
        merge_policy_from_settings, MergeOperation, MergePolicy, StableLogMergePolicy,
    };
    use crate::models::{IndexingPipelineId, NewSplits};

    fn split_metadata_for_test(
        split_id: &str,
        partition_id: u64,
        num_docs: usize,
        num_merge_ops: usize,
    ) -> SplitMetadata {
        SplitMetadata {
            split_id: split_id.to_string(),
            index_id: "test-index".to_string(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            num_docs,
            partition_id,
            num_merge_ops,
            create_timestamp: OffsetDateTime::now_utc().unix_timestamp(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_merge_planner_with_stable_custom_merge_policy() -> anyhow::Result<()> {
        let (merge_split_downloader_mailbox, merge_split_downloader_inbox) =
            create_mailbox("MergeSplitDownloader".to_string(), QueueCapacity::Unbounded);
        let pipeline_id = IndexingPipelineId {
            index_id: "test-index".to_string(),
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
            vec![],
            merge_policy,
            merge_split_downloader_mailbox,
        );
        let universe = Universe::new();

        let (merge_planner_mailbox, merge_planner_handle) =
            universe.spawn_builder().spawn(merge_planner);
        {
            // send one split
            let message = NewSplits {
                new_splits: vec![
                    split_metadata_for_test("1_1", 1, 2500, 0),
                    split_metadata_for_test("1_2", 2, 3000, 0),
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
                    split_metadata_for_test("2_1", 1, 2000, 0),
                    split_metadata_for_test("1_2", 2, 3000, 0),
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
                    split_metadata_for_test("3_1", 1, 1500, 0),
                    split_metadata_for_test("4_1", 1, 1000, 0),
                    split_metadata_for_test("2_2", 2, 2000, 0),
                    split_metadata_for_test("3_2", 2, 4000, 0),
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

        Ok(())
    }

    #[tokio::test]
    async fn test_merge_planner_priority() -> anyhow::Result<()> {
        let (merge_split_downloader_mailbox, merge_split_downloader_inbox) =
            create_mailbox("MergeSplitDownloader".to_string(), QueueCapacity::Unbounded);
        let pipeline_id = IndexingPipelineId {
            index_id: "test-index".to_string(),
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
        let universe = Universe::new();
        let (merge_planner_mailbox, merge_planner_handle) =
            universe.spawn_builder().spawn(merge_planner);
        // send 4 splits, offering 2 merge opportunities.
        let message = NewSplits {
            new_splits: vec![
                split_metadata_for_test("2_a", 2, 100, 2),
                split_metadata_for_test("2_b", 2, 100, 2),
                split_metadata_for_test("1_a", 1, 10, 1),
                split_metadata_for_test("1_b", 1, 10, 1),
            ],
        };
        merge_planner_mailbox.send_message(message).await?;
        merge_planner_handle.process_pending_and_observe().await;
        let merge_ops: Vec<TrackedObject<MergeOperation>> =
            merge_split_downloader_inbox.drain_for_test_typed();
        assert_eq!(merge_ops.len(), 2);
        assert_eq!(merge_ops[0].splits_as_slice()[0].num_merge_ops, 1);
        assert_eq!(merge_ops[1].splits_as_slice()[0].num_merge_ops, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_merge_planner_priority_only_queue_up_to_capacity() {
        let (merge_split_downloader_mailbox, merge_split_downloader_inbox) = create_mailbox(
            "MergeSplitDownloader".to_string(),
            QueueCapacity::Bounded(2),
        );
        let pipeline_id = IndexingPipelineId {
            index_id: "test-index".to_string(),
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
        let universe = Universe::new();
        let (merge_planner_mailbox, _) = universe.spawn_builder().spawn(merge_planner);
        tokio::task::spawn(async move {
            // Sending 200 splits offering 100 split opportunities
            let messages_with_merge_ops2 = NewSplits {
                new_splits: (0..10)
                    .flat_map(|partition_id| {
                        [
                            split_metadata_for_test(
                                &format!("{partition_id}_a_large"),
                                2,
                                1_000_000,
                                2,
                            ),
                            split_metadata_for_test(
                                &format!("{partition_id}_b_large"),
                                2,
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
                                &format!("{partition_id}_a_small"),
                                2,
                                100_000,
                                1,
                            ),
                            split_metadata_for_test(
                                &format!("{partition_id}_b_small"),
                                2,
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
    }
}
