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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
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
        QueueCapacity::Bounded(0)
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        let target_partition_ids = self.partitioned_young_splits.keys().cloned().collect_vec();
        self.handle(RefreshMetric, ctx).await?;
        self.send_merge_ops(ctx, &target_partition_ids).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<NewSplits> for MergePlanner {
    type Reply = ();

    async fn handle(
        &mut self,
        message: NewSplits,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let mut target_partition_ids = Vec::new();

        let partitioned_new_young_splits = message
            .new_splits
            .into_iter()
            .filter(|split| {
                let is_immature = !self.merge_policy.is_mature(split);
                if !is_immature {
                    info!(
                        split_id=%split.split_id(),
                        index_id=%self.pipeline_id.index_id,
                        source_id=%split.source_id,
                        num_docs=split.num_docs,
                        num_bytes=split.uncompressed_docs_size_in_bytes,
                        "Split is mature."
                    );
                }
                is_immature
            })
            .group_by(|split| split.partition_id);

        for (partition_id, new_young_splits) in &partitioned_new_young_splits {
            let young_splits = self
                .partitioned_young_splits
                .entry(partition_id)
                .or_default();
            for new_young_split in new_young_splits {
                // Due to the recycling of the mailbox of the merge planner, it is possible for
                // a split already in store to be received.
                let split_already_known = young_splits
                    .iter()
                    .any(|split| split.split_id() == new_young_split.split_id());
                if split_already_known {
                    continue;
                }
                young_splits.push(new_young_split);
            }
            target_partition_ids.push(partition_id);
        }
        self.send_merge_ops(ctx, &target_partition_ids).await?;
        Ok(())
    }
}

impl MergePlanner {
    pub fn new(
        pipeline_id: IndexingPipelineId,
        published_splits: Vec<SplitMetadata>,
        merge_policy: Arc<dyn MergePolicy>,
        merge_split_downloader_mailbox: Mailbox<MergeSplitDownloader>,
    ) -> MergePlanner {
        let mut partitioned_young_splits: HashMap<u64, Vec<SplitMetadata>> = HashMap::new();
        for split in published_splits {
            if !belongs_to_pipeline(&pipeline_id, &split) || merge_policy.is_mature(&split) {
                continue;
            }
            partitioned_young_splits
                .entry(split.partition_id)
                .or_default()
                .push(split);
        }
        MergePlanner {
            pipeline_id,
            partitioned_young_splits,
            merge_policy,
            merge_split_downloader_mailbox,
            ongoing_merge_operations_inventory: Inventory::default(),
        }
    }

    async fn send_merge_ops(
        &mut self,
        ctx: &ActorContext<Self>,
        target_partition_ids: &[u64],
    ) -> Result<(), ActorExitStatus> {
        for partition_id in target_partition_ids {
            if let Some(young_splits) = self.partitioned_young_splits.get_mut(partition_id) {
                let merge_operations = self.merge_policy.operations(young_splits);

                for merge_operation in merge_operations {
                    info!(merge_operation=?merge_operation, "Planned merge operation.");
                    let tracked_merge_operations = self
                        .ongoing_merge_operations_inventory
                        .track(merge_operation);
                    ctx.send_message(
                        &self.merge_split_downloader_mailbox,
                        tracked_merge_operations,
                    )
                    .await?;
                }
            }
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

#[async_trait]
impl Handler<RefreshMetric> for MergePlanner {
    type Reply = ();

    async fn handle(
        &mut self,
        _: RefreshMetric,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        INDEXER_METRICS
            .ongoing_num_merge_operations_total
            .with_label_values(&[&self.pipeline_id.index_id])
            .set(self.ongoing_merge_operations_inventory.list().len() as i64);
        ctx.schedule_self_msg(quickwit_actors::HEARTBEAT, RefreshMetric)
            .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;
    use quickwit_actors::{create_mailbox, QueueCapacity, Universe};
    use quickwit_config::merge_policy_config::StableLogMergePolicyConfig;
    use quickwit_metastore::SplitMetadata;
    use tantivy::TrackedObject;

    use crate::actors::MergePlanner;
    use crate::merge_policy::{MergeOperation, StableLogMergePolicy};
    use crate::models::{IndexingPipelineId, NewSplits};

    fn split_metadata_for_test(
        split_id: &str,
        partition_id: u64,
        num_docs: usize,
    ) -> SplitMetadata {
        SplitMetadata {
            split_id: split_id.to_string(),
            index_id: "test-index".to_string(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            num_docs,
            partition_id,
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

        let (merge_planner_mailbox, _) = universe.spawn_builder().spawn(merge_planner);

        {
            // send one split
            let message = NewSplits {
                new_splits: vec![
                    split_metadata_for_test("1_1", 1, 2500),
                    split_metadata_for_test("1_2", 2, 3000),
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
                    split_metadata_for_test("2_1", 1, 2000),
                    split_metadata_for_test("1_2", 2, 3000),
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
                    split_metadata_for_test("3_1", 1, 1500),
                    split_metadata_for_test("4_1", 1, 1000),
                    split_metadata_for_test("2_2", 2, 2000),
                    split_metadata_for_test("3_2", 2, 4000),
                ],
            };
            merge_planner_mailbox.send_message(message).await?;
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
}

#[derive(Clone, Debug, Serialize)]
pub struct MergePlannerState {
    ongoing_merge_operations: Vec<MergeOperation>,
}
