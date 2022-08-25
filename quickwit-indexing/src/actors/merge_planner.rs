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
use tracing::info;

use crate::actors::MergeSplitDownloader;
use crate::models::{IndexingPipelineId, NewSplits};
use crate::MergePolicy;

/// The merge planner decides when to start a merge or a demux task.
pub struct MergePlanner {
    pipeline_id: IndexingPipelineId,
    /// A young split is a split that has not reached maturity
    /// yet and can be candidate to merge and demux operations.
    partitioned_young_splits: HashMap<u64, Vec<SplitMetadata>>,
    merge_policy: Arc<dyn MergePolicy>,
    merge_split_downloader_mailbox: Mailbox<MergeSplitDownloader>,
}

#[async_trait]
impl Actor for MergePlanner {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}

    fn name(&self) -> String {
        "MergePlanner".to_string()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(0)
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        let target_partition_ids = self.partitioned_young_splits.keys().cloned().collect_vec();
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
                        pipeline_ord=%split.pipeline_ord,
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
            young_splits.extend(new_young_splits);
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
                    ctx.send_message(&self.merge_split_downloader_mailbox, merge_operation)
                        .await?;
                }
            }
        }
        Ok(())
    }
}

fn belongs_to_pipeline(pipeline_id: &IndexingPipelineId, split: &SplitMetadata) -> bool {
    pipeline_id.source_id == split.source_id
        && pipeline_id.node_id == split.node_id
        && pipeline_id.pipeline_ord == split.pipeline_ord
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::iter::FromIterator;
    use std::ops::RangeInclusive;

    use proptest::sample::select;
    use quickwit_actors::{create_test_mailbox, ObservationType, Universe};
    use tokio::runtime::Runtime;

    use super::*;
    use crate::actors::combine_partition_ids;
    use crate::actors::merge_executor::{demux_virtual_split, VirtualSplit};
    use crate::merge_policy::MergeOperation;
    use crate::{new_split_id, StableMultitenantWithTimestampMergePolicy};

    fn merge_time_range(splits: &[SplitMetadata]) -> Option<RangeInclusive<i64>> {
        let time_range_start = splits
            .iter()
            .flat_map(|split| &split.time_range)
            .map(|time_range| *time_range.start())
            .min()?;
        let time_range_end = splits
            .iter()
            .flat_map(|split| &split.time_range)
            .map(|time_range| *time_range.end())
            .max()?;
        Some(time_range_start..=time_range_end)
    }

    fn merge_tags(splits: &[SplitMetadata]) -> BTreeSet<String> {
        splits
            .iter()
            .flat_map(|split| split.tags.iter().cloned())
            .collect()
    }

    fn fake_merge(splits: &[SplitMetadata]) -> SplitMetadata {
        assert!(!splits.is_empty(), "Split list should not be empty.");
        let num_docs = splits.iter().map(|split| split.num_docs).sum();
        let size_in_bytes = splits
            .iter()
            .map(|split| split.uncompressed_docs_size_in_bytes)
            .sum();
        let time_range = merge_time_range(splits);
        let merged_split_id = new_split_id();
        let tags = merge_tags(splits);
        SplitMetadata {
            split_id: merged_split_id,
            partition_id: combine_partition_ids(splits),
            num_docs,
            uncompressed_docs_size_in_bytes: size_in_bytes,
            time_range,
            create_timestamp: 0,
            tags,
            footer_offsets: 0..100,
            ..Default::default()
        }
    }

    fn fake_demux(splits: &[SplitMetadata]) -> Vec<SplitMetadata> {
        assert!(!splits.is_empty(), "Split list should not be empty.");
        let num_docs: usize = splits.iter().map(|split| split.num_docs).sum();
        let size_in_bytes: u64 = splits
            .iter()
            .map(|split| split.uncompressed_docs_size_in_bytes)
            .sum();
        let time_range = merge_time_range(splits);
        let tags = merge_tags(splits);
        let mut demux_values_map = BTreeMap::new();
        for split in splits {
            let mut num_docs = split.num_docs;
            let mut demux_value = 0u64;
            while num_docs > 0 {
                let num_docs_to_insert = std::cmp::min(10_000, num_docs);
                let demux_count = demux_values_map.entry(demux_value).or_insert(0);
                *demux_count += num_docs_to_insert;
                demux_value += 1;
                num_docs -= num_docs_to_insert;
            }
        }
        let input_split = VirtualSplit::new(demux_values_map);
        let demuxed_splits = demux_virtual_split(input_split, 10_000_000, 20_000_000, splits.len());
        let mut splits_metadata = Vec::new();
        for demuxed_split in demuxed_splits {
            let split_metadata = SplitMetadata {
                split_id: new_split_id(),
                source_id: "test-source".to_string(),
                node_id: "test-node".to_string(),
                pipeline_ord: 0,
                partition_id: combine_partition_ids(splits),
                num_docs: demuxed_split.total_num_docs(),
                uncompressed_docs_size_in_bytes: (size_in_bytes as f32 / num_docs as f32) as u64
                    * demuxed_split.total_num_docs() as u64,
                time_range: time_range.clone(),
                create_timestamp: 0,
                tags: tags.clone(),
                demux_num_ops: 1,
                footer_offsets: 0..100,
            };
            splits_metadata.push(split_metadata);
        }
        splits_metadata
    }

    fn apply_merge(
        split_index: &mut HashMap<String, SplitMetadata>,
        merge_op: &MergeOperation,
    ) -> Vec<SplitMetadata> {
        for split in merge_op.splits() {
            assert!(split_index.remove(split.split_id()).is_some());
        }
        let splits = match merge_op {
            MergeOperation::Merge { splits, .. } => {
                vec![fake_merge(splits)]
            }
            MergeOperation::Demux { splits, .. } => fake_demux(splits),
        };
        for split in splits.iter() {
            split_index.insert(split.split_id().to_string(), split.clone());
        }
        splits
    }

    async fn test_aux_simulate_merge_planner<Pred: Fn(&[SplitMetadata]) -> bool>(
        merge_policy: Arc<dyn MergePolicy>,
        incoming_splits: Vec<SplitMetadata>,
        predicate: Pred,
    ) -> anyhow::Result<()> {
        let (merge_op_mailbox, merge_op_inbox) = create_test_mailbox::<MergeSplitDownloader>();
        let pipeline_id = IndexingPipelineId {
            index_id: "test-index".to_string(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };
        let merge_planner =
            MergePlanner::new(pipeline_id, Vec::new(), merge_policy, merge_op_mailbox);
        let universe = Universe::new();
        let mut split_index: HashMap<String, SplitMetadata> = HashMap::default();
        let (merge_planner_mailbox, merge_planner_handler) =
            universe.spawn_actor(merge_planner).spawn();
        for split in incoming_splits {
            split_index.insert(split.split_id().to_string(), split.clone());
            merge_planner_mailbox
                .send_message(NewSplits {
                    new_splits: vec![split],
                })
                .await?;
            loop {
                let obs = merge_planner_handler.process_pending_and_observe().await;
                assert_eq!(obs.obs_type, ObservationType::Alive);
                let merge_ops: Vec<MergeOperation> = merge_op_inbox
                    .drain_for_test()
                    .into_iter()
                    .flat_map(|op| op.downcast::<MergeOperation>())
                    .map(|op| *op)
                    .collect();
                if merge_ops.is_empty() {
                    break;
                }
                for merge_op in merge_ops {
                    let splits = apply_merge(&mut split_index, &merge_op);
                    merge_planner_mailbox
                        .send_message(NewSplits { new_splits: splits })
                        .await?;
                }
            }
            let split_metadatas: Vec<SplitMetadata> = split_index.values().cloned().collect();
            assert!(predicate(&split_metadatas));
        }
        Ok(())
    }

    /// Mock split meta helper.
    fn mock_split_meta_from_num_docs(
        time_range: RangeInclusive<i64>,
        num_docs: u64,
    ) -> SplitMetadata {
        SplitMetadata {
            split_id: crate::new_split_id(),
            partition_id: 3u64,
            num_docs: num_docs as usize,
            uncompressed_docs_size_in_bytes: 256u64 * num_docs,
            time_range: Some(time_range),
            create_timestamp: 0,
            tags: BTreeSet::from_iter(vec!["tenant_id:1".to_string(), "tenant_id:2".to_string()]),
            footer_offsets: 0..100,
            ..Default::default()
        }
    }

    async fn aux_test_simulate_merge_planner_num_docs<TPred: Fn(&[SplitMetadata]) -> bool>(
        merge_policy: Arc<dyn MergePolicy>,
        batch_num_docs: &[usize],
        predicate: TPred,
    ) -> anyhow::Result<()> {
        let split_metadatas: Vec<SplitMetadata> = batch_num_docs
            .iter()
            .cloned()
            .enumerate()
            .map(|(split_ord, num_docs)| {
                let time_first = split_ord as i64 * 1_000;
                let time_last = time_first + 999;
                let time_range = time_first..=time_last;
                mock_split_meta_from_num_docs(time_range, num_docs as u64)
            })
            .collect();
        test_aux_simulate_merge_planner(merge_policy, split_metadatas, predicate).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_simulate_merge_planner_constant_case() -> anyhow::Result<()> {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        aux_test_simulate_merge_planner_num_docs(
            Arc::new(merge_policy.clone()),
            &vec![10_000; 100_000],
            |splits| {
                let num_docs = splits.iter().map(|split| split.num_docs as u64).sum();
                splits.len() <= merge_policy.max_num_splits_ideal_case(num_docs)
            },
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_simulate_merge_and_demux() -> anyhow::Result<()> {
        let merge_policy = StableMultitenantWithTimestampMergePolicy {
            demux_field_name: Some("tenant_id".to_owned()),
            demux_enabled: true,
            ..Default::default()
        };
        aux_test_simulate_merge_planner_num_docs(
            Arc::new(merge_policy.clone()),
            &vec![1_000_000; 120],
            |splits| {
                let num_big_splits = splits
                    .iter()
                    .filter(|split| split.num_docs >= 10_000_000)
                    .count();
                if num_big_splits > 5 {
                    let demuxed_num_splits = splits
                        .iter()
                        .filter(|split| split.demux_num_ops > 0)
                        .count();
                    return demuxed_num_splits > 0 && demuxed_num_splits % 6 == 0;
                }
                splits
                    .iter()
                    .filter(|split| split.demux_num_ops > 0)
                    .count()
                    == 0
            },
        )
        .await?;
        Ok(())
    }

    use proptest::prelude::*;

    fn proptest_config() -> ProptestConfig {
        let mut proptest_config = ProptestConfig::with_cases(20);
        proptest_config.max_shrink_iters = 600;
        proptest_config
    }

    proptest! {
        #![proptest_config(proptest_config())]
        #[test]
        fn test_proptest_simulate_stable_multitenant_merge_planner_adversarial(batch_num_docs in proptest::collection::vec(select(&[11, 1_990, 10_000, 50_000, 310_000][..]), 1..1_000)) {
            let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
            let rt = Runtime::new().unwrap();
            rt.block_on(
            aux_test_simulate_merge_planner_num_docs(
                Arc::new(merge_policy.clone()),
                &batch_num_docs,
                |splits| {
                    let num_docs = splits.iter().map(|split| split.num_docs as u64).sum();
                    splits.len() <= merge_policy.max_num_splits_worst_case(num_docs)
                },
            )).unwrap();
        }
    }

    #[tokio::test]
    async fn test_simulate_stable_multitenant_merge_planner_ideal_case() -> anyhow::Result<()> {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        aux_test_simulate_merge_planner_num_docs(
            Arc::new(merge_policy.clone()),
            &vec![10_000; 1_000],
            |splits| {
                let num_docs = splits.iter().map(|split| split.num_docs as u64).sum();
                splits.len() <= merge_policy.max_num_splits_ideal_case(num_docs)
            },
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_simulate_stable_multitenant_merge_planner_bug() -> anyhow::Result<()> {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        let vals = &[11, 11, 11, 11, 11, 11, 310000, 11, 11, 11, 11, 11, 11, 11];
        aux_test_simulate_merge_planner_num_docs(
            Arc::new(merge_policy.clone()),
            &vals[..],
            |splits| {
                let num_docs = splits.iter().map(|split| split.num_docs as u64).sum();
                splits.len() <= merge_policy.max_num_splits_worst_case(num_docs)
            },
        )
        .await?;
        Ok(())
    }
}
