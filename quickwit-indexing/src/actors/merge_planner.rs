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

use std::sync::Arc;

use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Mailbox, SyncActor};
use quickwit_metastore::SplitMetadata;
use tracing::info;

use crate::merge_policy::MergeOperation;
use crate::models::MergePlannerMessage;
use crate::MergePolicy;

/// The merge planner decides when to start a merge or a demux task.
pub struct MergePlanner {
    /// A young split is a split that has not reached maturity
    /// yet and can be candidate to merge and demux operations.
    young_splits: Vec<SplitMetadata>,
    merge_policy: Arc<dyn MergePolicy>,
    merge_split_downloader_mailbox: Mailbox<MergeOperation>,
}

impl Actor for MergePlanner {
    type Message = MergePlannerMessage;

    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}

    fn name(&self) -> String {
        "MergePlanner".to_string()
    }
}

impl SyncActor for MergePlanner {
    fn process_message(
        &mut self,
        message: MergePlannerMessage,
        ctx: &ActorContext<Self::Message>,
    ) -> Result<(), ActorExitStatus> {
        for split in message.new_splits {
            self.process_new_split(split, ctx)?;
        }
        Ok(())
    }
}

impl MergePlanner {
    pub fn new(
        merge_policy: Arc<dyn MergePolicy>,
        merge_split_downloader_mailbox: Mailbox<MergeOperation>,
    ) -> MergePlanner {
        MergePlanner {
            young_splits: Vec::new(),
            merge_policy,
            merge_split_downloader_mailbox,
        }
    }

    fn process_new_split(
        &mut self,
        split: SplitMetadata,
        ctx: &ActorContext<MergePlannerMessage>,
    ) -> Result<(), ActorExitStatus> {
        if self.merge_policy.is_mature(&split) {
            info!(split_id=%split.split_id, num_records=split.num_records, size_in_bytes=split.size_in_bytes, "mature-split");
            return Ok(());
        }
        self.add_split(split);
        let merge_candidates = self.merge_policy.operations(&mut self.young_splits);
        for merge_operation in merge_candidates {
            info!(merge_operation=?merge_operation, "planning-merge");
            ctx.send_message_blocking(&self.merge_split_downloader_mailbox, merge_operation)?;
        }
        Ok(())
    }

    pub fn add_split(&mut self, split: SplitMetadata) {
        self.young_splits.push(split);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::iter::FromIterator;
    use std::ops::RangeInclusive;
    use std::time::UNIX_EPOCH;

    use proptest::sample::select;
    use quickwit_actors::{create_test_mailbox, ObservationType, Universe};
    use quickwit_metastore::SplitState;
    use tokio::runtime::Runtime;

    use super::*;
    use crate::actors::merge_executor::{demux_virtual_split, VirtualSplit};
    use crate::{new_split_id, StableMultitenantWithTimestampMergePolicy};

    fn merged_timestamp(splits: &[SplitMetadata]) -> Option<RangeInclusive<i64>> {
        let time_start = splits
            .iter()
            .flat_map(|split| {
                split
                    .time_range
                    .as_ref()
                    .map(|time_range| *time_range.start())
            })
            .min()?;
        let time_end = splits
            .iter()
            .flat_map(|split| {
                split
                    .time_range
                    .as_ref()
                    .map(|time_range| *time_range.end())
            })
            .max()?;
        Some(time_start..=time_end)
    }

    fn compute_merge_tags(splits: &[SplitMetadata]) -> HashSet<String> {
        let mut tag_set: HashSet<String> = HashSet::new();
        for split in splits {
            for tag in &split.tags {
                tag_set.insert(tag.clone());
            }
        }
        tag_set
    }

    fn fake_merge(splits: &[SplitMetadata]) -> SplitMetadata {
        assert!(!splits.is_empty(), "Splits is not empty.");
        let update_timestamp = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let num_records = splits.iter().map(|split| split.num_records).sum();
        let size_in_bytes = splits.iter().map(|split| split.size_in_bytes).sum();
        let time_range = merged_timestamp(splits);
        let merged_split_id = new_split_id();
        let tags = compute_merge_tags(splits);
        SplitMetadata {
            split_id: merged_split_id,
            num_records,
            size_in_bytes,
            time_range,
            split_state: SplitState::Published,
            update_timestamp,
            tags,
            demux_num_ops: 0,
        }
    }

    fn fake_demux(splits: &[SplitMetadata]) -> Vec<SplitMetadata> {
        assert!(!splits.is_empty(), "Splits must not be empty.");
        let update_timestamp = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let num_records: usize = splits.iter().map(|split| split.num_records).sum();
        let size_in_bytes: u64 = splits.iter().map(|split| split.size_in_bytes).sum();
        let time_range = merged_timestamp(splits);
        let tags = compute_merge_tags(splits);
        let mut demux_values_map = BTreeMap::new();
        for split in splits {
            let mut num_docs = split.num_records;
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
                num_records: demuxed_split.total_num_docs(),
                size_in_bytes: (size_in_bytes as f32 / num_records as f32) as u64
                    * demuxed_split.total_num_docs() as u64,
                time_range: time_range.clone(),
                split_state: SplitState::Published,
                update_timestamp,
                tags: tags.clone(),
                demux_num_ops: 1,
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
            assert!(split_index.remove(&split.split_id).is_some());
        }
        let splits = match merge_op {
            MergeOperation::Merge { splits, .. } => {
                vec![fake_merge(splits)]
            }
            MergeOperation::Demux { splits, .. } => fake_demux(splits),
        };
        for split in splits.iter() {
            split_index.insert(split.split_id.clone(), split.clone());
        }
        splits
    }

    async fn test_aux_simulate_merge_planner<Pred: Fn(&[SplitMetadata]) -> bool>(
        merge_policy: Arc<dyn MergePolicy>,
        incoming_splits: Vec<SplitMetadata>,
        predicate: Pred,
    ) -> anyhow::Result<()> {
        let (merge_op_mailbox, merge_op_inbox) = create_test_mailbox::<MergeOperation>();
        let merge_planner = MergePlanner::new(merge_policy, merge_op_mailbox);
        let universe = Universe::new();
        let mut split_index: HashMap<String, SplitMetadata> = HashMap::default();
        let (merge_planner_mailbox, merge_planner_handler) =
            universe.spawn_actor(merge_planner).spawn_sync();
        for split in incoming_splits {
            split_index.insert(split.split_id.clone(), split.clone());
            universe
                .send_message(
                    &merge_planner_mailbox,
                    MergePlannerMessage {
                        new_splits: vec![split],
                    },
                )
                .await?;
            loop {
                let obs = merge_planner_handler.process_pending_and_observe().await;
                assert_eq!(obs.obs_type, ObservationType::Alive);
                let merge_ops = merge_op_inbox.drain_available_message_for_test();
                if merge_ops.is_empty() {
                    break;
                }
                for merge_op in merge_ops {
                    let splits = apply_merge(&mut split_index, &merge_op);
                    universe
                        .send_message(
                            &merge_planner_mailbox,
                            MergePlannerMessage { new_splits: splits },
                        )
                        .await?;
                }
            }
            let split_metadatas: Vec<SplitMetadata> = split_index.values().cloned().collect();
            assert!(predicate(&split_metadatas));
        }
        Ok(())
    }

    /// Mock split meta helper.
    fn mock_split_meta_from_num_records(
        time_range: RangeInclusive<i64>,
        num_records: u64,
    ) -> SplitMetadata {
        SplitMetadata {
            split_id: crate::new_split_id(),
            split_state: SplitState::Published,
            num_records: num_records as usize,
            size_in_bytes: 256u64 * num_records,
            time_range: Some(time_range),
            update_timestamp: 0,
            tags: HashSet::from_iter(vec!["tenant_id:1".to_string(), "tenant_id:2".to_string()]),
            demux_num_ops: 0,
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
            .map(|(split_ord, num_records)| {
                let time_first = split_ord as i64 * 1_000;
                let time_last = time_first + 999;
                let time_range = time_first..=time_last;
                mock_split_meta_from_num_records(time_range, num_records as u64)
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
                let num_docs = splits.iter().map(|split| split.num_records as u64).sum();
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
                    .filter(|split| split.num_records >= 10_000_000)
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
        fn test_proptest_simulate_stable_multitenant_merge_planner_adversarial(batch_num_records in proptest::collection::vec(select(&[11, 1_990, 10_000, 50_000, 310_000][..]), 1..1_000)) {
            let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
            let rt = Runtime::new().unwrap();
            rt.block_on(
            aux_test_simulate_merge_planner_num_docs(
                Arc::new(merge_policy.clone()),
                &batch_num_records,
                |splits| {
                    let num_docs = splits.iter().map(|split| split.num_records as u64).sum();
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
                let num_docs = splits.iter().map(|split| split.num_records as u64).sum();
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
                let num_docs = splits.iter().map(|split| split.num_records as u64).sum();
                splits.len() <= merge_policy.max_num_splits_worst_case(num_docs)
            },
        )
        .await?;
        Ok(())
    }
}
