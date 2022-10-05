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

use quickwit_config::merge_policy_config::ConstWriteAmplificationMergePolicyConfig;
use quickwit_config::IndexingSettings;
use quickwit_metastore::SplitMetadata;

use super::MergeOperation;
use crate::merge_policy::MergePolicy;

/// The `ConstWriteAmplificationMergePolicy` has been designed for a use
/// case where there are a several index partitions with different sizes,
/// and partitions tend to be searched separately. (e.g. partitioning by tenant.)
///
/// In that case, the StableLogMergePolicy would tend to target the same number
/// of docs for all tenants. Assuming a merge factor of 10 and a target num docs of 10 millions,
/// The write amplification observed for a small tenant, emitting splits of 1
/// document would be 7.
///
/// These extra merges have the benefit of making less splits, but really we are
/// over-trading write amplification for read amplification here.
///
/// The `ConstWriteAmplificationMergePolicy` is very simple. It targets a number
/// of merges instead, and stops once this number of merges is reached.
///
/// Only splits with the same number of merge operations are merged together,
/// and for a given merge operation, we build split in a greedy way.
/// After sorting the splits per creation date, we append splits one after the
/// other until we either reach `max_merge_factor` or we exceed the
/// targeted` split_num_docs`.
#[derive(Debug, Clone)]
pub struct ConstWriteAmplificationMergePolicy {
    config: ConstWriteAmplificationMergePolicyConfig,
    split_num_docs_target: usize,
}

impl Default for ConstWriteAmplificationMergePolicy {
    fn default() -> Self {
        ConstWriteAmplificationMergePolicy {
            config: Default::default(),
            split_num_docs_target: IndexingSettings::default_split_num_docs_target(),
        }
    }
}

impl ConstWriteAmplificationMergePolicy {
    pub fn new(
        config: ConstWriteAmplificationMergePolicyConfig,
        split_num_docs_target: usize,
    ) -> Self {
        ConstWriteAmplificationMergePolicy {
            config,
            split_num_docs_target,
        }
    }

    #[cfg(test)]
    fn for_test() -> ConstWriteAmplificationMergePolicy {
        let config = ConstWriteAmplificationMergePolicyConfig {
            max_merge_ops: 3,
            merge_factor: 3,
            max_merge_factor: 5,
        };
        Self::new(config, 10_000_000)
    }

    /// Returns a merge operation within one `num_merge_ops` level if one can be built from the
    /// given splits. This method assumes that the splits are sorted by reverse creation date
    /// and have all the same `num_merge_ops`.
    fn single_merge_operation_within_num_merge_op_level(
        &self,
        splits: &mut Vec<SplitMetadata>,
    ) -> Option<MergeOperation> {
        let mut num_splits_in_merge = 0;
        let mut num_docs_in_merge = 0;
        for split in splits.iter().take(self.config.max_merge_factor) {
            num_docs_in_merge += split.num_docs;
            num_splits_in_merge += 1;
            if num_docs_in_merge >= self.split_num_docs_target {
                break;
            }
        }
        if (num_docs_in_merge < self.split_num_docs_target)
            && (num_splits_in_merge < self.config.merge_factor)
        {
            return None;
        }
        assert!(num_splits_in_merge >= 2);
        let splits_in_merge = splits.drain(0..num_splits_in_merge).collect();
        let merge_operation = MergeOperation::new_merge_operation(splits_in_merge);
        Some(merge_operation)
    }

    fn merge_operations_within_num_merge_op_level(
        &self,
        splits: &mut Vec<SplitMetadata>,
    ) -> Vec<MergeOperation> {
        splits.sort_by(|left, right| {
            left.create_timestamp
                .cmp(&right.create_timestamp)
                .then_with(|| left.split_id().cmp(right.split_id()))
        });
        let mut merge_operations = Vec::new();
        while let Some(merge_op) = self.single_merge_operation_within_num_merge_op_level(splits) {
            merge_operations.push(merge_op);
        }
        merge_operations
    }
}

impl MergePolicy for ConstWriteAmplificationMergePolicy {
    fn operations(&self, splits: &mut Vec<SplitMetadata>) -> Vec<MergeOperation> {
        let mut group_by_num_merge_ops: HashMap<usize, Vec<SplitMetadata>> = HashMap::default();
        let mut mature_splits = Vec::new();
        for split in splits.drain(..) {
            if self.is_mature(&split) {
                mature_splits.push(split);
            } else {
                group_by_num_merge_ops
                    .entry(split.num_merge_ops)
                    .or_default()
                    .push(split);
            }
        }
        splits.extend(mature_splits);
        let mut merge_operations = Vec::new();
        for splits_in_group in group_by_num_merge_ops.values_mut() {
            let merge_ops = self.merge_operations_within_num_merge_op_level(splits_in_group);
            merge_operations.extend(merge_ops);
            splits.append(splits_in_group);
        }
        merge_operations
    }

    fn is_mature(&self, split: &SplitMetadata) -> bool {
        if split.num_merge_ops >= self.config.max_merge_ops {
            return true;
        }
        if split.num_docs >= self.split_num_docs_target {
            return true;
        }
        false
    }

    #[cfg(test)]
    fn check_is_valid(&self, merge_op: &MergeOperation, _remaining_splits: &[SplitMetadata]) {
        use std::collections::HashSet;
        assert!(merge_op.splits_as_slice().len() <= self.config.max_merge_factor);
        if merge_op.splits_as_slice().len() < self.config.merge_factor {
            let num_docs: usize = merge_op
                .splits_as_slice()
                .iter()
                .map(|split| split.num_docs)
                .sum();
            let last_split_num_docs = merge_op.splits_as_slice().last().unwrap().num_docs;
            assert!(num_docs >= self.split_num_docs_target);
            assert!(num_docs - last_split_num_docs < self.split_num_docs_target);
        }
        let num_merge_ops: HashSet<usize> = merge_op
            .splits_as_slice()
            .iter()
            .map(|merge_op| merge_op.num_merge_ops)
            .collect();
        assert_eq!(num_merge_ops.len(), 1);
        assert!(num_merge_ops.into_iter().next().unwrap() < self.config.max_merge_ops);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use quickwit_metastore::SplitMetadata;
    use rand::seq::SliceRandom;

    use super::ConstWriteAmplificationMergePolicy;
    use crate::merge_policy::MergeOperation;
    use crate::MergePolicy;

    #[test]
    fn test_const_write_amplification_merge_policy_empty() {
        let mut splits = Vec::new();
        let merge_policy = ConstWriteAmplificationMergePolicy::for_test();
        assert!(merge_policy.operations(&mut splits).is_empty());
    }

    #[test]
    fn test_const_write_merge_policy_single_split() {
        let mut splits = vec![SplitMetadata {
            split_id: "01GE1R0KBFQHJ76030RYRAS8QA".to_string(),
            num_docs: 1,
            create_timestamp: 1665000000,
            num_merge_ops: 4,
            ..Default::default()
        }];
        let merge_policy = ConstWriteAmplificationMergePolicy::for_test();
        let operations: Vec<MergeOperation> = merge_policy.operations(&mut splits);
        assert!(operations.is_empty());
        assert_eq!(splits.len(), 1);
    }

    #[test]
    fn test_const_write_merge_policy_simple() {
        let merge_policy = ConstWriteAmplificationMergePolicy::for_test();
        let mut splits = (0..merge_policy.config.merge_factor)
            .map(|i| SplitMetadata {
                split_id: format!("split-{i}"),
                num_docs: 1_000,
                num_merge_ops: 1,
                ..Default::default()
            })
            .collect();
        let operations: Vec<MergeOperation> = merge_policy.operations(&mut splits);
        assert_eq!(operations.len(), 1);
        assert_eq!(
            operations[0].splits_as_slice().len(),
            merge_policy.config.merge_factor
        );
    }

    #[test]
    fn test_const_write_merge_policy_merge_factor_max() {
        let merge_policy = ConstWriteAmplificationMergePolicy::for_test();
        let mut splits =
            (0..merge_policy.config.max_merge_factor + merge_policy.config.merge_factor - 1)
                .map(|i| SplitMetadata {
                    split_id: format!("split-{i}"),
                    num_docs: 1_000,
                    num_merge_ops: 1,
                    ..Default::default()
                })
                .collect();
        let operations: Vec<MergeOperation> = merge_policy.operations(&mut splits);
        assert_eq!(operations.len(), 1);
        assert_eq!(
            operations[0].splits_as_slice().len(),
            merge_policy.config.max_merge_factor
        );
    }

    #[test]
    fn test_const_write_merge_policy_older_first() {
        let merge_policy = ConstWriteAmplificationMergePolicy::for_test();
        let mut splits: Vec<SplitMetadata> = (0..merge_policy.config.max_merge_factor)
            .map(|i| SplitMetadata {
                split_id: format!("split-{i}"),
                num_docs: 1_000,
                create_timestamp: 1_664_000_000i64 + i as i64,
                num_merge_ops: 1,
                ..Default::default()
            })
            .collect();
        splits.shuffle(&mut rand::thread_rng());
        let operations: Vec<MergeOperation> = merge_policy.operations(&mut splits);
        assert_eq!(operations.len(), 1);
        assert_eq!(
            operations[0].splits_as_slice().len(),
            merge_policy.config.max_merge_factor
        );
        let split_ids: Vec<&str> = operations[0]
            .splits_as_slice()
            .iter()
            .map(|split| split.split_id())
            .collect();
        assert_eq!(
            &split_ids[..],
            &["split-0", "split-1", "split-2", "split-3", "split-4"]
        );
    }

    #[test]
    fn test_const_write_merge_policy_target_num_docs() {
        let merge_policy = ConstWriteAmplificationMergePolicy::for_test();
        let mut splits = (0..4)
            .map(|i| SplitMetadata {
                split_id: format!("split-{i}"),
                num_docs: (merge_policy.split_num_docs_target + 2) / 3,
                num_merge_ops: 1,
                ..Default::default()
            })
            .collect();
        let operations: Vec<MergeOperation> = merge_policy.operations(&mut splits);
        assert_eq!(operations.len(), 1);
        assert_eq!(operations[0].splits_as_slice().len(), 3);
    }

    #[test]
    fn test_const_write_amp_merge_policy_proptest() {
        let merge_policy = ConstWriteAmplificationMergePolicy::for_test();
        crate::merge_policy::tests::proptest_merge_policy(&merge_policy);
    }

    #[tokio::test]
    async fn test_simulate_const_write_amplification_merge_policy() -> anyhow::Result<()> {
        let merge_policy = ConstWriteAmplificationMergePolicy::for_test();
        let vals = vec![1; 1_211]; //< 1_211 splits with a single doc each.
        let final_splits = crate::merge_policy::tests::aux_test_simulate_merge_planner_num_docs(
            Arc::new(merge_policy.clone()),
            &vals[..],
            |splits| {
                let mut num_merge_ops_counts: HashMap<usize, usize> = HashMap::default();
                for split in splits {
                    *num_merge_ops_counts.entry(split.num_merge_ops).or_default() += 1;
                }
                for split in splits {
                    assert!(split.num_merge_ops <= merge_policy.config.max_merge_ops);
                }
                for i in 0..merge_policy.config.max_merge_ops {
                    assert!(
                        num_merge_ops_counts.get(&i).copied().unwrap_or(0)
                            < merge_policy.config.merge_factor
                    );
                }
            },
        )
        .await?;
        assert_eq!(final_splits.len(), 49);
        Ok(())
    }
}
