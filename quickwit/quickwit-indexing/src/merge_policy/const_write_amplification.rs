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

use std::collections::HashMap;
use std::ops::RangeInclusive;

use quickwit_config::IndexingSettings;
use quickwit_config::merge_policy_config::ConstWriteAmplificationMergePolicyConfig;
use quickwit_metastore::{SplitMaturity, SplitMetadata};
use time::OffsetDateTime;
use tracing::info;

use super::MergeOperation;
use crate::merge_policy::MergePolicy;

// Smallest number of splits in a finalize merge.
const FINALIZE_MIN_MERGE_FACTOR: usize = 3;

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
        use std::time::Duration;

        let config = ConstWriteAmplificationMergePolicyConfig {
            max_merge_ops: 3,
            merge_factor: 3,
            max_merge_factor: 5,
            maturation_period: Duration::from_secs(3600),
            max_finalize_merge_operations: 0,
            max_finalize_split_num_docs: None,
        };
        Self::new(config, 10_000_000)
    }

    /// Returns a merge operation within one `num_merge_ops` level if one can be built from the
    /// given splits. This method assumes that the splits are sorted by reverse creation date
    /// and have all the same `num_merge_ops`.
    fn single_merge_operation_within_num_merge_op_level(
        &self,
        splits: &mut Vec<SplitMetadata>,
        merge_factor_range: RangeInclusive<usize>,
    ) -> Option<MergeOperation> {
        let mut num_splits_in_merge = 0;
        let mut num_docs_in_merge = 0;
        for split in splits.iter().take(*merge_factor_range.end()) {
            num_docs_in_merge += split.num_docs;
            num_splits_in_merge += 1;
            if num_docs_in_merge >= self.split_num_docs_target {
                break;
            }
        }
        if (num_docs_in_merge < self.split_num_docs_target)
            && (num_splits_in_merge < *merge_factor_range.start())
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
        while let Some(merge_op) =
            self.single_merge_operation_within_num_merge_op_level(splits, self.merge_factor_range())
        {
            merge_operations.push(merge_op);
        }
        merge_operations
    }

    fn merge_factor_range(&self) -> RangeInclusive<usize> {
        self.config.merge_factor..=self.config.max_merge_factor
    }
}

impl MergePolicy for ConstWriteAmplificationMergePolicy {
    fn operations(&self, splits: &mut Vec<SplitMetadata>) -> Vec<MergeOperation> {
        let mut group_by_num_merge_ops: HashMap<usize, Vec<SplitMetadata>> = HashMap::default();
        let mut mature_splits = Vec::new();
        let now = OffsetDateTime::now_utc();
        for split in splits.drain(..) {
            if split.is_mature(now) {
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
            // we readd the splits that are not used in a merge operation into the splits vector.
            splits.append(splits_in_group);
        }
        merge_operations
    }

    fn finalize_operations(&self, splits: &mut Vec<SplitMetadata>) -> Vec<MergeOperation> {
        if self.config.max_finalize_merge_operations == 0 {
            return Vec::new();
        }

        let now = OffsetDateTime::now_utc();

        // We first isolate mature splits. Let's not touch them.
        let (mature_splits, mut young_splits): (Vec<SplitMetadata>, Vec<SplitMetadata>) =
            splits.drain(..).partition(|split: &SplitMetadata| {
                if let Some(max_finalize_split_num_docs) = self.config.max_finalize_split_num_docs
                    && split.num_docs > max_finalize_split_num_docs
                {
                    return true;
                }
                split.is_mature(now)
            });
        splits.extend(mature_splits);

        // We then sort the split by reverse creation date and split id.
        // You may notice that reverse is the opposite of the rest of the policy.
        //
        // This is because these are the youngest splits. If we limit ourselves in the number of
        // merge we will operate, we might as well focus on the young == smaller ones for that
        // last merge.
        young_splits.sort_by(|left, right| {
            left.create_timestamp
                .cmp(&right.create_timestamp)
                .reverse()
                .then_with(|| left.split_id().cmp(right.split_id()))
        });
        let mut merge_operations = Vec::new();
        while merge_operations.len() < self.config.max_finalize_merge_operations {
            let min_merge_factor = FINALIZE_MIN_MERGE_FACTOR.min(self.config.max_merge_factor);
            let merge_factor_range = min_merge_factor..=self.config.max_merge_factor;
            if let Some(merge_op) = self.single_merge_operation_within_num_merge_op_level(
                &mut young_splits,
                merge_factor_range,
            ) {
                merge_operations.push(merge_op);
            } else {
                break;
            }
        }

        // We readd the young splits that are not used in any merge operation.
        splits.extend(young_splits);

        assert!(merge_operations.len() <= self.config.max_finalize_merge_operations);

        let num_splits_per_merge_op: Vec<usize> =
            merge_operations.iter().map(|op| op.splits.len()).collect();
        let num_docs_per_merge_op: Vec<usize> = merge_operations
            .iter()
            .map(|op| op.splits.iter().map(|split| split.num_docs).sum::<usize>())
            .collect();
        info!(
            num_splits_per_merge_op=?num_splits_per_merge_op,
            num_docs_per_merge_op=?num_docs_per_merge_op,
            "finalize merge operation");
        merge_operations
    }

    fn split_maturity(&self, split_num_docs: usize, split_num_merge_ops: usize) -> SplitMaturity {
        if split_num_merge_ops >= self.config.max_merge_ops {
            return SplitMaturity::Mature;
        }
        if split_num_docs >= self.split_num_docs_target {
            return SplitMaturity::Mature;
        }
        SplitMaturity::Immature {
            maturation_period: self.config.maturation_period,
        }
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
    use std::time::Duration;

    use quickwit_metastore::{SplitMaturity, SplitMetadata};
    use rand::seq::SliceRandom;
    use time::OffsetDateTime;

    use super::ConstWriteAmplificationMergePolicy;
    use crate::MergePolicy;
    use crate::merge_policy::MergeOperation;
    use crate::merge_policy::tests::create_splits;

    #[test]
    fn test_split_is_mature() {
        let merge_policy = ConstWriteAmplificationMergePolicy::for_test();
        let split = create_splits(&merge_policy, vec![9_000_000])
            .into_iter()
            .next()
            .unwrap();
        // Split under split_num_docs_target, num_merge_ops < max_merge_ops and created before now()
        // - maturation_period is not mature.
        assert_eq!(
            merge_policy.split_maturity(split.num_docs, split.num_merge_ops),
            SplitMaturity::Immature {
                maturation_period: Duration::from_secs(3600)
            }
        );
        // Split with docs > split_num_docs_target is mature.
        assert_eq!(
            merge_policy
                .split_maturity(merge_policy.split_num_docs_target + 1, split.num_merge_ops),
            SplitMaturity::Mature
        );

        // Split with num_merge_ops >= max_merge_ops is mature
        assert_eq!(
            merge_policy.split_maturity(split.num_docs, merge_policy.config.max_merge_ops),
            SplitMaturity::Mature
        );
    }

    #[test]
    fn test_const_write_amplification_merge_policy_empty() {
        let mut splits = Vec::new();
        let merge_policy = ConstWriteAmplificationMergePolicy::for_test();
        assert!(merge_policy.operations(&mut splits).is_empty());
    }

    #[test]
    fn test_const_write_merge_policy_single_split() {
        let merge_policy = ConstWriteAmplificationMergePolicy::for_test();
        let mut splits = vec![SplitMetadata {
            split_id: "01GE1R0KBFQHJ76030RYRAS8QA".to_string(),
            num_docs: 1,
            create_timestamp: 1665000000,
            maturity: merge_policy.split_maturity(1, 0),
            num_merge_ops: 4,
            ..Default::default()
        }];
        let operations: Vec<MergeOperation> = merge_policy.operations(&mut splits);
        assert!(operations.is_empty());
        assert_eq!(splits.len(), 1);
    }

    #[test]
    fn test_const_write_merge_policy_simple() {
        let merge_policy = ConstWriteAmplificationMergePolicy::for_test();
        let create_timestamp = OffsetDateTime::now_utc().unix_timestamp();
        let mut splits = (0..merge_policy.config.merge_factor)
            .map(|i| SplitMetadata {
                split_id: format!("split-{i}"),
                num_docs: 1_000,
                num_merge_ops: 1,
                create_timestamp,
                maturity: merge_policy.split_maturity(1_000, 1),
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
        let time_to_maturity = merge_policy.split_maturity(1_000, 1);
        let create_timestamp = OffsetDateTime::now_utc().unix_timestamp();
        let mut splits =
            (0..merge_policy.config.max_merge_factor + merge_policy.config.merge_factor - 1)
                .map(|i| SplitMetadata {
                    split_id: format!("split-{i}"),
                    num_docs: 1_000,
                    num_merge_ops: 1,
                    create_timestamp,
                    maturity: time_to_maturity,
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
        let time_to_maturity = merge_policy.split_maturity(1_000, 1);
        let now_timestamp: i64 = OffsetDateTime::now_utc().unix_timestamp();
        let mut splits: Vec<SplitMetadata> = (0..merge_policy.config.max_merge_factor)
            .map(|i| SplitMetadata {
                split_id: format!("split-{i}"),
                num_docs: 1_000,
                num_merge_ops: 1,
                create_timestamp: now_timestamp + i as i64,
                maturity: time_to_maturity,
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
        let create_timestamp = OffsetDateTime::now_utc().unix_timestamp();
        let mut splits = (0..4)
            .map(|i| {
                let num_docs = merge_policy.split_num_docs_target.div_ceil(3);
                let time_to_maturity = merge_policy.split_maturity(num_docs, 1);
                SplitMetadata {
                    split_id: format!("split-{i}"),
                    num_docs,
                    num_merge_ops: 1,
                    create_timestamp,
                    maturity: time_to_maturity,
                    ..Default::default()
                }
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
            &|splits| {
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

    #[tokio::test]
    async fn test_simulate_const_write_amplification_merge_policy_with_finalize() {
        let mut merge_policy = ConstWriteAmplificationMergePolicy::for_test();
        merge_policy.config.max_merge_factor = 10;
        merge_policy.config.merge_factor = 10;
        merge_policy.split_num_docs_target = 10_000_000;

        let vals: Vec<usize> = vec![1; 9 + 90 + 900]; //< 1_211 splits with a single doc each.

        let num_final_splits_given_max_finalize_merge_operations =
            |split_num_docs: Vec<usize>, max_finalize_merge_operations: usize| {
                let mut merge_policy_clone = merge_policy.clone();
                merge_policy_clone.config.max_finalize_merge_operations =
                    max_finalize_merge_operations;
                async move {
                    crate::merge_policy::tests::aux_test_simulate_merge_planner_num_docs(
                        Arc::new(merge_policy_clone),
                        &split_num_docs[..],
                        &|_splits| {},
                    )
                    .await
                    .unwrap()
                }
            };

        assert_eq!(
            num_final_splits_given_max_finalize_merge_operations(vals.clone(), 0)
                .await
                .len(),
            27
        );
        assert_eq!(
            num_final_splits_given_max_finalize_merge_operations(vals.clone(), 1)
                .await
                .len(),
            18
        );
        assert_eq!(
            num_final_splits_given_max_finalize_merge_operations(vals.clone(), 2)
                .await
                .len(),
            9
        );
        assert_eq!(
            num_final_splits_given_max_finalize_merge_operations(vals.clone(), 3)
                .await
                .len(),
            3
        );
        assert_eq!(
            num_final_splits_given_max_finalize_merge_operations(vec![1; 6], 1)
                .await
                .len(),
            1
        );
        assert_eq!(
            num_final_splits_given_max_finalize_merge_operations(vec![1; 3], 1)
                .await
                .len(),
            1
        );
        assert_eq!(
            num_final_splits_given_max_finalize_merge_operations(vec![1; 2], 1)
                .await
                .len(),
            2
        );

        // We check that the youngest splits are merged in priority.
        let final_splits = num_final_splits_given_max_finalize_merge_operations(
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            1,
        )
        .await;
        assert_eq!(final_splits.len(), 2);

        let mut split_num_docs: Vec<usize> = final_splits
            .iter()
            .map(|split| split.num_docs)
            .collect::<Vec<_>>();
        split_num_docs.sort();
        assert_eq!(split_num_docs[0], 11);
        assert_eq!(split_num_docs[1], 55);
    }

    #[tokio::test]
    async fn test_simulate_const_write_amplification_merge_policy_with_finalize_max_num_docs() {
        let mut merge_policy = ConstWriteAmplificationMergePolicy::for_test();
        merge_policy.config.max_merge_factor = 10;
        merge_policy.config.merge_factor = 10;
        merge_policy.split_num_docs_target = 10_000_000;
        merge_policy.config.max_finalize_split_num_docs = Some(999_999);
        merge_policy.config.max_finalize_merge_operations = 3;

        let split_num_docs: Vec<usize> = vec![999_999, 1_000_000, 999_999, 999_999];

        let final_splits = crate::merge_policy::tests::aux_test_simulate_merge_planner_num_docs(
            Arc::new(merge_policy),
            &split_num_docs[..],
            &|_splits| {},
        )
        .await
        .unwrap();

        assert_eq!(final_splits.len(), 2);
        let mut split_num_docs: Vec<usize> =
            final_splits.iter().map(|split| split.num_docs).collect();
        split_num_docs.sort();
        assert_eq!(split_num_docs[0], 1_000_000);
        assert_eq!(split_num_docs[1], 999_999 * 3);
    }
}
