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

use quickwit_metastore::SplitMetadata;

use super::MergeOperation;
use crate::merge_policy::MergePolicy;

#[derive(Debug)]
pub struct ConstWriteAmplification {
    max_merge_ops: usize,
    merge_factor: usize,
    max_merge_factor: usize,
    split_num_docs_target: usize,
}

impl Default for ConstWriteAmplification {
    fn default() -> ConstWriteAmplification {
        ConstWriteAmplification {
            max_merge_ops: 4,
            merge_factor: 10,
            max_merge_factor: 12,
            split_num_docs_target: 10_000_000,
        }
    }
}

impl ConstWriteAmplification {
    /// Merge operations within one level.
    /// This method assumes that the splits are sorted by inverse creation date.
    fn single_merge_operation_within_num_merge_op_level(
        &self,
        splits: &mut Vec<SplitMetadata>,
    ) -> Option<MergeOperation> {
        let mut num_splits_in_merge = 0;
        let mut num_docs_in_merge = 0;
        for split in splits.iter().take(self.max_merge_factor) {
            num_docs_in_merge += split.num_docs;
            num_splits_in_merge += 1;
            if num_docs_in_merge >= self.split_num_docs_target {
                break;
            }
        }
        if (num_docs_in_merge < self.split_num_docs_target) && (num_splits_in_merge < self.merge_factor) {
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
        splits.sort_by_key(|split| split.create_timestamp);
        let mut merge_operations = Vec::new();
        while let Some(merge_op) = self.single_merge_operation_within_num_merge_op_level(splits) {
            merge_operations.push(merge_op);
        }
        merge_operations
    }
}

impl MergePolicy for ConstWriteAmplification {
    fn operations(&self, splits: &mut Vec<SplitMetadata>) -> Vec<MergeOperation> {
        let mut group_by_num_merge_ops: HashMap<usize, Vec<SplitMetadata>> = HashMap::default();
        for split in splits.drain(..) {
            if self.is_mature(&split) {
                continue;
            }
            group_by_num_merge_ops
                .entry(split.num_merge_ops)
                .or_default()
                .push(split);
        }
        let mut merge_operations = Vec::new();
        for splits_in_group in group_by_num_merge_ops.values_mut() {
            let merge_ops = self.merge_operations_within_num_merge_op_level(splits);
            merge_operations.extend(merge_ops);
            splits.append(splits_in_group);
        }
        merge_operations
    }

    fn is_mature(&self, split: &SplitMetadata) -> bool {
        if split.num_merge_ops >= self.max_merge_ops {
            return true;
        }
        if split.num_docs >= self.split_num_docs_target {
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use quickwit_metastore::SplitMetadata;

    use super::ConstWriteAmplification;
    use crate::MergePolicy;

    #[test]
    fn test_const_write_amplification_merge_policy_empty() {
        let mut splits = Vec::new();
        let merge_policy = ConstWriteAmplification::default();
        assert!(merge_policy.operations(&mut splits).is_empty());
    }

    // fn create_split(
    //     id: usize,
    //     num_docs: usize,
    //     creation_timestamp: i64,
    //     num_merge_ops: usize) -> SplitMetadata {
    //     SplitMetadata {
    //         split_id: format!("split_{:02}", split_ord),
    //         num_docs,
    //         creation_timestamp,
    //         num_merge_ops,
    //         ..Default::default()
    //     }
    // }

    fn pow_of_10(n: usize) -> usize {
        10usize.pow(n as u32)
    }

    prop_compose! {
        fn num_docs_around_power_of_ten()(
            pow_ten in 1usize..5usize,
            diff in -2isize..2isize
        ) -> usize {
            (pow_of_10(pow_ten) as isize + diff).max(0isize) as usize
        }
    }

    fn num_docs_strategy() -> impl Strategy<Value = usize> {
        prop_oneof![0usize..10_000_000usize, num_docs_around_power_of_ten()]
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

    prop_compose! {
      fn splits_strategy()(
            splits in prop::collection::vec(split_strategy(), 0..100)
      ) -> Vec<SplitMetadata> {
            splits
      }
    }

    proptest! {
        #[test]
        fn test_merge_policy_proptest(mut splits in splits_strategy()) {
            let original_num_splits = splits.len();
            let merge_policy = ConstWriteAmplification::default();
            let operations: Vec<crate::merge_policy::MergeOperation> = merge_policy.operations(&mut splits);
            {   // Merge policies should not drop splits
                let num_splits_in_merge: usize = operations.iter().map(|op| op.splits().len()).sum();
                assert_eq!(num_splits_in_merge + splits.len(), original_num_splits);
            }
            for merge_op in &operations {
                // Merge policies should not merge a single split.
                assert!(merge_op.splits().len() >= 2);
            }
        }
    }
}
