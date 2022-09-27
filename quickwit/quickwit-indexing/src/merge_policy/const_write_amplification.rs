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
        if splits.len() < self.merge_factor {
            return None;
        }
        let mut splits_in_merge = Vec::new();
        let mut num_docs_in_merge = 0;
        for _ in 0..self.max_merge_factor {
            if let Some(split) = splits.pop() {
                num_docs_in_merge += split.num_docs;
                splits_in_merge.push(split);
                // If the resulting split will reach the num docs target,
                // we stop adding extra splits.
                if num_docs_in_merge >= self.split_num_docs_target {
                    break;
                }
            } else {
                break;
            }
        }
        let merge_operation = MergeOperation::new_merge_operation(splits_in_merge);
        Some(merge_operation)
    }

    fn merge_operations_within_num_merge_op_level(
        &self,
        splits: &mut Vec<SplitMetadata>,
    ) -> Vec<MergeOperation> {
        splits.sort_by_key(|split| Reverse(split.create_timestamp));
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
    use super::ConstWriteAmplification;
    use crate::MergePolicy;

    #[test]
    fn test_const_write_amplification_merge_policy_empty() {
        let mut splits = Vec::new();
        let merge_policy = ConstWriteAmplification::default();
        assert!(merge_policy.operations(&mut splits).is_empty());
    }
}
