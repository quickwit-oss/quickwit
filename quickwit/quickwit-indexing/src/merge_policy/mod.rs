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

mod const_write_amplification;
mod nop_merge_policy;
mod stable_log_merge_policy;

use std::fmt;
use std::sync::Arc;

pub use nop_merge_policy::NopMergePolicy;
use quickwit_config::IndexingSettings;
use quickwit_metastore::SplitMetadata;
pub(crate) use stable_log_merge_policy::StableLogMergePolicy;

use crate::new_split_id;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MergeOperationType {
    Merge,
    DeleteAndMerge,
}

impl fmt::Display for MergeOperationType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone)]
pub struct MergeOperation {
    pub merge_split_id: String,
    pub splits: Vec<SplitMetadata>,
    pub operation_type: MergeOperationType,
}

impl MergeOperation {
    pub fn new_merge_operation(splits: Vec<SplitMetadata>) -> Self {
        Self {
            merge_split_id: new_split_id(),
            splits,
            operation_type: MergeOperationType::Merge,
        }
    }

    pub fn new_delete_and_merge_operation(split: SplitMetadata) -> Self {
        Self {
            merge_split_id: new_split_id(),
            splits: vec![split],
            operation_type: MergeOperationType::DeleteAndMerge,
        }
    }

    pub fn splits_as_slice(&self) -> &[SplitMetadata] {
        self.splits.as_slice()
    }
}

impl fmt::Debug for MergeOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Merge(operation_type={}, merged_split_id={},splits=[",
            self.operation_type, self.merge_split_id
        )?;
        for split in &self.splits {
            write!(f, "{},", split.split_id())?;
        }
        write!(f, "])")?;
        Ok(())
    }
}

/// A merge policy wraps the logic that decides what should be merged.
/// The SplitMetadata must be extracted from the splits `Vec`.
///
/// It is called by the merge planner whenever a new split is added.
pub trait MergePolicy: Send + Sync + fmt::Debug {
    /// Returns the list of merge operations that should be performed.
    fn operations(&self, splits: &mut Vec<SplitMetadata>) -> Vec<MergeOperation>;
    /// A mature split is a split that won't undergo a merge operation in the future.
    fn is_mature(&self, split: &SplitMetadata) -> bool;
    /// Describe a bunch of properties specific to the given merge policy.
    /// This method is used in proptesting.
    ///
    /// - `merge_op` is a merge operation emitted by this merge policy.
    /// - `remaining_splits` is the list of remaining splits.
    #[cfg(test)]
    fn check_is_valid(&self, _merge_op: &MergeOperation, _remaining_splits: &[SplitMetadata]) {}
}

pub fn merge_policy_from_settings(indexing_settings: &IndexingSettings) -> Arc<dyn MergePolicy> {
    if !indexing_settings.merge_enabled {
        return Arc::new(NopMergePolicy);
    }
    let stable_log_merge_policy = StableLogMergePolicy {
        merge_factor: indexing_settings.merge_policy.merge_factor,
        max_merge_factor: indexing_settings.merge_policy.max_merge_factor,
        split_num_docs_target: indexing_settings.split_num_docs_target,
        ..Default::default()
    };
    Arc::new(stable_log_merge_policy)
}

pub fn default_merge_policy() -> Arc<dyn MergePolicy> {
    merge_policy_from_settings(&IndexingSettings::default())
}

struct SplitShortDebug<'a>(&'a SplitMetadata);

impl<'a> fmt::Debug for SplitShortDebug<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Split")
            .field("split_id", &self.0.split_id())
            .field("num_docs", &self.0.num_docs)
            .finish()
    }
}

fn splits_short_debug(splits: &[SplitMetadata]) -> Vec<SplitShortDebug> {
    splits.iter().map(SplitShortDebug).collect()
}

#[cfg(test)]
pub mod tests {

    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    use std::ops::RangeInclusive;

    use proptest::prelude::*;
    use rand::seq::SliceRandom;

    use super::*;

    fn pow_of_10(n: usize) -> usize {
        10usize.pow(n as u32)
    }

    prop_compose! {
        fn num_docs_around_power_of_ten()(
            pow_ten in 1usize..5usize,
            diff in -2isize..2isize
        ) -> usize {
            (pow_of_10(pow_ten) as isize + diff).max(1isize) as usize
        }
    }

    fn num_docs_strategy() -> impl Strategy<Value = usize> {
        prop_oneof![1usize..10_000_000usize, num_docs_around_power_of_ten()]
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

    pub(crate) fn create_splits(num_docs_vec: Vec<usize>) -> Vec<SplitMetadata> {
        let num_docs_with_timestamp = num_docs_vec
            .into_iter()
            // we give the same timestamp to all of them and rely on stable sort to keep the split
            // order.
            .map(|num_docs| (num_docs, (1630563067..=1630564067)))
            .collect();
        create_splits_with_timestamps(num_docs_with_timestamp)
    }

    fn create_splits_with_timestamps(
        num_docs_vec: Vec<(usize, RangeInclusive<i64>)>,
    ) -> Vec<SplitMetadata> {
        num_docs_vec
            .into_iter()
            .enumerate()
            .map(|(split_ord, (num_docs, time_range))| SplitMetadata {
                split_id: format!("split_{:02}", split_ord),
                num_docs,
                time_range: Some(time_range),
                ..Default::default()
            })
            .collect()
    }

    // Creates a checksum for a given merge operation.
    // This does not take in account the merge split id,
    // and is split order independent.
    fn compute_checksum_op(op: &MergeOperation) -> u64 {
        let mut checksum = 0u64;
        for split in op.splits_as_slice() {
            let mut hasher = DefaultHasher::default();
            hasher.write(split.split_id.as_bytes());
            checksum ^= hasher.finish();
        }
        checksum
    }

    // Creates a checksum for a set of operations.
    // This checksum does not depend on the order of the merrge operations,
    // nor the merge split ids.
    fn compute_checksum_ops(ops: &[MergeOperation]) -> u64 {
        let mut checksum = 0u64;
        for op in ops {
            let op_checksum = compute_checksum_op(op);
            let mut hasher = DefaultHasher::default();
            hasher.write_u64(op_checksum);
            checksum ^= hasher.finish();
        }
        checksum
    }

    fn compare_merge_operations(left_ops: &[MergeOperation], right_ops: &[MergeOperation]) -> bool {
        compute_checksum_ops(left_ops) == compute_checksum_ops(right_ops)
    }

    pub(crate) fn proptest_merge_policy(merge_policy: &dyn MergePolicy) {
        proptest!(|(mut splits in prop::collection::vec(split_strategy(), 0..100))| {
            let mut cloned_splits = splits.clone();
            cloned_splits.shuffle(&mut rand::thread_rng());

            let original_num_splits = splits.len();

            let mut operations: Vec<MergeOperation> = merge_policy.operations(&mut splits);
            let operations_after_shuffle = merge_policy.operations(&mut cloned_splits);
            assert!(compare_merge_operations(&operations[..],
                &operations_after_shuffle[..]),
                "Merge policy result should be independent from the original order.");

            let num_splits_in_merge: usize = operations.iter().map(|op| op.splits_as_slice().len()).sum();

            assert_eq!(
                num_splits_in_merge + splits.len(), original_num_splits,
                "Splits should not be lost."
            );

            // assert!(
            //     merge_policy.operations(&mut splits).is_empty(),
            //     "Merge policy are expected to return all available merge operations."
            // );

            for merge_op in &mut operations {
                assert_eq!(merge_op.operation_type, MergeOperationType::Merge,
                    "A merge policy should only emit Merge operations."
                );
                assert!(merge_op.splits_as_slice().len() >= 2,
            "Merge policies should not suggest merging a single split.");
                for split in merge_op.splits_as_slice() {
                    assert!(!merge_policy.is_mature(split), "Merges should not contain mature splits.");
                }
                merge_policy.check_is_valid(merge_op, &splits[..]);
            }
        });
    }
}
