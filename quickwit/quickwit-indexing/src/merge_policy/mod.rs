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

mod stable_log_merge_policy;

use std::fmt;

use quickwit_metastore::SplitMetadata;
pub use stable_log_merge_policy::StableLogMergePolicy;

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

/// A merge policy wraps the logic that decide what should be merged.
/// The SplitMetadata must be extracted from the splits `Vec`.
///
/// It is called by the merge planner whenever a new split is added.
pub trait MergePolicy: Send + Sync + fmt::Debug {
    /// Returns the list of merge operations that should be performed.
    fn operations(&self, splits: &mut Vec<SplitMetadata>) -> Vec<MergeOperation>;
    /// A mature split is a split that won't undergo a merge operation in the future.
    fn is_mature(&self, split: &SplitMetadata) -> bool;
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
mod tests {

    use std::ops::RangeInclusive;

    use super::*;

    fn create_splits(num_docs_vec: Vec<usize>) -> Vec<SplitMetadata> {
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

    #[test]
    fn test_split_is_mature() {
        let merge_policy = StableLogMergePolicy::default();
        // Split under max_merge_docs is not mature.
        let mut split = create_splits(vec![9_000_000]).into_iter().next().unwrap();
        assert!(!merge_policy.is_mature(&split));
        // All splits are mature when merge is disabled.
        let merge_policy_with_disabled_merge = StableLogMergePolicy {
            merge_enabled: false,
            ..Default::default()
        };
        assert!(merge_policy_with_disabled_merge.is_mature(&split));
        // Split under max_merge_docs is not mature.
        assert!(!merge_policy.is_mature(&split));
        // Split with docs > max_merge_docs is mature.
        split.num_docs = merge_policy.split_num_docs_target + 1;
        assert!(merge_policy.is_mature(&split));
    }

    #[test]
    fn test_build_split_levels() {
        let merge_policy = StableLogMergePolicy::default();
        let splits = Vec::new();
        let split_groups = merge_policy.build_split_levels(&splits);
        assert!(split_groups.is_empty());
    }

    #[test]
    fn test_stable_log_merge_policy_build_split_simple() {
        let merge_policy = StableLogMergePolicy::default();
        let splits = create_splits(vec![100_000, 100_000, 100_000, 800_000, 900_000]);
        let split_groups = merge_policy.build_split_levels(&splits);
        assert_eq!(&split_groups, &[0..3, 3..5]);
    }

    #[test]
    fn test_stable_log_merge_policy_build_split_perfect_world() {
        let merge_policy = StableLogMergePolicy::default();
        let splits = create_splits(vec![
            100_000, 100_000, 100_000, 100_000, 100_000, 100_000, 100_000, 100_000, 800_000,
            1_600_000,
        ]);
        let split_groups = merge_policy.build_split_levels(&splits);
        assert_eq!(&split_groups, &[0..8, 8..10]);
    }

    #[test]
    fn test_stable_log_merge_policy_build_split_decreasing() {
        let merge_policy = StableLogMergePolicy::default();
        let splits = create_splits(vec![
            100_000, 100_000, 100_000, 100_000, 100_000, 100_000, 100_000, 100_000, 800_000,
            100_000, 1_600_000,
        ]);
        let split_groups = merge_policy.build_split_levels(&splits);
        assert_eq!(&split_groups, &[0..8, 8..11]);
    }

    #[test]
    #[should_panic(expected = "All splits are expected to be smaller than `max_merge_docs`.")]
    fn test_stable_log_merge_policy_build_split_panics_if_exceeding_max_merge_docs() {
        let merge_policy = StableLogMergePolicy::default();
        let splits = create_splits(vec![11_000_000]);
        merge_policy.build_split_levels(&splits);
    }

    #[test]
    fn test_stable_log_merge_policy_not_enough_splits() {
        let merge_policy = StableLogMergePolicy::default();
        let mut splits = create_splits(vec![100; 7]);
        assert_eq!(splits.len(), 7);
        assert!(merge_policy.operations(&mut splits).is_empty());
    }

    #[test]
    fn test_stable_log_merge_policy_just_enough_splits_for_a_merge() {
        let merge_policy = StableLogMergePolicy::default();
        let mut splits = create_splits(vec![100; 10]);
        let mut merge_ops = merge_policy.operations(&mut splits);
        assert!(splits.is_empty());
        assert_eq!(merge_ops.len(), 1);
        let merge_op = merge_ops.pop().unwrap();
        let mut merge_segment_ids: Vec<String> = merge_op
            .splits_as_slice()
            .iter()
            .map(|split| split.split_id().to_string())
            .collect();
        merge_segment_ids.sort();
        assert_eq!(
            merge_segment_ids,
            &[
                "split_00", "split_01", "split_02", "split_03", "split_04", "split_05", "split_06",
                "split_07", "split_08", "split_09"
            ]
        );
    }

    #[test]
    fn test_stable_log_merge_policy_many_splits_on_same_level() {
        let merge_policy = StableLogMergePolicy::default();
        let mut splits = create_splits(vec![100; 13]);
        let mut merge_ops = merge_policy.operations(&mut splits);
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].split_id(), "split_00");
        assert_eq!(merge_ops.len(), 1);
        let merge_op = merge_ops.pop().unwrap();
        let mut merge_split_ids: Vec<String> = merge_op
            .splits_as_slice()
            .iter()
            .map(|split| split.split_id().to_string())
            .collect();
        merge_split_ids.sort();
        assert_eq!(
            merge_split_ids,
            &[
                "split_01", "split_02", "split_03", "split_04", "split_05", "split_06", "split_07",
                "split_08", "split_09", "split_10", "split_11", "split_12"
            ]
        );
    }

    #[test]
    fn test_stable_log_merge_policy_splits_below_min_level() {
        let merge_policy = StableLogMergePolicy::default();
        let mut splits = create_splits(vec![
            100, 1000, 10_000, 10_000, 10_000, 10_000, 10_000, 40_000, 40_000, 40_000,
        ]);
        let mut merge_ops = merge_policy.operations(&mut splits);
        assert_eq!(splits.len(), 0);
        assert_eq!(merge_ops.len(), 1);
        let merge_op = merge_ops.pop().unwrap();
        let mut merge_split_ids: Vec<String> = merge_op
            .splits_as_slice()
            .iter()
            .map(|split| split.split_id().to_string())
            .collect();
        merge_split_ids.sort();
        assert_eq!(
            merge_split_ids,
            &[
                "split_00", "split_01", "split_02", "split_03", "split_04", "split_05", "split_06",
                "split_07", "split_08", "split_09"
            ]
        );
    }

    #[test]
    fn test_stable_log_merge_policy_splits_above_min_level() {
        let merge_policy = StableLogMergePolicy::default();
        let mut splits = create_splits(vec![
            100_000, 1_000_000, 1_000_000, 1_000_000, 1_000_000, 1_000_000, 1_000_000, 1_000_000,
        ]);
        let merge_ops = merge_policy.operations(&mut splits);
        assert_eq!(splits.len(), 8);
        assert_eq!(merge_ops.len(), 0);
    }

    #[test]
    fn test_stable_log_merge_policy_above_max_merge_docs_is_ignored() {
        let merge_policy = StableLogMergePolicy::default();
        let mut splits = create_splits(vec![
            100_000, 100_000, 100_000, 100_000, 100_000,
            10_000_000, // this split should not interfere with the merging of other splits
            100_000, 100_000, 100_000, 100_000, 100_000,
        ]);
        let merge_ops = merge_policy.operations(&mut splits);
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].num_docs, 10_000_000);
        assert_eq!(merge_ops.len(), 1);
    }

    #[test]
    fn test_merge_policy_splits_too_large_are_ignored() {
        let merge_policy = StableLogMergePolicy::default();
        let mut splits = create_splits(vec![9_999_999, 10_000_000]);
        let merge_ops = merge_policy.operations(&mut splits);
        assert_eq!(splits.len(), 2);
        assert_eq!(splits[0].num_docs, 9_999_999);
        assert_eq!(splits[1].num_docs, 10_000_000);
        assert!(merge_ops.is_empty());
    }

    #[test]
    fn test_merge_policy_splits_entire_level_reach_merge_max_doc() {
        let merge_policy = StableLogMergePolicy::default();
        let mut splits = create_splits(vec![5_000_000, 5_000_000]);
        let merge_ops = merge_policy.operations(&mut splits);
        assert!(splits.is_empty());
        assert_eq!(merge_ops.len(), 1);
        assert_eq!(merge_ops[0].splits_as_slice().len(), 2);
    }

    #[test]
    fn test_merge_policy_last_merge_can_have_a_lower_merge_factor() {
        let merge_policy = StableLogMergePolicy::default();
        let mut splits = create_splits(vec![9_999_997, 9_999_998, 9_999_999]);
        let merge_ops = merge_policy.operations(&mut splits);
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].num_docs, 9_999_997);
        assert_eq!(merge_ops.len(), 1);
        assert_eq!(merge_ops[0].splits_as_slice().len(), 2);
    }

    #[test]
    fn test_merge_policy_no_merge_with_only_one_split() {
        let merge_policy = StableLogMergePolicy::default();
        let mut splits = create_splits(vec![9_999_999]);
        let merge_ops = merge_policy.operations(&mut splits);
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].num_docs, 9_999_999);
        assert!(merge_ops.is_empty());
    }

    #[test]
    fn test_stable_log_merge_policy_max_num_splits_worst_case() {
        let merge_policy = StableLogMergePolicy::default();
        assert_eq!(merge_policy.max_num_splits_worst_case(99), 9);
        assert_eq!(merge_policy.max_num_splits_worst_case(1_000_000), 27);
        assert_eq!(merge_policy.max_num_splits_worst_case(2_000_000), 36);
        assert_eq!(merge_policy.max_num_splits_worst_case(3_000_000), 36);
        assert_eq!(merge_policy.max_num_splits_worst_case(4_000_000), 36);
        assert_eq!(merge_policy.max_num_splits_worst_case(5_000_000), 45);
        assert_eq!(merge_policy.max_num_splits_worst_case(7_000_000), 45);
        assert_eq!(merge_policy.max_num_splits_worst_case(10_000_000), 45);
        assert_eq!(merge_policy.max_num_splits_worst_case(20_000_000), 54);
        assert_eq!(merge_policy.max_num_splits_worst_case(100_000_000), 63);
        assert_eq!(merge_policy.max_num_splits_worst_case(1_000_000_000), 153);
    }

    #[test]
    fn test_stable_log_merge_policy_max_num_splits_ideal_case() {
        let merge_policy = StableLogMergePolicy::default();
        assert_eq!(merge_policy.max_num_splits_ideal_case(1_000_000), 18);
        assert_eq!(merge_policy.max_num_splits_ideal_case(99), 9);
        assert_eq!(merge_policy.max_num_splits_ideal_case(2_000_000), 20);
        assert_eq!(merge_policy.max_num_splits_ideal_case(3_000_000), 21);
        assert_eq!(merge_policy.max_num_splits_ideal_case(4_000_000), 22);
        assert_eq!(merge_policy.max_num_splits_ideal_case(5_000_000), 23);
        assert_eq!(merge_policy.max_num_splits_ideal_case(7_000_000), 25);
        assert_eq!(merge_policy.max_num_splits_ideal_case(10_000_000), 27);
        assert_eq!(merge_policy.max_num_splits_ideal_case(100_000_000), 37);
        assert_eq!(merge_policy.max_num_splits_ideal_case(1_000_000_000), 127);
    }

    #[test]
    fn test_stable_log_merge_policy_merge_not_enabled() {
        let merge_policy = StableLogMergePolicy {
            merge_enabled: false,
            ..Default::default()
        };
        let mut splits = create_splits(vec![100; 10]);
        let merge_ops = merge_policy.operations(&mut splits);
        assert_eq!(splits.len(), 10);
        assert_eq!(merge_ops.len(), 0);
    }
}
