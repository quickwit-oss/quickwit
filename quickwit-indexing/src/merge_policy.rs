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

use std::cmp::Reverse;
use std::fmt;
use std::ops::Range;

use quickwit_metastore::SplitMetadata;
use tracing::debug;

#[derive(Debug, Eq, PartialEq)]
pub enum MergeOrDemux {
    Merge,
    Demux,
}
pub struct MergeOperation {
    pub splits: Vec<SplitMetadata>,
    pub op_type: MergeOrDemux,
}

impl fmt::Debug for MergeOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MergeOperation(t={:?},splits=[", &self.op_type)?;
        for split in &self.splits {
            write!(f, "{}", &split.split_id)?;
        }
        write!(f, "])")?;
        Ok(())
    }
}

/// A merge policy wraps the logic that decide what should be merged.
/// The SplitMetadata must be extracted from the splits `Vec`.
///
/// It is called by the merge planner whenever a new split is added.
pub trait MergePolicy: Send + Sync {
    /// Returns the list of operations that should be performed.
    /// This functions will be called on subset of `SplitMetadata`
    /// for which the number of demux is the same.
    fn operations(&self, splits: &mut Vec<SplitMetadata>) -> Vec<MergeOperation>;
    /// A mature split is a split that won't undergo merge or demux operation in the future.
    fn is_mature(&self, split: &SplitMetadata) -> bool;
}

/// StableMultitenantMergePolicy is a rather naive implementation optimized
/// for splits produced by a rather stable stream of splits,
/// with incoming documents ordered more or less as expected time, so that splits are
/// time pruning is efficient out of the box.
///
/// The logic goes as follows.
/// Each splits has
/// - a number of documents
/// - a number of demux operations
/// - an end time
///
/// We start by sorting the splits by reverse date... The most recent split are
/// coming first.
/// We iterate through the splits and assign the them to increasing levels.
/// Level 0 will receive `{split_i}` for i within `[0..l_0)`
/// ...
/// Level k will receive `{split_i}` for i within `[l_{k-1}..l_k)`
///
/// The limit at which we change level are simply defined as
/// `l_0 = 3 x self.min_level_num_docs`.
///
/// The assuming level N-1, has been built, level N is given by
/// `l_N = min(num_docs(split_l_{N_1})` * 3, self.max_merge_docs)`
/// We stop once l_N = self.max_merge_docs is reached.
///
/// As a result, each level interval is at least 3 times larger than the previous one,
/// forming a logscale over the number of documents.
///
/// Because we stop merging splits reaching a size larger than if it would result in a size larger
/// than `target_num_docs`.
#[derive(Clone)]
pub struct StableMultitenantWithTimestampMergePolicy {
    pub target_demux_ops: usize,
    /// We never merge segments larger than this size.
    pub max_merge_docs: usize,
    pub min_level_num_docs: usize,
    pub merge_factor: usize,
    pub merge_factor_max: usize,
}

impl Default for StableMultitenantWithTimestampMergePolicy {
    fn default() -> Self {
        StableMultitenantWithTimestampMergePolicy {
            target_demux_ops: 1,
            max_merge_docs: 10_000_000,
            min_level_num_docs: 100_000,
            merge_factor: 8,
            merge_factor_max: 12,
        }
    }
}

fn remove_matching_items<T, Pred: Fn(&T) -> bool>(items: &mut Vec<T>, predicate: Pred) -> Vec<T> {
    let mut matching_items = Vec::new();
    let mut i = 0;
    while i < items.len() {
        if predicate(&items[i]) {
            let matching_item = items.remove(i);
            matching_items.push(matching_item);
        } else {
            i += 1;
        }
    }
    matching_items
}

impl MergePolicy for StableMultitenantWithTimestampMergePolicy {
    fn operations(&self, splits: &mut Vec<SplitMetadata>) -> Vec<MergeOperation> {
        if splits.is_empty() {
            return Vec::new();
        }
        debug!(splits=?splits, "merge policy");
        let original_num_splits = splits.len();
        // First we isolate splits that are too large.
        // We will read them at the end.
        let splits_too_large =
            remove_matching_items(splits, |split| split.num_records >= self.max_merge_docs);

        let mut merge_operations: Vec<MergeOperation> = Vec::new();
        // We stable sort the splits, most recent first.
        splits.sort_by_key(|split| {
            let time_end = split
                .time_range
                .as_ref()
                .map(|time_range| Reverse(*time_range.end()));
            (time_end, split.num_records)
        });
        // Splits should naturally have an increasing num_merge
        let split_levels = self.build_split_levels(splits);
        for split_range in split_levels.into_iter().rev() {
            if split_range.len() < self.merge_factor {
                continue;
            }
            let num_splits_in_merge = split_range.len().min(self.merge_factor_max);
            let mut splits_in_merge: Vec<SplitMetadata> = Vec::new();
            for split_ord in split_range.rev().take(num_splits_in_merge) {
                let split = splits.swap_remove(split_ord);
                splits_in_merge.push(split);
            }
            let merge_operation = MergeOperation {
                splits: splits_in_merge,
                op_type: MergeOrDemux::Merge,
            };
            merge_operations.push(merge_operation);
        }
        splits.extend(splits_too_large);
        debug_assert_eq!(
            original_num_splits,
            merge_operations
                .iter()
                .map(|op| op.splits.len())
                .sum::<usize>()
                + splits.len(),
            "The merge policy is supposed to keep the number of splits."
        );
        merge_operations
    }

    fn is_mature(&self, split: &SplitMetadata) -> bool {
        // split.demux_signature == self.target_demux_ops &&
        // TODO: include demux signature in the is_mature logic.
        split.num_records > self.max_merge_docs
    }
}

impl StableMultitenantWithTimestampMergePolicy {
    /// This function groups splits in levels.
    ///
    /// It assumes that splits are almost sorted by their increasing size,
    /// but should behave decently (not create too many levels) if they are not.
    ///
    /// All splits are required to have a number of records lower than
    /// `self.max_merge_docs`
    pub(crate) fn build_split_levels(&self, splits: &[SplitMetadata]) -> Vec<Range<usize>> {
        assert!(
            splits
                .iter()
                .all(|split| split.num_records < self.max_merge_docs),
            "All splits are expected to be smaller than `max_merge_docs`."
        );
        if splits.is_empty() {
            return Vec::new();
        }

        let mut split_levels: Vec<Range<usize>> = Vec::new();
        let mut current_level_start_ord = 0;
        let mut current_level_max_docs = (splits[0].num_records * 3).max(self.min_level_num_docs);

        for (split_ord, split) in splits.iter().enumerate() {
            if split.num_records >= current_level_max_docs {
                split_levels.push(current_level_start_ord..split_ord);
                current_level_start_ord = split_ord;
                current_level_max_docs = 3 * split.num_records;
            }
        }
        split_levels.push(current_level_start_ord..splits.len());
        split_levels
    }

    fn case_levels_given_growth_factor(&self, growth_factor: usize) -> Vec<usize> {
        assert!(self.min_level_num_docs > 0);
        assert!(self.merge_factor > 1);
        assert!(self.merge_factor_max >= self.merge_factor);
        assert!(self.max_merge_docs > self.min_level_num_docs);
        let mut levels_start_num_docs = vec![1];
        let mut level_end_doc = self.min_level_num_docs;
        while level_end_doc < self.max_merge_docs {
            levels_start_num_docs.push(level_end_doc);
            level_end_doc *= growth_factor;
        }
        levels_start_num_docs.push(self.max_merge_docs);
        levels_start_num_docs
    }

    pub fn max_num_splits_ideal_case(&self, num_docs: u64) -> usize {
        let levels = self.case_levels_given_growth_factor(self.merge_factor);
        self.max_num_splits_knowning_levels(num_docs, &levels, true)
    }

    pub fn max_num_splits_worst_case(&self, num_docs: u64) -> usize {
        let levels = self.case_levels_given_growth_factor(3);
        self.max_num_splits_knowning_levels(num_docs, &levels, false)
    }

    fn max_num_splits_knowning_levels(
        &self,
        mut num_docs: u64,
        levels: &[usize],
        sorted: bool,
    ) -> usize {
        assert!(is_sorted(levels));
        if num_docs == 0 {
            return 0;
        }
        let (&head, tail) = levels.split_first().unwrap();
        if num_docs < head as u64 {
            return 0;
        }
        let first_level_min_saturation_docs = if sorted {
            head * (self.merge_factor - 1)
        } else {
            head + (self.merge_factor - 2)
        };
        if tail.is_empty() || num_docs <= first_level_min_saturation_docs as u64 {
            return (num_docs as usize + head - 1) / head;
        }
        num_docs -= first_level_min_saturation_docs as u64;
        self.merge_factor - 1 + self.max_num_splits_knowning_levels(num_docs, tail, sorted)
    }
}

fn is_sorted(els: &[usize]) -> bool {
    els.windows(2).all(|w| w[0] <= w[1])
}

#[derive(Debug, Eq, PartialEq)]
struct SplitGroup {
    num_merge_ops: usize,
    split_range: Range<usize>,
}

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;

    use super::*;

    #[test]
    fn test_build_split_levels() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        let splits = Vec::new();
        let split_groups = merge_policy.build_split_levels(&splits);
        assert!(split_groups.is_empty());
    }

    fn create_splits(num_docs_vec: Vec<usize>) -> Vec<SplitMetadata> {
        let num_records_with_timestamp = num_docs_vec
            .into_iter()
            // we give the same timestamp to all of them and rely on stable sort to keep the split
            // order.
            .map(|num_records| (num_records, (1630563067..=1630564067)))
            .collect();
        create_splits_with_timestamps(num_records_with_timestamp)
    }

    fn create_splits_with_timestamps(
        num_docs_vec: Vec<(usize, RangeInclusive<i64>)>,
    ) -> Vec<SplitMetadata> {
        num_docs_vec
            .into_iter()
            .enumerate()
            .map(|(split_ord, (num_records, time_range))| SplitMetadata {
                split_id: format!("split_{:02}", split_ord),
                num_records,
                time_range: Some(time_range),
                ..Default::default()
            })
            .collect()
    }

    #[test]
    fn test_stable_multitenant_merge_policy_build_split_simple() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        let splits = create_splits(vec![100_000, 100_000, 100_000, 800_000, 900_000]);
        let split_groups = merge_policy.build_split_levels(&splits);
        assert_eq!(&split_groups, &[0..3, 3..5]);
    }

    #[test]
    fn test_stable_multitenant_merge_policy_build_split_perfect_world() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        let splits = create_splits(vec![
            100_000, 100_000, 100_000, 100_000, 100_000, 100_000, 100_000, 100_000, 800_000,
            1_600_000,
        ]);
        let split_groups = merge_policy.build_split_levels(&splits);
        assert_eq!(&split_groups, &[0..8, 8..10]);
    }

    #[test]
    fn test_stable_multitenant_merge_policy_build_split_decreasing() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        let splits = create_splits(vec![
            100_000, 100_000, 100_000, 100_000, 100_000, 100_000, 100_000, 100_000, 800_000,
            100_000, 1_600_000,
        ]);
        let split_groups = merge_policy.build_split_levels(&splits);
        assert_eq!(&split_groups, &[0..8, 8..11]);
    }

    #[test]
    #[should_panic(expected = "All splits are expected to be smaller than `max_merge_docs`.")]
    fn test_stable_multitenant_merge_policy_build_split_panics_if_exceeding_max_merge_docs() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        let splits = create_splits(vec![11_000_000]);
        merge_policy.build_split_levels(&splits);
    }

    #[test]
    fn test_stable_multitenant_merge_policy_not_enough_splits() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        let mut splits = create_splits(vec![100; 7]);
        assert_eq!(splits.len(), 7);
        assert!(merge_policy.operations(&mut splits).is_empty());
    }

    #[test]
    fn test_stable_multitenant_merge_policy_just_enough_splits_for_a_merge() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        let mut splits = create_splits(vec![100; 10]);
        let mut merge_ops = merge_policy.operations(&mut splits);
        assert!(splits.is_empty());
        assert_eq!(merge_ops.len(), 1);
        let merge_op = merge_ops.pop().unwrap();
        let mut merge_segment_ids: Vec<String> = merge_op
            .splits
            .into_iter()
            .map(|split| split.split_id)
            .collect();
        merge_segment_ids.sort();
        assert_eq!(merge_op.op_type, MergeOrDemux::Merge);
        assert_eq!(
            merge_segment_ids,
            &[
                "split_00", "split_01", "split_02", "split_03", "split_04", "split_05", "split_06",
                "split_07", "split_08", "split_09"
            ]
        );
    }

    #[test]
    fn test_stable_multitenant_merge_policy_many_splits_on_same_level() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        let mut splits = create_splits(vec![100; 13]);
        let mut merge_ops = merge_policy.operations(&mut splits);
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].split_id, "split_00");
        assert_eq!(merge_ops.len(), 1);
        let merge_op = merge_ops.pop().unwrap();
        let mut merge_split_ids: Vec<String> = merge_op
            .splits
            .into_iter()
            .map(|split| split.split_id)
            .collect();
        merge_split_ids.sort();
        assert_eq!(
            merge_split_ids,
            &[
                "split_01", "split_02", "split_03", "split_04", "split_05", "split_06", "split_07",
                "split_08", "split_09", "split_10", "split_11", "split_12"
            ]
        );
        assert_eq!(merge_op.op_type, MergeOrDemux::Merge);
    }

    #[test]
    fn test_stable_multitenant_merge_policy_splits_below_min_level() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        let mut splits = create_splits(vec![
            100, 1000, 10_000, 10_000, 10_000, 10_000, 10_000, 40_000, 40_000, 40_000,
        ]);
        let mut merge_ops = merge_policy.operations(&mut splits);
        assert_eq!(splits.len(), 0);
        assert_eq!(merge_ops.len(), 1);
        let merge_op = merge_ops.pop().unwrap();
        let mut merge_split_ids: Vec<String> = merge_op
            .splits
            .into_iter()
            .map(|split| split.split_id)
            .collect();
        merge_split_ids.sort();
        assert_eq!(
            merge_split_ids,
            &[
                "split_00", "split_01", "split_02", "split_03", "split_04", "split_05", "split_06",
                "split_07", "split_08", "split_09"
            ]
        );
        assert_eq!(merge_op.op_type, MergeOrDemux::Merge);
    }

    #[test]
    fn test_stable_multitenant_merge_policy_splits_above_min_level() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        let mut splits = create_splits(vec![
            100_000, 1_000_000, 1_000_000, 1_000_000, 1_000_000, 1_000_000, 1_000_000, 1_000_000,
        ]);
        let merge_ops = merge_policy.operations(&mut splits);
        assert_eq!(splits.len(), 8);
        assert_eq!(merge_ops.len(), 0);
    }

    #[test]
    fn test_stable_multitenant_merge_policy_above_max_merge_docs_is_ignored() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        let mut splits = create_splits(vec![
            100_000, 100_000, 100_000, 100_000, 100_000,
            10_000_000, // this split should not interfere with the merging of other splits
            100_000, 100_000, 100_000, 100_000, 100_000,
        ]);
        let merge_ops = merge_policy.operations(&mut splits);
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].num_records, 10_000_000);
        assert_eq!(merge_ops.len(), 1);
    }

    #[test]
    fn test_stable_multitenant_merge_policy_max_num_splits_worst_case() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
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
    fn test_stable_multitenant_merge_policy_max_num_splits_ideal_case() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
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
}
