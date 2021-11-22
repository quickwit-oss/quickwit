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

use itertools::Itertools;
use quickwit_index_config::match_tag_field_name;
use quickwit_metastore::SplitMetadata;
use tracing::debug;

use crate::new_split_id;

pub enum MergeOperation {
    Merge {
        merge_split_id: String,
        splits: Vec<SplitMetadata>,
    },
    Demux {
        demux_split_ids: Vec<String>,
        splits: Vec<SplitMetadata>,
    },
}

impl MergeOperation {
    pub fn new_merge_operation(splits: Vec<SplitMetadata>) -> MergeOperation {
        MergeOperation::Merge {
            merge_split_id: new_split_id(),
            splits,
        }
    }

    pub fn splits(&self) -> &[SplitMetadata] {
        match self {
            MergeOperation::Merge { splits, .. } | MergeOperation::Demux { splits, .. } => {
                splits.as_slice()
            }
        }
    }
}

impl fmt::Debug for MergeOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MergeOperation::Merge {
                merge_split_id: split_id,
                splits,
            } => {
                write!(f, "Merge(merged_split_id={},splits=[", split_id)?;
                for split in splits {
                    write!(f, "{},", &split.split_id)?;
                }
                write!(f, "])")?;
            }
            MergeOperation::Demux {
                demux_split_ids,
                splits,
            } => {
                write!(f, "Merge(demux_splits=[")?;
                for split_id in demux_split_ids {
                    write!(f, "{},", split_id)?;
                }
                write!(f, "], input_splits=[")?;
                for split in splits {
                    write!(f, "{},", &split.split_id)?;
                }
                write!(f, "])")?;
            }
        }
        Ok(())
    }
}

/// A merge policy wraps the logic that decide what should be merged or demux.
/// The SplitMetadata must be extracted from the splits `Vec`.
///
/// It is called by the merge planner whenever a new split is added.
pub trait MergePolicy: Send + Sync + fmt::Debug {
    /// Returns the list of operations that should be performed either
    /// merge or demux operations.
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
/// The policy first build the merge operations and then the demux operations if
/// a `demux_field_name` is present.
///
/// 1. Build merge operations
/// We start by sorting the splits by reverse date so that the most recent splits are
/// coming first.
/// We iterate through the splits and assign them to increasing levels.
/// Level 0 will receive `{split_i}` for i within `[0..l_0)`
/// ...
/// Level k will receive `{split_i}` for i within `[l_{k-1}..l_k)`
///
/// The limit at which we change level is simply defined as
/// `l_0 = 3 x self.min_level_num_docs`.
///
/// Assuming level N-1 has been built, level N is given by
/// `l_N = min(num_docs(split_l_{N_1})` * 3, self.max_merge_docs)`.
/// We stop once l_N = self.max_merge_docs is reached.
///
/// As a result, each level interval is at least 3 times larger than the previous one,
/// forming a logscale over the number of documents.
///
/// Because we stop merging splits reaching a size larger than if it would result in a size larger
/// than `target_num_docs`.
///
/// 2. Build demux operations if `demux_field_name` is present
/// We start by sorting the splits by date so that the oldest splits are first demuxed.
/// This will avoid leaving an old split alone that will be demuxed with younger splits
/// in the future.
///
/// The logic is simple: as long as we have more than `max_merge_docs * demux_factor` docs in
/// the splits candidates, we take splits until having `num docs >= max_merge_docs * demux_factor`,
/// build a demux operation with it, and loop.
#[derive(Clone, Debug)]
pub struct StableMultitenantWithTimestampMergePolicy {
    /// We never merge segments larger than this size.
    pub max_merge_docs: usize,
    pub min_level_num_docs: usize,
    pub merge_factor: usize,
    pub merge_factor_max: usize,
    pub demux_factor: usize,
    pub demux_field_name: Option<String>,
    pub merge_enabled: bool,
    pub demux_enabled: bool,
}

impl Default for StableMultitenantWithTimestampMergePolicy {
    fn default() -> Self {
        StableMultitenantWithTimestampMergePolicy {
            max_merge_docs: 10_000_000,
            min_level_num_docs: 100_000,
            merge_factor: 10,
            merge_factor_max: 12,
            demux_factor: 6,
            demux_field_name: None,
            merge_enabled: true,
            demux_enabled: false,
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

struct SplitShortDebug<'a>(&'a SplitMetadata);

impl<'a> fmt::Debug for SplitShortDebug<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Split")
            .field("split_id", &self.0.split_id)
            .field("num_docs", &self.0.num_docs)
            .finish()
    }
}

fn splits_short_debug(splits: &[SplitMetadata]) -> Vec<SplitShortDebug> {
    splits.iter().map(SplitShortDebug).collect()
}

impl MergePolicy for StableMultitenantWithTimestampMergePolicy {
    fn operations(&self, splits: &mut Vec<SplitMetadata>) -> Vec<MergeOperation> {
        let original_num_splits = splits.len();
        let mut operations = self.merge_operations(splits);
        operations.append(&mut self.demux_operations(splits));
        debug_assert_eq!(
            original_num_splits,
            operations.iter().map(|op| op.splits().len()).sum::<usize>() + splits.len(),
            "The merge policy is supposed to keep the number of splits."
        );
        operations
    }

    fn is_mature(&self, split: &SplitMetadata) -> bool {
        self.is_mature_for_merge(split) && self.is_mature_for_demux(split)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum MergeCandidateSize {
    /// The split candidate is too small to be considered for execution.
    TooSmall,
    /// The split candidate is good to go.
    ValidSplit,
    /// We should not add an extra split in this candidate.
    /// This can happen for any of the two following reasons:
    /// - the number of splits involved already reached `merge_factor_max`.
    /// - the overall number of docs that will end up in the merged segment already
    /// exceeds `max_merge_docs`.
    OneMoreSplitWouldBeTooBig,
}

impl StableMultitenantWithTimestampMergePolicy {
    /// A mature split for merge is a split that won't undergo merge operation in the future.
    fn is_mature_for_merge(&self, split: &SplitMetadata) -> bool {
        // Once a split has been demuxed, we don't want to merge it even in the
        // case where its number of docs is under `max_merge_docs`.
        split.num_docs >= self.max_merge_docs || split.demux_num_ops > 0
    }

    /// A mature split for demux is a split that won't undergo demux operation in the future.
    /// Maturity is reached when the split met one of this condition:
    /// - has already been demuxed (split.demux_generation > 0).
    /// - has less than one demux value in the tags set. Demux is useful only if we can demux at
    ///   least 2 values...
    /// - has less than `max_merge_docs` as we don't want to demux splits too small
    /// - has to many `num docs >= self.max_merge_docs * self.max_merge_docs`. A split normally has
    ///   size less than `2 * max_merge_docs`. As `max_merge_docs` can change through time, we must
    ///   protect against too big splits as it will break the building of demux operations, see
    ///   [`build_first_demux_operation`].
    fn is_mature_for_demux(&self, split: &SplitMetadata) -> bool {
        let demux_field_name = if let Some(demux_field_name) = self.demux_field_name.as_ref() {
            demux_field_name
        } else {
            // All splits are considered mature if there is no demux field as no
            // split will be demuxed.
            return true;
        };
        if split.num_docs >= self.demux_factor * self.max_merge_docs {
            return true;
        }
        let split_tags_contains_less_than_2_demux_values = split
            .tags
            .iter()
            .filter(|tag| match_tag_field_name(demux_field_name, tag))
            .count()
            < 2;
        split_tags_contains_less_than_2_demux_values
            || (split.num_docs < self.max_merge_docs || split.demux_num_ops > 0)
    }

    fn merge_operations(&self, splits: &mut Vec<SplitMetadata>) -> Vec<MergeOperation> {
        if !self.merge_enabled || splits.is_empty() {
            return Vec::new();
        }
        // First we isolate splits that are mature.
        let splits_not_for_merge =
            remove_matching_items(splits, |split| self.is_mature_for_merge(split));

        let mut merge_operations: Vec<MergeOperation> = Vec::new();
        // We stable sort the splits, most recent first.
        splits.sort_by_key(|split| {
            let time_end = split
                .time_range
                .as_ref()
                .map(|time_range| Reverse(*time_range.end()));
            (time_end, split.num_docs)
        });
        debug!(splits=?splits_short_debug(&splits[..]), "merge-policy-run");

        // Splits should naturally have an increasing num_merge
        let split_levels = self.build_split_levels(splits);
        for split_range in split_levels.into_iter().rev() {
            debug!(splits=?splits_short_debug(&splits[split_range.clone()]));
            if let Some(merge_range) = self.merge_candidate_from_level(splits, split_range) {
                debug!(merge_range=?merge_range, "merge-candidate");
                let splits_in_merge: Vec<SplitMetadata> = splits.drain(merge_range).collect();
                let merge_operation = MergeOperation::new_merge_operation(splits_in_merge);
                merge_operations.push(merge_operation);
            } else {
                debug!("no-merge");
            }
        }
        splits.extend(splits_not_for_merge);
        merge_operations
    }

    /// This function builds demux operations for splits that have >= `max_merge_docs` and
    /// < `max_merge_docs * demux_factor` and have been demux less than `max-demux_generation`
    /// times.
    /// We might authorize several demuxing in the future.
    fn demux_operations(&self, splits: &mut Vec<SplitMetadata>) -> Vec<MergeOperation> {
        if !self.demux_enabled || self.demux_field_name.is_none() || splits.is_empty() {
            return Vec::new();
        }
        // First we isolate splits which are mature.
        // We will append them at the end.
        let excluded_splits =
            remove_matching_items(splits, |split| self.is_mature_for_demux(split));

        let mut merge_operations: Vec<MergeOperation> = Vec::new();
        // Stable sort the splits, old first. We want old splits to be demuxed first
        // and leave recent splits to be demuxed later, demux might benefit from
        // this time consistency.
        splits.sort_by_key(|split| {
            split
                .time_range
                .as_ref()
                .map(|time_range| *time_range.end())
        });
        merge_operations.append(&mut self.build_first_demux_operation(splits));
        splits.extend(excluded_splits);
        merge_operations
    }

    /// This function builds demux operations for splits that have >= `max_merge_docs` and
    /// < `max_merge_docs * demux_factor` and have been demux less than `max-demux_generation`
    /// times.
    /// The logic is simple: as long as we have more than `max_merge_docs * demux_factor` docs in
    /// the given splits, we take splits until having `num docs >= max_merge_docs * demux_factor`,
    /// build a demux operation with it, and loop.
    pub(crate) fn build_first_demux_operation(
        &self,
        splits: &mut Vec<SplitMetadata>,
    ) -> Vec<MergeOperation> {
        assert!(self.demux_factor > 1, "Demux factor must be > 1");
        assert!(
            splits.iter().all(|split| split.demux_num_ops == 0),
            "All splits are expected to have never been demuxed."
        );
        assert!(
            splits
                .iter()
                .all(|split| split.num_docs < self.max_merge_docs * self.demux_factor),
            "Each split size must satisfy `max_merge_docs <= size < demux_factor * max_merge_docs`"
        );
        let mut total_num_docs_left: usize = splits.iter().map(|split| split.num_docs).sum();
        if splits.is_empty() || total_num_docs_left < self.demux_factor * self.max_merge_docs {
            return Vec::new();
        }
        let mut operations = Vec::new();
        while !splits.is_empty() && total_num_docs_left >= self.demux_factor * self.max_merge_docs {
            let mut end_split_idx = 0;
            let mut num_docs_to_demux = 0;
            for (split_idx, split) in splits.iter().enumerate().take(self.demux_factor) {
                num_docs_to_demux += split.num_docs;
                if num_docs_to_demux >= self.demux_factor * self.max_merge_docs {
                    end_split_idx = split_idx;
                    break;
                }
            }
            assert!(
                end_split_idx > 0,
                "Impossible state with splits.len() > 0 and total docs > demux factor * max merge \
                 docs. This should never happened."
            );
            let splits_for_demux: Vec<SplitMetadata> = splits.drain(0..end_split_idx + 1).collect();
            total_num_docs_left -= num_docs_to_demux;
            let merge_operation = MergeOperation::Demux {
                demux_split_ids: (0..self.demux_factor).map(|_| new_split_id()).collect_vec(),
                splits: splits_for_demux,
            };
            operations.push(merge_operation);
        }
        operations
    }

    /// This function groups splits in levels.
    ///
    /// It assumes that splits are almost sorted by their increasing size,
    /// but should behave decently (not create too many levels) if they are not.
    ///
    /// All splits are required to have a number of documents lower than
    /// `self.max_merge_docs`
    pub(crate) fn build_split_levels(&self, splits: &[SplitMetadata]) -> Vec<Range<usize>> {
        assert!(
            splits
                .iter()
                .all(|split| split.num_docs < self.max_merge_docs),
            "All splits are expected to be smaller than `max_merge_docs`."
        );
        if splits.is_empty() {
            return Vec::new();
        }

        let mut split_levels: Vec<Range<usize>> = Vec::new();
        let mut current_level_start_ord = 0;
        let mut current_level_max_docs = (splits[0].num_docs * 3).max(self.min_level_num_docs);

        let mut levels = vec![(0..current_level_max_docs)]; // for logging only
        for (split_ord, split) in splits.iter().enumerate() {
            if split.num_docs >= current_level_max_docs {
                split_levels.push(current_level_start_ord..split_ord);
                current_level_start_ord = split_ord;
                current_level_max_docs = 3 * split.num_docs;
                levels.push(split.num_docs..current_level_max_docs)
            }
        }
        debug!(levels=?levels);
        split_levels.push(current_level_start_ord..splits.len());
        split_levels
    }

    /// Given splits tries to select a subrange of level_range that would be a good merge candidate.
    fn merge_candidate_from_level(
        &self,
        splits: &[SplitMetadata],
        level_range: Range<usize>,
    ) -> Option<Range<usize>> {
        let merge_candidate_end = level_range.end;
        let mut merge_candidate_start = merge_candidate_end;
        for split_ord in level_range.rev() {
            if self.merge_candidate_size(&splits[merge_candidate_start..merge_candidate_end])
                == MergeCandidateSize::OneMoreSplitWouldBeTooBig
            {
                break;
            }
            merge_candidate_start = split_ord;
        }
        if self.merge_candidate_size(&splits[merge_candidate_start..merge_candidate_end])
            == MergeCandidateSize::TooSmall
        {
            return None;
        }
        Some(merge_candidate_start..merge_candidate_end)
    }

    /// Returns `MergeCandidateSize` iff we should stop adding extra split into this
    /// merge candidate.
    fn merge_candidate_size(&self, splits: &[SplitMetadata]) -> MergeCandidateSize {
        // We don't perform merge with a single segment. We
        // may relax this in the future in order to compact deletes.
        if splits.len() <= 1 {
            return MergeCandidateSize::TooSmall;
        }

        // There are already enough splits in this merge.
        if splits.len() >= self.merge_factor_max {
            return MergeCandidateSize::OneMoreSplitWouldBeTooBig;
        }
        let num_docs_in_merge: usize = splits.iter().map(|split| split.num_docs).sum();

        // The resulting split will exceed `max_merge_docs`.
        if num_docs_in_merge >= self.max_merge_docs {
            return MergeCandidateSize::OneMoreSplitWouldBeTooBig;
        }

        if splits.len() < self.merge_factor {
            return MergeCandidateSize::TooSmall;
        }

        MergeCandidateSize::ValidSplit
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::iter::FromIterator;
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

    fn create_splits_with_tags(
        num_docs_vec: Vec<usize>,
        demux_field_name: &str,
        tag_counts: &[usize],
    ) -> Vec<SplitMetadata> {
        num_docs_vec
            .into_iter()
            .zip(tag_counts.iter())
            .enumerate()
            .map(|(split_ord, (num_docs, &tag_count))| SplitMetadata {
                split_id: format!("split_{:02}", split_ord),
                num_docs,
                tags: (0..tag_count)
                    .into_iter()
                    .map(|i| format!("{}:{}", demux_field_name, i))
                    .collect::<BTreeSet<_>>(),
                ..Default::default()
            })
            .collect()
    }

    #[test]
    fn test_split_is_mature_with_no_demux_field() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        // Split under max_merge_docs and demux_generation = 0 is not mature.
        let mut split = create_splits(vec![9_000_000]).into_iter().next().unwrap();
        assert!(!merge_policy.is_mature(&split));
        // Split under max_merge_docs and demux_generation = 1 is mature.
        split.demux_num_ops = 1;
        assert!(merge_policy.is_mature(&split));
        // Split with docs > max_merge_docs and demux_generation = 0 is mature.
        split.num_docs = merge_policy.max_merge_docs + 1;
        split.demux_num_ops = 0;
        assert!(merge_policy.is_mature(&split));
        // Split with docs > max_merge_docs and demux_generation = 1 is mature.
        split.num_docs = merge_policy.max_merge_docs + 1;
        split.demux_num_ops = 1;
        assert!(merge_policy.is_mature(&split));
    }

    #[test]
    fn test_split_is_mature_with_demux_field() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy {
            demux_field_name: Some("demux_field".to_owned()),
            ..Default::default()
        };
        let mut split = create_splits(vec![9_000_000]).into_iter().next().unwrap();
        // Add a number of tags > 1 so that it can be a candidate for demux.
        let demux_tags = BTreeSet::from_iter(vec![
            "demux_field:1".to_string(),
            "demux_field:2".to_string(),
        ]);
        split.tags = demux_tags;
        // Good candidates for merge or demux.
        // Split under max_merge_docs and demux_generation = 0 is not mature.
        assert!(!merge_policy.is_mature(&split));
        // Split with docs >= max_merge_docs and demux_generation = 0 is not mature.
        split.num_docs = merge_policy.max_merge_docs + 1;
        split.demux_num_ops = 0;
        assert!(!merge_policy.is_mature(&split));
        // Split with docs > max_merge_docs, demux_generation of 0 and wrong tags is also mature.
        split.num_docs = 100;
        split.demux_num_ops = 1;
        assert!(merge_policy.is_mature(&split));

        // Mature splits.
        // Split under max_merge_docs and demux_generation = 1 is mature.
        split.num_docs = 100;
        split.demux_num_ops = 1;
        assert!(merge_policy.is_mature(&split));
        // Split with docs > max_merge_docs and demux_generation = 1 is mature.
        split.num_docs = merge_policy.max_merge_docs + 1;
        split.demux_num_ops = 1;
        assert!(merge_policy.is_mature(&split));
        // Split with num docs >= max_merge_docs * demux_factor is mature.
        split.num_docs = merge_policy.demux_factor * merge_policy.max_merge_docs;
        split.demux_num_ops = 0;
        assert!(merge_policy.is_mature(&split));
        // Split with docs > max_merge_docs, demux_generation of 0 and wrong tags is also mature.
        let other_tags = BTreeSet::from_iter(vec![
            "other_field:1".to_string(),
            "other_field:2".to_string(),
        ]);
        split.tags = other_tags;
        split.num_docs = merge_policy.max_merge_docs + 1;
        split.demux_num_ops = 0;
        assert!(merge_policy.is_mature(&split));
    }

    #[test]
    fn test_build_split_levels() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        let splits = Vec::new();
        let split_groups = merge_policy.build_split_levels(&splits);
        assert!(split_groups.is_empty());
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
            .splits()
            .iter()
            .map(|split| split.split_id.clone())
            .collect();
        merge_segment_ids.sort();
        assert!(matches!(merge_op, MergeOperation::Merge { .. }));
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
            .splits()
            .iter()
            .map(|split| split.split_id.clone())
            .collect();
        merge_split_ids.sort();
        assert_eq!(
            merge_split_ids,
            &[
                "split_01", "split_02", "split_03", "split_04", "split_05", "split_06", "split_07",
                "split_08", "split_09", "split_10", "split_11", "split_12"
            ]
        );
        assert!(matches!(merge_op, MergeOperation::Merge { .. }));
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
            .splits()
            .iter()
            .map(|split| split.split_id.clone())
            .collect();
        merge_split_ids.sort();
        assert_eq!(
            merge_split_ids,
            &[
                "split_00", "split_01", "split_02", "split_03", "split_04", "split_05", "split_06",
                "split_07", "split_08", "split_09"
            ]
        );
        assert!(matches!(merge_op, MergeOperation::Merge { .. }));
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
        assert_eq!(splits[0].num_docs, 10_000_000);
        assert_eq!(merge_ops.len(), 1);
    }

    #[test]
    fn test_merge_policy_splits_too_large_are_ignored() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        let mut splits = create_splits(vec![9_999_999, 10_000_000]);
        let merge_ops = merge_policy.operations(&mut splits);
        assert_eq!(splits.len(), 2);
        assert_eq!(splits[0].num_docs, 9_999_999);
        assert_eq!(splits[1].num_docs, 10_000_000);
        assert!(merge_ops.is_empty());
    }

    #[test]
    fn test_merge_policy_splits_entire_level_reach_merge_max_doc() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        let mut splits = create_splits(vec![5_000_000, 5_000_000]);
        let merge_ops = merge_policy.operations(&mut splits);
        assert!(splits.is_empty());
        assert_eq!(merge_ops.len(), 1);
        assert!(matches!(merge_ops[0], MergeOperation::Merge { .. }));
        assert_eq!(merge_ops[0].splits().len(), 2);
    }

    #[test]
    fn test_merge_policy_last_merge_can_have_a_lower_merge_factor() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        let mut splits = create_splits(vec![9_999_997, 9_999_998, 9_999_999]);
        let merge_ops = merge_policy.operations(&mut splits);
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].num_docs, 9_999_997);
        assert_eq!(merge_ops.len(), 1);
        assert!(matches!(merge_ops[0], MergeOperation::Merge { .. }));
        assert_eq!(merge_ops[0].splits().len(), 2);
    }

    #[test]
    fn test_merge_policy_no_merge_with_only_one_split() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy::default();
        let mut splits = create_splits(vec![9_999_999]);
        let merge_ops = merge_policy.operations(&mut splits);
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].num_docs, 9_999_999);
        assert!(merge_ops.is_empty());
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

    #[test]
    fn test_demux_one_operation_and_filter_out_irrelevant_splits() {
        let demux_field_name = "demux_field_name";
        let merge_policy = StableMultitenantWithTimestampMergePolicy {
            max_merge_docs: 10_000_000,
            min_level_num_docs: 100_000,
            merge_factor: 10,
            merge_factor_max: 12,
            demux_factor: 6,
            demux_field_name: Some(demux_field_name.to_string()),
            merge_enabled: true,
            demux_enabled: true,
        };
        let mut demux_candidates = create_splits_with_tags(
            vec![
                10_000_000, 10_000_000, 12_000_000, 14_000_000, 10_000_000, 10_000_001, 10_000_002,
                10_000_004, 10_000_005, 60_000_000,
            ],
            demux_field_name,
            &[0, 1, 2, 3, 3, 4, 5, 6, 10],
        );
        let mut other_splits =
            create_splits_with_tags(vec![10_000_000], "other_demux_field_name", &[5]);
        demux_candidates.append(&mut other_splits);
        let merge_ops = merge_policy.demux_operations(&mut demux_candidates);
        assert_eq!(demux_candidates.len(), 4);
        assert_eq!(merge_ops.len(), 1);
        assert!(matches!(merge_ops[0], MergeOperation::Demux { .. }));
        assert_eq!(merge_ops[0].splits().len(), 6);
    }

    #[test]
    fn test_demux_one_operation_with_1_normal_splits_and_1_huge_splits() {
        let demux_field_name = "demux_field_name";
        let merge_policy = StableMultitenantWithTimestampMergePolicy {
            demux_field_name: Some(demux_field_name.to_string()),
            demux_enabled: true,
            ..Default::default()
        };
        let mut demux_candidates = create_splits_with_tags(
            vec![50_000_000, 10_000_000, 12_000_000],
            demux_field_name,
            &[2, 2, 2],
        );
        let merge_ops = merge_policy.demux_operations(&mut demux_candidates);
        assert_eq!(demux_candidates.len(), 1);
        assert_eq!(demux_candidates[0].split_id, "split_02");
        assert_eq!(merge_ops.len(), 1);
        assert!(matches!(merge_ops[0], MergeOperation::Demux { .. }));
        assert_eq!(merge_ops[0].splits().len(), 2);
    }

    #[test]
    fn test_should_ignore_demux_operation_with_1_huge_split() {
        let demux_field_name = "demux_field_name";
        let merge_policy = StableMultitenantWithTimestampMergePolicy {
            demux_field_name: Some(demux_field_name.to_string()),
            ..Default::default()
        };
        let mut demux_candidates =
            create_splits_with_tags(vec![60_000_000], demux_field_name, &[2]);
        let merge_ops = merge_policy.demux_operations(&mut demux_candidates);
        assert_eq!(demux_candidates.len(), 1);
        assert_eq!(merge_ops.len(), 0);
    }

    #[test]
    fn test_demux_two_operations() {
        let demux_field_name = "demux_field_name";
        let merge_policy = StableMultitenantWithTimestampMergePolicy {
            demux_field_name: Some(demux_field_name.to_string()),
            merge_enabled: true,
            demux_enabled: true,
            ..Default::default()
        };
        let mut splits = create_splits_with_tags(
            vec![
                19_999_999, 19_999_999, 10_000_000, 10_000_001, 10_000_002, 10_000_004, 10_000_005,
                19_999_999, 19_999_999, 10_000_000, 10_000_001, 10_000_002, 10_000_004, 10_000_005,
            ],
            demux_field_name,
            &[5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5],
        );
        let merge_ops = merge_policy.demux_operations(&mut splits);
        assert_eq!(splits.len(), 5);
        assert_eq!(merge_ops.len(), 2);
        assert!(matches!(merge_ops[0], MergeOperation::Demux { .. }));
        assert!(matches!(merge_ops[1], MergeOperation::Demux { .. }));
        assert_eq!(merge_ops[0].splits().len(), 5);
        assert_eq!(merge_ops[1].splits().len(), 4);
    }

    #[test]
    fn test_stable_multitenant_merge_policy_merge_not_enabled() {
        let merge_policy = StableMultitenantWithTimestampMergePolicy {
            merge_enabled: false,
            ..Default::default()
        };
        let mut splits = create_splits(vec![100; 10]);
        let merge_ops = merge_policy.operations(&mut splits);
        assert_eq!(splits.len(), 10);
        assert_eq!(merge_ops.len(), 0);
    }

    #[test]
    fn test_stable_multitenant_merge_policy_demux_not_enabled() {
        let demux_field_name = "demux_field_name";
        let merge_policy = StableMultitenantWithTimestampMergePolicy {
            demux_field_name: Some(demux_field_name.to_string()),
            demux_enabled: false,
            ..Default::default()
        };
        let mut splits = create_splits_with_tags(vec![10_000_000; 10], demux_field_name, &[10; 10]);
        let merge_ops = merge_policy.demux_operations(&mut splits);
        assert_eq!(merge_ops.len(), 0);
    }
}
