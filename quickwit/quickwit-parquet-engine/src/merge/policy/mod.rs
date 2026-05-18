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

//! Parquet merge policy for compaction.
//!
//! Decides which Parquet splits within a compaction scope should be merged.
//! This is a pure computational module with no I/O or actor dependencies.
//!
//! The trait mirrors the shape of `quickwit_indexing::MergePolicy` but operates
//! on [`ParquetSplitMetadata`] instead of Tantivy's `SplitMetadata`. When
//! Nadav's `quickwit-compaction` crate adds Parquet support, the
//! `CompactionPlanner` can call into this trait directly.

pub mod const_write_amplification;
pub mod scope;

use std::fmt;
use std::time::Duration;

pub use const_write_amplification::{
    ConstWriteAmplificationParquetMergePolicy, ParquetMergePolicyConfig,
};
pub use scope::{CompactionScope, group_by_compaction_scope};

use crate::split::{ParquetSplitId, ParquetSplitKind, ParquetSplitMetadata};

/// A merge operation to be executed: merge the input splits into one output.
#[derive(Debug, Clone)]
pub struct ParquetMergeOperation {
    /// New split ID for the merged output.
    pub merge_split_id: ParquetSplitId,
    /// The input splits being merged. All share the same `num_merge_ops`
    /// level and the same windowed compaction scope (sort_fields +
    /// window). In the default form they also share `rg_partition_prefix_len`;
    /// in promotion mode (`target_prefix_len_override` is `Some`) they
    /// may differ in prefix length up to the target — see
    /// [`Self::promote_legacy`].
    pub splits: Vec<ParquetSplitMetadata>,
    /// When `Some(N)`, this operation is a legacy-promotion merge:
    /// inputs may carry `rg_partition_prefix_len < N` (they will be
    /// opened through [`crate::storage::LegacyInputAdapter`] with
    /// `target_prefix_len = N`) and the output will declare
    /// `rg_partition_prefix_len = N`. When `None`, the operation is a
    /// regular merge and all inputs must agree on prefix_len (default
    /// behaviour).
    pub target_prefix_len_override: Option<u32>,
}

impl ParquetMergeOperation {
    /// Create a regular merge operation consuming the given splits.
    ///
    /// Generates a fresh split ID for the merged output. The `kind` is inferred
    /// from the first split (all splits in a merge share the same kind).
    ///
    /// # Invariant checks (debug builds panic, all builds emit metrics)
    ///
    /// - **MP-1**: all splits share the same `num_merge_ops` level
    /// - **MP-2**: at least 2 input splits
    /// - **MP-3**: all splits share the same compaction scope (sort_fields + window)
    ///
    /// For legacy-promotion operations (inputs at different
    /// `rg_partition_prefix_len`), use [`Self::promote_legacy`] instead.
    pub fn new(splits: Vec<ParquetSplitMetadata>) -> Self {
        Self::check_mp1_mp2_mp3(&splits);
        let kind = splits
            .first()
            .map(|s| s.kind)
            .unwrap_or(ParquetSplitKind::Metrics);
        Self {
            merge_split_id: ParquetSplitId::generate(kind),
            splits,
            target_prefix_len_override: None,
        }
    }

    /// Create a legacy-promotion merge operation.
    ///
    /// Inputs may have heterogeneous `rg_partition_prefix_len` as long
    /// as every input's value is `<= target_prefix_len`. The executor
    /// opens any input with `prefix_len < target_prefix_len` through
    /// [`crate::storage::LegacyInputAdapter`] with `target` set to the
    /// override; inputs already at the target are opened directly via
    /// the streaming reader.
    ///
    /// All other MP-3 dimensions (sort_fields, window) still must
    /// agree — only the prefix-len equality is relaxed.
    ///
    /// # Panics (debug builds) / metrics (all builds)
    ///
    /// - **MP-1**: all splits share the same `num_merge_ops` level
    /// - **MP-2**: at least 2 input splits
    /// - **MP-3 (relaxed)**: all splits share sort_fields + window
    /// - All inputs' `rg_partition_prefix_len <= target_prefix_len`. Inputs above the target are a
    ///   planner bug — they shouldn't be demoted, only promoted.
    pub fn promote_legacy(splits: Vec<ParquetSplitMetadata>, target_prefix_len: u32) -> Self {
        Self::check_mp1_mp2_mp3(&splits);
        // Every input must be promotable: prefix_len <= target.
        // Demoting (input > target) is not the adapter's contract.
        for (i, split) in splits.iter().enumerate() {
            assert!(
                split.rg_partition_prefix_len <= target_prefix_len,
                "promote_legacy: input {i} has rg_partition_prefix_len = {} > target_prefix_len = \
                 {target_prefix_len}; the adapter cannot demote a higher prefix to a lower one. \
                 Pick a target >= max(inputs' prefix_len) or exclude this input.",
                split.rg_partition_prefix_len,
            );
        }
        let kind = splits
            .first()
            .map(|s| s.kind)
            .unwrap_or(ParquetSplitKind::Metrics);
        Self {
            merge_split_id: ParquetSplitId::generate(kind),
            splits,
            target_prefix_len_override: Some(target_prefix_len),
        }
    }

    fn check_mp1_mp2_mp3(splits: &[ParquetSplitMetadata]) {
        use quickwit_dst::check_invariant;
        use quickwit_dst::invariants::{InvariantId, merge_policy};

        // MP-2: minimum split count.
        check_invariant!(
            InvariantId::MP2,
            merge_policy::has_minimum_splits(splits.len()),
            ": got {} splits",
            splits.len()
        );

        // MP-1: level homogeneity.
        let levels: Vec<u32> = splits.iter().map(|s| s.num_merge_ops).collect();
        check_invariant!(
            InvariantId::MP1,
            merge_policy::all_same_merge_level(&levels),
            ": levels={:?}",
            levels
        );

        // MP-3: scope homogeneity (sort_fields + window).
        let sort_fields_vec: Vec<&str> = splits.iter().map(|s| s.sort_fields.as_str()).collect();
        let windows: Vec<(i64, i64)> = splits
            .iter()
            .map(|s| match &s.window {
                Some(w) => (w.start, w.end - w.start),
                None => (0, 0),
            })
            .collect();
        check_invariant!(
            InvariantId::MP3,
            merge_policy::all_same_compaction_scope(&sort_fields_vec, &windows)
        );
    }

    /// Returns the input splits as a slice.
    pub fn splits_as_slice(&self) -> &[ParquetSplitMetadata] {
        &self.splits
    }

    /// Total size in bytes across all input splits.
    pub fn total_size_bytes(&self) -> u64 {
        self.splits.iter().map(|s| s.size_bytes).sum()
    }

    /// Total number of rows across all input splits.
    pub fn total_num_rows(&self) -> u64 {
        self.splits.iter().map(|s| s.num_rows).sum()
    }
}

/// Whether a split is eligible for further merging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParquetSplitMaturity {
    /// Split will not undergo further merges.
    Mature,
    /// Split can still be merged. After `maturation_period` elapses from
    /// creation, the split becomes mature regardless of size.
    Immature { maturation_period: Duration },
}

/// Decides which Parquet splits to merge within a single compaction scope.
///
/// # Contract
///
/// - The caller passes all immature splits for one compaction scope in `splits`.
/// - `operations()` drains splits that participate in merges from the vec.
/// - Remaining splits (not in any operation) stay in the vec.
/// - Invariant: `splits_before = splits_in_ops + splits_remaining`.
///
/// This contract mirrors `quickwit_indexing::MergePolicy` for consistency.
pub trait ParquetMergePolicy: Send + Sync + fmt::Debug {
    /// Returns merge operations for the given splits.
    fn operations(&self, splits: &mut Vec<ParquetSplitMetadata>) -> Vec<ParquetMergeOperation>;

    /// Like `operations` but with a lower merge factor for finalization of
    /// cold windows. Called by the planner when a window has received no new
    /// splits for a configured duration.
    fn finalize_operations(
        &self,
        _splits: &mut Vec<ParquetSplitMetadata>,
    ) -> Vec<ParquetMergeOperation> {
        Vec::new()
    }

    /// Determines whether a split is mature (no further merges needed).
    fn split_maturity(&self, size_bytes: u64, num_merge_ops: u32) -> ParquetSplitMaturity;

    /// Validate invariants of a merge operation (for property testing).
    #[cfg(test)]
    fn check_is_valid(
        &self,
        _merge_op: &ParquetMergeOperation,
        _remaining_splits: &[ParquetSplitMetadata],
    ) {
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::SystemTime;

    use super::*;
    use crate::split::{ParquetSplitId, ParquetSplitKind, TimeRange};

    fn make_split(
        split_id: &str,
        num_merge_ops: u32,
        sort_fields: &str,
        window: Option<(i64, i64)>,
    ) -> ParquetSplitMetadata {
        ParquetSplitMetadata {
            kind: ParquetSplitKind::Metrics,
            split_id: ParquetSplitId::new(split_id),
            index_uid: "test:001".to_string(),
            partition_id: 0,
            time_range: TimeRange::new(1000, 2000),
            num_rows: 100,
            size_bytes: 1_000_000,
            metric_names: HashSet::new(),
            low_cardinality_tags: Default::default(),
            high_cardinality_tag_keys: Default::default(),
            created_at: SystemTime::now(),
            parquet_file: format!("{split_id}.parquet"),
            window: window.map(|(start, dur)| start..start + dur),
            sort_fields: sort_fields.to_string(),
            num_merge_ops,
            row_keys_proto: None,
            zonemap_regexes: Default::default(),
            rg_partition_prefix_len: 0,
        }
    }

    #[test]
    #[should_panic(expected = "MP-1 violated")]
    fn test_mp1_mixed_merge_levels_panics() {
        // MP-1: constructing a merge op with splits at different levels
        // must panic in debug builds.
        let splits = vec![
            make_split("l0", 0, "a|ts/V2", Some((0, 3600))),
            make_split("l1", 1, "a|ts/V2", Some((0, 3600))),
        ];
        ParquetMergeOperation::new(splits);
    }

    #[test]
    #[should_panic(expected = "MP-2 violated")]
    fn test_mp2_single_split_panics() {
        // MP-2: constructing a merge op with < 2 splits must panic.
        let splits = vec![make_split("s0", 0, "a|ts/V2", Some((0, 3600)))];
        ParquetMergeOperation::new(splits);
    }

    #[test]
    #[should_panic(expected = "MP-3 violated")]
    fn test_mp3_mixed_sort_fields_panics() {
        // MP-3: constructing a merge op with different sort_fields must panic.
        let splits = vec![
            make_split("s0", 0, "a|b|ts/V2", Some((0, 3600))),
            make_split("s1", 0, "a|ts/V2", Some((0, 3600))),
        ];
        ParquetMergeOperation::new(splits);
    }

    #[test]
    #[should_panic(expected = "MP-3 violated")]
    fn test_mp3_mixed_window_duration_panics() {
        // MP-3: same start but different duration must panic.
        let splits = vec![
            make_split("s0", 0, "a|ts/V2", Some((0, 900))),
            make_split("s1", 0, "a|ts/V2", Some((0, 1800))),
        ];
        ParquetMergeOperation::new(splits);
    }

    #[test]
    fn test_valid_merge_operation_succeeds() {
        // A well-formed merge op should not panic.
        let splits = vec![
            make_split("s0", 0, "a|ts/V2", Some((0, 3600))),
            make_split("s1", 0, "a|ts/V2", Some((0, 3600))),
        ];
        let op = ParquetMergeOperation::new(splits);
        assert_eq!(op.splits.len(), 2);
        assert!(
            op.target_prefix_len_override.is_none(),
            "regular merges don't set the override",
        );
    }

    /// Legacy-promotion happy path: a prefix_len=0 split + a prefix_len=2
    /// split with target=2. Both inputs share the windowed scope; the
    /// operation records `target_prefix_len_override = Some(2)`.
    #[test]
    fn test_promote_legacy_pairs_legacy_with_aligned_peer() {
        let mut legacy = make_split("legacy", 0, "metric_name|service|ts/V2", Some((0, 3600)));
        legacy.rg_partition_prefix_len = 0;

        let mut aligned = make_split("aligned", 0, "metric_name|service|ts/V2", Some((0, 3600)));
        aligned.rg_partition_prefix_len = 2;

        let op = ParquetMergeOperation::promote_legacy(vec![legacy, aligned], 2);
        assert_eq!(op.splits.len(), 2);
        assert_eq!(op.target_prefix_len_override, Some(2));
    }

    /// Promotion requires all inputs to have `prefix_len <= target`.
    /// Passing an input whose prefix_len exceeds the target is a planner
    /// bug — the adapter cannot DEMOTE alignment, only promote.
    #[test]
    #[should_panic(expected = "cannot demote a higher prefix")]
    fn test_promote_legacy_rejects_higher_prefix_input() {
        let mut legacy = make_split("legacy", 0, "a|b|ts/V2", Some((0, 3600)));
        legacy.rg_partition_prefix_len = 0;

        let mut too_high = make_split("too_high", 0, "a|b|ts/V2", Some((0, 3600)));
        too_high.rg_partition_prefix_len = 3;

        // target = 2, but too_high.rg_partition_prefix_len = 3.
        ParquetMergeOperation::promote_legacy(vec![legacy, too_high], 2);
    }

    /// Promotion still requires MP-3 on the non-prefix scope
    /// dimensions: sort_fields + window. Mixed sort_fields must still
    /// panic.
    #[test]
    #[should_panic(expected = "MP-3 violated")]
    fn test_promote_legacy_still_enforces_sort_fields() {
        let mut a = make_split("a", 0, "metric_name|ts/V2", Some((0, 3600)));
        a.rg_partition_prefix_len = 0;
        let mut b = make_split("b", 0, "different|schema/V2", Some((0, 3600)));
        b.rg_partition_prefix_len = 1;
        ParquetMergeOperation::promote_legacy(vec![a, b], 1);
    }

    /// All inputs at the target prefix_len (no actual legacy promotion
    /// happening) — the constructor still accepts it. The executor
    /// will just open every input directly without the adapter.
    /// Useful when a planner produces a uniform op that happens to be
    /// at the same target.
    #[test]
    fn test_promote_legacy_all_at_target_is_valid() {
        let mut a = make_split("a", 0, "a|ts/V2", Some((0, 3600)));
        a.rg_partition_prefix_len = 1;
        let mut b = make_split("b", 0, "a|ts/V2", Some((0, 3600)));
        b.rg_partition_prefix_len = 1;
        let op = ParquetMergeOperation::promote_legacy(vec![a, b], 1);
        assert_eq!(op.target_prefix_len_override, Some(1));
    }
}
