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
    /// The input splits being merged (all from the same compaction scope and
    /// `num_merge_ops` level).
    pub splits: Vec<ParquetSplitMetadata>,
}

impl ParquetMergeOperation {
    /// Create a new merge operation consuming the given splits.
    ///
    /// Generates a fresh split ID for the merged output. The `kind` is inferred
    /// from the first split (all splits in a merge share the same kind).
    ///
    /// # Invariant checks (debug builds panic, all builds emit metrics)
    ///
    /// - **MP-1**: all splits share the same `num_merge_ops` level
    /// - **MP-2**: at least 2 input splits
    /// - **MP-3**: all splits share the same compaction scope (sort_fields + window)
    pub fn new(splits: Vec<ParquetSplitMetadata>) -> Self {
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

        let kind = splits
            .first()
            .map(|s| s.kind)
            .unwrap_or(ParquetSplitKind::Metrics);
        Self {
            merge_split_id: ParquetSplitId::generate(kind),
            splits,
        }
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
    }
}
