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
    pub fn new(splits: Vec<ParquetSplitMetadata>) -> Self {
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
