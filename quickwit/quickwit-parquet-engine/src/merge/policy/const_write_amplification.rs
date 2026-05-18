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

//! Constant write amplification merge policy for Parquet splits.
//!
//! Adapted from `quickwit_indexing::merge_policy::ConstWriteAmplificationMergePolicy`
//! but using byte size instead of document count as the primary size metric.
//! Parquet rows vary wildly in size depending on tag density, so byte size is
//! the meaningful convergence target.
//!
//! # Algorithm
//!
//! 1. Separate mature splits from immature.
//! 2. Group immature splits by `num_merge_ops` (the merge level).
//! 3. Within each level, sort by creation time (oldest first) and greedily accumulate splits until
//!    `merge_factor` or `target_split_size_bytes`.
//! 4. Each merge operation contains splits from exactly one level.
//!
//! This bounds write amplification: each byte is rewritten at most
//! `max_merge_ops` times across the split's lifetime.

use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::time::{Duration, SystemTime};

use tracing::info;

use super::{ParquetMergeOperation, ParquetMergePolicy, ParquetSplitMaturity};
use crate::split::ParquetSplitMetadata;

/// Smallest number of splits in a finalize merge.
const FINALIZE_MIN_MERGE_FACTOR: usize = 2;

/// Configuration for the constant write amplification Parquet merge policy.
#[derive(Debug, Clone)]
pub struct ParquetMergePolicyConfig {
    /// Minimum number of splits to trigger a merge.
    pub merge_factor: usize,
    /// Maximum number of splits in a single merge operation.
    pub max_merge_factor: usize,
    /// Maximum number of merge operations a split can undergo before becoming
    /// mature. Bounds total write amplification.
    pub max_merge_ops: u32,
    /// Target size for merged output splits in bytes. When accumulated bytes
    /// reach this threshold, a merge is triggered even if `merge_factor` is
    /// not reached.
    pub target_split_size_bytes: u64,
    /// Duration after creation when a split becomes mature regardless of size
    /// or merge count.
    pub maturation_period: Duration,
    /// Maximum number of merge operations emitted by `finalize_operations`.
    /// Set to 0 to disable finalization.
    pub max_finalize_merge_operations: usize,
}

impl Default for ParquetMergePolicyConfig {
    fn default() -> Self {
        Self {
            merge_factor: 10,
            max_merge_factor: 12,
            max_merge_ops: 4,
            target_split_size_bytes: 256 * 1024 * 1024, // 256 MiB
            maturation_period: Duration::from_secs(48 * 3600), // 48 hours
            max_finalize_merge_operations: 3,
        }
    }
}

/// Constant write amplification merge policy for Parquet splits.
///
/// Only splits with the same `num_merge_ops` level are merged together.
/// After sorting by creation date, splits are greedily accumulated until
/// reaching `max_merge_factor` or `target_split_size_bytes`.
#[derive(Debug, Clone)]
pub struct ConstWriteAmplificationParquetMergePolicy {
    config: ParquetMergePolicyConfig,
}

impl ConstWriteAmplificationParquetMergePolicy {
    /// Create a new policy with the given configuration.
    pub fn new(config: ParquetMergePolicyConfig) -> Self {
        Self { config }
    }

    /// Returns the merge factor range for normal operations.
    fn merge_factor_range(&self) -> RangeInclusive<usize> {
        self.config.merge_factor..=self.config.max_merge_factor
    }

    /// Check if a split is mature based on its metadata.
    fn is_split_mature(&self, split: &ParquetSplitMetadata) -> bool {
        if split.num_merge_ops >= self.config.max_merge_ops {
            return true;
        }
        if split.size_bytes >= self.config.target_split_size_bytes {
            return true;
        }
        let elapsed = split.created_at.elapsed().unwrap_or(Duration::ZERO);
        elapsed >= self.config.maturation_period
    }

    /// Try to build a single merge operation from the front of a sorted split
    /// list. Returns `None` if there aren't enough splits to merge.
    ///
    /// Assumes `splits` are sorted by creation time (oldest first).
    fn single_merge_operation(
        &self,
        splits: &mut Vec<ParquetSplitMetadata>,
        merge_factor_range: RangeInclusive<usize>,
    ) -> Option<ParquetMergeOperation> {
        let mut num_splits_in_merge = 0;
        let mut total_bytes_in_merge: u64 = 0;

        for split in splits.iter().take(*merge_factor_range.end()) {
            total_bytes_in_merge += split.size_bytes;
            num_splits_in_merge += 1;
            if total_bytes_in_merge >= self.config.target_split_size_bytes {
                break;
            }
        }

        // Not enough to merge: fewer than merge_factor and total bytes under target.
        if total_bytes_in_merge < self.config.target_split_size_bytes
            && num_splits_in_merge < *merge_factor_range.start()
        {
            return None;
        }

        debug_assert!(
            num_splits_in_merge >= 2,
            "merge operations must contain at least 2 splits"
        );
        let splits_in_merge: Vec<ParquetSplitMetadata> =
            splits.drain(0..num_splits_in_merge).collect();
        Some(ParquetMergeOperation::new(splits_in_merge))
    }

    /// Build all merge operations within a single `num_merge_ops` level.
    fn merge_operations_within_level(
        &self,
        splits: &mut Vec<ParquetSplitMetadata>,
    ) -> Vec<ParquetMergeOperation> {
        sort_splits_oldest_first(splits);

        let mut operations = Vec::new();
        while let Some(op) = self.single_merge_operation(splits, self.merge_factor_range()) {
            operations.push(op);
        }
        operations
    }
}

/// Sort splits by creation time (oldest first), then by split ID for
/// determinism. Uses seconds-since-epoch to avoid `SystemTime` comparison
/// issues across platforms.
fn sort_splits_oldest_first(splits: &mut [ParquetSplitMetadata]) {
    splits.sort_by(|a, b| {
        created_at_secs(a)
            .cmp(&created_at_secs(b))
            .then_with(|| a.split_id.as_str().cmp(b.split_id.as_str()))
    });
}

/// Sort splits by creation time (newest first), then by split ID.
fn sort_splits_newest_first(splits: &mut [ParquetSplitMetadata]) {
    splits.sort_by(|a, b| {
        created_at_secs(a)
            .cmp(&created_at_secs(b))
            .reverse()
            .then_with(|| a.split_id.as_str().cmp(b.split_id.as_str()))
    });
}

/// Extract seconds since epoch from a split's `created_at` field.
fn created_at_secs(split: &ParquetSplitMetadata) -> u64 {
    split
        .created_at
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

impl ParquetMergePolicy for ConstWriteAmplificationParquetMergePolicy {
    fn operations(&self, splits: &mut Vec<ParquetSplitMetadata>) -> Vec<ParquetMergeOperation> {
        let mut group_by_level: HashMap<u32, Vec<ParquetSplitMetadata>> = HashMap::new();
        let mut mature_splits = Vec::new();

        for split in splits.drain(..) {
            if self.is_split_mature(&split) {
                mature_splits.push(split);
            } else {
                group_by_level
                    .entry(split.num_merge_ops)
                    .or_default()
                    .push(split);
            }
        }

        // Mature splits go back into the vec untouched.
        splits.extend(mature_splits);

        let mut merge_operations = Vec::new();
        for level_splits in group_by_level.values_mut() {
            let ops = self.merge_operations_within_level(level_splits);
            merge_operations.extend(ops);
            // Un-merged splits at this level go back into the vec.
            splits.append(level_splits);
        }

        merge_operations
    }

    fn finalize_operations(
        &self,
        splits: &mut Vec<ParquetSplitMetadata>,
    ) -> Vec<ParquetMergeOperation> {
        if self.config.max_finalize_merge_operations == 0 {
            return Vec::new();
        }

        // Separate mature splits — don't touch them.
        let mut group_by_level: HashMap<u32, Vec<ParquetSplitMetadata>> = HashMap::new();
        let mut mature_splits = Vec::new();
        for split in splits.drain(..) {
            if self.is_split_mature(&split) {
                mature_splits.push(split);
            } else {
                group_by_level
                    .entry(split.num_merge_ops)
                    .or_default()
                    .push(split);
            }
        }
        splits.extend(mature_splits);

        let min_merge_factor = FINALIZE_MIN_MERGE_FACTOR.min(self.config.max_merge_factor);
        let merge_factor_range = min_merge_factor..=self.config.max_merge_factor;

        // Within each level, sort youngest/smallest first. If we limit the
        // number of finalize merges, we focus on the young/small ones for
        // maximum compaction.
        let mut merge_operations = Vec::new();
        for level_splits in group_by_level.values_mut() {
            sort_splits_newest_first(level_splits);
            while merge_operations.len() < self.config.max_finalize_merge_operations {
                if let Some(op) =
                    self.single_merge_operation(level_splits, merge_factor_range.clone())
                {
                    merge_operations.push(op);
                } else {
                    break;
                }
            }
            // Un-merged splits at this level go back.
            splits.append(level_splits);
        }

        let num_splits_per_op: Vec<usize> =
            merge_operations.iter().map(|op| op.splits.len()).collect();
        let bytes_per_op: Vec<u64> = merge_operations
            .iter()
            .map(|op| op.total_size_bytes())
            .collect();
        info!(
            num_splits_per_op = ?num_splits_per_op,
            bytes_per_op = ?bytes_per_op,
            "finalize merge operations"
        );

        merge_operations
    }

    fn split_maturity(&self, size_bytes: u64, num_merge_ops: u32) -> ParquetSplitMaturity {
        if num_merge_ops >= self.config.max_merge_ops {
            return ParquetSplitMaturity::Mature;
        }
        if size_bytes >= self.config.target_split_size_bytes {
            return ParquetSplitMaturity::Mature;
        }
        ParquetSplitMaturity::Immature {
            maturation_period: self.config.maturation_period,
        }
    }

    #[cfg(test)]
    fn check_is_valid(
        &self,
        merge_op: &ParquetMergeOperation,
        _remaining_splits: &[ParquetSplitMetadata],
    ) {
        use std::collections::HashSet;

        // Must not exceed max_merge_factor.
        assert!(merge_op.splits.len() <= self.config.max_merge_factor);

        // If fewer than merge_factor, total bytes must have reached target.
        if merge_op.splits.len() < self.config.merge_factor {
            let total_bytes: u64 = merge_op.splits.iter().map(|s| s.size_bytes).sum();
            let last_split_bytes = merge_op.splits.last().unwrap().size_bytes;
            assert!(
                total_bytes >= self.config.target_split_size_bytes,
                "under-factor merge must be size-triggered: total_bytes={total_bytes}, \
                 target={target}",
                target = self.config.target_split_size_bytes,
            );
            assert!(
                total_bytes - last_split_bytes < self.config.target_split_size_bytes,
                "should not have accumulated beyond target before adding last split"
            );
        }

        // MC-LEVEL: all splits must have the same num_merge_ops.
        let levels: HashSet<u32> = merge_op.splits.iter().map(|s| s.num_merge_ops).collect();
        assert_eq!(
            levels.len(),
            1,
            "all splits in a merge must be at the same level"
        );

        // MC-WA: no split should have reached max_merge_ops.
        let level = *levels.iter().next().unwrap();
        assert!(
            level < self.config.max_merge_ops,
            "mature splits (level {level} >= max_merge_ops {}) should not be merged",
            self.config.max_merge_ops
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::{Duration, SystemTime};

    use proptest::prelude::*;
    use rand::seq::SliceRandom;

    use super::*;
    use crate::split::{ParquetSplitId, ParquetSplitKind, TimeRange};

    /// Create a policy suitable for testing (small values for fast iteration).
    fn test_policy() -> ConstWriteAmplificationParquetMergePolicy {
        let config = ParquetMergePolicyConfig {
            merge_factor: 3,
            max_merge_factor: 5,
            max_merge_ops: 3,
            target_split_size_bytes: 256 * 1024 * 1024, // 256 MiB
            maturation_period: Duration::from_secs(3600),
            max_finalize_merge_operations: 3,
        };
        ConstWriteAmplificationParquetMergePolicy::new(config)
    }

    /// Build a test split with explicit parameters.
    fn make_split(
        split_id: &str,
        size_bytes: u64,
        num_merge_ops: u32,
        created_at: SystemTime,
    ) -> ParquetSplitMetadata {
        ParquetSplitMetadata {
            kind: ParquetSplitKind::Metrics,
            split_id: ParquetSplitId::new(split_id),
            index_uid: "test-index:001".to_string(),
            partition_id: 0,
            time_range: TimeRange::new(1000, 2000),
            num_rows: 1000,
            size_bytes,
            metric_names: HashSet::new(),
            low_cardinality_tags: Default::default(),
            high_cardinality_tag_keys: Default::default(),
            created_at,
            parquet_file: format!("{split_id}.parquet"),
            window: Some(0..3600),
            sort_fields: "metric_name|host|timestamp/V2".to_string(),
            num_merge_ops,
            row_keys_proto: None,
            zonemap_regexes: Default::default(),
            rg_partition_prefix_len: 0,
        }
    }

    fn now() -> SystemTime {
        SystemTime::now()
    }

    fn secs_ago(secs: u64) -> SystemTime {
        SystemTime::now() - Duration::from_secs(secs)
    }

    // ── Unit Tests ──────────────────────────────────────────────────

    #[test]
    fn test_empty_input() {
        let policy = test_policy();
        let mut splits = Vec::new();
        let ops = policy.operations(&mut splits);
        assert!(ops.is_empty());
        assert!(splits.is_empty());
    }

    #[test]
    fn test_single_split() {
        let policy = test_policy();
        let mut splits = vec![make_split("s0", 1_000_000, 0, now())];
        let ops = policy.operations(&mut splits);
        assert!(ops.is_empty());
        assert_eq!(splits.len(), 1);
    }

    #[test]
    fn test_two_splits_below_merge_factor() {
        let policy = test_policy(); // merge_factor = 3
        let mut splits = vec![
            make_split("s0", 1_000_000, 0, now()),
            make_split("s1", 1_000_000, 0, now()),
        ];
        let ops = policy.operations(&mut splits);
        assert!(ops.is_empty(), "2 splits < merge_factor=3 should not merge");
        assert_eq!(splits.len(), 2);
    }

    #[test]
    fn test_all_mature_by_merge_ops() {
        let policy = test_policy(); // max_merge_ops = 3
        let mut splits = vec![
            make_split("s0", 1_000_000, 3, now()),
            make_split("s1", 1_000_000, 3, now()),
            make_split("s2", 1_000_000, 3, now()),
        ];
        let ops = policy.operations(&mut splits);
        assert!(ops.is_empty(), "mature splits should not be merged");
        assert_eq!(splits.len(), 3);
    }

    #[test]
    fn test_all_mature_by_size() {
        let policy = test_policy(); // target = 256 MiB
        let big = 300 * 1024 * 1024; // 300 MiB > target
        let mut splits = vec![
            make_split("s0", big, 0, now()),
            make_split("s1", big, 0, now()),
            make_split("s2", big, 0, now()),
        ];
        let ops = policy.operations(&mut splits);
        assert!(ops.is_empty(), "size-mature splits should not be merged");
        assert_eq!(splits.len(), 3);
    }

    #[test]
    fn test_exactly_merge_factor() {
        let policy = test_policy(); // merge_factor = 3
        let mut splits = vec![
            make_split("s0", 1_000_000, 0, secs_ago(30)),
            make_split("s1", 1_000_000, 0, secs_ago(20)),
            make_split("s2", 1_000_000, 0, secs_ago(10)),
        ];
        let ops = policy.operations(&mut splits);
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].splits.len(), 3);
        assert!(splits.is_empty(), "all splits consumed");
    }

    #[test]
    fn test_more_than_max_merge_factor() {
        let policy = test_policy(); // max_merge_factor = 5, merge_factor = 3
        let mut splits: Vec<ParquetSplitMetadata> = (0..8)
            .map(|i| make_split(&format!("s{i}"), 1_000_000, 0, secs_ago(80 - i * 10)))
            .collect();
        let ops = policy.operations(&mut splits);
        // First op takes max_merge_factor=5, then 3 remain >= merge_factor=3 -> second op.
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].splits.len(), 5);
        assert_eq!(ops[1].splits.len(), 3);
        assert!(splits.is_empty());
    }

    #[test]
    fn test_first_op_capped_at_max_merge_factor() {
        let policy = test_policy(); // merge_factor = 3, max_merge_factor = 5
        let mut splits: Vec<ParquetSplitMetadata> = (0..6)
            .map(|i| make_split(&format!("s{i}"), 1_000_000, 0, secs_ago(60 - i * 10)))
            .collect();
        let ops = policy.operations(&mut splits);
        // First op takes max_merge_factor=5, then 1 remains < merge_factor=3.
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].splits.len(), 5);
        assert_eq!(splits.len(), 1, "1 leftover below merge_factor");
    }

    #[test]
    fn test_two_operations_at_same_level() {
        let policy = test_policy(); // merge_factor = 3, max_merge_factor = 5
        let mut splits: Vec<ParquetSplitMetadata> = (0..10)
            .map(|i| make_split(&format!("s{i:02}"), 1_000_000, 0, secs_ago(100 - i * 10)))
            .collect();
        let ops = policy.operations(&mut splits);
        assert_eq!(
            ops.len(),
            2,
            "10 splits should produce 2 ops (5+5 or 5+3+leftover)"
        );
        let total_consumed: usize = ops.iter().map(|op| op.splits.len()).sum();
        assert_eq!(total_consumed + splits.len(), 10);
    }

    #[test]
    fn test_mixed_levels() {
        let policy = test_policy(); // merge_factor = 3
        let mut splits = vec![
            // Level 0: 3 splits
            make_split("l0_s0", 1_000_000, 0, secs_ago(30)),
            make_split("l0_s1", 1_000_000, 0, secs_ago(20)),
            make_split("l0_s2", 1_000_000, 0, secs_ago(10)),
            // Level 1: 3 splits
            make_split("l1_s0", 1_000_000, 1, secs_ago(30)),
            make_split("l1_s1", 1_000_000, 1, secs_ago(20)),
            make_split("l1_s2", 1_000_000, 1, secs_ago(10)),
        ];
        let ops = policy.operations(&mut splits);
        assert_eq!(ops.len(), 2, "one op per level");
        assert!(splits.is_empty());

        // Verify each op has homogeneous levels.
        for op in &ops {
            let levels: HashSet<u32> = op.splits.iter().map(|s| s.num_merge_ops).collect();
            assert_eq!(levels.len(), 1);
        }
    }

    #[test]
    fn test_size_triggered_merge() {
        let policy = test_policy(); // merge_factor = 3, target = 256 MiB
        let big = 100 * 1024 * 1024; // 100 MiB each
        let mut splits = vec![
            make_split("s0", big, 0, secs_ago(30)),
            make_split("s1", big, 0, secs_ago(20)),
            make_split("s2", big, 0, secs_ago(10)), // total 300 MiB > target
        ];
        let ops = policy.operations(&mut splits);
        // 3 splits >= merge_factor=3, so this merges regardless of size.
        // But also total_bytes >= target, so size-triggered too.
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].splits.len(), 3);
    }

    #[test]
    fn test_size_triggered_below_merge_factor() {
        // Size trigger can cause a merge with fewer than merge_factor splits.
        let config = ParquetMergePolicyConfig {
            merge_factor: 10,
            max_merge_factor: 12,
            max_merge_ops: 4,
            target_split_size_bytes: 100 * 1024 * 1024, // 100 MiB
            maturation_period: Duration::from_secs(48 * 3600),
            max_finalize_merge_operations: 0,
        };
        let policy = ConstWriteAmplificationParquetMergePolicy::new(config);

        let big = 60 * 1024 * 1024; // 60 MiB each
        let mut splits = vec![
            make_split("s0", big, 0, secs_ago(30)),
            make_split("s1", big, 0, secs_ago(20)),
            // 2 splits, total 120 MiB > target 100 MiB
        ];
        let ops = policy.operations(&mut splits);
        assert_eq!(ops.len(), 1, "size-triggered merge with 2 splits");
        assert_eq!(ops[0].splits.len(), 2);
    }

    #[test]
    fn test_oldest_first_ordering() {
        let policy = test_policy();
        let mut splits = vec![
            make_split("newest", 1_000_000, 0, secs_ago(10)),
            make_split("middle", 1_000_000, 0, secs_ago(50)),
            make_split("oldest", 1_000_000, 0, secs_ago(100)),
        ];
        let ops = policy.operations(&mut splits);
        assert_eq!(ops.len(), 1);
        let ids: Vec<&str> = ops[0].splits.iter().map(|s| s.split_id.as_str()).collect();
        assert_eq!(ids, &["oldest", "middle", "newest"]);
    }

    #[test]
    fn test_split_maturity_by_merge_ops() {
        let policy = test_policy(); // max_merge_ops = 3
        assert_eq!(
            policy.split_maturity(1_000_000, 3),
            ParquetSplitMaturity::Mature,
        );
        assert_eq!(
            policy.split_maturity(1_000_000, 4),
            ParquetSplitMaturity::Mature,
        );
    }

    #[test]
    fn test_split_maturity_by_size() {
        let policy = test_policy(); // target = 256 MiB
        assert_eq!(
            policy.split_maturity(300 * 1024 * 1024, 0),
            ParquetSplitMaturity::Mature,
        );
    }

    #[test]
    fn test_split_maturity_immature() {
        let policy = test_policy();
        assert_eq!(
            policy.split_maturity(1_000_000, 0),
            ParquetSplitMaturity::Immature {
                maturation_period: Duration::from_secs(3600)
            },
        );
    }

    #[test]
    fn test_finalize_merges_below_merge_factor() {
        let policy = test_policy(); // merge_factor=3, finalize min=2
        let mut splits = vec![
            make_split("s0", 1_000_000, 0, now()),
            make_split("s1", 1_000_000, 0, now()),
        ];
        // Normal operations should not merge 2 splits (below merge_factor=3).
        let ops = policy.operations(&mut splits);
        assert!(ops.is_empty());
        assert_eq!(splits.len(), 2);

        // But finalize should merge them (min_merge_factor=2).
        let ops = policy.finalize_operations(&mut splits);
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].splits.len(), 2);
        assert!(splits.is_empty());
    }

    #[test]
    fn test_finalize_disabled() {
        let config = ParquetMergePolicyConfig {
            max_finalize_merge_operations: 0,
            ..test_policy().config.clone()
        };
        let policy = ConstWriteAmplificationParquetMergePolicy::new(config);
        let mut splits = vec![
            make_split("s0", 1_000_000, 0, now()),
            make_split("s1", 1_000_000, 0, now()),
        ];
        let ops = policy.finalize_operations(&mut splits);
        assert!(ops.is_empty());
        assert_eq!(splits.len(), 2);
    }

    #[test]
    fn test_finalize_respects_max_operations() {
        let config = ParquetMergePolicyConfig {
            merge_factor: 3,
            max_merge_factor: 3,
            max_finalize_merge_operations: 1,
            ..test_policy().config.clone()
        };
        let policy = ConstWriteAmplificationParquetMergePolicy::new(config);
        let mut splits: Vec<ParquetSplitMetadata> = (0..6)
            .map(|i| make_split(&format!("s{i}"), 1_000_000, 0, secs_ago(i * 10)))
            .collect();

        let ops = policy.finalize_operations(&mut splits);
        assert_eq!(ops.len(), 1, "limited to max_finalize_merge_operations=1");
        assert_eq!(splits.len(), 3);
    }

    #[test]
    fn test_mature_splits_excluded_from_finalize() {
        let policy = test_policy(); // max_merge_ops = 3
        let mut splits = vec![
            make_split("mature1", 1_000_000, 3, now()),
            make_split("mature2", 1_000_000, 3, now()),
            make_split("young1", 1_000_000, 0, now()),
            make_split("young2", 1_000_000, 0, now()),
        ];
        let ops = policy.finalize_operations(&mut splits);
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].splits.len(), 2);
        // Only young splits should be in the merge.
        for split in &ops[0].splits {
            assert!(split.num_merge_ops < 3);
        }
        // Mature splits remain.
        assert_eq!(splits.len(), 2);
        assert!(splits.iter().all(|s| s.num_merge_ops >= 3));
    }

    #[test]
    fn test_finalize_respects_mc_level_invariant() {
        // Bug: finalize_operations() did not group by num_merge_ops level,
        // so splits from different levels could be merged together. This
        // violates MC-LEVEL and causes the merged output to be stamped with
        // max(num_merge_ops) + 1, prematurely maturing lower-level data.
        let policy = test_policy(); // merge_factor=3, max_merge_ops=3
        let mut splits = vec![
            // Two level-0 splits
            make_split("l0_a", 1_000_000, 0, now()),
            make_split("l0_b", 1_000_000, 0, now()),
            // One level-1 split
            make_split("l1_a", 1_000_000, 1, now()),
        ];
        let ops = policy.finalize_operations(&mut splits);

        // MC-LEVEL: every operation must contain splits from exactly one level.
        for op in &ops {
            let levels: HashSet<u32> = op.splits.iter().map(|s| s.num_merge_ops).collect();
            assert_eq!(
                levels.len(),
                1,
                "finalize produced a merge mixing levels: {:?}",
                levels
            );
        }
    }

    // ── Property Tests ──────────────────────────────────────────────

    prop_compose! {
        fn parquet_split_strategy()(
            num_merge_ops in 0u32..5u32,
            size_bytes in 1u64..500_000_000u64,
            num_rows in 1u64..10_000_000u64,
            created_secs_ago in 0u64..200u64,
            split_ord in 0u32..1_000_000u32,
        ) -> ParquetSplitMetadata {
            let split_id = format!("prop_split_{split_ord:06}_{num_merge_ops}");
            let created_at = SystemTime::now() - Duration::from_secs(created_secs_ago);
            ParquetSplitMetadata {
                kind: ParquetSplitKind::Metrics,
                split_id: ParquetSplitId::new(split_id),
                index_uid: "test:prop".to_string(),
                partition_id: 0,
                time_range: TimeRange::new(1000, 2000),
                num_rows,
                size_bytes,
                metric_names: HashSet::new(),
                low_cardinality_tags: Default::default(),
                high_cardinality_tag_keys: Default::default(),
                created_at,
                parquet_file: String::new(),
                window: Some(0..3600),
                sort_fields: "metric_name|host|timestamp/V2".to_string(),
                num_merge_ops,
                row_keys_proto: None,
                zonemap_regexes: Default::default(),
                rg_partition_prefix_len: 0,
            }
        }
    }

    /// Compute a checksum for a set of operations that is independent of
    /// operation order (but depends on which splits are in which op).
    fn operations_checksum(ops: &[ParquetMergeOperation]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut checksum = 0u64;
        for op in ops {
            let mut op_hash = 0u64;
            for split in &op.splits {
                let mut h = DefaultHasher::new();
                split.split_id.as_str().hash(&mut h);
                op_hash ^= h.finish();
            }
            let mut h = DefaultHasher::new();
            h.write_u64(op_hash);
            checksum ^= h.finish();
        }
        checksum
    }

    proptest! {
        #[test]
        fn proptest_merge_policy_invariants(
            mut splits in prop::collection::vec(parquet_split_strategy(), 0..80)
        ) {
            let policy = test_policy();

            let original_total_bytes: u64 = splits.iter().map(|s| s.size_bytes).sum();
            let original_count = splits.len();

            // MC-IDEMPOTENT / MC-ORDER-INDEPENDENT: shuffle produces same result.
            let mut cloned = splits.clone();
            cloned.shuffle(&mut rand::rng());

            let ops = policy.operations(&mut splits);
            let ops_shuffled = policy.operations(&mut cloned);

            prop_assert_eq!(
                operations_checksum(&ops),
                operations_checksum(&ops_shuffled),
                "policy must be order-independent"
            );

            // MC-CONSERVE: total bytes in ops + remaining = original.
            let ops_bytes: u64 = ops.iter()
                .flat_map(|op| op.splits.iter())
                .map(|s| s.size_bytes)
                .sum();
            let remaining_bytes: u64 = splits.iter().map(|s| s.size_bytes).sum();
            prop_assert_eq!(
                ops_bytes + remaining_bytes,
                original_total_bytes,
                "byte conservation violated"
            );

            // Count conservation.
            let ops_count: usize = ops.iter().map(|op| op.splits.len()).sum();
            prop_assert_eq!(
                ops_count + splits.len(),
                original_count,
                "split count conservation violated"
            );

            for op in &ops {
                // Each op has >= 2 splits.
                prop_assert!(
                    op.splits.len() >= 2,
                    "merge op must have >= 2 splits, got {}",
                    op.splits.len()
                );

                // MC-LEVEL: all splits in op have same num_merge_ops.
                let levels: HashSet<u32> = op.splits.iter().map(|s| s.num_merge_ops).collect();
                prop_assert_eq!(
                    levels.len(), 1,
                    "all splits in merge must be same level, got {:?}", levels
                );

                // MC-WA: no mature splits in ops.
                for split in &op.splits {
                    prop_assert!(
                        !policy.is_split_mature(split),
                        "mature split {} should not be in merge",
                        split.split_id
                    );
                }

                // Validate policy-specific invariants.
                policy.check_is_valid(op, &splits);
            }
        }
    }

    proptest! {
        #[test]
        fn proptest_finalize_respects_mc_level(
            mut splits in prop::collection::vec(parquet_split_strategy(), 0..80)
        ) {
            let policy = test_policy();
            let original_count = splits.len();
            let original_total_bytes: u64 = splits.iter().map(|s| s.size_bytes).sum();

            let ops = policy.finalize_operations(&mut splits);

            // MC-CONSERVE for finalize.
            let ops_bytes: u64 = ops.iter()
                .flat_map(|op| op.splits.iter())
                .map(|s| s.size_bytes)
                .sum();
            let remaining_bytes: u64 = splits.iter().map(|s| s.size_bytes).sum();
            prop_assert_eq!(
                ops_bytes + remaining_bytes,
                original_total_bytes,
                "finalize byte conservation violated"
            );

            let ops_count: usize = ops.iter().map(|op| op.splits.len()).sum();
            prop_assert_eq!(
                ops_count + splits.len(),
                original_count,
                "finalize split count conservation violated"
            );

            for op in &ops {
                prop_assert!(
                    op.splits.len() >= 2,
                    "finalize merge op must have >= 2 splits"
                );

                // MC-LEVEL: all splits in op have same num_merge_ops.
                let levels: HashSet<u32> = op.splits.iter().map(|s| s.num_merge_ops).collect();
                prop_assert_eq!(
                    levels.len(), 1,
                    "finalize mixed levels: {:?}", levels
                );

                // MC-WA: no mature splits.
                for split in &op.splits {
                    prop_assert!(
                        !policy.is_split_mature(split),
                        "mature split in finalize merge"
                    );
                }
            }
        }
    }

    // ── Simulation Test ─────────────────────────────────────────────

    #[test]
    fn test_simulation_convergence() {
        let config = ParquetMergePolicyConfig {
            merge_factor: 3,
            max_merge_factor: 5,
            max_merge_ops: 3,
            target_split_size_bytes: 256 * 1024 * 1024,
            maturation_period: Duration::from_secs(999_999), // never time-expire
            max_finalize_merge_operations: 0,
        };
        let policy = ConstWriteAmplificationParquetMergePolicy::new(config);

        // Simulate 200 ingested splits of 10 MiB each.
        let mut all_splits: Vec<ParquetSplitMetadata> = (0..200)
            .map(|i| {
                make_split(
                    &format!("ingest_{i:03}"),
                    10 * 1024 * 1024,
                    0,
                    secs_ago(200 - i),
                )
            })
            .collect();

        let max_rounds = 20;
        for round in 0..max_rounds {
            let ops = policy.operations(&mut all_splits);
            if ops.is_empty() {
                break;
            }

            // Simulate merges: create output splits with incremented merge ops.
            for op in &ops {
                let total_bytes: u64 = op.splits.iter().map(|s| s.size_bytes).sum();
                let total_rows: u64 = op.splits.iter().map(|s| s.num_rows).sum();
                let max_merge_ops = op.splits.iter().map(|s| s.num_merge_ops).max().unwrap();
                let merged = make_split(
                    &format!("merged_r{round}_{}", op.merge_split_id),
                    total_bytes,
                    max_merge_ops + 1,
                    now(),
                );
                // Override the auto-generated fields.
                let mut merged = merged;
                merged.num_rows = total_rows;
                all_splits.push(merged);
            }
        }

        // Verify bounded write amplification.
        for split in &all_splits {
            assert!(
                split.num_merge_ops <= policy.config.max_merge_ops,
                "split {} has num_merge_ops={}, exceeds max={}",
                split.split_id,
                split.num_merge_ops,
                policy.config.max_merge_ops,
            );
        }

        // Verify convergence: at each non-mature level, fewer than merge_factor splits.
        let mut level_counts: HashMap<u32, usize> = HashMap::new();
        for split in &all_splits {
            if split.num_merge_ops < policy.config.max_merge_ops {
                *level_counts.entry(split.num_merge_ops).or_default() += 1;
            }
        }
        for (level, count) in &level_counts {
            assert!(
                *count < policy.config.merge_factor,
                "level {level} has {count} splits, should be < merge_factor={}",
                policy.config.merge_factor,
            );
        }
    }
}
