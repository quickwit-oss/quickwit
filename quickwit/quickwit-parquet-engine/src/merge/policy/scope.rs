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

//! Compaction scope grouping for Parquet splits.
//!
//! Splits are eligible for merging only when they share the same compaction
//! scope. A scope captures the key dimensions that must match: index,
//! partition, sort schema, and time window.
//!
//! Future extensions (Phase 3+) will add `source_id` to the scope when
//! that field is populated on `ParquetSplitMetadata`.

use std::collections::HashMap;

use crate::split::ParquetSplitMetadata;

/// The compaction scope key.
///
/// Splits sharing the same scope are candidates for merging.
/// Splits with different scopes must **never** be merged together.
///
/// # Invariant MC-SCOPE
///
/// All splits in a [`super::ParquetMergeOperation`] must share the same
/// `CompactionScope`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CompactionScope {
    /// Index unique identifier (includes incarnation).
    pub index_uid: String,
    /// Partition ID computed from the index's routing expression.
    /// Splits with different partition IDs must not be merged.
    pub partition_id: u64,
    /// Husky-style sort schema string (e.g., "metric_name|host|timestamp/V2").
    pub sort_fields: String,
    /// Window start in epoch seconds.
    pub window_start_secs: i64,
    /// Window duration in seconds. The merge engine requires all inputs to
    /// agree on both start and duration, so a config change that alters the
    /// window size must not cause old and new splits to be merged.
    pub window_duration_secs: i64,
    /// Number of leading sort schema columns whose transitions align with
    /// row group boundaries. Splits with different alignment claims live
    /// in different compaction buckets so the merge engine can rely on a
    /// uniform input format. See
    /// [`crate::split::ParquetSplitMetadata::rg_partition_prefix_len`].
    pub rg_partition_prefix_len: u32,
}

impl CompactionScope {
    /// Extract compaction scope from split metadata.
    ///
    /// Returns `None` if the split has no window (pre-Phase-31 splits that
    /// cannot participate in compaction).
    pub fn from_split(split: &ParquetSplitMetadata) -> Option<Self> {
        let window = split.window.as_ref()?;
        Some(Self {
            index_uid: split.index_uid.clone(),
            partition_id: split.partition_id,
            sort_fields: split.sort_fields.clone(),
            window_start_secs: window.start,
            window_duration_secs: window.end - window.start,
            rg_partition_prefix_len: split.rg_partition_prefix_len,
        })
    }
}

/// Group splits by compaction scope.
///
/// Returns only groups with >= 2 splits, since single-split groups cannot
/// produce a merge operation. Splits without a window (pre-Phase-31) are
/// silently excluded.
pub fn group_by_compaction_scope(
    splits: Vec<ParquetSplitMetadata>,
) -> HashMap<CompactionScope, Vec<ParquetSplitMetadata>> {
    let mut groups: HashMap<CompactionScope, Vec<ParquetSplitMetadata>> = HashMap::new();
    for split in splits {
        if let Some(scope) = CompactionScope::from_split(&split) {
            groups.entry(scope).or_default().push(split);
        }
    }
    groups.retain(|_scope, splits| splits.len() >= 2);
    groups
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::split::{ParquetSplitId, ParquetSplitKind, ParquetSplitMetadata, TimeRange};

    /// Build a test split with the given scope fields.
    fn test_split(
        index_uid: &str,
        sort_fields: &str,
        window_start: Option<i64>,
        window_duration: u32,
    ) -> ParquetSplitMetadata {
        test_split_with_partition(index_uid, 0, sort_fields, window_start, window_duration)
    }

    fn test_split_with_partition(
        index_uid: &str,
        partition_id: u64,
        sort_fields: &str,
        window_start: Option<i64>,
        window_duration: u32,
    ) -> ParquetSplitMetadata {
        test_split_full(
            index_uid,
            partition_id,
            sort_fields,
            window_start,
            window_duration,
            0,
        )
    }

    fn test_split_full(
        index_uid: &str,
        partition_id: u64,
        sort_fields: &str,
        window_start: Option<i64>,
        window_duration: u32,
        rg_partition_prefix_len: u32,
    ) -> ParquetSplitMetadata {
        let mut builder = ParquetSplitMetadata::metrics_builder()
            .split_id(ParquetSplitId::generate(ParquetSplitKind::Metrics))
            .index_uid(index_uid)
            .partition_id(partition_id)
            .time_range(TimeRange::new(1000, 2000))
            .sort_fields(sort_fields)
            .rg_partition_prefix_len(rg_partition_prefix_len);

        if let Some(start) = window_start {
            builder = builder
                .window_start_secs(start)
                .window_duration_secs(window_duration);
        }

        builder.build()
    }

    #[test]
    fn test_empty_input() {
        let result = group_by_compaction_scope(vec![]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_single_split_filtered() {
        let splits = vec![test_split(
            "idx:001",
            "metric_name|host|timestamp/V2",
            Some(1000),
            3600,
        )];
        let result = group_by_compaction_scope(splits);
        assert!(result.is_empty(), "single-split groups should be filtered");
    }

    #[test]
    fn test_two_splits_same_scope() {
        let splits = vec![
            test_split("idx:001", "metric_name|host|timestamp/V2", Some(1000), 3600),
            test_split("idx:001", "metric_name|host|timestamp/V2", Some(1000), 3600),
        ];
        let result = group_by_compaction_scope(splits);
        assert_eq!(result.len(), 1);
        let group = result.values().next().unwrap();
        assert_eq!(group.len(), 2);
    }

    #[test]
    fn test_different_index_uid() {
        let splits = vec![
            test_split("idx:001", "metric_name|host|timestamp/V2", Some(1000), 3600),
            test_split("idx:002", "metric_name|host|timestamp/V2", Some(1000), 3600),
        ];
        let result = group_by_compaction_scope(splits);
        // Each group has only 1 split, so both are filtered.
        assert!(result.is_empty());
    }

    #[test]
    fn test_different_sort_fields() {
        let splits = vec![
            test_split("idx:001", "metric_name|host|timestamp/V2", Some(1000), 3600),
            test_split("idx:001", "metric_name|timestamp/V2", Some(1000), 3600),
        ];
        let result = group_by_compaction_scope(splits);
        assert!(result.is_empty());
    }

    #[test]
    fn test_different_window_duration() {
        // Bug: only window_start was in the scope key, so splits with the
        // same start but different durations (e.g. after a config change)
        // would be grouped together. The merge engine requires all inputs
        // to agree on both window_start and window_duration.
        let sort = "metric_name|host|timestamp/V2";
        let splits = vec![
            test_split("idx:001", sort, Some(0), 900),  // 0..900
            test_split("idx:001", sort, Some(0), 1800), // 0..1800
        ];
        let result = group_by_compaction_scope(splits);
        assert!(
            result.is_empty(),
            "different window durations must not be grouped together"
        );
    }

    #[test]
    fn test_different_window_start() {
        let splits = vec![
            test_split("idx:001", "metric_name|host|timestamp/V2", Some(1000), 3600),
            test_split("idx:001", "metric_name|host|timestamp/V2", Some(4600), 3600),
        ];
        let result = group_by_compaction_scope(splits);
        assert!(result.is_empty());
    }

    #[test]
    fn test_no_window_excluded() {
        let splits = vec![
            test_split("idx:001", "metric_name|host|timestamp/V2", None, 0),
            test_split("idx:001", "metric_name|host|timestamp/V2", None, 0),
        ];
        let result = group_by_compaction_scope(splits);
        assert!(
            result.is_empty(),
            "splits without window should be excluded"
        );
    }

    #[test]
    fn test_mixed_with_and_without_window() {
        let splits = vec![
            test_split("idx:001", "metric_name|host|timestamp/V2", Some(1000), 3600),
            test_split("idx:001", "metric_name|host|timestamp/V2", Some(1000), 3600),
            test_split("idx:001", "metric_name|host|timestamp/V2", None, 0),
        ];
        let result = group_by_compaction_scope(splits);
        assert_eq!(result.len(), 1);
        let group = result.values().next().unwrap();
        assert_eq!(group.len(), 2, "only windowed splits should be grouped");
    }

    #[test]
    fn test_multiple_scopes() {
        let splits = vec![
            // Scope A: 3 splits
            test_split("idx:001", "metric_name|host|timestamp/V2", Some(1000), 3600),
            test_split("idx:001", "metric_name|host|timestamp/V2", Some(1000), 3600),
            test_split("idx:001", "metric_name|host|timestamp/V2", Some(1000), 3600),
            // Scope B: 2 splits (different window)
            test_split("idx:001", "metric_name|host|timestamp/V2", Some(4600), 3600),
            test_split("idx:001", "metric_name|host|timestamp/V2", Some(4600), 3600),
            // Scope C: 1 split (filtered)
            test_split("idx:002", "metric_name|host|timestamp/V2", Some(1000), 3600),
        ];
        let result = group_by_compaction_scope(splits);
        assert_eq!(result.len(), 2, "should have 2 groups (scope C filtered)");

        let scope_a = CompactionScope {
            index_uid: "idx:001".to_string(),
            partition_id: 0,
            sort_fields: "metric_name|host|timestamp/V2".to_string(),
            window_start_secs: 1000,
            window_duration_secs: 3600,
            rg_partition_prefix_len: 0,
        };
        let scope_b = CompactionScope {
            index_uid: "idx:001".to_string(),
            partition_id: 0,
            sort_fields: "metric_name|host|timestamp/V2".to_string(),
            window_start_secs: 4600,
            window_duration_secs: 3600,
            rg_partition_prefix_len: 0,
        };

        assert_eq!(result[&scope_a].len(), 3);
        assert_eq!(result[&scope_b].len(), 2);
    }

    #[test]
    fn test_different_partition_id() {
        let sort = "metric_name|host|timestamp/V2";
        let splits = vec![
            test_split_with_partition("idx:001", 1, sort, Some(1000), 3600),
            test_split_with_partition("idx:001", 2, sort, Some(1000), 3600),
        ];
        let result = group_by_compaction_scope(splits);
        assert!(
            result.is_empty(),
            "different partition_ids should not be grouped"
        );
    }

    #[test]
    fn test_same_partition_id() {
        let sort = "metric_name|host|timestamp/V2";
        let splits = vec![
            test_split_with_partition("idx:001", 42, sort, Some(1000), 3600),
            test_split_with_partition("idx:001", 42, sort, Some(1000), 3600),
        ];
        let result = group_by_compaction_scope(splits);
        assert_eq!(result.len(), 1);
        let group = result.values().next().unwrap();
        assert_eq!(group.len(), 2);
    }

    #[test]
    fn test_from_split_returns_none_for_no_window() {
        let split = test_split("idx:001", "metric_name|host|timestamp/V2", None, 0);
        assert!(CompactionScope::from_split(&split).is_none());
    }

    #[test]
    fn test_from_split_returns_scope() {
        let split = test_split_with_partition(
            "idx:001",
            7,
            "metric_name|host|timestamp/V2",
            Some(7200),
            3600,
        );
        let scope = CompactionScope::from_split(&split).unwrap();
        assert_eq!(scope.index_uid, "idx:001");
        assert_eq!(scope.partition_id, 7);
        assert_eq!(scope.sort_fields, "metric_name|host|timestamp/V2");
        assert_eq!(scope.window_start_secs, 7200);
        assert_eq!(scope.window_duration_secs, 3600);
        assert_eq!(scope.rg_partition_prefix_len, 0);
    }

    #[test]
    fn test_different_rg_partition_prefix_len_separated() {
        // A legacy split (prefix=0) and a single-RG-aligned split (prefix=3)
        // share every other scope dimension but must NOT be merged together,
        // because the merge engine requires uniform input format.
        let sort = "metric_name|host|timestamp/V2";
        let splits = vec![
            test_split_full("idx:001", 0, sort, Some(1000), 3600, 0),
            test_split_full("idx:001", 0, sort, Some(1000), 3600, 3),
        ];
        let result = group_by_compaction_scope(splits);
        assert!(
            result.is_empty(),
            "splits with different rg_partition_prefix_len must end up in separate groups"
        );
    }

    #[test]
    fn test_same_rg_partition_prefix_len_grouped() {
        let sort = "metric_name|host|timestamp/V2";
        let splits = vec![
            test_split_full("idx:001", 0, sort, Some(1000), 3600, 1),
            test_split_full("idx:001", 0, sort, Some(1000), 3600, 1),
            test_split_full("idx:001", 0, sort, Some(1000), 3600, 1),
        ];
        let result = group_by_compaction_scope(splits);
        assert_eq!(result.len(), 1, "matching prefix splits group together");
        let group = result.values().next().unwrap();
        assert_eq!(group.len(), 3);

        let scope = result.keys().next().unwrap();
        assert_eq!(scope.rg_partition_prefix_len, 1);
    }

    #[test]
    fn test_three_distinct_prefix_buckets() {
        // Each prefix value forms its own bucket. Even with 6 splits sharing
        // every other dimension, 3 different prefixes → 3 buckets of 2 each.
        let sort = "metric_name|host|timestamp/V2";
        let splits = vec![
            test_split_full("idx:001", 0, sort, Some(1000), 3600, 0),
            test_split_full("idx:001", 0, sort, Some(1000), 3600, 0),
            test_split_full("idx:001", 0, sort, Some(1000), 3600, 1),
            test_split_full("idx:001", 0, sort, Some(1000), 3600, 1),
            test_split_full("idx:001", 0, sort, Some(1000), 3600, 3),
            test_split_full("idx:001", 0, sort, Some(1000), 3600, 3),
        ];
        let result = group_by_compaction_scope(splits);
        assert_eq!(result.len(), 3, "three prefix values → three buckets");
        for group in result.values() {
            assert_eq!(group.len(), 2);
        }
    }
}
