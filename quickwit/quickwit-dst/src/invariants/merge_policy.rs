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

//! Shared merge policy invariant checks.
//!
//! These pure functions are the single source of truth for merge operation
//! validity, used by both Stateright models and production code.

/// MP-1: all splits in a merge operation must share the same `num_merge_ops`.
///
/// If splits from different levels are merged, the output gets stamped with
/// `max(levels) + 1`, prematurely maturing lower-level data and breaking
/// the bounded write amplification guarantee.
pub fn all_same_merge_level(num_merge_ops: &[u32]) -> bool {
    match num_merge_ops.first() {
        None => true,
        Some(&first) => num_merge_ops.iter().all(|&n| n == first),
    }
}

/// MP-2: every merge operation must have at least 2 input splits.
///
/// Merging a single split is a no-op that wastes I/O. Merging zero splits
/// is nonsensical.
pub fn has_minimum_splits(count: usize) -> bool {
    count >= 2
}

/// MP-3: all splits in a merge operation must share the same compaction scope.
///
/// The scope is defined by `(sort_fields, window_start, window_duration)`.
/// The merge engine validates that all inputs agree on these; a policy bug
/// that groups incompatible splits will cause the merge to fail.
///
/// `index_uid` and `partition_id` are also part of the scope but are
/// typically enforced by the grouping layer before the policy runs.
pub fn all_same_compaction_scope(
    sort_fields: &[&str],
    windows: &[(i64, i64)], // (start, duration) pairs
) -> bool {
    let same_sort = match sort_fields.first() {
        None => true,
        Some(&first) => sort_fields.iter().all(|&s| s == first),
    };
    let same_window = match windows.first() {
        None => true,
        Some(&first) => windows.iter().all(|w| *w == first),
    };
    same_sort && same_window
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_same_merge_level() {
        assert!(all_same_merge_level(&[]));
        assert!(all_same_merge_level(&[0]));
        assert!(all_same_merge_level(&[2, 2, 2]));
        assert!(!all_same_merge_level(&[0, 1]));
        assert!(!all_same_merge_level(&[0, 0, 1]));
    }

    #[test]
    fn test_has_minimum_splits() {
        assert!(!has_minimum_splits(0));
        assert!(!has_minimum_splits(1));
        assert!(has_minimum_splits(2));
        assert!(has_minimum_splits(100));
    }

    #[test]
    fn test_all_same_compaction_scope() {
        // Empty is vacuously true.
        assert!(all_same_compaction_scope(&[], &[]));

        // Same scope.
        assert!(all_same_compaction_scope(
            &["a|b|ts/V2", "a|b|ts/V2"],
            &[(0, 3600), (0, 3600)],
        ));

        // Different sort fields.
        assert!(!all_same_compaction_scope(
            &["a|b|ts/V2", "a|ts/V2"],
            &[(0, 3600), (0, 3600)],
        ));

        // Same start, different duration.
        assert!(!all_same_compaction_scope(
            &["a|b|ts/V2", "a|b|ts/V2"],
            &[(0, 900), (0, 1800)],
        ));

        // Different start.
        assert!(!all_same_compaction_scope(
            &["a|b|ts/V2", "a|b|ts/V2"],
            &[(0, 3600), (3600, 3600)],
        ));
    }
}
