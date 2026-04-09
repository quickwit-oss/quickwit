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

//! Deterministic timeseries ID computation for the Parquet pipeline.
//!
//! The timeseries ID is a hash of all columns that identify a unique timeseries,
//! excluding temporal data (timestamps) and metric values. Two data points
//! from the same timeseries always produce the same ID, regardless of when
//! they were recorded or their payload.
//!
//! # Stability contract
//!
//! The hash value for a given set of inputs **must never change** across builds,
//! Rust versions, or process restarts. It is persisted in Parquet files and used
//! as a sort/grouping key. Any change would silently corrupt compaction and
//! query results.
//!
//! The implementation uses SipHash-2-4 with fixed keys `(0, 0)` from the
//! `siphasher` crate, and feeds bytes via Rust's `Hash` trait. The `Hash`
//! implementation for `str` writes `bytes ++ [0xFF]` and for `u8` writes
//! `[byte]`; this has been stable since Rust 1.0. A pinned stability test
//! (`test_hash_stability_pinned`) will catch any regression.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use siphasher::sip::SipHasher;

/// Tag columns excluded from the timeseries ID hash.
///
/// Temporal and value columns vary per data point within a timeseries and
/// must not contribute to the hash.
///
/// - `timestamp_secs` and `value` are fixed fields on `MetricDataPoint`
///   (not in the tags HashMap) and are excluded by construction — they are
///   never passed to the hash function.
/// - `start_timestamp_secs` is the OTLP delta-window start time, stored
///   as a tag. It varies per data point so must be excluded here.
/// - `timestamp` is the generic well-known timestamp name from the sort
///   schema system. Excluded defensively in case it appears as a
///   user-provided attribute.
/// - DDSketch value columns (`count`, `sum`, `min`, `max`, `flags`,
///   `keys`, `counts`) are per-data-point sketch components.
pub const EXCLUDED_TAGS: &[&str] = &[
    "count",
    "counts",
    "flags",
    "keys",
    "max",
    "min",
    "start_timestamp_secs",
    "sum",
    "timestamp",
    "timestamp_secs",
    "value",
];

/// Compute a deterministic timeseries ID from metric identity columns.
///
/// # Hash inputs (in order)
///
/// 1. `metric_name` (string, via `Hash` trait — writes bytes + 0xFF)
/// 2. `metric_type` (u8, via `Hash` trait — writes single byte)
/// 3. For each tag in **lexicographic key order**, excluding [`EXCLUDED_TAGS`]:
///    - tag key (string)
///    - tag value (string)
///
/// The tag sort ensures the result is independent of HashMap iteration order.
///
/// # Algorithm
///
/// SipHash-2-4 with fixed keys `(0, 0)` via the `siphasher` crate.
/// Returns the 64-bit hash truncated to `i64` for the `timeseries_id`
/// sort schema column.
pub fn compute_timeseries_id(
    metric_name: &str,
    metric_type: u8,
    tags: &HashMap<String, String>,
) -> i64 {
    let mut hasher = SipHasher::new_with_keys(0, 0);

    metric_name.hash(&mut hasher);
    metric_type.hash(&mut hasher);

    // Collect and sort tags for deterministic ordering.
    let mut sorted_tags: Vec<(&str, &str)> = tags
        .iter()
        .filter(|(k, _)| !EXCLUDED_TAGS.contains(&k.as_str()))
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    sorted_tags.sort_unstable_by_key(|(k, _)| *k);

    for (key, value) in sorted_tags {
        key.hash(&mut hasher);
        value.hash(&mut hasher);
    }

    hasher.finish() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------
    // Stability: pinned expected values
    //
    // If any of these fail, the on-disk hash contract is broken.
    // DO NOT update the expected values — fix the implementation.
    // ---------------------------------------------------------------

    #[test]
    fn test_hash_stability_pinned_no_tags() {
        let tags = HashMap::new();
        let id = compute_timeseries_id("cpu.usage", 0, &tags);
        assert_eq!(
            id, 8585688161913568022,
            "pinned hash for (cpu.usage, Gauge, no tags) must not change"
        );
    }

    #[test]
    fn test_hash_stability_pinned_with_tags() {
        let mut tags = HashMap::new();
        tags.insert("env".to_string(), "prod".to_string());
        tags.insert("host".to_string(), "node-1".to_string());
        tags.insert("service".to_string(), "api".to_string());

        let id = compute_timeseries_id("cpu.usage", 0, &tags);
        assert_eq!(
            id, -1249054409005369755,
            "pinned hash for (cpu.usage, Gauge, env=prod, host=node-1, service=api) must not change"
        );
    }

    #[test]
    fn test_hash_stability_pinned_sum_type() {
        let mut tags = HashMap::new();
        tags.insert("service".to_string(), "web".to_string());

        let id = compute_timeseries_id("http.requests", 1, &tags);
        assert_eq!(
            id, -2615727422124831097,
            "pinned hash for (http.requests, Sum, service=web) must not change"
        );
    }

    // ---------------------------------------------------------------
    // Core invariant: same series identity → same hash
    // ---------------------------------------------------------------

    #[test]
    fn test_deterministic_same_input() {
        let mut tags = HashMap::new();
        tags.insert("service".to_string(), "api".to_string());
        tags.insert("env".to_string(), "prod".to_string());
        tags.insert("host".to_string(), "node-1".to_string());

        let id1 = compute_timeseries_id("cpu.usage", 0, &tags);
        let id2 = compute_timeseries_id("cpu.usage", 0, &tags);
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_same_series_different_timestamps_same_id() {
        // Two data points from the same series that differ only in
        // temporal/value columns (which are excluded) must hash equal.
        let mut tags1 = HashMap::new();
        tags1.insert("service".to_string(), "api".to_string());
        tags1.insert("host".to_string(), "node-1".to_string());
        tags1.insert("start_timestamp_secs".to_string(), "1000".to_string());
        tags1.insert("timestamp_secs".to_string(), "1010".to_string());
        tags1.insert("value".to_string(), "42.5".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("service".to_string(), "api".to_string());
        tags2.insert("host".to_string(), "node-1".to_string());
        tags2.insert("start_timestamp_secs".to_string(), "2000".to_string());
        tags2.insert("timestamp_secs".to_string(), "2010".to_string());
        tags2.insert("value".to_string(), "99.9".to_string());

        let id1 = compute_timeseries_id("cpu.usage", 0, &tags1);
        let id2 = compute_timeseries_id("cpu.usage", 0, &tags2);
        assert_eq!(
            id1, id2,
            "same series with different timestamps/values must produce the same hash"
        );
    }

    // ---------------------------------------------------------------
    // Discrimination: different identity → different hash
    // ---------------------------------------------------------------

    #[test]
    fn test_different_metric_name_different_id() {
        let mut tags = HashMap::new();
        tags.insert("service".to_string(), "api".to_string());

        let id1 = compute_timeseries_id("cpu.usage", 0, &tags);
        let id2 = compute_timeseries_id("mem.usage", 0, &tags);
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_different_metric_type_different_id() {
        let mut tags = HashMap::new();
        tags.insert("service".to_string(), "api".to_string());

        let id_gauge = compute_timeseries_id("requests", 0, &tags);
        let id_sum = compute_timeseries_id("requests", 1, &tags);
        assert_ne!(id_gauge, id_sum);
    }

    #[test]
    fn test_different_tag_value_different_id() {
        let mut tags1 = HashMap::new();
        tags1.insert("service".to_string(), "api".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("service".to_string(), "web".to_string());

        let id1 = compute_timeseries_id("cpu.usage", 0, &tags1);
        let id2 = compute_timeseries_id("cpu.usage", 0, &tags2);
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_extra_tag_different_id() {
        let mut tags1 = HashMap::new();
        tags1.insert("service".to_string(), "api".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("service".to_string(), "api".to_string());
        tags2.insert("env".to_string(), "prod".to_string());

        let id1 = compute_timeseries_id("cpu.usage", 0, &tags1);
        let id2 = compute_timeseries_id("cpu.usage", 0, &tags2);
        assert_ne!(id1, id2, "adding a tag must change the hash");
    }

    #[test]
    fn test_tag_key_value_not_interchangeable() {
        // ("key", "value") vs ("value", "key") — different tag names.
        let mut tags1 = HashMap::new();
        tags1.insert("key".to_string(), "value".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("value".to_string(), "key".to_string());

        let id1 = compute_timeseries_id("m", 0, &tags1);
        let id2 = compute_timeseries_id("m", 0, &tags2);
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_boundary_ambiguity_key_value_split() {
        // {"ab": "c"} vs {"a": "bc"} must differ.
        // This works because str::hash writes bytes + 0xFF separator.
        let mut tags1 = HashMap::new();
        tags1.insert("ab".to_string(), "c".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("a".to_string(), "bc".to_string());

        let id1 = compute_timeseries_id("m", 0, &tags1);
        let id2 = compute_timeseries_id("m", 0, &tags2);
        assert_ne!(
            id1, id2,
            "different key/value splits must produce different hashes"
        );
    }

    // ---------------------------------------------------------------
    // Order independence
    // ---------------------------------------------------------------

    #[test]
    fn test_tag_insertion_order_independent() {
        let mut tags1 = HashMap::new();
        tags1.insert("z_tag".to_string(), "z_val".to_string());
        tags1.insert("a_tag".to_string(), "a_val".to_string());
        tags1.insert("m_tag".to_string(), "m_val".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("a_tag".to_string(), "a_val".to_string());
        tags2.insert("m_tag".to_string(), "m_val".to_string());
        tags2.insert("z_tag".to_string(), "z_val".to_string());

        let id1 = compute_timeseries_id("cpu.usage", 0, &tags1);
        let id2 = compute_timeseries_id("cpu.usage", 0, &tags2);
        assert_eq!(id1, id2, "tag insertion order must not affect the hash");
    }

    // ---------------------------------------------------------------
    // Excluded columns: every entry in EXCLUDED_TAGS must be ignored
    // ---------------------------------------------------------------

    #[test]
    fn test_all_excluded_tags_are_ignored() {
        let mut base_tags = HashMap::new();
        base_tags.insert("service".to_string(), "api".to_string());
        base_tags.insert("env".to_string(), "prod".to_string());

        let base_id = compute_timeseries_id("cpu.usage", 0, &base_tags);

        for &excluded in EXCLUDED_TAGS {
            let mut tags_with_excluded = base_tags.clone();
            tags_with_excluded.insert(excluded.to_string(), "some_value".to_string());

            let id = compute_timeseries_id("cpu.usage", 0, &tags_with_excluded);
            assert_eq!(
                id, base_id,
                "excluded tag '{}' must not affect the hash",
                excluded
            );
        }
    }

    #[test]
    fn test_excluded_tags_with_varying_values() {
        let mut tags1 = HashMap::new();
        tags1.insert("service".to_string(), "api".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("service".to_string(), "api".to_string());

        // Add every excluded tag with different values to each.
        for (i, &excluded) in EXCLUDED_TAGS.iter().enumerate() {
            tags1.insert(excluded.to_string(), format!("val_a_{}", i));
            tags2.insert(excluded.to_string(), format!("val_b_{}", i));
        }

        let id1 = compute_timeseries_id("cpu.usage", 0, &tags1);
        let id2 = compute_timeseries_id("cpu.usage", 0, &tags2);
        assert_eq!(
            id1, id2,
            "excluded tags with different values must not affect the hash"
        );
    }

    // ---------------------------------------------------------------
    // Edge cases
    // ---------------------------------------------------------------

    #[test]
    fn test_empty_tags() {
        let tags = HashMap::new();
        // Must not panic; value is tested by the pinned stability test.
        let _ = compute_timeseries_id("cpu.usage", 0, &tags);
    }

    #[test]
    fn test_empty_string_tag_key_and_value() {
        let mut tags1 = HashMap::new();
        tags1.insert("".to_string(), "".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("a".to_string(), "".to_string());

        let id1 = compute_timeseries_id("m", 0, &tags1);
        let id2 = compute_timeseries_id("m", 0, &tags2);
        assert_ne!(id1, id2, "empty key must differ from non-empty key");
    }

    #[test]
    fn test_unicode_tags() {
        let mut tags1 = HashMap::new();
        tags1.insert("region".to_string(), "\u{1F30D}".to_string()); // globe emoji

        let mut tags2 = HashMap::new();
        tags2.insert("region".to_string(), "\u{1F30E}".to_string()); // different globe

        let id1 = compute_timeseries_id("m", 0, &tags1);
        let id2 = compute_timeseries_id("m", 0, &tags2);
        assert_ne!(id1, id2, "different unicode values must produce different hashes");
    }

    #[test]
    fn test_many_tags() {
        let mut tags = HashMap::new();
        for i in 0..100 {
            tags.insert(format!("tag_{:03}", i), format!("val_{}", i));
        }
        let id1 = compute_timeseries_id("m", 0, &tags);
        let id2 = compute_timeseries_id("m", 0, &tags);
        assert_eq!(id1, id2, "hash must be deterministic with many tags");
    }

    #[test]
    fn test_empty_metric_name() {
        let tags = HashMap::new();
        let id1 = compute_timeseries_id("", 0, &tags);
        let id2 = compute_timeseries_id("x", 0, &tags);
        assert_ne!(id1, id2);
    }
}

/// Property-based tests for order independence and collision resistance.
#[cfg(test)]
mod proptests {
    use super::*;

    use proptest::prelude::*;

    /// Generate a HashMap<String, String> with 0..max_tags entries.
    fn arb_tags(max_tags: usize) -> impl Strategy<Value = HashMap<String, String>> {
        proptest::collection::hash_map("[a-z]{1,8}", "[a-z0-9]{0,16}", 0..max_tags)
    }

    proptest! {
        /// Core property: for any tag set, the hash must be the same
        /// regardless of which order the entries are iterated.
        ///
        /// We verify this by computing twice (HashMap iteration order is
        /// not guaranteed to be the same across builds, but the sort inside
        /// compute_timeseries_id must canonicalize it). We also rebuild
        /// the map in reverse-sorted key order to force a different
        /// internal layout.
        #[test]
        fn prop_order_independent(
            metric_name in "[a-z.]{1,20}",
            metric_type in 0u8..5,
            tags in arb_tags(15),
        ) {
            let id_original = compute_timeseries_id(&metric_name, metric_type, &tags);

            // Rebuild the HashMap with keys inserted in reverse-sorted order.
            let mut keys: Vec<&String> = tags.keys().collect();
            keys.sort();
            keys.reverse();
            let mut reversed = HashMap::with_capacity(tags.len());
            for key in keys {
                reversed.insert(key.clone(), tags[key].clone());
            }

            let id_reversed = compute_timeseries_id(&metric_name, metric_type, &reversed);
            prop_assert_eq!(id_original, id_reversed,
                "hash must be identical regardless of tag insertion order");
        }

        /// Excluded tags must never affect the hash.
        #[test]
        fn prop_excluded_tags_ignored(
            metric_name in "[a-z.]{1,20}",
            metric_type in 0u8..5,
            base_tags in arb_tags(10),
            excluded_value in "[a-z0-9]{1,16}",
        ) {
            let base_id = compute_timeseries_id(&metric_name, metric_type, &base_tags);

            for &excluded_key in EXCLUDED_TAGS {
                let mut augmented = base_tags.clone();
                augmented.insert(excluded_key.to_string(), excluded_value.clone());

                let augmented_id = compute_timeseries_id(&metric_name, metric_type, &augmented);
                prop_assert_eq!(base_id, augmented_id,
                    "excluded tag '{}' must not change the hash", excluded_key);
            }
        }

        /// Adding a non-excluded tag must (almost certainly) change the hash.
        /// We use a fresh key name that won't collide with excluded tags.
        #[test]
        fn prop_extra_tag_changes_hash(
            metric_name in "[a-z.]{1,20}",
            metric_type in 0u8..5,
            base_tags in arb_tags(5),
            extra_key in "xtag_[a-z]{1,8}",
            extra_value in "[a-z0-9]{1,8}",
        ) {
            // Skip if the extra key already exists in base_tags.
            prop_assume!(!base_tags.contains_key(&extra_key));

            let base_id = compute_timeseries_id(&metric_name, metric_type, &base_tags);

            let mut augmented = base_tags.clone();
            augmented.insert(extra_key, extra_value);
            let augmented_id = compute_timeseries_id(&metric_name, metric_type, &augmented);

            prop_assert_ne!(base_id, augmented_id,
                "adding a non-excluded tag should change the hash (collision is theoretically \
                 possible but astronomically unlikely for 64-bit hash)");
        }
    }
}
