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
/// The hash includes:
/// - `metric_name`
/// - `metric_type` (Gauge=0, Sum=1, etc.)
/// - All tag key-value pairs except temporal columns (see [`EXCLUDED_TAGS`])
///
/// Tags are sorted by key before hashing so the result is independent of
/// HashMap iteration order.
///
/// Uses SipHash-2-4 with fixed keys for deterministic, well-distributed output.
/// Returns an `i64` for the `timeseries_id` sort schema column.
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
    fn test_different_tags_different_id() {
        let mut tags1 = HashMap::new();
        tags1.insert("service".to_string(), "api".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("service".to_string(), "web".to_string());

        let id1 = compute_timeseries_id("cpu.usage", 0, &tags1);
        let id2 = compute_timeseries_id("cpu.usage", 0, &tags2);
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_excludes_start_timestamp_secs() {
        let mut tags1 = HashMap::new();
        tags1.insert("service".to_string(), "api".to_string());
        tags1.insert("start_timestamp_secs".to_string(), "1000".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("service".to_string(), "api".to_string());
        tags2.insert("start_timestamp_secs".to_string(), "2000".to_string());

        let id1 = compute_timeseries_id("cpu.usage", 0, &tags1);
        let id2 = compute_timeseries_id("cpu.usage", 0, &tags2);
        assert_eq!(id1, id2, "start_timestamp_secs should not affect the hash");
    }

    #[test]
    fn test_empty_tags() {
        let tags = HashMap::new();
        let id = compute_timeseries_id("cpu.usage", 0, &tags);
        // Just verify it doesn't panic and returns a value.
        let _ = id;
    }

    #[test]
    fn test_tag_insertion_order_independent() {
        // Build two HashMaps with the same entries but different insertion order.
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

    #[test]
    fn test_tag_key_value_not_interchangeable() {
        // Ensure ("a", "b") and ("b", "a") produce different hashes.
        let mut tags1 = HashMap::new();
        tags1.insert("key".to_string(), "value".to_string());

        let mut tags2 = HashMap::new();
        tags2.insert("value".to_string(), "key".to_string());

        let id1 = compute_timeseries_id("m", 0, &tags1);
        let id2 = compute_timeseries_id("m", 0, &tags2);
        assert_ne!(id1, id2);
    }
}
