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

//! Planning logic for mature-merge: pure, synchronous functions that decide
//! which splits to merge and how to group them. No I/O, no actors.
//!
//! The main entry point is [`plan_merge_operations_for_index`].

use std::collections::HashMap;
use std::time::Duration;

use quickwit_config::IndexConfig;
use quickwit_metastore::SplitMetadata;
use time::OffsetDateTime;

use crate::mature_merge::MatureMergeConfig;
use crate::merge_policy::MergeOperation;

/// Computes the earliest UTC-day midnight (seconds since epoch) that is safe to merge,
/// given the index's retention policy and the current time.
fn retention_safety_cutoff_secs(
    index_config: &IndexConfig,
    now_secs: i64,
    config: &MatureMergeConfig,
) -> Option<i64> {
    let retention_policy = index_config.retention_policy_opt.as_ref()?;
    let period = retention_policy.retention_period().ok()?;
    let retention_safety_buffer = Duration::from_hours(config.retention_safety_buffer_days * 24);
    if period <= retention_safety_buffer {
        // No safe window: exclude every split by returning a cutoff in the far future.
        return Some(i64::MAX);
    }
    let cutoff_raw = now_secs - period.as_secs() as i64 + retention_safety_buffer.as_secs() as i64;
    // Round up to the next day boundary so we never partially exclude a day bucket.
    Some((cutoff_raw / 86400 + 1) * 86400)
}

/// Converts a single day-bucket group of eligible splits into one or more balanced
/// [`MergeOperation`]s respecting constraints.
fn plan_operations_for_group(
    mut group_splits: Vec<SplitMetadata>,
    config: &MatureMergeConfig,
) -> Vec<MergeOperation> {
    if group_splits.len() < config.min_merge_group_size {
        return Vec::new();
    }
    if !group_splits
        .iter()
        .all(|s| s.num_docs < config.input_split_max_num_docs)
    {
        return Vec::new();
    }
    // Sort ascending by end time so each sub-operation covers the most compact range.
    group_splits.sort_by_key(|s| s.time_range.as_ref().map_or(0, |r| *r.end()));

    let n = group_splits.len();
    let total_docs: usize = group_splits.iter().map(|s| s.num_docs).sum();

    // Minimum number of balanced operations needed to respect both per-operation limits.
    let k = ((n + config.max_merge_group_size - 1) / config.max_merge_group_size)
        .max((total_docs + config.split_target_num_docs - 1) / config.split_target_num_docs)
        .max(1);

    // Divide into k balanced chunks (first chunks are ≥ last chunks by at most 1 split).
    let chunk_size = (n + k - 1) / k;
    group_splits
        .chunks(chunk_size)
        .filter(|chunk| chunk.len() >= config.min_merge_group_size)
        .map(|chunk| MergeOperation::new_merge_operation(chunk.to_vec()))
        .collect()
}

/// Group by UTC day (floored to midnight in seconds) of the split's time range,
/// and returns one or more [`MergeOperation`]s per group that meets the size
/// threshold.
///
/// Rules:
/// - Splits without a `time_range` are skipped (cannot assign a day).
/// - A split is only assigned to a bucket when *both* `time_range.start()` and `time_range.end()`
///   fall on the same UTC day (i.e., the split does not span midnight).
/// - Immature splits are excluded.
/// - Splits whose `time_range.end()` falls within the retention safety buffer are excluded.
pub fn plan_merge_operations_for_index(
    index_config: &IndexConfig,
    splits: Vec<SplitMetadata>,
    now: OffsetDateTime,
    config: &MatureMergeConfig,
) -> Vec<MergeOperation> {
    let now_secs = now.unix_timestamp();

    let earliest_cutoff_timestamp = retention_safety_cutoff_secs(index_config, now_secs, config);

    // Key: (partition_id, doc_mapping_uid_string, day_bucket_seconds)
    let mut groups: HashMap<(u64, String, i64), Vec<SplitMetadata>> = HashMap::new();

    for split in splits {
        // Only mature splits.
        if !split.is_mature(now) {
            continue;
        }

        // The timestamp field is required
        let Some(ref time_range) = split.time_range else {
            continue;
        };

        let start_day = time_range.start() / 86400;
        let end_day = time_range.end() / 86400;

        // Both endpoints must fall on the same UTC day.
        if start_day != end_day {
            continue;
        }

        // Check that we are not too close to the retention cutoff.
        if let Some(cutoff) = earliest_cutoff_timestamp {
            if *time_range.end() < cutoff {
                continue;
            }
        }

        let day_bucket = start_day * 86400;
        let key = (
            split.partition_id,
            split.doc_mapping_uid.to_string(),
            day_bucket,
        );
        groups.entry(key).or_default().push(split);
    }

    let mut operations = Vec::new();
    for (_key, group_splits) in groups {
        operations.extend(plan_operations_for_group(group_splits, config));
    }
    operations
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_config::{IndexConfig, RetentionPolicy};
    use quickwit_metastore::{SplitMaturity, SplitMetadata};
    use quickwit_proto::types::{DocMappingUid, IndexUid};
    use time::OffsetDateTime;

    use super::*;

    /// Builds a mature [`SplitMetadata`] for use in tests.
    ///
    /// - `day_bucket`: UTC day expressed as seconds-since-epoch (midnight). For example `day_bucket
    ///   = 0` means 1970-01-01, `day_bucket = 86400` means 1970-01-02.
    fn mature_split_for_test(
        split_id: &str,
        index_uid: &IndexUid,
        partition_id: u64,
        doc_mapping_uid: DocMappingUid,
        num_docs: usize,
        day_bucket: i64,
    ) -> SplitMetadata {
        SplitMetadata {
            split_id: split_id.to_string(),
            index_uid: index_uid.clone(),
            partition_id,
            num_docs,
            doc_mapping_uid,
            // Both endpoints on the same UTC day — the split spans one hour.
            time_range: Some(day_bucket..=(day_bucket + 3600)),
            maturity: SplitMaturity::Mature,
            ..Default::default()
        }
    }

    fn index_config_no_retention() -> IndexConfig {
        IndexConfig::for_test("test-index", "s3://test-bucket/test-index")
    }

    fn index_config_with_retention(period: &str) -> IndexConfig {
        let mut config = index_config_no_retention();
        config.retention_policy_opt = Some(RetentionPolicy {
            retention_period: period.to_string(),
            evaluation_schedule: "daily".to_string(),
            timestamp_type: Default::default(),
        });
        config
    }

    // UTC day 0 = 1970-01-01.  Use a recent-ish day to avoid the retention buffer.
    // We use day 20000 (approx 2024-10) so splits are "recent" relative to a "now" we control.
    const RECENT_DAY: i64 = 20_000 * 86400;

    fn now_well_after_recent_day() -> OffsetDateTime {
        // 1 day after the splits' day — they are mature but not in a retention buffer.
        OffsetDateTime::from_unix_timestamp(RECENT_DAY + 86400 + 1).unwrap()
    }

    #[test]
    fn test_plan_basic() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let doc_mapping_uid = DocMappingUid::random();
        let splits: Vec<SplitMetadata> = (0..10)
            .map(|i| {
                mature_split_for_test(
                    &format!("split-{i}"),
                    &index_uid,
                    1,
                    doc_mapping_uid,
                    100,
                    RECENT_DAY,
                )
            })
            .collect();

        let operations = plan_merge_operations_for_index(
            &index_config_no_retention(),
            splits,
            now_well_after_recent_day(),
            &MatureMergeConfig::default(),
        );

        assert_eq!(operations.len(), 1);
        assert_eq!(operations[0].splits.len(), 10);
    }

    #[test]
    fn test_plan_below_threshold() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let doc_mapping_uid = DocMappingUid::random();
        // Only 4 splits — below MIN_MERGE_GROUP_SIZE (6).
        let splits: Vec<SplitMetadata> = (0..4)
            .map(|i| {
                mature_split_for_test(
                    &format!("split-{i}"),
                    &index_uid,
                    1,
                    doc_mapping_uid,
                    100,
                    RECENT_DAY,
                )
            })
            .collect();

        let operations = plan_merge_operations_for_index(
            &index_config_no_retention(),
            splits,
            now_well_after_recent_day(),
            &MatureMergeConfig::default(),
        );

        assert!(operations.is_empty(), "expected no operations for 4 splits");
    }

    #[test]
    fn test_plan_immature_splits_excluded() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let doc_mapping_uid = DocMappingUid::random();
        let now = now_well_after_recent_day();
        let now_ts = now.unix_timestamp();

        // All splits are immature (maturation period far in the future).
        let splits: Vec<SplitMetadata> = (0..10)
            .map(|i| {
                let mut split = mature_split_for_test(
                    &format!("split-{i}"),
                    &index_uid,
                    1,
                    doc_mapping_uid,
                    100,
                    RECENT_DAY,
                );
                split.maturity = SplitMaturity::Immature {
                    maturation_period: Duration::from_secs(999_999),
                };
                // Make sure create_timestamp is recent so the split is truly immature.
                split.create_timestamp = now_ts;
                split
            })
            .collect();

        let operations = plan_merge_operations_for_index(
            &index_config_no_retention(),
            splits,
            now,
            &MatureMergeConfig::default(),
        );

        assert!(operations.is_empty(), "immature splits should be excluded");
    }

    #[test]
    fn test_plan_multiday_split_skipped() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let doc_mapping_uid = DocMappingUid::random();

        // 10 splits, but each one spans midnight (start on day N, end on day N+1).
        let splits: Vec<SplitMetadata> = (0..10)
            .map(|i| {
                let mut split = mature_split_for_test(
                    &format!("split-{i}"),
                    &index_uid,
                    1,
                    doc_mapping_uid,
                    100,
                    RECENT_DAY,
                );
                // Extend time_range to cross midnight.
                split.time_range = Some(RECENT_DAY - 3600..=RECENT_DAY + 3600);
                split
            })
            .collect();

        let operations = plan_merge_operations_for_index(
            &index_config_no_retention(),
            splits,
            now_well_after_recent_day(),
            &MatureMergeConfig::default(),
        );

        assert!(operations.is_empty(), "multi-day splits should be skipped");
    }

    #[test]
    fn test_plan_retention_safety_buffer() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let doc_mapping_uid = DocMappingUid::random();

        // Retention period = 90 days.  Safety buffer = 30 days.
        // Splits must have time_range.end >= now - 90d + 30d = now - 60d.
        // We put splits at RECENT_DAY but set "now" to be RECENT_DAY + 91 days.
        // Then: cutoff_raw = (RECENT_DAY + 91d) - 90d + 30d = RECENT_DAY + 31d
        //       cutoff = RECENT_DAY + 32d (rounded up to next day boundary)
        // Because RECENT_DAY + 3600 < cutoff, splits should be excluded.
        let now_ts = RECENT_DAY + 91 * 86400;
        let now = OffsetDateTime::from_unix_timestamp(now_ts).unwrap();

        let splits: Vec<SplitMetadata> = (0..10)
            .map(|i| {
                mature_split_for_test(
                    &format!("split-{i}"),
                    &index_uid,
                    1,
                    doc_mapping_uid,
                    100,
                    RECENT_DAY,
                )
            })
            .collect();

        let config = index_config_with_retention("90 days");

        let merge_config = MatureMergeConfig {
            retention_safety_buffer_days: 30,
            ..MatureMergeConfig::default()
        };
        let operations = plan_merge_operations_for_index(&config, splits, now, &merge_config);

        assert!(
            operations.is_empty(),
            "splits within retention safety buffer should be excluded"
        );
    }

    #[test]
    fn test_plan_retention_period_too_short_skipped() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let doc_mapping_uid = DocMappingUid::random();

        let splits: Vec<SplitMetadata> = (0..10)
            .map(|i| {
                mature_split_for_test(
                    &format!("split-{i}"),
                    &index_uid,
                    1,
                    doc_mapping_uid,
                    100,
                    RECENT_DAY,
                )
            })
            .collect();

        // Retention period of 3 days is <= retention_safety_buffer_days (default 5 days)
        // so the index should be skipped entirely.
        let config = index_config_with_retention("3 days");

        let operations = plan_merge_operations_for_index(
            &config,
            splits,
            now_well_after_recent_day(),
            &MatureMergeConfig::default(),
        );

        assert!(
            operations.is_empty(),
            "index with short retention should produce no operations"
        );
    }

    #[test]
    fn test_plan_different_partitions_grouped_separately() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let doc_mapping_uid = DocMappingUid::random();

        // 6 splits per partition, two partitions => 2 separate merge operations.
        let splits: Vec<SplitMetadata> = (0..12)
            .map(|i| {
                mature_split_for_test(
                    &format!("split-{i}"),
                    &index_uid,
                    i as u64 / 6, // partition 0 for i in 0..6, partition 1 for i in 6..12
                    doc_mapping_uid,
                    100,
                    RECENT_DAY,
                )
            })
            .collect();

        let mut operations = plan_merge_operations_for_index(
            &index_config_no_retention(),
            splits,
            now_well_after_recent_day(),
            &MatureMergeConfig::default(),
        );
        operations.sort_by_key(|op| op.splits[0].partition_id);

        assert_eq!(operations.len(), 2);
        assert!(operations[0].splits.iter().all(|s| s.partition_id == 0));
        assert!(operations[1].splits.iter().all(|s| s.partition_id == 1));
    }
}
