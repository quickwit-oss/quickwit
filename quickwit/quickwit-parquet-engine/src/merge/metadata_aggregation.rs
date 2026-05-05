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

//! Metadata assembly for Parquet merge output splits.
//!
//! Each [`MergeOutputFile`] carries both physical metadata (rows, bytes,
//! row_keys, zonemaps) and per-output logical metadata (metric_names, tags,
//! time_range) extracted from the actual rows during the merge write pass.
//! This module combines those with invariant fields (kind, index_uid,
//! partition_id, sort_fields, window) from the input splits to produce
//! complete [`ParquetSplitMetadata`].

use std::collections::HashSet;
use std::time::SystemTime;

use anyhow::{Result, bail};

use super::MergeOutputFile;
use crate::split::{ParquetSplitId, ParquetSplitMetadata};

/// Builds complete [`ParquetSplitMetadata`] for a merge output file.
///
/// Data-dependent fields (metric_names, time_range, tags) come from the
/// `MergeOutputFile`, which extracted them from the actual rows in this
/// output during the merge write pass. Invariant fields (kind, index_uid,
/// partition_id, sort_fields, window) come from the input splits (all must
/// agree due to compaction scope grouping / MP-3).
///
/// # Preconditions
///
/// All input splits must share the same kind, index_uid, partition_id,
/// sort_fields, and window.
pub fn merge_parquet_split_metadata(
    inputs: &[ParquetSplitMetadata],
    output: &MergeOutputFile,
) -> Result<ParquetSplitMetadata> {
    if inputs.is_empty() {
        bail!("merge_parquet_split_metadata requires at least one input split");
    }

    let first = &inputs[0];

    // Validate invariant fields: all inputs must agree on these.
    for (i, input) in inputs.iter().enumerate().skip(1) {
        if input.kind != first.kind {
            bail!(
                "input {} has kind {:?}, expected {:?}",
                i,
                input.kind,
                first.kind
            );
        }
        if input.index_uid != first.index_uid {
            bail!(
                "input {} has index_uid '{}', expected '{}'",
                i,
                input.index_uid,
                first.index_uid
            );
        }
        if input.partition_id != first.partition_id {
            bail!(
                "input {} has partition_id {}, expected {}",
                i,
                input.partition_id,
                first.partition_id
            );
        }
        if input.sort_fields != first.sort_fields {
            bail!(
                "input {} has sort_fields '{}', expected '{}'",
                i,
                input.sort_fields,
                first.sort_fields
            );
        }
        if input.window != first.window {
            bail!(
                "input {} has window {:?}, expected {:?}",
                i,
                input.window,
                first.window
            );
        }
        if input.rg_partition_prefix_len != first.rg_partition_prefix_len {
            bail!(
                "input {} has rg_partition_prefix_len {}, expected {} — splits with different \
                 prefix lengths must not appear in the same merge",
                i,
                input.rg_partition_prefix_len,
                first.rg_partition_prefix_len
            );
        }
    }

    // Each merge adds one to the lineage depth. The policy uses this to
    // decide when a split is "mature" (reached max_merge_ops).
    let num_merge_ops = inputs
        .iter()
        .map(|s| s.num_merge_ops)
        .max()
        .expect("at least one input")
        + 1;

    // The generated split ID determines the expected filename. The caller
    // (ParquetMergeExecutor) renames the merge engine's output file to match
    // this name before handing it to the uploader.
    let split_id = ParquetSplitId::generate(first.kind);
    let parquet_file = format!("{split_id}.parquet");

    // `rg_partition_prefix_len` propagation rule: a single-row-group
    // output vacuously satisfies any prefix claim (no boundary to
    // misalign), so we keep the inputs' prefix. Multi-RG output with
    // arbitrary row-count-driven boundaries (the only kind the current
    // merge writer can produce) cannot honor a non-zero claim and must
    // reset to 0. PR-6 (streaming column-major merge engine) will
    // produce sort-prefix-aligned multi-RG output and propagate the
    // prefix unconditionally.
    //
    // This must agree with the value the writer embeds in the file's
    // `qh.rg_partition_prefix_len` KV — see `write_merge_outputs`.
    let output_prefix_len = if output.num_row_groups <= 1 {
        first.rg_partition_prefix_len
    } else {
        0
    };

    // Data-dependent fields come from the MergeOutputFile (extracted from
    // this output's actual rows during the merge write pass).
    let mut metadata = ParquetSplitMetadata {
        kind: first.kind,
        partition_id: first.partition_id,
        split_id,
        index_uid: first.index_uid.clone(),
        time_range: output.time_range,
        num_rows: output.num_rows as u64,
        size_bytes: output.size_bytes,
        metric_names: output.metric_names.clone(),
        low_cardinality_tags: output.low_cardinality_tags.clone(),
        high_cardinality_tag_keys: HashSet::new(),
        created_at: SystemTime::now(),
        parquet_file,
        window: first.window.clone(),
        sort_fields: first.sort_fields.clone(),
        num_merge_ops,
        row_keys_proto: output.row_keys_proto.clone(),
        zonemap_regexes: output.zonemap_regexes.clone(),
        rg_partition_prefix_len: output_prefix_len,
    };

    // Finalize: tag sets may exceed the cardinality threshold.
    metadata.finalize_tag_cardinality();

    Ok(metadata)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use super::*;
    use crate::split::{ParquetSplitId, ParquetSplitKind, ParquetSplitMetadata, TimeRange};

    /// Helper to build a test split with the given properties.
    fn make_test_split(
        split_id: &str,
        time_range: (u64, u64),
        num_merge_ops: u32,
    ) -> ParquetSplitMetadata {
        ParquetSplitMetadata::metrics_builder()
            .split_id(ParquetSplitId::new(split_id))
            .index_uid("test-index:00000000000000000000000001")
            .partition_id(42)
            .time_range(TimeRange::new(time_range.0, time_range.1))
            .num_rows(100)
            .size_bytes(5000)
            .sort_fields("metric_name|host|timestamp_secs/V2")
            .window_start_secs(1000)
            .window_duration_secs(3600)
            .num_merge_ops(num_merge_ops)
            .build()
    }

    fn make_output(num_rows: usize, size_bytes: u64) -> MergeOutputFile {
        make_output_with_metadata(num_rows, size_bytes, (1000, 2000), &["cpu.usage"])
    }

    fn make_output_with_metadata(
        num_rows: usize,
        size_bytes: u64,
        time_range: (u64, u64),
        metric_names: &[&str],
    ) -> MergeOutputFile {
        make_output_full(num_rows, size_bytes, 1, time_range, metric_names)
    }

    fn make_output_full(
        num_rows: usize,
        size_bytes: u64,
        num_row_groups: usize,
        time_range: (u64, u64),
        metric_names: &[&str],
    ) -> MergeOutputFile {
        MergeOutputFile {
            path: PathBuf::from("/tmp/merged.parquet"),
            num_rows,
            num_row_groups,
            size_bytes,
            row_keys_proto: Some(vec![0x08, 0x01]),
            zonemap_regexes: HashMap::from([("metric_name".to_string(), "cpu\\..*".to_string())]),
            metric_names: metric_names.iter().map(|s| s.to_string()).collect(),
            time_range: TimeRange::new(time_range.0, time_range.1),
            low_cardinality_tags: HashMap::new(),
        }
    }

    #[test]
    fn test_invariant_fields_from_inputs() {
        let inputs = vec![
            make_test_split("s0", (1000, 1500), 0),
            make_test_split("s1", (1200, 2000), 0),
        ];
        let output = make_output(200, 9000);
        let result = merge_parquet_split_metadata(&inputs, &output).unwrap();

        // Invariant fields come from inputs.
        assert_eq!(result.kind, ParquetSplitKind::Metrics);
        assert_eq!(result.index_uid, "test-index:00000000000000000000000001");
        assert_eq!(result.partition_id, 42);
        assert_eq!(result.sort_fields, "metric_name|host|timestamp_secs/V2");
        assert_eq!(result.window, Some(1000..4600));
        assert_eq!(result.num_merge_ops, 1);
        assert!(result.parquet_file.ends_with(".parquet"));
    }

    #[test]
    fn test_data_fields_from_output() {
        let inputs = vec![
            make_test_split("s0", (1000, 1500), 0),
            make_test_split("s1", (1200, 2000), 0),
        ];
        let output = make_output_with_metadata(200, 9000, (1000, 2000), &["cpu.usage", "mem.used"]);
        let result = merge_parquet_split_metadata(&inputs, &output).unwrap();

        // Data-dependent fields come from the output, not inputs.
        assert_eq!(result.time_range.start_secs, 1000);
        assert_eq!(result.time_range.end_secs, 2000);
        assert_eq!(result.num_rows, 200);
        assert_eq!(result.size_bytes, 9000);
        assert_eq!(result.metric_names.len(), 2);
        assert!(result.metric_names.contains("cpu.usage"));
        assert!(result.metric_names.contains("mem.used"));
        assert_eq!(result.row_keys_proto, Some(vec![0x08, 0x01]));
        assert_eq!(
            result.zonemap_regexes.get("metric_name").unwrap(),
            "cpu\\..*"
        );
    }

    #[test]
    fn test_tags_from_output() {
        let inputs = vec![
            make_test_split("s0", (1000, 2000), 0),
            make_test_split("s1", (1000, 2000), 0),
        ];
        let mut output = make_output(200, 9000);
        output
            .low_cardinality_tags
            .entry("service".to_string())
            .or_default()
            .insert("web".to_string());
        output
            .low_cardinality_tags
            .entry("service".to_string())
            .or_default()
            .insert("api".to_string());

        let result = merge_parquet_split_metadata(&inputs, &output).unwrap();

        let service_values = result.low_cardinality_tags.get("service").unwrap();
        assert_eq!(service_values.len(), 2);
        assert!(service_values.contains("web"));
        assert!(service_values.contains("api"));
    }

    #[test]
    fn test_tag_cardinality_promotion() {
        let inputs = vec![make_test_split("s0", (1000, 2000), 0)];
        let mut output = make_output(200, 9000);
        // Put 1001 unique host values in the output — exceeds threshold.
        for i in 0..1001 {
            output
                .low_cardinality_tags
                .entry("host".to_string())
                .or_default()
                .insert(format!("host-{i}"));
        }

        let result = merge_parquet_split_metadata(&inputs, &output).unwrap();

        assert!(result.high_cardinality_tag_keys.contains("host"));
        assert!(!result.low_cardinality_tags.contains_key("host"));
    }

    #[test]
    fn test_num_merge_ops_max_plus_one() {
        let inputs = vec![
            make_test_split("s0", (1000, 2000), 2),
            make_test_split("s1", (1000, 2000), 2),
            make_test_split("s2", (1000, 2000), 2),
        ];
        let output = make_output(300, 12000);
        let result = merge_parquet_split_metadata(&inputs, &output).unwrap();

        assert_eq!(result.num_merge_ops, 3); // max(2,2,2) + 1
    }

    #[test]
    fn test_empty_inputs_error() {
        let output = make_output(0, 0);
        let result = merge_parquet_split_metadata(&[], &output);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("at least one input")
        );
    }

    #[test]
    fn test_mismatched_kind_error() {
        let s0 = make_test_split("s0", (1000, 2000), 0);
        let mut s1 = make_test_split("s1", (1000, 2000), 0);
        s1.kind = ParquetSplitKind::Sketches;

        let output = make_output(200, 9000);
        let result = merge_parquet_split_metadata(&[s0, s1], &output);
        assert!(result.is_err());
    }

    #[test]
    fn test_mismatched_index_uid_error() {
        let s0 = make_test_split("s0", (1000, 2000), 0);
        let mut s1 = make_test_split("s1", (1000, 2000), 0);
        s1.index_uid = "other-index:00000000000000000000000002".to_string();

        let output = make_output(200, 9000);
        let result = merge_parquet_split_metadata(&[s0, s1], &output);
        assert!(result.is_err());
    }

    #[test]
    fn test_mismatched_partition_id_error() {
        let s0 = make_test_split("s0", (1000, 2000), 0);
        let mut s1 = make_test_split("s1", (1000, 2000), 0);
        s1.partition_id = 99;

        let output = make_output(200, 9000);
        let result = merge_parquet_split_metadata(&[s0, s1], &output);
        assert!(result.is_err());
    }

    #[test]
    fn test_mismatched_sort_fields_error() {
        let s0 = make_test_split("s0", (1000, 2000), 0);
        let mut s1 = make_test_split("s1", (1000, 2000), 0);
        s1.sort_fields = "different|schema/V2".to_string();

        let output = make_output(200, 9000);
        let result = merge_parquet_split_metadata(&[s0, s1], &output);
        assert!(result.is_err());
    }

    #[test]
    fn test_mismatched_window_error() {
        let s0 = make_test_split("s0", (1000, 2000), 0);
        let mut s1 = make_test_split("s1", (1000, 2000), 0);
        s1.window = Some(2000..5600);

        let output = make_output(200, 9000);
        let result = merge_parquet_split_metadata(&[s0, s1], &output);
        assert!(result.is_err());
    }

    #[test]
    fn test_mismatched_rg_partition_prefix_len_error() {
        let s0 = make_test_split("s0", (1000, 2000), 0);
        let mut s1 = make_test_split("s1", (1000, 2000), 0);
        s1.rg_partition_prefix_len = 1;

        let output = make_output(200, 9000);
        let result = merge_parquet_split_metadata(&[s0, s1], &output);
        let err = result.expect_err("merge must reject mismatched prefix lengths");
        let msg = err.to_string();
        assert!(
            msg.contains("rg_partition_prefix_len"),
            "error should mention rg_partition_prefix_len, got: {msg}"
        );
    }

    #[test]
    fn test_output_prefix_len_demoted_when_multi_rg() {
        // The current merge writer rolls over RGs at row count, not at
        // sort-prefix transitions. When the output ends up with > 1 RG,
        // the boundaries are at arbitrary places and the inputs' prefix
        // claim cannot be honored — the output's prefix must be 0.
        let mut s0 = make_test_split("s0", (1000, 2000), 0);
        let mut s1 = make_test_split("s1", (1000, 2000), 0);
        s0.rg_partition_prefix_len = 3;
        s1.rg_partition_prefix_len = 3;

        let output = make_output_full(200, 9000, 2, (1000, 2000), &["cpu.usage"]);
        let result = merge_parquet_split_metadata(&[s0, s1], &output).unwrap();
        assert_eq!(result.rg_partition_prefix_len, 0);
    }

    #[test]
    fn test_output_prefix_len_preserved_when_single_rg() {
        // A single-RG output vacuously satisfies any prefix alignment
        // claim (one RG, no boundary to misalign). Propagate the inputs'
        // prefix so the merge output stays in the same compaction bucket
        // as the inputs, instead of leaking into the prefix=0 bucket on
        // every merge.
        let mut s0 = make_test_split("s0", (1000, 2000), 0);
        let mut s1 = make_test_split("s1", (1000, 2000), 0);
        s0.rg_partition_prefix_len = 3;
        s1.rg_partition_prefix_len = 3;

        let output = make_output_full(200, 9000, 1, (1000, 2000), &["cpu.usage"]);
        let result = merge_parquet_split_metadata(&[s0, s1], &output).unwrap();
        assert_eq!(result.rg_partition_prefix_len, 3);
    }

    #[test]
    fn test_fresh_split_id_generated() {
        let inputs = vec![
            make_test_split("s0", (1000, 2000), 0),
            make_test_split("s1", (1000, 2000), 0),
        ];
        let output = make_output(200, 9000);
        let result = merge_parquet_split_metadata(&inputs, &output).unwrap();

        assert_ne!(result.split_id.as_str(), "s0");
        assert_ne!(result.split_id.as_str(), "s1");
        assert!(result.split_id.as_str().starts_with("metrics_"));
        assert_eq!(result.parquet_file, format!("{}.parquet", result.split_id));
    }

    #[test]
    fn test_none_row_keys_propagated() {
        let inputs = vec![make_test_split("s0", (1000, 2000), 0)];
        let output = make_output_with_metadata(100, 5000, (1000, 2000), &[]);
        let mut output = output;
        output.row_keys_proto = None;
        output.zonemap_regexes = HashMap::new();

        let result = merge_parquet_split_metadata(&inputs, &output).unwrap();

        assert!(result.row_keys_proto.is_none());
        assert!(result.zonemap_regexes.is_empty());
    }
}
