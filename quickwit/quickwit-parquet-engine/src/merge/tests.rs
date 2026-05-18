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

//! Tests for the sorted k-way merge engine.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{
    Array, DictionaryArray, Int64Array, RecordBatch, StringArray, UInt8Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use tempfile::TempDir;

use crate::merge::{MergeConfig, merge_sorted_parquet_files};
use crate::sorted_series::SORTED_SERIES_COLUMN;
use crate::split::{ParquetSplitId, ParquetSplitKind, ParquetSplitMetadata, TimeRange};
use crate::storage::{ParquetWriter, ParquetWriterConfig};
use crate::table_config::{ProductType, TableConfig};

const TEST_SORT_FIELDS: &str = "metric_name|timeseries_id|timestamp_secs/V2";

/// Helper: create a sorted Parquet file from raw column data.
///
/// The batch is sorted and written using the standard ParquetWriter to ensure
/// the file has correct column ordering, sorted_series, and metadata.
fn write_test_split(
    dir: &std::path::Path,
    name: &str,
    metric_names: &[&str],
    timestamps: &[i64],
    values: &[f64],
    timeseries_ids: &[i64],
) -> PathBuf {
    assert_eq!(metric_names.len(), timestamps.len());
    assert_eq!(metric_names.len(), values.len());
    assert_eq!(metric_names.len(), timeseries_ids.len());

    // Build dictionary-encoded metric_name column.
    let metric_name_array: DictionaryArray<Int32Type> =
        metric_names.iter().map(|s| Some(*s)).collect();

    let timestamp_array =
        UInt64Array::from(timestamps.iter().map(|&t| t as u64).collect::<Vec<u64>>());
    let value_array = arrow::array::Float64Array::from(values.to_vec());
    let tsid_array = Int64Array::from(timeseries_ids.to_vec());
    // metric_type: 0 = gauge for all rows.
    let metric_type_array = UInt8Array::from(vec![0u8; metric_names.len()]);

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "metric_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("timeseries_id", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(metric_name_array),
            Arc::new(metric_type_array),
            Arc::new(timestamp_array),
            Arc::new(value_array),
            Arc::new(tsid_array),
        ],
    )
    .unwrap();

    write_test_split_with_config(dir, name, &batch, ParquetWriterConfig::default())
}

/// Write a test split with a custom writer config (e.g., small row group size).
fn write_test_split_with_config(
    dir: &std::path::Path,
    name: &str,
    batch: &RecordBatch,
    writer_config: ParquetWriterConfig,
) -> PathBuf {
    let table_config = TableConfig {
        product_type: ProductType::Metrics,
        sort_fields: Some(TEST_SORT_FIELDS.to_string()),
        window_duration_secs: 900,
    };
    let writer = ParquetWriter::new(writer_config, &table_config).unwrap();

    // Build minimal split metadata so qh.sort_fields is embedded in the
    // Parquet KV metadata. The merge engine validates this on every input.
    let metadata = build_test_metadata();

    let path = dir.join(name);
    writer
        .write_to_file_with_metadata(batch, &path, Some(&metadata))
        .unwrap();
    path
}

#[test]
fn test_merge_two_non_overlapping_inputs() {
    let dir = TempDir::new().unwrap();

    // Input 1: metric "cpu" at times 100, 200.
    let input1 = write_test_split(
        dir.path(),
        "input1.parquet",
        &["cpu", "cpu"],
        &[100, 200],
        &[1.0, 2.0],
        &[42, 42],
    );

    // Input 2: metric "mem" at times 100, 200.
    let input2 = write_test_split(
        dir.path(),
        "input2.parquet",
        &["mem", "mem"],
        &[100, 200],
        &[3.0, 4.0],
        &[99, 99],
    );

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let config = MergeConfig {
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
    };

    let outputs = merge_sorted_parquet_files(&[input1, input2], &output_dir, &config).unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].num_rows, 4);

    // Read back and verify sort order.
    let batch = read_parquet_file(&outputs[0].path);
    assert_eq!(batch.num_rows(), 4);

    // metric_name should be sorted: cpu, cpu, mem, mem.
    let metric_names = extract_string_column(&batch, "metric_name");
    assert_eq!(metric_names, vec!["cpu", "cpu", "mem", "mem"]);

    // Within each metric, timestamps should be descending (sort default).
    let timestamps = extract_u64_column(&batch, "timestamp_secs");
    assert_eq!(timestamps, vec![200, 100, 200, 100]);

    // Verify row keys are present.
    assert!(outputs[0].row_keys_proto.is_some());
}

#[test]
fn test_merge_interleaved_inputs() {
    let dir = TempDir::new().unwrap();

    // Input 1: cpu at t=100, mem at t=200.
    let input1 = write_test_split(
        dir.path(),
        "input1.parquet",
        &["cpu", "mem"],
        &[100, 200],
        &[1.0, 2.0],
        &[42, 99],
    );

    // Input 2: cpu at t=200, mem at t=100.
    let input2 = write_test_split(
        dir.path(),
        "input2.parquet",
        &["cpu", "mem"],
        &[200, 100],
        &[3.0, 4.0],
        &[42, 99],
    );

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let config = MergeConfig {
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
    };

    let outputs = merge_sorted_parquet_files(&[input1, input2], &output_dir, &config).unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].num_rows, 4);

    let batch = read_parquet_file(&outputs[0].path);

    // Expected: cpu (t=200, t=100), mem (t=200, t=100).
    let metric_names = extract_string_column(&batch, "metric_name");
    assert_eq!(metric_names, vec!["cpu", "cpu", "mem", "mem"]);

    let timestamps = extract_u64_column(&batch, "timestamp_secs");
    assert_eq!(timestamps, vec![200, 100, 200, 100]);
}

#[test]
fn test_merge_single_input() {
    let dir = TempDir::new().unwrap();

    let input1 = write_test_split(
        dir.path(),
        "input1.parquet",
        &["cpu", "mem"],
        &[100, 200],
        &[1.0, 2.0],
        &[42, 99],
    );

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let config = MergeConfig {
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
    };

    let outputs = merge_sorted_parquet_files(&[input1], &output_dir, &config).unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].num_rows, 2);
}

#[test]
fn test_merge_multiple_outputs() {
    let dir = TempDir::new().unwrap();

    // Create inputs with 3 distinct metrics so we can split into 3 outputs.
    let input1 = write_test_split(
        dir.path(),
        "input1.parquet",
        &["alpha", "beta", "gamma"],
        &[100, 100, 100],
        &[1.0, 2.0, 3.0],
        &[1, 2, 3],
    );

    let input2 = write_test_split(
        dir.path(),
        "input2.parquet",
        &["alpha", "beta", "gamma"],
        &[200, 200, 200],
        &[4.0, 5.0, 6.0],
        &[1, 2, 3],
    );

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let config = MergeConfig {
        num_outputs: 3,
        writer_config: ParquetWriterConfig::default(),
    };

    let outputs = merge_sorted_parquet_files(&[input1, input2], &output_dir, &config).unwrap();

    // Should produce up to 3 outputs, each with non-overlapping metric names.
    assert!(!outputs.is_empty());
    assert!(outputs.len() <= 3);

    let total_rows: usize = outputs.iter().map(|o| o.num_rows).sum();
    assert_eq!(total_rows, 6);

    // Each output should have row keys and per-output metadata.
    let mut all_metric_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    for output in &outputs {
        assert!(output.row_keys_proto.is_some());
        // Each output has its own metric names (subset of all inputs).
        assert!(!output.metric_names.is_empty());
        all_metric_names.extend(output.metric_names.iter().cloned());
        // Time range should be valid.
        assert!(output.time_range.start_secs <= output.time_range.end_secs);
    }
    // The union of all output metric names should be the full set.
    assert!(all_metric_names.contains("alpha"));
    assert!(all_metric_names.contains("beta"));
    assert!(all_metric_names.contains("gamma"));
}

/// Verifies that per-output metadata (metric_names, time_range) is computed
/// from the actual rows in each output file, not aggregated from all inputs.
#[test]
fn test_merge_per_output_metadata_from_actual_rows() {
    let dir = TempDir::new().unwrap();

    // Input 1: metric "cpu" at timestamps 100, 200
    let input1 = write_test_split(
        dir.path(),
        "in1.parquet",
        &["cpu", "cpu"],
        &[100, 200],
        &[1.0, 2.0],
        &[1, 1],
    );
    // Input 2: metric "mem" at timestamps 300, 400
    let input2 = write_test_split(
        dir.path(),
        "in2.parquet",
        &["mem", "mem"],
        &[300, 400],
        &[3.0, 4.0],
        &[2, 2],
    );

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    // Single output: should contain all metrics and full time range.
    let config = MergeConfig {
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
    };
    let outputs =
        merge_sorted_parquet_files(&[input1.clone(), input2.clone()], &output_dir, &config)
            .unwrap();
    assert_eq!(outputs.len(), 1);
    let output = &outputs[0];
    assert!(output.metric_names.contains("cpu"));
    assert!(output.metric_names.contains("mem"));
    assert_eq!(output.time_range.start_secs, 100);
    assert_eq!(output.time_range.end_secs, 401); // end is exclusive

    // Two outputs: each should have only its own metrics and time range.
    let output_dir2 = dir.path().join("output2");
    std::fs::create_dir_all(&output_dir2).unwrap();
    let config2 = MergeConfig {
        num_outputs: 2,
        writer_config: ParquetWriterConfig::default(),
    };
    let outputs2 = merge_sorted_parquet_files(&[input1, input2], &output_dir2, &config2).unwrap();
    assert_eq!(outputs2.len(), 2);

    // After sorted merge, "cpu" (sorted_series for tsid=1) and "mem" (tsid=2)
    // are in separate outputs. Each output should have only one metric.
    let output_a = &outputs2[0];
    let output_b = &outputs2[1];
    assert_eq!(output_a.metric_names.len(), 1);
    assert_eq!(output_b.metric_names.len(), 1);
    assert_ne!(output_a.metric_names, output_b.metric_names);

    // Time ranges should be disjoint or specific to each output's rows.
    assert!(output_a.time_range.start_secs <= output_a.time_range.end_secs);
    assert!(output_b.time_range.start_secs <= output_b.time_range.end_secs);
}

#[test]
fn test_merge_empty_inputs() {
    let dir = TempDir::new().unwrap();

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let config = MergeConfig {
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
    };

    // Zero inputs should error.
    let result = merge_sorted_parquet_files(&[], &output_dir, &config);
    assert!(result.is_err());
}

#[test]
fn test_merge_verifies_parquet_kv_metadata() {
    let dir = TempDir::new().unwrap();

    let input1 = write_test_split(
        dir.path(),
        "input1.parquet",
        &["cpu"],
        &[100],
        &[1.0],
        &[42],
    );

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let config = MergeConfig {
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
    };

    let outputs = merge_sorted_parquet_files(&[input1], &output_dir, &config).unwrap();
    assert_eq!(outputs.len(), 1);

    // Read back the Parquet file and verify KV metadata.
    let file = std::fs::File::open(&outputs[0].path).unwrap();
    let reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let kv_metadata = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .expect("kv metadata must be present");

    let find_kv = |key: &str| -> Option<String> {
        kv_metadata
            .iter()
            .find(|kv| kv.key == key)
            .and_then(|kv| kv.value.clone())
    };

    assert_eq!(find_kv("qh.sort_fields").as_deref(), Some(TEST_SORT_FIELDS));
    assert_eq!(find_kv("qh.window_start").as_deref(), Some("0"));
    assert_eq!(find_kv("qh.window_duration_secs").as_deref(), Some("900"));
    // num_merge_ops = max(input.num_merge_ops) + 1. Input has 0, so output = 1.
    assert_eq!(find_kv("qh.num_merge_ops").as_deref(), Some("1"));
    assert!(find_kv("qh.row_keys").is_some());
}

#[test]
fn test_merge_preserves_husky_column_ordering() {
    let dir = TempDir::new().unwrap();

    let input1 = write_test_split(
        dir.path(),
        "input1.parquet",
        &["cpu"],
        &[100],
        &[1.0],
        &[42],
    );

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let config = MergeConfig {
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
    };

    let outputs = merge_sorted_parquet_files(&[input1], &output_dir, &config).unwrap();
    let batch = read_parquet_file(&outputs[0].path);

    let schema = batch.schema();
    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    // Sort schema columns first (metric_name, timestamp_secs), then
    // sorted_series, then remaining alphabetically.
    let metric_pos = col_names.iter().position(|n| *n == "metric_name").unwrap();
    let ts_pos = col_names
        .iter()
        .position(|n| *n == "timestamp_secs")
        .unwrap();
    let ss_pos = col_names
        .iter()
        .position(|n| *n == SORTED_SERIES_COLUMN)
        .unwrap();

    assert!(
        metric_pos < ts_pos,
        "metric_name should come before timestamp_secs"
    );
    assert!(
        ts_pos < ss_pos,
        "timestamp_secs should come before sorted_series"
    );

    // Remaining columns after sorted_series should be alphabetical.
    let remaining: Vec<&str> = col_names[ss_pos + 1..].to_vec();
    let mut sorted_remaining = remaining.clone();
    sorted_remaining.sort();
    assert_eq!(remaining, sorted_remaining);
}

// ---- Test helpers ----

fn read_parquet_file(path: &std::path::Path) -> RecordBatch {
    let file = std::fs::File::open(path).unwrap();
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let reader = builder.build().unwrap();

    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert!(!batches.is_empty(), "expected at least one batch");

    let schema = batches[0].schema();
    arrow::compute::concat_batches(&schema, &batches).unwrap()
}

/// Extract string values from a column that may be Utf8, LargeUtf8,
/// or Dictionary-encoded. Handles all representations uniformly.
fn extract_string_column(batch: &RecordBatch, name: &str) -> Vec<String> {
    let idx = batch.schema().index_of(name).unwrap();
    let col = batch.column(idx);

    // Try Dictionary<Int32, Utf8> first (most common in our pipeline).
    if let Some(dict) = col.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        return (0..dict.len())
            .map(|i| {
                let key = dict.keys().value(i) as usize;
                values.value(key).to_string()
            })
            .collect();
    }

    // Try plain Utf8.
    if let Some(str_arr) = col.as_any().downcast_ref::<StringArray>() {
        return (0..str_arr.len())
            .map(|i| str_arr.value(i).to_string())
            .collect();
    }

    panic!(
        "column '{}' is neither Dict<Int32, Utf8> nor Utf8, got {:?}",
        name,
        col.data_type()
    );
}

fn extract_u64_column(batch: &RecordBatch, name: &str) -> Vec<u64> {
    let idx = batch.schema().index_of(name).unwrap();
    let col = batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("expected UInt64");
    col.values().to_vec()
}

fn extract_f64_column(batch: &RecordBatch, name: &str) -> Vec<f64> {
    let idx = batch.schema().index_of(name).unwrap();
    let col = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("expected Float64");
    col.values().to_vec()
}

fn extract_binary_column(batch: &RecordBatch, name: &str) -> Vec<Vec<u8>> {
    let idx = batch.schema().index_of(name).unwrap();
    let col = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>()
        .expect("expected Binary");
    (0..col.len()).map(|i| col.value(i).to_vec()).collect()
}

// ---- Edge case tests ----

#[test]
fn test_merge_schema_evolution_extra_column() {
    // Input 1 has "extra_tag" column, Input 2 does not.
    // MC-4: output should have the union of all columns.
    let dir = TempDir::new().unwrap();

    // We'll write input1 with an extra column by building a custom batch.
    let metric_name_array: DictionaryArray<Int32Type> = vec![Some("cpu")].into_iter().collect();
    let timestamp_array = UInt64Array::from(vec![100u64]);
    let value_array = arrow::array::Float64Array::from(vec![1.0]);
    let tsid_array = Int64Array::from(vec![42i64]);
    let metric_type_array = UInt8Array::from(vec![0u8]);
    let extra_tag_array: DictionaryArray<Int32Type> = vec![Some("us-east")].into_iter().collect();

    let schema_with_extra = Arc::new(Schema::new(vec![
        Field::new(
            "metric_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("timeseries_id", DataType::Int64, false),
        Field::new(
            "extra_tag",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]));

    let batch_with_extra = RecordBatch::try_new(
        schema_with_extra,
        vec![
            Arc::new(metric_name_array),
            Arc::new(metric_type_array),
            Arc::new(timestamp_array),
            Arc::new(value_array),
            Arc::new(tsid_array),
            Arc::new(extra_tag_array),
        ],
    )
    .unwrap();

    // Write input1 with the extra column.
    let table_config = TableConfig {
        product_type: ProductType::Metrics,
        sort_fields: Some(TEST_SORT_FIELDS.to_string()),
        window_duration_secs: 900,
    };
    let writer_config = ParquetWriterConfig::default();
    let writer = ParquetWriter::new(writer_config, &table_config).unwrap();
    let path1 = dir.path().join("input1_extra.parquet");
    writer
        .write_to_file_with_metadata(&batch_with_extra, &path1, Some(&build_test_metadata()))
        .unwrap();

    // Input 2 without the extra column.
    let input2 = write_test_split(
        dir.path(),
        "input2.parquet",
        &["mem"],
        &[200],
        &[2.0],
        &[99],
    );

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let config = MergeConfig {
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
    };

    let outputs = merge_sorted_parquet_files(&[path1, input2], &output_dir, &config).unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].num_rows, 2);

    // MC-4: output has all columns including extra_tag.
    let batch = read_parquet_file(&outputs[0].path);
    assert!(
        batch.schema().index_of("extra_tag").is_ok(),
        "output must contain extra_tag column (MC-4)"
    );
}

#[test]
fn test_merge_mc2_row_contents_preserved() {
    // MC-2: verify that actual data values survive merge unchanged.
    let dir = TempDir::new().unwrap();

    let input1 = write_test_split(
        dir.path(),
        "input1.parquet",
        &["cpu", "cpu"],
        &[200, 100],
        &[1.5, 2.5],
        &[42, 42],
    );

    let input2 = write_test_split(
        dir.path(),
        "input2.parquet",
        &["cpu", "mem"],
        &[150, 300],
        &[3.5, 4.5],
        &[42, 99],
    );

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let config = MergeConfig {
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
    };

    let outputs = merge_sorted_parquet_files(&[input1, input2], &output_dir, &config).unwrap();
    let batch = read_parquet_file(&outputs[0].path);

    // Collect all (metric_name, timestamp, value) triples from output.
    let names = extract_string_column(&batch, "metric_name");
    let timestamps = extract_u64_column(&batch, "timestamp_secs");
    let values = extract_f64_column(&batch, "value");

    let mut output_triples: Vec<(String, u64, f64)> = names
        .into_iter()
        .zip(timestamps)
        .zip(values)
        .map(|((n, t), v)| (n, t, v))
        .collect();
    output_triples.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    // Expected triples (sorted by name then timestamp).
    let mut expected = vec![
        ("cpu".to_string(), 100u64, 2.5),
        ("cpu".to_string(), 150u64, 3.5),
        ("cpu".to_string(), 200u64, 1.5),
        ("mem".to_string(), 300u64, 4.5),
    ];
    expected.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    assert_eq!(
        output_triples, expected,
        "MC-2: row contents must be preserved"
    );
}

#[test]
fn test_merge_dm5_sorted_series_preserved() {
    // DM-5: sorted_series values are carried through merge, not recomputed.
    let dir = TempDir::new().unwrap();

    let input1 = write_test_split(
        dir.path(),
        "input1.parquet",
        &["cpu"],
        &[100],
        &[1.0],
        &[42],
    );

    let input2 = write_test_split(
        dir.path(),
        "input2.parquet",
        &["cpu"],
        &[200],
        &[2.0],
        &[42],
    );

    // Read back sorted_series from inputs.
    let input1_batch = read_parquet_file(&input1);
    let input2_batch = read_parquet_file(&input2);
    let input1_ss = extract_binary_column(&input1_batch, SORTED_SERIES_COLUMN);
    let input2_ss = extract_binary_column(&input2_batch, SORTED_SERIES_COLUMN);

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let config = MergeConfig {
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
    };

    let outputs = merge_sorted_parquet_files(&[input1, input2], &output_dir, &config).unwrap();
    let output_batch = read_parquet_file(&outputs[0].path);
    let output_ss = extract_binary_column(&output_batch, SORTED_SERIES_COLUMN);

    // All inputs had the same metric and timeseries_id, so sorted_series
    // should be identical across all rows.
    assert_eq!(
        input1_ss[0], input2_ss[0],
        "same series should have same key"
    );

    // DM-5: output sorted_series values must match input values.
    for ss in &output_ss {
        assert_eq!(
            ss, &input1_ss[0],
            "DM-5: sorted_series must be preserved through merge"
        );
    }
}

// ---- Row-level interleaving and boundary tests ----

#[test]
fn test_merge_deep_interleaving() {
    // Construct inputs where the sorted output must interleave rows from
    // the middle of different inputs — not just append one after another.
    //
    // Input 1: alpha@300, beta@100
    // Input 2: alpha@100, beta@300
    //
    // After sort by (metric_name ASC, timestamp DESC):
    //   alpha@300 (input 1), alpha@100 (input 2), beta@300 (input 2), beta@100 (input 1)
    //
    // This forces row 0 of input 1, row 0 of input 2, row 1 of input 2, row 1 of input 1.
    let dir = TempDir::new().unwrap();

    let input1 = write_test_split(
        dir.path(),
        "input1.parquet",
        &["alpha", "beta"],
        &[300, 100],
        &[1.0, 2.0],
        &[10, 20],
    );

    let input2 = write_test_split(
        dir.path(),
        "input2.parquet",
        &["alpha", "beta"],
        &[100, 300],
        &[3.0, 4.0],
        &[10, 20],
    );

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let config = MergeConfig {
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
    };

    let outputs = merge_sorted_parquet_files(&[input1, input2], &output_dir, &config).unwrap();
    let batch = read_parquet_file(&outputs[0].path);

    let names = extract_string_column(&batch, "metric_name");
    let timestamps = extract_u64_column(&batch, "timestamp_secs");
    let values = extract_f64_column(&batch, "value");

    // Verify deep interleaving: alpha rows come first, then beta rows.
    // Within each metric, timestamps are descending.
    assert_eq!(names, vec!["alpha", "alpha", "beta", "beta"]);
    assert_eq!(timestamps, vec![300, 100, 300, 100]);

    // Verify which input each row came from via the value column:
    // alpha@300 = 1.0 (input1), alpha@100 = 3.0 (input2),
    // beta@300 = 4.0 (input2), beta@100 = 2.0 (input1).
    assert_eq!(values, vec![1.0, 3.0, 4.0, 2.0]);
}

#[test]
fn test_merge_duplicate_timestamps() {
    // MC-1: duplicate timestamps within the same series must all survive.
    // Two inputs contribute rows for the same metric at the same timestamp
    // with different values.
    let dir = TempDir::new().unwrap();

    let input1 = write_test_split(
        dir.path(),
        "input1.parquet",
        &["cpu", "cpu"],
        &[100, 100], // same timestamp, different values
        &[1.0, 2.0],
        &[42, 42], // same timeseries_id
    );

    let input2 = write_test_split(
        dir.path(),
        "input2.parquet",
        &["cpu", "cpu"],
        &[100, 100],
        &[3.0, 4.0],
        &[42, 42],
    );

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let config = MergeConfig {
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
    };

    let outputs = merge_sorted_parquet_files(&[input1, input2], &output_dir, &config).unwrap();
    let batch = read_parquet_file(&outputs[0].path);

    // MC-1: all 4 rows must survive — no deduplication.
    assert_eq!(batch.num_rows(), 4);

    let timestamps = extract_u64_column(&batch, "timestamp_secs");
    assert_eq!(timestamps, vec![100, 100, 100, 100]);

    // MC-2: all 4 values must be present.
    let mut values = extract_f64_column(&batch, "value");
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(values, vec![1.0, 2.0, 3.0, 4.0]);
}

#[test]
fn test_merge_per_output_schema_strips_null_columns() {
    // When M=2, an output file whose rows all came from an input that
    // lacks a column should not have that column (all-null stripped).
    let dir = TempDir::new().unwrap();

    // Input 1 has "extra_tag", metric "alpha".
    let metric_name_array: DictionaryArray<Int32Type> = vec![Some("alpha")].into_iter().collect();
    let timestamp_array = UInt64Array::from(vec![100u64]);
    let value_array = arrow::array::Float64Array::from(vec![1.0]);
    let tsid_array = Int64Array::from(vec![42i64]);
    let metric_type_array = UInt8Array::from(vec![0u8]);
    let extra_tag_array: DictionaryArray<Int32Type> = vec![Some("us-east")].into_iter().collect();

    let schema_with_extra = Arc::new(Schema::new(vec![
        Field::new(
            "metric_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("timeseries_id", DataType::Int64, false),
        Field::new(
            "extra_tag",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]));

    let batch_with_extra = RecordBatch::try_new(
        schema_with_extra,
        vec![
            Arc::new(metric_name_array),
            Arc::new(metric_type_array),
            Arc::new(timestamp_array),
            Arc::new(value_array),
            Arc::new(tsid_array),
            Arc::new(extra_tag_array),
        ],
    )
    .unwrap();

    let table_config = TableConfig {
        product_type: ProductType::Metrics,
        sort_fields: Some(TEST_SORT_FIELDS.to_string()),
        window_duration_secs: 900,
    };
    let writer_config = ParquetWriterConfig::default();
    let writer = ParquetWriter::new(writer_config, &table_config).unwrap();
    let path1 = dir.path().join("input1_extra.parquet");
    writer
        .write_to_file_with_metadata(&batch_with_extra, &path1, Some(&build_test_metadata()))
        .unwrap();

    // Input 2 without extra_tag, metric "zeta" (sorts after "alpha").
    let input2 = write_test_split(
        dir.path(),
        "input2.parquet",
        &["zeta"],
        &[200],
        &[2.0],
        &[99],
    );

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    // Request M=2 — alpha goes to output 1, zeta goes to output 2.
    let config = MergeConfig {
        num_outputs: 2,
        writer_config: ParquetWriterConfig::default(),
    };

    let outputs = merge_sorted_parquet_files(&[path1, input2], &output_dir, &config).unwrap();
    assert_eq!(outputs.len(), 2, "should produce 2 output files");

    // Output 1 (alpha) should have extra_tag.
    let batch1 = read_parquet_file(&outputs[0].path);
    let names1 = extract_string_column(&batch1, "metric_name");
    assert_eq!(names1, vec!["alpha"]);
    assert!(
        batch1.schema().index_of("extra_tag").is_ok(),
        "output 1 (alpha) must have extra_tag"
    );

    // Output 2 (zeta) should NOT have extra_tag — it would be all-null.
    let batch2 = read_parquet_file(&outputs[1].path);
    let names2 = extract_string_column(&batch2, "metric_name");
    assert_eq!(names2, vec!["zeta"]);
    assert!(
        batch2.schema().index_of("extra_tag").is_err(),
        "output 2 (zeta) must not have extra_tag (all-null stripped)"
    );
}

#[test]
fn test_merge_output_type_reflects_data() {
    // The output column type should be determined by the actual data,
    // not by the input types. Low-cardinality string columns should be
    // dictionary-encoded in the output regardless of input encoding.
    let dir = TempDir::new().unwrap();

    // Both inputs have the same metric repeated — low cardinality.
    let input1 = write_test_split(
        dir.path(),
        "input1.parquet",
        &["cpu", "cpu", "cpu", "cpu"],
        &[100, 200, 300, 400],
        &[1.0, 2.0, 3.0, 4.0],
        &[42, 42, 42, 42],
    );

    let input2 = write_test_split(
        dir.path(),
        "input2.parquet",
        &["cpu", "cpu", "cpu", "cpu"],
        &[150, 250, 350, 450],
        &[5.0, 6.0, 7.0, 8.0],
        &[42, 42, 42, 42],
    );

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let config = MergeConfig {
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
    };

    let outputs = merge_sorted_parquet_files(&[input1, input2], &output_dir, &config).unwrap();
    let batch = read_parquet_file(&outputs[0].path);

    // metric_name has 1 distinct value across 8 rows — should be dictionary-encoded.
    let schema = batch.schema();
    let mn_idx = schema.index_of("metric_name").unwrap();
    let mn_type = schema.field(mn_idx).data_type();
    assert!(
        matches!(mn_type, DataType::Dictionary(_, _)),
        "low-cardinality metric_name should be dictionary-encoded, got {:?}",
        mn_type
    );
}

#[test]
fn test_merge_cross_row_group_interleaving() {
    // Test that the merge correctly handles inputs with multiple Parquet
    // row groups. We write inputs with a tiny row group size (3 rows) so
    // that each input spans multiple row groups, then verify that rows
    // from different row groups of different inputs interleave correctly.
    //
    // Input 1 (row group size 3):
    //   RG0: alpha@300, alpha@200, alpha@100  (one series, 3 rows)
    //   RG1: beta@300, beta@200, beta@100     (another series, 3 rows)
    //   RG2: gamma@300, gamma@200             (partial row group, 2 rows)
    //
    // Input 2 (row group size 3):
    //   RG0: alpha@250, alpha@150, alpha@50   (overlaps input 1's alpha range)
    //   RG1: beta@250, beta@150, beta@50      (overlaps input 1's beta range)
    //   RG2: delta@300, delta@200             (new series not in input 1)
    //
    // The sorted output must interleave rows from across row group
    // boundaries: e.g., alpha@300 (input1 RG0), alpha@250 (input2 RG0),
    // alpha@200 (input1 RG0), alpha@150 (input2 RG0), etc.

    let dir = TempDir::new().unwrap();
    let small_rg_config = ParquetWriterConfig::default().with_row_group_size(3);

    // Input 1: 8 rows across 3 row groups.
    let names1: Vec<&str> = vec![
        "alpha", "alpha", "alpha", "beta", "beta", "beta", "gamma", "gamma",
    ];
    let ts1: Vec<i64> = vec![300, 200, 100, 300, 200, 100, 300, 200];
    let vals1: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
    let tsids1: Vec<i64> = vec![10, 10, 10, 20, 20, 20, 30, 30];

    let batch1 = build_test_batch(&names1, &ts1, &vals1, &tsids1);
    let input1 = write_test_split_with_config(
        dir.path(),
        "input1.parquet",
        &batch1,
        small_rg_config.clone(),
    );

    // Verify input1 actually has multiple row groups.
    let input1_rg_count = count_row_groups(&input1);
    assert!(
        input1_rg_count > 1,
        "input1 must have multiple row groups (got {})",
        input1_rg_count
    );

    // Input 2: 8 rows across 3 row groups, interleaving with input 1.
    let names2: Vec<&str> = vec![
        "alpha", "alpha", "alpha", "beta", "beta", "beta", "delta", "delta",
    ];
    let ts2: Vec<i64> = vec![250, 150, 50, 250, 150, 50, 300, 200];
    let vals2: Vec<f64> = vec![11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0];
    let tsids2: Vec<i64> = vec![10, 10, 10, 20, 20, 20, 40, 40];

    let batch2 = build_test_batch(&names2, &ts2, &vals2, &tsids2);
    let input2 =
        write_test_split_with_config(dir.path(), "input2.parquet", &batch2, small_rg_config);

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let config = MergeConfig {
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
    };

    let outputs = merge_sorted_parquet_files(&[input1, input2], &output_dir, &config).unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].num_rows, 16, "MC-1: all 16 rows must survive");

    let batch = read_parquet_file(&outputs[0].path);
    let names = extract_string_column(&batch, "metric_name");
    let timestamps = extract_u64_column(&batch, "timestamp_secs");
    let values = extract_f64_column(&batch, "value");

    // Expected sort order: metric_name ASC, then timestamp DESC within series.
    // alpha: 300(v=1), 250(v=11), 200(v=2), 150(v=12), 100(v=3), 50(v=13)
    // beta:  300(v=4), 250(v=14), 200(v=5), 150(v=15), 100(v=6), 50(v=16)
    // delta: 300(v=17), 200(v=18)
    // gamma: 300(v=7), 200(v=8)
    let expected_names = vec![
        "alpha", "alpha", "alpha", "alpha", "alpha", "alpha", "beta", "beta", "beta", "beta",
        "beta", "beta", "delta", "delta", "gamma", "gamma",
    ];
    let expected_ts: Vec<u64> = vec![
        300, 250, 200, 150, 100, 50, 300, 250, 200, 150, 100, 50, 300, 200, 300, 200,
    ];
    let expected_vals = vec![
        1.0, 11.0, 2.0, 12.0, 3.0, 13.0, 4.0, 14.0, 5.0, 15.0, 6.0, 16.0, 17.0, 18.0, 7.0, 8.0,
    ];

    assert_eq!(names, expected_names, "metric names must be sorted");
    assert_eq!(
        timestamps, expected_ts,
        "timestamps must be descending within series"
    );
    assert_eq!(
        values, expected_vals,
        "values must match — proving rows from different row groups of different inputs \
         interleave correctly"
    );
}

#[test]
fn test_merge_cross_record_batch_interleaving() {
    // Test that the merge correctly handles inputs that yield multiple
    // Arrow RecordBatches when read. We force a read batch size of 2 rows,
    // so each input is read as multiple RecordBatches that must be
    // concatenated before the merge can operate on them.
    //
    // This exercises the concat_batches path in read_inputs and proves
    // that RecordBatch boundaries within a file don't affect the merge.
    let dir = TempDir::new().unwrap();

    // Input 1: 6 rows, will be read as 3 batches of 2 rows each.
    let input1 = write_test_split(
        dir.path(),
        "input1.parquet",
        &["alpha", "alpha", "beta", "beta", "gamma", "gamma"],
        &[200, 100, 200, 100, 200, 100],
        &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        &[10, 10, 20, 20, 30, 30],
    );

    // Input 2: 6 rows, will be read as 3 batches of 2 rows each.
    let input2 = write_test_split(
        dir.path(),
        "input2.parquet",
        &["alpha", "alpha", "beta", "beta", "gamma", "gamma"],
        &[150, 50, 150, 50, 150, 50],
        &[11.0, 12.0, 13.0, 14.0, 15.0, 16.0],
        &[10, 10, 20, 20, 30, 30],
    );

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let config = MergeConfig {
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
    };

    // Force read batch size of 2 — each 6-row file yields 3 RecordBatches.
    let outputs = crate::merge::merge_sorted_parquet_files_with_read_batch_size(
        &[input1, input2],
        &output_dir,
        &config,
        2, // 2 rows per RecordBatch
    )
    .unwrap();

    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].num_rows, 12, "MC-1: all 12 rows must survive");

    let batch = read_parquet_file(&outputs[0].path);
    let names = extract_string_column(&batch, "metric_name");
    let timestamps = extract_u64_column(&batch, "timestamp_secs");
    let values = extract_f64_column(&batch, "value");

    // Expected: alpha(200,150,100,50), beta(200,150,100,50), gamma(200,150,100,50)
    assert_eq!(
        names,
        vec![
            "alpha", "alpha", "alpha", "alpha", "beta", "beta", "beta", "beta", "gamma", "gamma",
            "gamma", "gamma",
        ]
    );
    assert_eq!(
        timestamps,
        vec![200, 150, 100, 50, 200, 150, 100, 50, 200, 150, 100, 50]
    );
    // Values trace back to which input each row came from:
    // alpha: 200(v=1,input1), 150(v=11,input2), 100(v=2,input1), 50(v=12,input2)
    assert_eq!(
        values,
        vec![
            1.0, 11.0, 2.0, 12.0, 3.0, 13.0, 4.0, 14.0, 5.0, 15.0, 6.0, 16.0
        ]
    );
}

/// Build a minimal ParquetSplitMetadata for test files.
/// The merge engine validates qh.sort_fields on every input.
fn build_test_metadata() -> ParquetSplitMetadata {
    build_test_metadata_with_prefix_len(0)
}

/// Build minimal `ParquetSplitMetadata` with an explicit
/// `rg_partition_prefix_len`. Used by tests that exercise the merge
/// engine's prefix-consistency validation.
fn build_test_metadata_with_prefix_len(prefix_len: u32) -> ParquetSplitMetadata {
    ParquetSplitMetadata::metrics_builder()
        .split_id(ParquetSplitId::generate(ParquetSplitKind::Metrics))
        .index_uid("test-index:0")
        .sort_fields(TEST_SORT_FIELDS)
        .window_duration_secs(900)
        .window_start_secs(0)
        .time_range(TimeRange::new(0, 1))
        .rg_partition_prefix_len(prefix_len)
        .build()
}

/// Write a test split with a caller-supplied `rg_partition_prefix_len`
/// embedded in the file's KV metadata. Used to exercise the merge
/// engine's prefix-consistency validation on real on-disk files.
fn write_test_split_with_prefix_len(
    dir: &std::path::Path,
    name: &str,
    metric_names: &[&str],
    timestamps: &[i64],
    values: &[f64],
    timeseries_ids: &[i64],
    prefix_len: u32,
) -> PathBuf {
    let batch = build_test_batch(metric_names, timestamps, values, timeseries_ids);
    let table_config = TableConfig {
        product_type: ProductType::Metrics,
        sort_fields: Some(TEST_SORT_FIELDS.to_string()),
        window_duration_secs: 900,
    };
    let writer = ParquetWriter::new(ParquetWriterConfig::default(), &table_config).unwrap();
    let metadata = build_test_metadata_with_prefix_len(prefix_len);
    let path = dir.join(name);
    writer
        .write_to_file_with_metadata(&batch, &path, Some(&metadata))
        .unwrap();
    path
}

#[test]
fn test_merge_rejects_mismatched_rg_partition_prefix_len() {
    let dir = TempDir::new().unwrap();

    // input1 declares prefix_len = 1 (metric_name boundary alignment).
    let input1 = write_test_split_with_prefix_len(
        dir.path(),
        "input1.parquet",
        &["cpu", "cpu"],
        &[100, 200],
        &[1.0, 2.0],
        &[42, 42],
        1,
    );

    // input2 declares prefix_len = 0 (legacy default).
    let input2 = write_test_split_with_prefix_len(
        dir.path(),
        "input2.parquet",
        &["mem", "mem"],
        &[100, 200],
        &[3.0, 4.0],
        &[99, 99],
        0,
    );

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();
    let config = MergeConfig {
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
    };

    let result = merge_sorted_parquet_files(&[input1, input2], &output_dir, &config);
    let err = result.expect_err("merge must reject mismatched prefix lengths");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("rg_partition_prefix_len"),
        "error should mention rg_partition_prefix_len, got: {msg}"
    );
}

#[test]
fn test_merge_accepts_matching_rg_partition_prefix_len() {
    // Sanity check: when both inputs declare the same prefix_len AND the
    // merged output fits in a single row group, the output preserves the
    // inputs' prefix (single-RG vacuously satisfies any alignment claim).
    // Verify both at the MergeOutputFile struct level and on disk.
    let dir = TempDir::new().unwrap();

    let input1 = write_test_split_with_prefix_len(
        dir.path(),
        "input1.parquet",
        &["cpu", "cpu"],
        &[100, 200],
        &[1.0, 2.0],
        &[42, 42],
        2,
    );

    let input2 = write_test_split_with_prefix_len(
        dir.path(),
        "input2.parquet",
        &["mem", "mem"],
        &[100, 200],
        &[3.0, 4.0],
        &[99, 99],
        2,
    );

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();
    let config = MergeConfig {
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
    };

    let outputs = merge_sorted_parquet_files(&[input1, input2], &output_dir, &config).unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].num_rows, 4);
    assert_eq!(
        outputs[0].num_row_groups, 1,
        "4 rows fit in one row group with the default 128K threshold"
    );

    // The output file's KV metadata must record the preserved prefix.
    let report = crate::storage::inspect_parquet_page_stats(&outputs[0].path, 0).unwrap();
    assert_eq!(
        report.rg_partition_prefix_len, 2,
        "single-RG merge output should preserve the inputs' prefix in its KV metadata"
    );
}

#[test]
fn test_merge_demotes_prefix_when_output_is_multi_rg() {
    // When the merged output spans more than one row group, the writer
    // cannot guarantee the inputs' prefix alignment claim and must
    // record prefix=0 in the output's KV. Force the multi-RG case by
    // setting a tiny `row_group_size` in the writer config.
    let dir = TempDir::new().unwrap();

    let input1 = write_test_split_with_prefix_len(
        dir.path(),
        "input1.parquet",
        &["cpu", "cpu", "cpu"],
        &[100, 200, 300],
        &[1.0, 2.0, 3.0],
        &[42, 42, 42],
        2,
    );

    let input2 = write_test_split_with_prefix_len(
        dir.path(),
        "input2.parquet",
        &["mem", "mem", "mem"],
        &[100, 200, 300],
        &[4.0, 5.0, 6.0],
        &[99, 99, 99],
        2,
    );

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();
    let config = MergeConfig {
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default().with_row_group_size(2),
    };

    let outputs = merge_sorted_parquet_files(&[input1, input2], &output_dir, &config).unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].num_rows, 6);
    assert!(
        outputs[0].num_row_groups > 1,
        "row_group_size=2 with 6 rows should produce multiple row groups, got {}",
        outputs[0].num_row_groups
    );

    let report = crate::storage::inspect_parquet_page_stats(&outputs[0].path, 0).unwrap();
    assert_eq!(
        report.rg_partition_prefix_len, 0,
        "multi-RG merge output cannot honor the inputs' prefix and must record 0"
    );
}

/// Build a test RecordBatch from raw column data (shared by test helpers).
fn build_test_batch(
    metric_names: &[&str],
    timestamps: &[i64],
    values: &[f64],
    timeseries_ids: &[i64],
) -> RecordBatch {
    let n = metric_names.len();
    assert_eq!(n, timestamps.len());
    assert_eq!(n, values.len());
    assert_eq!(n, timeseries_ids.len());

    let metric_name_array: DictionaryArray<Int32Type> =
        metric_names.iter().map(|s| Some(*s)).collect();
    let timestamp_array =
        UInt64Array::from(timestamps.iter().map(|&t| t as u64).collect::<Vec<u64>>());
    let value_array = arrow::array::Float64Array::from(values.to_vec());
    let tsid_array = Int64Array::from(timeseries_ids.to_vec());
    let metric_type_array = UInt8Array::from(vec![0u8; n]);

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "metric_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("timeseries_id", DataType::Int64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(metric_name_array),
            Arc::new(metric_type_array),
            Arc::new(timestamp_array),
            Arc::new(value_array),
            Arc::new(tsid_array),
        ],
    )
    .unwrap()
}

/// Count the number of row groups in a Parquet file.
fn count_row_groups(path: &std::path::Path) -> usize {
    let file = std::fs::File::open(path).unwrap();
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    builder.metadata().num_row_groups()
}

#[test]
fn test_merge_single_input_splits_into_multiple_outputs() {
    // Regression test: when a single input contains multiple series
    // (alpha, beta, gamma), the k-way merge produces one contiguous run
    // covering all rows. The output boundary computation must still detect
    // series transitions within that run and split into M outputs.
    //
    // Without the fix, transition_indices stays empty (only inter-run
    // transitions were checked), so num_outputs=3 produces 1 output.
    let dir = TempDir::new().unwrap();

    // Single input with 3 distinct series, 2 rows each.
    let input1 = write_test_split(
        dir.path(),
        "input1.parquet",
        &["alpha", "alpha", "beta", "beta", "gamma", "gamma"],
        &[200, 100, 200, 100, 200, 100],
        &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        &[10, 10, 20, 20, 30, 30],
    );

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let config = MergeConfig {
        num_outputs: 3,
        writer_config: ParquetWriterConfig::default(),
    };

    let outputs = merge_sorted_parquet_files(&[input1], &output_dir, &config).unwrap();

    // Must produce 3 outputs — one per series.
    assert_eq!(
        outputs.len(),
        3,
        "single input with 3 series and num_outputs=3 must produce 3 files, got {}",
        outputs.len()
    );

    // Each output should have exactly 2 rows.
    for (i, output) in outputs.iter().enumerate() {
        assert_eq!(
            output.num_rows, 2,
            "output {} should have 2 rows, got {}",
            i, output.num_rows
        );
    }

    // Verify each output contains exactly one series.
    let batch0 = read_parquet_file(&outputs[0].path);
    let batch1 = read_parquet_file(&outputs[1].path);
    let batch2 = read_parquet_file(&outputs[2].path);

    let names0 = extract_string_column(&batch0, "metric_name");
    let names1 = extract_string_column(&batch1, "metric_name");
    let names2 = extract_string_column(&batch2, "metric_name");

    assert_eq!(names0, vec!["alpha", "alpha"]);
    assert_eq!(names1, vec!["beta", "beta"]);
    assert_eq!(names2, vec!["gamma", "gamma"]);

    // Total rows preserved (MC-1).
    let total: usize = outputs.iter().map(|o| o.num_rows).sum();
    assert_eq!(total, 6);
}

#[test]
fn test_merge_split_after_oversized_series() {
    // Regression test: when an early series has far more rows than the
    // target per output, the boundary algorithm must still split at
    // subsequent transitions. With series row counts [6, 1, 1] and
    // num_outputs=3, all 3 transitions are available and 3 outputs
    // should be produced.
    let dir = TempDir::new().unwrap();

    // alpha: 6 rows, beta: 1 row, gamma: 1 row.
    let input1 = write_test_split(
        dir.path(),
        "input1.parquet",
        &[
            "alpha", "alpha", "alpha", "alpha", "alpha", "alpha", "beta", "gamma",
        ],
        &[600, 500, 400, 300, 200, 100, 100, 100],
        &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
        &[10, 10, 10, 10, 10, 10, 20, 30],
    );

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let config = MergeConfig {
        num_outputs: 3,
        writer_config: ParquetWriterConfig::default(),
    };

    let outputs = merge_sorted_parquet_files(&[input1], &output_dir, &config).unwrap();

    // 3 distinct series and num_outputs=3: must produce 3 files.
    assert_eq!(
        outputs.len(),
        3,
        "oversized first series must not prevent splitting at later transitions, got {} outputs",
        outputs.len()
    );

    let names0 = extract_string_column(&read_parquet_file(&outputs[0].path), "metric_name");
    let names1 = extract_string_column(&read_parquet_file(&outputs[1].path), "metric_name");
    let names2 = extract_string_column(&read_parquet_file(&outputs[2].path), "metric_name");

    assert!(names0.iter().all(|n| n == "alpha"));
    assert_eq!(names1, vec!["beta"]);
    assert_eq!(names2, vec!["gamma"]);

    let total: usize = outputs.iter().map(|o| o.num_rows).sum();
    assert_eq!(total, 8);
}

#[test]
fn test_merge_descending_pre_timestamp_column() {
    // Verify that a sort schema with a descending pre-timestamp column
    // produces correct merge output. The sorted_series encoding must
    // invert storekey bytes for descending columns so that ascending
    // memcmp on the composite key matches the physical sort order.
    //
    // Sort schema: metric_name ASC | -service (DESC) | timeseries_id | timestamp_secs
    // Physical order: within same metric_name, service is descending
    // (zebra before alpha).
    let dir = TempDir::new().unwrap();

    let desc_sort_fields = "metric_name|-service|timeseries_id|timestamp_secs/V2";

    // Build input with a "service" column.
    let metric_names: Vec<&str> = vec!["cpu", "cpu", "cpu", "cpu"];
    let services: Vec<&str> = vec!["alpha", "alpha", "zebra", "zebra"];
    let timestamps: Vec<u64> = vec![200, 100, 200, 100];
    let values: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0];
    let tsids: Vec<i64> = vec![10, 10, 20, 20];

    let metric_name_array: DictionaryArray<Int32Type> =
        metric_names.iter().map(|s| Some(*s)).collect();
    let service_array: DictionaryArray<Int32Type> = services.iter().map(|s| Some(*s)).collect();
    let timestamp_array = UInt64Array::from(timestamps);
    let value_array = arrow::array::Float64Array::from(values);
    let tsid_array = Int64Array::from(tsids);
    let metric_type_array = UInt8Array::from(vec![0u8; 4]);

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "metric_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new(
            "service",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("timeseries_id", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(metric_name_array),
            Arc::new(metric_type_array),
            Arc::new(service_array),
            Arc::new(timestamp_array),
            Arc::new(value_array),
            Arc::new(tsid_array),
        ],
    )
    .unwrap();

    // Write with the descending sort schema.
    let table_config = TableConfig {
        product_type: ProductType::Metrics,
        sort_fields: Some(desc_sort_fields.to_string()),
        window_duration_secs: 900,
    };
    let writer = ParquetWriter::new(ParquetWriterConfig::default(), &table_config).unwrap();

    let metadata = ParquetSplitMetadata::metrics_builder()
        .split_id(ParquetSplitId::generate(ParquetSplitKind::Metrics))
        .index_uid("test-index:0")
        .sort_fields(desc_sort_fields)
        .window_duration_secs(900)
        .window_start_secs(0)
        .time_range(TimeRange::new(0, 1))
        .build();

    let input_path = dir.path().join("input_desc.parquet");
    writer
        .write_to_file_with_metadata(&batch, &input_path, Some(&metadata))
        .unwrap();

    // Read back and verify the input is sorted correctly:
    // cpu/zebra (DESC service) before cpu/alpha.
    let input_batch = read_parquet_file(&input_path);
    let input_services = extract_string_column(&input_batch, "service");
    assert_eq!(
        input_services,
        vec!["zebra", "zebra", "alpha", "alpha"],
        "input must be sorted with service descending"
    );

    // Now merge — this should preserve the sort order.
    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let config = MergeConfig {
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
    };

    let outputs = merge_sorted_parquet_files(&[input_path], &output_dir, &config).unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].num_rows, 4);

    let output_batch = read_parquet_file(&outputs[0].path);
    let output_services = extract_string_column(&output_batch, "service");

    // Service must still be descending within same metric_name:
    // zebra before alpha.
    assert_eq!(
        output_services,
        vec!["zebra", "zebra", "alpha", "alpha"],
        "merge output must preserve descending service order"
    );
}

// ---- Proptest DST: property-based invariant verification ----

mod proptests {
    use proptest::prelude::*;

    use super::*;

    /// Strategy to generate a list of metric names from a small set.
    fn metric_name_strategy() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("alpha".to_string()),
            Just("beta".to_string()),
            Just("gamma".to_string()),
            Just("delta".to_string()),
        ]
    }

    /// Generate a single input file's worth of data: metric names, timestamps,
    /// values, and timeseries IDs. All vectors have the same length.
    fn input_data_strategy(
        max_rows: usize,
    ) -> impl Strategy<Value = (Vec<String>, Vec<i64>, Vec<f64>, Vec<i64>)> {
        (1..=max_rows).prop_flat_map(|n| {
            (
                proptest::collection::vec(metric_name_strategy(), n),
                proptest::collection::vec(100i64..1000i64, n),
                proptest::collection::vec(-1e6f64..1e6f64, n),
                proptest::collection::vec(1i64..100i64, n),
            )
        })
    }

    /// Write a test split from generated data and return its path.
    fn write_generated_split(
        dir: &std::path::Path,
        name: &str,
        metric_names: &[String],
        timestamps: &[i64],
        values: &[f64],
        timeseries_ids: &[i64],
    ) -> PathBuf {
        write_test_split(
            dir,
            name,
            &metric_names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            timestamps,
            values,
            timeseries_ids,
        )
    }

    proptest! {
        /// MC-1: Row count is preserved through merge.
        /// MC-3: Output is sorted by (sorted_series ASC, timestamp_secs per sort schema).
        /// MC-2: All input data values appear in the output.
        #[test]
        fn prop_merge_preserves_invariants(
            data1 in input_data_strategy(20),
            data2 in input_data_strategy(20),
            num_outputs in 1usize..=3,
        ) {
            let dir = TempDir::new().unwrap();

            let input1 = write_generated_split(
                dir.path(), "input1.parquet",
                &data1.0, &data1.1, &data1.2, &data1.3,
            );
            let input2 = write_generated_split(
                dir.path(), "input2.parquet",
                &data2.0, &data2.1, &data2.2, &data2.3,
            );

            let output_dir = dir.path().join("output");
            std::fs::create_dir_all(&output_dir).unwrap();

            let config = MergeConfig {
                num_outputs,
                writer_config: ParquetWriterConfig::default(),
            };

            let total_input_rows = data1.0.len() + data2.0.len();

            let outputs = merge_sorted_parquet_files(
                &[input1, input2], &output_dir, &config,
            ).unwrap();

            // MC-1: total rows preserved.
            let total_output_rows: usize = outputs.iter().map(|o| o.num_rows).sum();
            prop_assert_eq!(total_output_rows, total_input_rows,
                "MC-1: input rows {} != output rows {}", total_input_rows, total_output_rows);

            // MC-3: each output is sorted (verified by check_invariant! inside
            // the merge engine, but also verify here explicitly).
            for output in &outputs {
                let batch = read_parquet_file(&output.path);
                let ss = extract_binary_column(&batch, SORTED_SERIES_COLUMN);
                let ts = extract_u64_column(&batch, "timestamp_secs");

                for i in 0..batch.num_rows().saturating_sub(1) {
                    // sorted_series non-decreasing.
                    prop_assert!(ss[i] <= ss[i + 1],
                        "MC-3: sorted_series decreased at row {}", i);
                    // Within same series, timestamp non-increasing.
                    if ss[i] == ss[i + 1] {
                        prop_assert!(ts[i] >= ts[i + 1],
                            "MC-3: timestamp not descending at row {}: {} < {}",
                            i, ts[i], ts[i + 1]);
                    }
                }
            }

            // MC-2: all input values appear in the output (check via value column).
            let mut input_values: Vec<f64> = data1.2.iter().chain(data2.2.iter()).copied().collect();
            input_values.sort_by(|a, b| a.partial_cmp(b).unwrap());

            let mut output_values: Vec<f64> = Vec::new();
            for output in &outputs {
                let batch = read_parquet_file(&output.path);
                output_values.extend(extract_f64_column(&batch, "value"));
            }
            output_values.sort_by(|a, b| a.partial_cmp(b).unwrap());

            prop_assert_eq!(input_values, output_values,
                "MC-2: values must be preserved through merge");

            // Metadata: every output has row keys.
            for output in &outputs {
                prop_assert!(output.row_keys_proto.is_some(),
                    "every output must have row keys");
            }
        }

        /// Test with 3+ inputs and varying output counts.
        #[test]
        fn prop_merge_many_inputs(
            data1 in input_data_strategy(10),
            data2 in input_data_strategy(10),
            data3 in input_data_strategy(10),
            num_outputs in 1usize..=4,
        ) {
            let dir = TempDir::new().unwrap();

            let input1 = write_generated_split(
                dir.path(), "i1.parquet", &data1.0, &data1.1, &data1.2, &data1.3,
            );
            let input2 = write_generated_split(
                dir.path(), "i2.parquet", &data2.0, &data2.1, &data2.2, &data2.3,
            );
            let input3 = write_generated_split(
                dir.path(), "i3.parquet", &data3.0, &data3.1, &data3.2, &data3.3,
            );

            let output_dir = dir.path().join("output");
            std::fs::create_dir_all(&output_dir).unwrap();

            let config = MergeConfig {
                num_outputs,
                writer_config: ParquetWriterConfig::default(),
            };

            let total_input_rows = data1.0.len() + data2.0.len() + data3.0.len();

            let outputs = merge_sorted_parquet_files(
                &[input1, input2, input3], &output_dir, &config,
            ).unwrap();

            // MC-1: row count preserved.
            let total_output_rows: usize = outputs.iter().map(|o| o.num_rows).sum();
            prop_assert_eq!(total_output_rows, total_input_rows);

            // MC-3: each output sorted.
            for output in &outputs {
                let batch = read_parquet_file(&output.path);
                let ss = extract_binary_column(&batch, SORTED_SERIES_COLUMN);
                for i in 0..batch.num_rows().saturating_sub(1) {
                    prop_assert!(ss[i] <= ss[i + 1],
                        "MC-3: sorted_series decreased at row {}", i);
                }
            }
        }
    }
}
