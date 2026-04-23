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
    Array, DictionaryArray, Int64Array, RecordBatch, StringArray, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use tempfile::TempDir;

use crate::merge::{MergeConfig, merge_sorted_parquet_files};
use crate::sorted_series::SORTED_SERIES_COLUMN;
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
    let metric_name_array: DictionaryArray<Int32Type> = metric_names
        .iter()
        .map(|s| Some(*s))
        .collect();

    let timestamp_array = UInt64Array::from(
        timestamps.iter().map(|&t| t as u64).collect::<Vec<u64>>(),
    );
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

    // Use the standard writer which sorts, adds sorted_series, reorders.
    let table_config = TableConfig {
        product_type: ProductType::Metrics,
        sort_fields: Some(TEST_SORT_FIELDS.to_string()),
        window_duration_secs: 900,
    };
    let writer_config = ParquetWriterConfig::default();
    let writer = ParquetWriter::new(writer_config, &table_config).unwrap();

    let path = dir.join(name);
    writer
        .write_to_file_with_metadata(&batch, &path, None)
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
        sort_fields: TEST_SORT_FIELDS.to_string(),
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
        window_start_secs: Some(0),
        window_duration_secs: 900,
        input_num_merge_ops: 0,
    };

    let outputs = merge_sorted_parquet_files(&[input1, input2], &output_dir, &config).unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].num_rows, 4);

    // Read back and verify sort order.
    let batch = read_parquet_file(&outputs[0].path);
    assert_eq!(batch.num_rows(), 4);

    // metric_name should be sorted: cpu, cpu, mem, mem.
    let metric_names = extract_dict_string_column(&batch, "metric_name");
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
        sort_fields: TEST_SORT_FIELDS.to_string(),
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
        window_start_secs: Some(0),
        window_duration_secs: 900,
        input_num_merge_ops: 1,
    };

    let outputs = merge_sorted_parquet_files(&[input1, input2], &output_dir, &config).unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].num_rows, 4);

    let batch = read_parquet_file(&outputs[0].path);

    // Expected: cpu (t=200, t=100), mem (t=200, t=100).
    let metric_names = extract_dict_string_column(&batch, "metric_name");
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
        sort_fields: TEST_SORT_FIELDS.to_string(),
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
        window_start_secs: Some(0),
        window_duration_secs: 900,
        input_num_merge_ops: 0,
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
        sort_fields: TEST_SORT_FIELDS.to_string(),
        num_outputs: 3,
        writer_config: ParquetWriterConfig::default(),
        window_start_secs: Some(0),
        window_duration_secs: 900,
        input_num_merge_ops: 0,
    };

    let outputs = merge_sorted_parquet_files(&[input1, input2], &output_dir, &config).unwrap();

    // Should produce up to 3 outputs, each with non-overlapping metric names.
    assert!(!outputs.is_empty());
    assert!(outputs.len() <= 3);

    let total_rows: usize = outputs.iter().map(|o| o.num_rows).sum();
    assert_eq!(total_rows, 6);

    // Each output should have row keys.
    for output in &outputs {
        assert!(output.row_keys_proto.is_some());
    }
}

#[test]
fn test_merge_empty_inputs() {
    let dir = TempDir::new().unwrap();

    let output_dir = dir.path().join("output");
    std::fs::create_dir_all(&output_dir).unwrap();

    let config = MergeConfig {
        sort_fields: TEST_SORT_FIELDS.to_string(),
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
        window_start_secs: Some(0),
        window_duration_secs: 900,
        input_num_merge_ops: 0,
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
        sort_fields: TEST_SORT_FIELDS.to_string(),
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
        window_start_secs: Some(900),
        window_duration_secs: 900,
        input_num_merge_ops: 2,
    };

    let outputs = merge_sorted_parquet_files(&[input1], &output_dir, &config).unwrap();
    assert_eq!(outputs.len(), 1);

    // Read back the Parquet file and verify KV metadata.
    let file = std::fs::File::open(&outputs[0].path).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap();
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

    assert_eq!(
        find_kv("qh.sort_fields").as_deref(),
        Some(TEST_SORT_FIELDS)
    );
    assert_eq!(find_kv("qh.window_start").as_deref(), Some("900"));
    assert_eq!(find_kv("qh.window_duration_secs").as_deref(), Some("900"));
    // num_merge_ops = input_num_merge_ops (2) + 1 = 3.
    assert_eq!(find_kv("qh.num_merge_ops").as_deref(), Some("3"));
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
        sort_fields: TEST_SORT_FIELDS.to_string(),
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
        window_start_secs: Some(0),
        window_duration_secs: 900,
        input_num_merge_ops: 0,
    };

    let outputs = merge_sorted_parquet_files(&[input1], &output_dir, &config).unwrap();
    let batch = read_parquet_file(&outputs[0].path);

    let schema = batch.schema();
    let col_names: Vec<&str> = schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    // Sort schema columns first (metric_name, timestamp_secs), then
    // sorted_series, then remaining alphabetically.
    let metric_pos = col_names.iter().position(|n| *n == "metric_name").unwrap();
    let ts_pos = col_names.iter().position(|n| *n == "timestamp_secs").unwrap();
    let ss_pos = col_names.iter().position(|n| *n == SORTED_SERIES_COLUMN).unwrap();

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

fn extract_dict_string_column(batch: &RecordBatch, name: &str) -> Vec<String> {
    let idx = batch.schema().index_of(name).unwrap();
    let col = batch.column(idx);
    let dict = col
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .expect("expected Dict<Int32, Utf8>");
    let values = dict
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    (0..dict.len())
        .map(|i| {
            let key = dict.keys().value(i) as usize;
            values.value(key).to_string()
        })
        .collect()
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
    let metric_name_array: DictionaryArray<Int32Type> =
        vec![Some("cpu")].into_iter().collect();
    let timestamp_array = UInt64Array::from(vec![100u64]);
    let value_array = arrow::array::Float64Array::from(vec![1.0]);
    let tsid_array = Int64Array::from(vec![42i64]);
    let metric_type_array = UInt8Array::from(vec![0u8]);
    let extra_tag_array: DictionaryArray<Int32Type> =
        vec![Some("us-east")].into_iter().collect();

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
        .write_to_file_with_metadata(&batch_with_extra, &path1, None)
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
        sort_fields: TEST_SORT_FIELDS.to_string(),
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
        window_start_secs: Some(0),
        window_duration_secs: 900,
        input_num_merge_ops: 0,
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
        sort_fields: TEST_SORT_FIELDS.to_string(),
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
        window_start_secs: Some(0),
        window_duration_secs: 900,
        input_num_merge_ops: 0,
    };

    let outputs = merge_sorted_parquet_files(&[input1, input2], &output_dir, &config).unwrap();
    let batch = read_parquet_file(&outputs[0].path);

    // Collect all (metric_name, timestamp, value) triples from output.
    let names = extract_dict_string_column(&batch, "metric_name");
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

    assert_eq!(output_triples, expected, "MC-2: row contents must be preserved");
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
        sort_fields: TEST_SORT_FIELDS.to_string(),
        num_outputs: 1,
        writer_config: ParquetWriterConfig::default(),
        window_start_secs: Some(0),
        window_duration_secs: 900,
        input_num_merge_ops: 0,
    };

    let outputs =
        merge_sorted_parquet_files(&[input1, input2], &output_dir, &config).unwrap();
    let output_batch = read_parquet_file(&outputs[0].path);
    let output_ss = extract_binary_column(&output_batch, SORTED_SERIES_COLUMN);

    // All inputs had the same metric and timeseries_id, so sorted_series
    // should be identical across all rows.
    assert_eq!(input1_ss[0], input2_ss[0], "same series should have same key");

    // DM-5: output sorted_series values must match input values.
    for ss in &output_ss {
        assert_eq!(
            ss, &input1_ss[0],
            "DM-5: sorted_series must be preserved through merge"
        );
    }
}

// ---- Proptest DST: property-based invariant verification ----

mod proptests {
    use super::*;
    use proptest::prelude::*;

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
        /// MC-3: Output is sorted by (sorted_series ASC, timestamp_secs DESC).
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
                sort_fields: TEST_SORT_FIELDS.to_string(),
                num_outputs,
                writer_config: ParquetWriterConfig::default(),
                window_start_secs: Some(0),
                window_duration_secs: 900,
                input_num_merge_ops: 0,
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
                sort_fields: TEST_SORT_FIELDS.to_string(),
                num_outputs,
                writer_config: ParquetWriterConfig::default(),
                window_start_secs: Some(0),
                window_duration_secs: 900,
                input_num_merge_ops: 0,
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
