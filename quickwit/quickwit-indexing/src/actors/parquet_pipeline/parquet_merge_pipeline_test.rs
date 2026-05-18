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

//! Integration test for the Parquet merge pipeline.
//!
//! Tests the full actor chain: seeds splits → planner plans merge →
//! downloader downloads from storage → executor merges → uploader uploads →
//! publisher publishes with replaced_split_ids.
//!
//! Verifies:
//! - Staged metadata (num_rows, time_range, metric_names, tags, merge_ops, row_keys,
//!   zonemap_regexes)
//! - Merged Parquet file contents (row count, column values, sort order)
//! - Parquet KV headers (qh.sort_fields, qh.num_merge_ops, qh.row_keys, qh.zonemap_regexes)
//! - Cross-validation: metadata agrees with file contents

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, DictionaryArray, Float64Array, Int32Array, Int64Array, StringArray,
    UInt8Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use quickwit_actors::Universe;
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_common::test_utils::wait_until_predicate;
use quickwit_metastore::StageParquetSplitsRequestExt;
use quickwit_parquet_engine::merge::policy::{
    ConstWriteAmplificationParquetMergePolicy, ParquetMergePolicyConfig,
};
use quickwit_parquet_engine::sorted_series::SORTED_SERIES_COLUMN;
use quickwit_parquet_engine::split::{ParquetSplitId, ParquetSplitMetadata, TimeRange};
use quickwit_parquet_engine::storage::{ParquetWriter, ParquetWriterConfig};
use quickwit_parquet_engine::table_config::TableConfig;
use quickwit_parquet_engine::test_helpers::create_nullable_dict_array;
use quickwit_proto::metastore::{
    EmptyResponse, MetastoreServiceClient, MockMetastoreService, StageMetricsSplitsRequest,
};
use quickwit_storage::{RamStorage, Storage};

use super::parquet_merge_pipeline::{ParquetMergePipeline, ParquetMergePipelineParams};

// ---------------------------------------------------------------------------
// Test data helpers
// ---------------------------------------------------------------------------

/// Creates a RecordBatch with configurable metric name, timestamp range, and
/// tag values. This allows building diverse inputs to verify the merge engine
/// handles heterogeneous data correctly.
pub(super) fn create_custom_test_batch(
    metric_name: &str,
    ts_start: u64,
    num_rows: usize,
    service_val: &str,
    host_val: &str,
) -> RecordBatch {
    let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

    let fields = vec![
        Field::new("metric_name", dict_type.clone(), false),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("timeseries_id", DataType::Int64, false),
        Field::new("service", dict_type.clone(), true),
        Field::new("host", dict_type.clone(), true),
    ];
    let schema = Arc::new(ArrowSchema::new(fields));

    let metric_names: Vec<&str> = vec![metric_name; num_rows];
    let mn_keys: Vec<i32> = (0..num_rows).map(|i| i as i32).collect();
    let mn_values = StringArray::from(metric_names);
    let metric_name_arr: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(mn_keys), Arc::new(mn_values))
            .unwrap(),
    );

    let metric_type: ArrayRef = Arc::new(UInt8Array::from(vec![0u8; num_rows]));

    let timestamps: Vec<u64> = (0..num_rows).map(|i| ts_start + i as u64).collect();
    let timestamp_secs: ArrayRef = Arc::new(UInt64Array::from(timestamps));

    let values: Vec<f64> = (0..num_rows).map(|i| 42.0 + i as f64).collect();
    let value: ArrayRef = Arc::new(Float64Array::from(values));

    // Each row gets a unique timeseries_id based on the metric name hash + row
    // index. This ensures split-a and split-b have non-overlapping IDs.
    let ts_id_base: i64 = metric_name.bytes().map(|b| b as i64).sum::<i64>() * 1000;
    let timeseries_ids: Vec<i64> = (0..num_rows).map(|i| ts_id_base + i as i64).collect();
    let timeseries_id: ArrayRef = Arc::new(Int64Array::from(timeseries_ids));

    let service_vals: Vec<Option<&str>> = vec![Some(service_val); num_rows];
    let service: ArrayRef = create_nullable_dict_array(&service_vals);

    let host_vals: Vec<Option<&str>> = vec![Some(host_val); num_rows];
    let host: ArrayRef = create_nullable_dict_array(&host_vals);

    RecordBatch::try_new(
        schema,
        vec![
            metric_name_arr,
            metric_type,
            timestamp_secs,
            value,
            timeseries_id,
            service,
            host,
        ],
    )
    .unwrap()
}

/// Write a sorted Parquet file to the given directory using the standard
/// writer (which computes sorted_series, row_keys, zonemaps, and KV metadata).
pub(super) fn write_test_parquet_file(
    dir: &Path,
    filename: &str,
    batch: &RecordBatch,
    split_metadata: &ParquetSplitMetadata,
) -> u64 {
    let table_config = TableConfig::default();
    let writer = ParquetWriter::new(ParquetWriterConfig::default(), &table_config)
        .expect("failed to create ParquetWriter");

    let path = dir.join(filename);
    let (file_size, _write_metadata) = writer
        .write_to_file_with_metadata(batch, &path, Some(split_metadata))
        .expect("failed to write test Parquet file");
    file_size
}

/// Create a ParquetSplitMetadata consistent with the test Parquet writer.
pub(super) fn make_test_split_metadata(
    split_id: &str,
    num_rows: u64,
    size_bytes: u64,
    ts_start: u64,
    metric_name: &str,
) -> ParquetSplitMetadata {
    let table_config = TableConfig::default();
    ParquetSplitMetadata::metrics_builder()
        .split_id(ParquetSplitId::new(split_id))
        .index_uid("test-merge-index:00000000000000000000000001")
        .partition_id(0)
        .time_range(TimeRange::new(ts_start, ts_start + num_rows))
        .num_rows(num_rows)
        .size_bytes(size_bytes)
        .sort_fields(table_config.effective_sort_fields())
        .window_start_secs(0)
        .window_duration_secs(900)
        .add_metric_name(metric_name)
        .build()
}

/// Same as `make_test_split_metadata` but for sketch splits.
pub(super) fn make_test_sketch_split_metadata(
    split_id: &str,
    num_rows: u64,
    size_bytes: u64,
    ts_start: u64,
    metric_name: &str,
) -> ParquetSplitMetadata {
    let table_config = TableConfig::default();
    ParquetSplitMetadata::sketches_builder()
        .split_id(ParquetSplitId::new(split_id))
        .index_uid("datadog-sketches-test:00000000000000000000000001")
        .partition_id(0)
        .time_range(TimeRange::new(ts_start, ts_start + num_rows))
        .num_rows(num_rows)
        .size_bytes(size_bytes)
        .sort_fields(table_config.effective_sort_fields())
        .window_start_secs(0)
        .window_duration_secs(900)
        .add_metric_name(metric_name)
        .build()
}

// ---------------------------------------------------------------------------
// Parquet reading helpers (for verifying merged output)
// ---------------------------------------------------------------------------

/// Read a Parquet file from raw bytes into a single concatenated RecordBatch.
pub(super) fn read_parquet_from_bytes(data: &[u8]) -> RecordBatch {
    let bytes = bytes::Bytes::copy_from_slice(data);
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let reader = builder.build().unwrap();

    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert!(!batches.is_empty(), "expected at least one batch");

    let schema = batches[0].schema();
    arrow::compute::concat_batches(&schema, &batches).unwrap()
}

/// Extract the Parquet file-level KV metadata from raw bytes.
pub(super) fn extract_parquet_kv_metadata(data: &[u8]) -> HashMap<String, String> {
    let bytes = bytes::Bytes::copy_from_slice(data);
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let kv = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .expect("kv metadata must be present");
    kv.iter()
        .filter_map(|entry| entry.value.as_ref().map(|v| (entry.key.clone(), v.clone())))
        .collect()
}

/// Extract string values from a column that may be Dictionary-encoded or plain
/// Utf8. Handles all representations uniformly.
pub(super) fn extract_string_column(batch: &RecordBatch, name: &str) -> Vec<String> {
    let idx = batch.schema().index_of(name).unwrap();
    let col = batch.column(idx);

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

pub(super) fn extract_u64_column(batch: &RecordBatch, name: &str) -> Vec<u64> {
    let idx = batch.schema().index_of(name).unwrap();
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("expected UInt64")
        .values()
        .to_vec()
}

pub(super) fn extract_binary_column(batch: &RecordBatch, name: &str) -> Vec<Vec<u8>> {
    let idx = batch.schema().index_of(name).unwrap();
    let col = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>()
        .expect("expected Binary");
    (0..col.len()).map(|i| col.value(i).to_vec()).collect()
}

// ---------------------------------------------------------------------------
// Integration test
// ---------------------------------------------------------------------------

/// Full integration test: seed splits → merge → verify everything.
///
/// Creates 2 real sorted Parquet files with DIFFERENT data:
/// - split-a: 50 rows, metric "cpu.usage", timestamps 100–149, service="web", host="host-1"
/// - split-b: 50 rows, metric "mem.usage", timestamps 200–249, service="api", host="host-2"
///
/// Then verifies the merge pipeline:
/// 1. Plans a merge (merge_factor=2)
/// 2. Downloads files from storage
/// 3. Executes the merge via the k-way merge engine
/// 4. Uploads the merged output
/// 5. Publishes with replaced_split_ids matching the input splits
///
/// And asserts:
/// - Staged metadata correctness (num_rows, time_range, metric_names, tags, num_merge_ops,
///   row_keys, zonemaps)
/// - Merged Parquet file data (row count, column values, sort order)
/// - Parquet KV headers (qh.sort_fields, qh.num_merge_ops, qh.row_keys, qh.zonemap_regexes)
/// - Cross-validation: metadata matches file contents
#[tokio::test]
async fn test_merge_pipeline_end_to_end() {
    quickwit_common::setup_logging_for_tests();

    let universe = Universe::with_accelerated_time();
    let temp_dir = tempfile::tempdir().unwrap();
    let ram_storage: Arc<dyn Storage> = Arc::new(RamStorage::default());

    // --- Step 1: Create diverse Parquet files and upload to storage ---

    let batch_a = create_custom_test_batch("cpu.usage", 100, 50, "web", "host-1");
    let meta_a = make_test_split_metadata("split-a", 50, 0, 100, "cpu.usage");
    let size_a = write_test_parquet_file(temp_dir.path(), "split-a.parquet", &batch_a, &meta_a);
    let meta_a = {
        let mut m = meta_a;
        m.size_bytes = size_a;
        m.parquet_file = "split-a.parquet".to_string();
        m
    };

    let batch_b = create_custom_test_batch("mem.usage", 200, 50, "api", "host-2");
    let meta_b = make_test_split_metadata("split-b", 50, 0, 200, "mem.usage");
    let size_b = write_test_parquet_file(temp_dir.path(), "split-b.parquet", &batch_b, &meta_b);
    let meta_b = {
        let mut m = meta_b;
        m.size_bytes = size_b;
        m.parquet_file = "split-b.parquet".to_string();
        m
    };

    // Upload files to RamStorage.
    let content_a = std::fs::read(temp_dir.path().join("split-a.parquet")).unwrap();
    ram_storage
        .put(Path::new("split-a.parquet"), Box::new(content_a))
        .await
        .unwrap();
    let content_b = std::fs::read(temp_dir.path().join("split-b.parquet")).unwrap();
    ram_storage
        .put(Path::new("split-b.parquet"), Box::new(content_b))
        .await
        .unwrap();

    // --- Step 2: Set up mock metastore (capture staged metadata) ---

    let mut mock_metastore = MockMetastoreService::new();

    // Capture the staged split metadata for verification.
    let staged_metadata: Arc<std::sync::Mutex<Vec<ParquetSplitMetadata>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let staged_metadata_clone = staged_metadata.clone();

    mock_metastore.expect_stage_metrics_splits().returning(
        move |request: StageMetricsSplitsRequest| {
            let splits = request
                .deserialize_splits_metadata()
                .expect("failed to deserialize staged metadata");
            staged_metadata_clone.lock().unwrap().extend(splits);
            Ok(EmptyResponse {})
        },
    );

    // Capture the publish request to verify replaced_split_ids.
    let publish_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let publish_called_clone = publish_called.clone();
    let replaced_ids = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let replaced_ids_clone = replaced_ids.clone();

    mock_metastore
        .expect_publish_metrics_splits()
        .returning(move |request| {
            replaced_ids_clone
                .lock()
                .unwrap()
                .extend(request.replaced_split_ids.clone());
            publish_called_clone.store(true, Ordering::SeqCst);
            Ok(EmptyResponse {})
        });

    let metastore = MetastoreServiceClient::from_mock(mock_metastore);

    // --- Step 3: Spawn the merge pipeline ---

    let merge_policy = Arc::new(ConstWriteAmplificationParquetMergePolicy::new(
        ParquetMergePolicyConfig {
            merge_factor: 2,
            max_merge_factor: 2,
            max_merge_ops: 5,
            target_split_size_bytes: 256 * 1024 * 1024,
            maturation_period: Duration::from_secs(3600),
            max_finalize_merge_operations: 3,
        },
    ));

    let params = ParquetMergePipelineParams {
        index_uid: quickwit_proto::types::IndexUid::for_test("test-merge-index", 0),
        indexing_directory: TempDirectory::for_test(),
        metastore,
        storage: ram_storage.clone(),
        merge_policy,
        merge_scheduler_service: universe.get_or_spawn_one(),
        max_concurrent_split_uploads: 4,
        event_broker: EventBroker::default(),
        writer_config: ParquetWriterConfig::default(),
    };

    let initial_splits = vec![meta_a, meta_b];
    let pipeline = ParquetMergePipeline::new(params, Some(initial_splits), universe.spawn_ctx());
    let (_pipeline_mailbox, _pipeline_handle) = universe.spawn_builder().spawn(pipeline);

    // --- Step 4: Wait for publish with replaced_split_ids ---

    wait_until_predicate(
        || {
            let publish_called = publish_called.clone();
            async move { publish_called.load(Ordering::SeqCst) }
        },
        Duration::from_secs(30),
        Duration::from_millis(100),
    )
    .await
    .expect("timed out waiting for merge publish");

    // --- Step 5: Verify replaced_split_ids ---

    let mut replaced_sorted: Vec<String> = replaced_ids.lock().unwrap().clone();
    replaced_sorted.sort();
    assert_eq!(
        replaced_sorted,
        vec!["split-a".to_string(), "split-b".to_string()],
        "publish should replace both input splits"
    );

    // --- Step 6: Verify staged metadata ---

    let staged = staged_metadata.lock().unwrap().clone();
    assert_eq!(staged.len(), 1, "exactly one merged split should be staged");
    let merged_meta = &staged[0];

    // MC-1: total row count preserved.
    assert_eq!(merged_meta.num_rows, 100, "merged split must have 100 rows");

    // Time range covers both inputs: min(100) to max(249+1=250).
    assert_eq!(
        merged_meta.time_range.start_secs, 100,
        "time_range.start should be min of inputs"
    );
    assert_eq!(
        merged_meta.time_range.end_secs, 250,
        "time_range.end should be max timestamp + 1"
    );

    // Metric names from both inputs.
    let expected_metrics: HashSet<String> = ["cpu.usage", "mem.usage"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(
        merged_meta.metric_names, expected_metrics,
        "merged split must contain both metric names"
    );

    // num_merge_ops: max(0, 0) + 1 = 1.
    assert_eq!(
        merged_meta.num_merge_ops, 1,
        "first merge should set num_merge_ops to 1"
    );

    // Sort fields should be preserved from inputs.
    let table_config = TableConfig::default();
    assert_eq!(
        merged_meta.sort_fields,
        table_config.effective_sort_fields(),
        "sort_fields must be preserved through merge"
    );

    // Row keys should be present (the writer always computes them).
    assert!(
        merged_meta.row_keys_proto.is_some(),
        "merged split must have row_keys_proto"
    );
    let row_keys_bytes = merged_meta.row_keys_proto.as_ref().unwrap();
    assert!(
        !row_keys_bytes.is_empty(),
        "row_keys_proto must not be empty"
    );

    // Zonemap regexes should be present for string sort columns.
    assert!(
        !merged_meta.zonemap_regexes.is_empty(),
        "merged split must have zonemap regexes"
    );
    // metric_name is a string sort column, so it must have a zonemap regex.
    assert!(
        merged_meta.zonemap_regexes.contains_key("metric_name"),
        "zonemap_regexes must include metric_name; got keys: {:?}",
        merged_meta.zonemap_regexes.keys().collect::<Vec<_>>()
    );

    // low_cardinality_tags: only "service" is extracted in both the ingest
    // and merge paths (see extract_service_names in split_writer.rs and
    // merge/writer.rs). Other tag columns (host, env, etc.) are fully
    // covered by zonemap regexes and row keys for pruning — this field
    // is a secondary optimization for exact-match Postgres queries.
    assert!(
        merged_meta.low_cardinality_tags.contains_key("service"),
        "tags must include 'service'; got: {:?}",
        merged_meta.low_cardinality_tags.keys().collect::<Vec<_>>()
    );
    let service_tags = &merged_meta.low_cardinality_tags["service"];
    let expected_services: HashSet<String> = ["web", "api"].iter().map(|s| s.to_string()).collect();
    assert_eq!(
        *service_tags, expected_services,
        "service tag values must include both inputs"
    );

    // --- Step 7: Read back merged Parquet file and verify contents ---

    let merged_parquet_path = &merged_meta.parquet_file;
    let merged_bytes = ram_storage
        .get_all(Path::new(merged_parquet_path))
        .await
        .expect("merged parquet file must exist in storage");

    let merged_batch = read_parquet_from_bytes(&merged_bytes);

    // MC-1: row count matches metadata.
    assert_eq!(
        merged_batch.num_rows(),
        100,
        "Parquet file row count must match metadata"
    );
    assert_eq!(
        merged_batch.num_rows() as u64,
        merged_meta.num_rows,
        "Parquet row count must equal staged metadata num_rows"
    );

    // Verify all expected metric names are present.
    let metric_names_in_file: HashSet<String> = extract_string_column(&merged_batch, "metric_name")
        .into_iter()
        .collect();
    assert_eq!(
        metric_names_in_file, expected_metrics,
        "Parquet file must contain both metric names"
    );

    // Verify all expected timestamps are present.
    let timestamps_in_file = extract_u64_column(&merged_batch, "timestamp_secs");
    assert_eq!(
        timestamps_in_file.len(),
        100,
        "must have 100 timestamp values"
    );
    let ts_min = *timestamps_in_file.iter().min().unwrap();
    let ts_max = *timestamps_in_file.iter().max().unwrap();
    assert_eq!(ts_min, 100, "min timestamp must be 100");
    assert_eq!(ts_max, 249, "max timestamp must be 249");

    // Cross-validate: metadata time_range matches actual timestamps.
    assert_eq!(
        merged_meta.time_range.start_secs, ts_min,
        "metadata time_range.start must match actual min timestamp"
    );
    assert_eq!(
        merged_meta.time_range.end_secs,
        ts_max + 1,
        "metadata time_range.end must be actual max timestamp + 1"
    );

    // Verify all tag values from both inputs survive in the Parquet data
    // (even if the metadata doesn't track all tag columns yet).
    let services_in_file: HashSet<String> = extract_string_column(&merged_batch, "service")
        .into_iter()
        .collect();
    assert_eq!(
        services_in_file, expected_services,
        "Parquet file must contain both service values"
    );
    let expected_hosts: HashSet<String> =
        ["host-1", "host-2"].iter().map(|s| s.to_string()).collect();
    let hosts_in_file: HashSet<String> = extract_string_column(&merged_batch, "host")
        .into_iter()
        .collect();
    assert_eq!(
        hosts_in_file, expected_hosts,
        "Parquet file must contain both host values"
    );

    // Verify sort order: sorted_series column must be monotonically
    // non-decreasing (the fundamental invariant of the Parquet writer).
    let sorted_series = extract_binary_column(&merged_batch, SORTED_SERIES_COLUMN);
    for i in 1..sorted_series.len() {
        assert!(
            sorted_series[i] >= sorted_series[i - 1],
            "sorted_series must be monotonically non-decreasing at row {}: {:?} < {:?}",
            i,
            &sorted_series[i],
            &sorted_series[i - 1]
        );
    }

    // Verify sort order semantics: cpu.usage rows should come before
    // mem.usage rows (metric_name is the first sort column, ascending).
    let metric_name_vec = extract_string_column(&merged_batch, "metric_name");
    let first_mem_idx = metric_name_vec
        .iter()
        .position(|n| n == "mem.usage")
        .expect("must have mem.usage rows");
    let last_cpu_idx = metric_name_vec
        .iter()
        .rposition(|n| n == "cpu.usage")
        .expect("must have cpu.usage rows");
    assert!(
        last_cpu_idx < first_mem_idx,
        "all cpu.usage rows (last at {}) must precede all mem.usage rows (first at {})",
        last_cpu_idx,
        first_mem_idx
    );

    // --- Step 8: Verify Parquet KV metadata headers ---

    let kv_metadata = extract_parquet_kv_metadata(&merged_bytes);

    // qh.sort_fields must match the table config.
    let qh_sort_fields = kv_metadata
        .get("qh.sort_fields")
        .expect("qh.sort_fields must be present");
    assert_eq!(
        qh_sort_fields,
        &table_config.effective_sort_fields(),
        "qh.sort_fields must match table config"
    );

    // qh.num_merge_ops must be "1" (first merge).
    let qh_merge_ops = kv_metadata
        .get("qh.num_merge_ops")
        .expect("qh.num_merge_ops must be present");
    assert_eq!(qh_merge_ops, "1", "qh.num_merge_ops must be 1");

    // qh.row_keys must be present and non-empty.
    let qh_row_keys = kv_metadata
        .get("qh.row_keys")
        .expect("qh.row_keys must be present");
    assert!(!qh_row_keys.is_empty(), "qh.row_keys must not be empty");

    // qh.zonemap_regexes must be present and contain metric_name.
    let qh_zonemaps = kv_metadata
        .get("qh.zonemap_regexes")
        .expect("qh.zonemap_regexes must be present");
    let zonemaps_parsed: HashMap<String, String> =
        serde_json::from_str(qh_zonemaps).expect("qh.zonemap_regexes must be valid JSON");
    assert!(
        zonemaps_parsed.contains_key("metric_name"),
        "qh.zonemap_regexes must include metric_name"
    );

    // Cross-validate: metadata zonemap_regexes should match Parquet header.
    assert_eq!(
        merged_meta.zonemap_regexes, zonemaps_parsed,
        "metadata zonemap_regexes must match Parquet qh.zonemap_regexes"
    );

    universe.assert_quit().await;
}
