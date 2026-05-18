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

//! End-to-end pipeline tests exercising the merge engines on multi-input,
//! multi-metric, multi-row-group fixtures, in both n=1 and m:n (n > 1)
//! output configurations. Complements `parquet_merge_pipeline_test.rs`
//! (which covers the simpler two-input, one-metric-per-input case) with
//! the harder scenarios:
//!
//! - **Three inputs**, each carrying **three metrics** (`aaa.alpha`, `bbb.beta`,
//!   `ccc.gamma`). Across inputs, the metrics overlap and the per-metric
//!   timeseries IDs collide (each row's `timeseries_id` is derived from the
//!   metric name, so input-x, input-y, input-z all share the same set of IDs
//!   per metric). Timestamps within each (metric, timeseries) overlap across
//!   inputs but are unique — the merge must interleave rows from all three
//!   inputs heavily, not concatenate them.
//! - **Multi-row-group output** via `ParquetWriterConfig::row_group_size = 50`
//!   on the n=1 tests, so the 180-row merge output breaks into 4 row groups.
//!   Exercises the writer's multi-RG path in both engines.
//! - **Multi-row-group inputs with `rg_partition_prefix_len = 1`** in the
//!   bonus tests (`write_prefix_aligned_input`): the writer flushes one row
//!   group per distinct `metric_name`, so each input file carries three row
//!   groups in alignment with the sort prefix. The streaming engine reads
//!   these through its prefix-aware fast path.
//! - **m:n merges** in the bonus tests: a small
//!   `ParquetMergePipelineParams::target_split_size_bytes` forces the
//!   executor to ask the engine for `num_outputs > 1`. The bonus
//!   assertions cover the multi-output contract — sum-equals-total,
//!   internal monotonicity, inter-output `sorted_series` disjointness,
//!   and union-equals-full-set on metrics/services.
//!
//! Both `ParquetMergePipelineParams::use_streaming_engine = false` (the
//! in-memory engine) and `= true` (the streaming engine) are exercised
//! across all four scenarios (n=1 × {prefix_len=0}, n>1 × {prefix_len=1}).
//! Streaming-engine variants additionally assert
//! `PEAK_BODY_COL_PAGE_CACHE_LEN > 0` to confirm the flag routed through
//! the streaming path and not the in-memory fallback.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

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
use quickwit_proto::metastore::{
    EmptyResponse, MetastoreServiceClient, MockMetastoreService, StageMetricsSplitsRequest,
};
use quickwit_storage::{RamStorage, Storage};

use super::parquet_merge_pipeline::{ParquetMergePipeline, ParquetMergePipelineParams};
use super::parquet_merge_pipeline_test::{
    create_custom_test_batch, extract_binary_column, extract_string_column, extract_u64_column,
    read_parquet_from_bytes, write_test_parquet_file,
};

// ---------------------------------------------------------------------------
// Multi-metric fixture
// ---------------------------------------------------------------------------

/// Per-metric specification for one input file: `(metric_name, ts_start, num_rows)`.
/// All rows for a given metric share the same `service` / `host` tag values
/// across the file.
type MetricSpec<'a> = (&'a str, u64, usize);

/// Concatenate per-metric `RecordBatch`es into a single batch covering several
/// metrics. The writer downstream sorts the rows before writing, so the
/// caller does not need to sort.
fn create_multi_metric_batch(
    metrics: &[MetricSpec<'_>],
    service_val: &str,
    host_val: &str,
) -> RecordBatch {
    let batches: Vec<RecordBatch> = metrics
        .iter()
        .map(|(metric, ts_start, num_rows)| {
            create_custom_test_batch(metric, *ts_start, *num_rows, service_val, host_val)
        })
        .collect();
    let schema = batches[0].schema();
    arrow::compute::concat_batches(&schema, &batches).expect("concat multi-metric batches")
}

/// Build a `ParquetSplitMetadata` advertising multiple metric names and a
/// caller-supplied row count + time range. `prefix_len` controls the
/// `rg_partition_prefix_len` stamped into the input file — 0 for legacy
/// inputs, 1 for prefix-aligned inputs (each row group covers exactly one
/// `metric_name`).
fn make_multi_metric_split_metadata(
    split_id: &str,
    num_rows: u64,
    size_bytes: u64,
    ts_start: u64,
    ts_end: u64,
    metric_names: &[&str],
    prefix_len: u32,
) -> ParquetSplitMetadata {
    let table_config = TableConfig::default();
    let mut builder = ParquetSplitMetadata::metrics_builder()
        .split_id(ParquetSplitId::new(split_id))
        .index_uid("test-merge-index-multi:00000000000000000000000001")
        .partition_id(0)
        .time_range(TimeRange::new(ts_start, ts_end))
        .num_rows(num_rows)
        .size_bytes(size_bytes)
        .sort_fields(table_config.effective_sort_fields())
        .window_start_secs(0)
        .window_duration_secs(900)
        .rg_partition_prefix_len(prefix_len);
    for metric in metric_names {
        builder = builder.add_metric_name(*metric);
    }
    builder.build()
}

/// Write a multi-metric input file with `rg_partition_prefix_len = 1` and
/// one row group per distinct metric. Picks `row_group_size = per-metric row
/// count` so the writer naturally flushes at every metric boundary after
/// sorting — each row group ends up containing exactly one distinct
/// `metric_name`, satisfying the prefix-alignment invariant the writer
/// validates on close.
///
/// Returns the file size in bytes (the caller stamps this back into the
/// `ParquetSplitMetadata.size_bytes` field for the planner / executor).
fn write_prefix_aligned_input(
    dir: &Path,
    filename: &str,
    batch: &RecordBatch,
    split_metadata: &ParquetSplitMetadata,
    rows_per_metric: usize,
) -> u64 {
    let table_config = TableConfig::default();
    let writer_config = ParquetWriterConfig::default().with_row_group_size(rows_per_metric);
    let writer = ParquetWriter::new(writer_config, &table_config)
        .expect("test ParquetWriter (prefix-aligned)");
    let path = dir.join(filename);
    let (file_size, _write_metadata) = writer
        .write_to_file_with_metadata(batch, &path, Some(split_metadata))
        .expect("write_to_file_with_metadata for prefix-aligned input");
    file_size
}

/// Three canonical metric names sorted alphabetically. Picked so the sort
/// order is unambiguous (every byte-comparison resolves on the first
/// distinguishing character of the metric name).
const METRIC_AAA: &str = "aaa.alpha";
const METRIC_BBB: &str = "bbb.beta";
const METRIC_CCC: &str = "ccc.gamma";

/// Per-metric row count in each input. With three inputs and three metrics,
/// the total input/output row count is `3 * 3 * ROWS_PER_METRIC = 180`.
const ROWS_PER_METRIC_PER_INPUT: usize = 20;

/// Total rows across all three inputs. Held constant across all tests in this
/// module so per-test expectations stay self-consistent.
const TOTAL_INPUT_ROWS: u64 = 3 * 3 * ROWS_PER_METRIC_PER_INPUT as u64;

/// Output writer's row-group size. Picked small enough that the n=1 merge
/// (180 rows) produces 4 row groups and each n=3 output (~60 rows) produces
/// 2. Exercises the multi-row-group write path in both engines without
/// triggering prefix-alignment validation (we do not set
/// `rg_partition_prefix_len` on inputs).
const TEST_OUTPUT_ROW_GROUP_SIZE: usize = 50;

/// Three inputs that share metric names and per-metric `timeseries_id`
/// ranges (every row's `timeseries_id` is derived from the metric name
/// hash + per-input row index, so cross-input collisions for the same
/// `(metric_name, row_index_within_metric)` are intentional). Timestamps
/// are picked so each (metric, timeseries) appears in all three inputs at
/// three distinct overlapping timestamps — the merge must interleave row
/// streams from all three inputs, not concatenate them.
///
/// Returns `(file_paths, splits_metadata)` for direct use by the pipeline
/// (one entry per input, paths uploaded to the supplied storage).
async fn build_three_multi_metric_inputs(
    temp_dir: &Path,
    ram_storage: &Arc<dyn Storage>,
) -> (Vec<std::path::PathBuf>, Vec<ParquetSplitMetadata>) {
    // input-x: every metric at ts 100..120 with service=web, host=h1.
    let batch_x = create_multi_metric_batch(
        &[
            (METRIC_AAA, 100, ROWS_PER_METRIC_PER_INPUT),
            (METRIC_BBB, 100, ROWS_PER_METRIC_PER_INPUT),
            (METRIC_CCC, 100, ROWS_PER_METRIC_PER_INPUT),
        ],
        "web",
        "h1",
    );

    // input-y: every metric at ts 110..130 with service=api, host=h2.
    let batch_y = create_multi_metric_batch(
        &[
            (METRIC_AAA, 110, ROWS_PER_METRIC_PER_INPUT),
            (METRIC_BBB, 110, ROWS_PER_METRIC_PER_INPUT),
            (METRIC_CCC, 110, ROWS_PER_METRIC_PER_INPUT),
        ],
        "api",
        "h2",
    );

    // input-z: every metric at ts 120..140 with service=db, host=h3.
    let batch_z = create_multi_metric_batch(
        &[
            (METRIC_AAA, 120, ROWS_PER_METRIC_PER_INPUT),
            (METRIC_BBB, 120, ROWS_PER_METRIC_PER_INPUT),
            (METRIC_CCC, 120, ROWS_PER_METRIC_PER_INPUT),
        ],
        "db",
        "h3",
    );

    let mut paths = Vec::new();
    let mut splits = Vec::new();
    for (split_id, batch, ts_start, ts_end, service, host) in [
        ("split-x", &batch_x, 100, 120, "web", "h1"),
        ("split-y", &batch_y, 110, 130, "api", "h2"),
        ("split-z", &batch_z, 120, 140, "db", "h3"),
    ] {
        let _ = (service, host);
        let filename = format!("{split_id}.parquet");
        let num_rows = (3 * ROWS_PER_METRIC_PER_INPUT) as u64;
        let meta = make_multi_metric_split_metadata(
            split_id,
            num_rows,
            0, // size_bytes filled in below
            ts_start,
            ts_end,
            &[METRIC_AAA, METRIC_BBB, METRIC_CCC],
            0, // prefix_len = 0: legacy default, no per-RG alignment claim
        );
        let size_bytes = write_test_parquet_file(temp_dir, &filename, batch, &meta);
        // Re-build metadata now that size_bytes is known. Mirrors what the
        // simpler test does — keep both fields self-consistent.
        let meta = {
            let mut m = make_multi_metric_split_metadata(
                split_id,
                num_rows,
                size_bytes,
                ts_start,
                ts_end,
                &[METRIC_AAA, METRIC_BBB, METRIC_CCC],
                0,
            );
            m.parquet_file = filename.clone();
            m
        };
        let bytes_on_disk = std::fs::read(temp_dir.join(&filename)).unwrap();
        ram_storage
            .put(Path::new(&filename), Box::new(bytes_on_disk))
            .await
            .unwrap();
        paths.push(temp_dir.join(&filename));
        splits.push(meta);
    }
    (paths, splits)
}

/// Same shape and content as `build_three_multi_metric_inputs`, but each
/// input is written with `rg_partition_prefix_len = 1` and one row group
/// per distinct metric. With sort schema `metric_name | ...` and
/// `row_group_size = ROWS_PER_METRIC_PER_INPUT`, the writer naturally
/// flushes a row group every `ROWS_PER_METRIC_PER_INPUT` rows after
/// sorting — those flush boundaries align with metric_name transitions,
/// so each row group contains rows for exactly one distinct
/// `metric_name`. The writer's prefix-alignment self-check passes, and
/// the streaming engine reads the inputs as prefix_len=1 multi-row-group
/// files.
async fn build_three_prefix_aligned_multi_metric_inputs(
    temp_dir: &Path,
    ram_storage: &Arc<dyn Storage>,
) -> (Vec<std::path::PathBuf>, Vec<ParquetSplitMetadata>) {
    let batch_x = create_multi_metric_batch(
        &[
            (METRIC_AAA, 100, ROWS_PER_METRIC_PER_INPUT),
            (METRIC_BBB, 100, ROWS_PER_METRIC_PER_INPUT),
            (METRIC_CCC, 100, ROWS_PER_METRIC_PER_INPUT),
        ],
        "web",
        "h1",
    );
    let batch_y = create_multi_metric_batch(
        &[
            (METRIC_AAA, 110, ROWS_PER_METRIC_PER_INPUT),
            (METRIC_BBB, 110, ROWS_PER_METRIC_PER_INPUT),
            (METRIC_CCC, 110, ROWS_PER_METRIC_PER_INPUT),
        ],
        "api",
        "h2",
    );
    let batch_z = create_multi_metric_batch(
        &[
            (METRIC_AAA, 120, ROWS_PER_METRIC_PER_INPUT),
            (METRIC_BBB, 120, ROWS_PER_METRIC_PER_INPUT),
            (METRIC_CCC, 120, ROWS_PER_METRIC_PER_INPUT),
        ],
        "db",
        "h3",
    );

    let mut paths = Vec::new();
    let mut splits = Vec::new();
    for (split_id, batch, ts_start, ts_end) in [
        ("split-px", &batch_x, 100, 120),
        ("split-py", &batch_y, 110, 130),
        ("split-pz", &batch_z, 120, 140),
    ] {
        let filename = format!("{split_id}.parquet");
        let num_rows = (3 * ROWS_PER_METRIC_PER_INPUT) as u64;
        let meta = make_multi_metric_split_metadata(
            split_id,
            num_rows,
            0,
            ts_start,
            ts_end,
            &[METRIC_AAA, METRIC_BBB, METRIC_CCC],
            1, // prefix_len = 1: one row group per metric_name.
        );
        let size_bytes = write_prefix_aligned_input(
            temp_dir,
            &filename,
            batch,
            &meta,
            ROWS_PER_METRIC_PER_INPUT,
        );
        let meta = {
            let mut m = make_multi_metric_split_metadata(
                split_id,
                num_rows,
                size_bytes,
                ts_start,
                ts_end,
                &[METRIC_AAA, METRIC_BBB, METRIC_CCC],
                1,
            );
            m.parquet_file = filename.clone();
            m
        };
        let bytes_on_disk = std::fs::read(temp_dir.join(&filename)).unwrap();
        ram_storage
            .put(Path::new(&filename), Box::new(bytes_on_disk))
            .await
            .unwrap();
        paths.push(temp_dir.join(&filename));
        splits.push(meta);
    }
    (paths, splits)
}

// ---------------------------------------------------------------------------
// Mock metastore plumbing (captures staged + published state)
// ---------------------------------------------------------------------------

/// Handles returned by `mount_capturing_metastore`: caller waits on
/// `publish_called`, then reads `staged_metadata` / `replaced_ids`.
struct MetastoreCapture {
    metastore: MetastoreServiceClient,
    staged_metadata: Arc<std::sync::Mutex<Vec<ParquetSplitMetadata>>>,
    replaced_ids: Arc<std::sync::Mutex<Vec<String>>>,
    publish_called: Arc<std::sync::atomic::AtomicBool>,
}

fn mount_capturing_metastore() -> MetastoreCapture {
    let mut mock_metastore = MockMetastoreService::new();

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

    MetastoreCapture {
        metastore: MetastoreServiceClient::from_mock(mock_metastore),
        staged_metadata,
        replaced_ids,
        publish_called,
    }
}

// ---------------------------------------------------------------------------
// Pipeline-params builder
// ---------------------------------------------------------------------------

fn make_pipeline_params(
    universe: &Universe,
    metastore: MetastoreServiceClient,
    ram_storage: Arc<dyn Storage>,
    use_streaming_engine: bool,
    target_split_size_bytes: u64,
    max_merge_ops: u32,
) -> ParquetMergePipelineParams {
    // merge_factor = max_merge_factor = 3 lets the planner pick up all
    // three inputs in a single operation. `target_split_size_bytes` on
    // the policy controls when an additional merge-up is scheduled;
    // `target_split_size_bytes` on the pipeline params controls how the
    // executor splits the merge output. They are the same value in
    // production but kept independent in tests so the bonus test can
    // ask for a small output target without disturbing the planner.
    //
    // `max_merge_ops` bounds the cascade depth. n=1 tests use 5 (plenty
    // of headroom — they produce one output that doesn't re-trigger
    // the planner anyway). m:n tests use 1 to keep the test fixed at
    // one merge: outputs from the first merge land at
    // `num_merge_ops = 1` and the planner refuses to merge them again
    // because they've hit the policy's ceiling.
    let merge_policy = Arc::new(ConstWriteAmplificationParquetMergePolicy::new(
        ParquetMergePolicyConfig {
            merge_factor: 3,
            max_merge_factor: 3,
            max_merge_ops,
            target_split_size_bytes: 256 * 1024 * 1024,
            maturation_period: Duration::from_secs(3600),
            max_finalize_merge_operations: 3,
        },
    ));

    let writer_config =
        ParquetWriterConfig::default().with_row_group_size(TEST_OUTPUT_ROW_GROUP_SIZE);

    ParquetMergePipelineParams {
        index_uid: quickwit_proto::types::IndexUid::for_test("test-merge-index-multi", 0),
        indexing_directory: TempDirectory::for_test(),
        metastore,
        storage: ram_storage,
        merge_policy,
        merge_scheduler_service: universe.get_or_spawn_one(),
        max_concurrent_split_uploads: 4,
        event_broker: EventBroker::default(),
        writer_config,
        use_streaming_engine,
        target_split_size_bytes,
    }
}

// ---------------------------------------------------------------------------
// Shared assertions: rich n=1 case
// ---------------------------------------------------------------------------

/// Asserts the post-merge state for the canonical three-input,
/// three-metric, single-output fixture used by this module. Both engines
/// must produce a merged split that passes every check below.
async fn assert_three_input_three_metric_single_output_correct(
    staged_metadata: &Arc<std::sync::Mutex<Vec<ParquetSplitMetadata>>>,
    replaced_ids: &Arc<std::sync::Mutex<Vec<String>>>,
    ram_storage: &Arc<dyn Storage>,
) {
    // Publisher should replace exactly the three input splits.
    let mut replaced_sorted: Vec<String> = replaced_ids.lock().unwrap().clone();
    replaced_sorted.sort();
    assert_eq!(
        replaced_sorted,
        vec![
            "split-x".to_string(),
            "split-y".to_string(),
            "split-z".to_string(),
        ],
        "publish must replace all three input splits"
    );

    let staged = staged_metadata.lock().unwrap().clone();
    assert_eq!(
        staged.len(),
        1,
        "exactly one merged split should be staged for n=1 merge"
    );
    let merged_meta = &staged[0];

    // MC-1: every input row is in the output.
    assert_eq!(
        merged_meta.num_rows, TOTAL_INPUT_ROWS,
        "merged split must contain all {TOTAL_INPUT_ROWS} input rows"
    );

    // Time range is the union of inputs: ts_min = 100 (input-x's first row),
    // ts_max = 139 (input-z's last row), end_secs is max + 1.
    assert_eq!(
        merged_meta.time_range.start_secs, 100,
        "time_range.start should be the min timestamp across all inputs"
    );
    assert_eq!(
        merged_meta.time_range.end_secs, 140,
        "time_range.end should be max timestamp + 1"
    );

    // All three metric names must survive.
    let expected_metrics: HashSet<String> = [METRIC_AAA, METRIC_BBB, METRIC_CCC]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(
        merged_meta.metric_names, expected_metrics,
        "merged split must contain all three metric names from the input set"
    );

    // First merge over level-0 inputs.
    assert_eq!(
        merged_meta.num_merge_ops, 1,
        "first merge must set num_merge_ops to 1"
    );

    let table_config = TableConfig::default();
    assert_eq!(
        merged_meta.sort_fields,
        table_config.effective_sort_fields(),
        "sort_fields must be preserved through merge"
    );

    // Row keys + zonemaps must be populated.
    assert!(
        merged_meta
            .row_keys_proto
            .as_ref()
            .is_some_and(|b| !b.is_empty()),
        "row_keys_proto must be present and non-empty"
    );
    assert!(
        merged_meta.zonemap_regexes.contains_key("metric_name"),
        "zonemap_regexes must include metric_name; got keys: {:?}",
        merged_meta.zonemap_regexes.keys().collect::<Vec<_>>()
    );

    // Services across all three inputs must surface in low_cardinality_tags.
    let service_tags = &merged_meta.low_cardinality_tags["service"];
    let expected_services: HashSet<String> =
        ["web", "api", "db"].iter().map(|s| s.to_string()).collect();
    assert_eq!(
        *service_tags, expected_services,
        "service tag values must include all three inputs"
    );

    // Read the merged file back and verify content.
    let merged_bytes = ram_storage
        .get_all(Path::new(&merged_meta.parquet_file))
        .await
        .expect("merged parquet file must exist in storage");
    let merged_batch = read_parquet_from_bytes(&merged_bytes);

    assert_eq!(
        merged_batch.num_rows() as u64,
        TOTAL_INPUT_ROWS,
        "merged Parquet file row count must match expected total"
    );

    // sorted_series is monotonically non-decreasing — the fundamental
    // post-merge invariant.
    let sorted_series = extract_binary_column(&merged_batch, SORTED_SERIES_COLUMN);
    for i in 1..sorted_series.len() {
        assert!(
            sorted_series[i] >= sorted_series[i - 1],
            "sorted_series must be non-decreasing at row {i}: {:?} < {:?}",
            sorted_series[i],
            sorted_series[i - 1],
        );
    }

    // Sort-order semantics: with `metric_name` as the leading sort column
    // (ascending), every `aaa.alpha` row precedes every `bbb.beta` row,
    // and every `bbb.beta` row precedes every `ccc.gamma` row.
    let metric_name_vec = extract_string_column(&merged_batch, "metric_name");
    let last_aaa = metric_name_vec
        .iter()
        .rposition(|n| n == METRIC_AAA)
        .expect("aaa rows must be present");
    let first_bbb = metric_name_vec
        .iter()
        .position(|n| n == METRIC_BBB)
        .expect("bbb rows must be present");
    let last_bbb = metric_name_vec
        .iter()
        .rposition(|n| n == METRIC_BBB)
        .expect("bbb rows must be present");
    let first_ccc = metric_name_vec
        .iter()
        .position(|n| n == METRIC_CCC)
        .expect("ccc rows must be present");
    assert!(
        last_aaa < first_bbb,
        "all aaa rows must precede all bbb rows ({last_aaa} >= {first_bbb})",
    );
    assert!(
        last_bbb < first_ccc,
        "all bbb rows must precede all ccc rows ({last_bbb} >= {first_ccc})",
    );

    // Per-metric row count = 3 inputs * ROWS_PER_METRIC_PER_INPUT each.
    let expected_per_metric = (3 * ROWS_PER_METRIC_PER_INPUT) as u64;
    for metric in [METRIC_AAA, METRIC_BBB, METRIC_CCC] {
        let count = metric_name_vec.iter().filter(|n| *n == metric).count() as u64;
        assert_eq!(
            count, expected_per_metric,
            "metric {metric} must have exactly {expected_per_metric} rows; got {count}",
        );
    }

    // All timestamps from the union [100, 140) must appear, and the metadata
    // time_range must match the actual extrema.
    let timestamps_in_file = extract_u64_column(&merged_batch, "timestamp_secs");
    assert_eq!(
        timestamps_in_file.len() as u64,
        TOTAL_INPUT_ROWS,
        "timestamp column must have one entry per row"
    );
    let ts_min = *timestamps_in_file.iter().min().unwrap();
    let ts_max = *timestamps_in_file.iter().max().unwrap();
    assert_eq!(ts_min, 100, "min timestamp must be 100");
    assert_eq!(ts_max, 139, "max timestamp must be 139");
    assert_eq!(
        merged_meta.time_range.start_secs, ts_min,
        "metadata time_range.start must match actual min timestamp"
    );
    assert_eq!(
        merged_meta.time_range.end_secs,
        ts_max + 1,
        "metadata time_range.end must be max timestamp + 1"
    );

    // Service / host tag values from every input must appear in the file.
    let services_in_file: HashSet<String> = extract_string_column(&merged_batch, "service")
        .into_iter()
        .collect();
    assert_eq!(
        services_in_file, expected_services,
        "service column must contain all three input values"
    );
    let hosts_in_file: HashSet<String> = extract_string_column(&merged_batch, "host")
        .into_iter()
        .collect();
    let expected_hosts: HashSet<String> =
        ["h1", "h2", "h3"].iter().map(|s| s.to_string()).collect();
    assert_eq!(
        hosts_in_file, expected_hosts,
        "host column must contain all three input values"
    );
}

// ---------------------------------------------------------------------------
// Shared assertions: m:n case (n > 1)
// ---------------------------------------------------------------------------

/// Asserts the post-merge state for the canonical three-input fixture when
/// the merge produced **more than one output**. Both engines must satisfy
/// the m:n contract:
///
/// - Replacement covers all three inputs.
/// - The pipeline staged at least two output splits (proves splitting happened).
/// - The sum of per-output row counts equals the total input row count.
/// - Each output is internally sorted on `sorted_series`.
/// - Across outputs, the `sorted_series` partition is **disjoint** (no two
///   outputs share any `sorted_series` value — the merge engine splits at
///   series boundaries, never inside).
/// - The union of metric_names / services across outputs covers the full
///   input set.
/// - Every output declares `num_merge_ops = 1` (first merge over level-0
///   inputs) and has `row_keys_proto` + `metric_name` zonemap regex
///   populated.
async fn assert_three_input_three_metric_multi_output_correct(
    staged_metadata: &Arc<std::sync::Mutex<Vec<ParquetSplitMetadata>>>,
    replaced_ids: &Arc<std::sync::Mutex<Vec<String>>>,
    ram_storage: &Arc<dyn Storage>,
    expected_input_split_ids: &[&str],
) {
    let mut replaced_sorted: Vec<String> = replaced_ids.lock().unwrap().clone();
    replaced_sorted.sort();
    let mut expected_sorted: Vec<String> = expected_input_split_ids
        .iter()
        .map(|s| s.to_string())
        .collect();
    expected_sorted.sort();
    assert_eq!(
        replaced_sorted, expected_sorted,
        "publish must replace all three input splits",
    );

    let staged = staged_metadata.lock().unwrap().clone();
    assert!(
        staged.len() >= 2,
        "m:n merge must produce at least two outputs; got {}",
        staged.len()
    );

    let total_output_rows: u64 = staged.iter().map(|s| s.num_rows).sum();
    assert_eq!(
        total_output_rows, TOTAL_INPUT_ROWS,
        "sum of output row counts must equal total input rows",
    );

    // Each output internally sorted on sorted_series; collect ranges for
    // the disjointness check across outputs.
    let mut output_series_ranges: Vec<(Vec<u8>, Vec<u8>, String)> =
        Vec::with_capacity(staged.len());
    for meta in &staged {
        let bytes = ram_storage
            .get_all(Path::new(&meta.parquet_file))
            .await
            .expect("output parquet file must exist in storage");
        let batch = read_parquet_from_bytes(&bytes);
        assert_eq!(
            batch.num_rows() as u64,
            meta.num_rows,
            "output {} row count {} disagrees with metadata num_rows {}",
            meta.parquet_file,
            batch.num_rows(),
            meta.num_rows,
        );
        let series = extract_binary_column(&batch, SORTED_SERIES_COLUMN);
        assert!(
            !series.is_empty(),
            "every output must have at least one row (empty outputs should be dropped \
             by the engine)"
        );
        for i in 1..series.len() {
            assert!(
                series[i] >= series[i - 1],
                "output {} sorted_series not monotone at row {i}",
                meta.parquet_file,
            );
        }
        output_series_ranges.push((
            series.first().unwrap().clone(),
            series.last().unwrap().clone(),
            meta.parquet_file.clone(),
        ));
    }

    // Sort outputs by min_series for pairwise disjointness comparison.
    output_series_ranges.sort_by(|a, b| a.0.cmp(&b.0));
    for window in output_series_ranges.windows(2) {
        let (_, left_max, left_file) = &window[0];
        let (right_min, _, right_file) = &window[1];
        assert!(
            left_max < right_min,
            "outputs {} and {} overlap on sorted_series: \
             left max = {:?}, right min = {:?}",
            left_file,
            right_file,
            left_max,
            right_min,
        );
    }

    let union_metrics: HashSet<String> = staged
        .iter()
        .flat_map(|s| s.metric_names.iter().cloned())
        .collect();
    let expected_metrics: HashSet<String> = [METRIC_AAA, METRIC_BBB, METRIC_CCC]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(
        union_metrics, expected_metrics,
        "union of output metric_names must equal the full input set",
    );

    let union_services: HashSet<String> = staged
        .iter()
        .flat_map(|s| {
            s.low_cardinality_tags
                .get("service")
                .into_iter()
                .flat_map(|set| set.iter().cloned())
        })
        .collect();
    let expected_services: HashSet<String> =
        ["web", "api", "db"].iter().map(|s| s.to_string()).collect();
    assert_eq!(
        union_services, expected_services,
        "union of output services must equal the full input set",
    );

    for meta in &staged {
        assert_eq!(
            meta.num_merge_ops, 1,
            "output {} num_merge_ops must be 1 for the first merge",
            meta.parquet_file,
        );
        assert!(
            meta.row_keys_proto.as_ref().is_some_and(|b| !b.is_empty()),
            "output {} missing row_keys_proto",
            meta.parquet_file,
        );
        assert!(
            meta.zonemap_regexes.contains_key("metric_name"),
            "output {} missing metric_name zonemap regex",
            meta.parquet_file,
        );
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Run a merge pipeline over the canonical three-input multi-metric fixture
/// and apply the supplied assertion. Shared between the two engine variants
/// below. `target_split_size_bytes` drives the executor's `num_outputs`
/// calculation; pass `u64::MAX` (or anything bigger than the total input
/// size) for the n=1 case, or a small value to force m:n.
async fn run_three_input_multi_metric_merge<F, Fut>(
    use_streaming_engine: bool,
    target_split_size_bytes: u64,
    assertions: F,
) where
    F: for<'a> FnOnce(
        Arc<std::sync::Mutex<Vec<ParquetSplitMetadata>>>,
        Arc<std::sync::Mutex<Vec<String>>>,
        Arc<dyn Storage>,
    ) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    quickwit_common::setup_logging_for_tests();

    let universe = Universe::with_accelerated_time();
    let temp_dir = tempfile::tempdir().unwrap();
    let ram_storage: Arc<dyn Storage> = Arc::new(RamStorage::default());

    let (_paths, splits) = build_three_multi_metric_inputs(temp_dir.path(), &ram_storage).await;

    let capture = mount_capturing_metastore();
    let params = make_pipeline_params(
        &universe,
        capture.metastore.clone(),
        ram_storage.clone(),
        use_streaming_engine,
        target_split_size_bytes,
        5, // max_merge_ops: n=1 tests don't cascade, give plenty of headroom
    );

    let pipeline = ParquetMergePipeline::new(params, Some(splits), universe.spawn_ctx());
    let (_pipeline_mailbox, _pipeline_handle) = universe.spawn_builder().spawn(pipeline);

    wait_until_predicate(
        || {
            let publish_called = capture.publish_called.clone();
            async move { publish_called.load(Ordering::SeqCst) }
        },
        Duration::from_secs(60),
        Duration::from_millis(100),
    )
    .await
    .expect("timed out waiting for merge publish");

    assertions(capture.staged_metadata, capture.replaced_ids, ram_storage).await;

    universe.assert_quit().await;
}

/// In-memory engine, n=1 output, three multi-metric inputs with overlapping
/// timestamps and timeseries IDs. Verifies the merge correctly interleaves
/// rows across all three inputs and produces a single sorted output with
/// the full input rowset.
#[tokio::test]
async fn test_multi_metric_three_input_single_output_in_memory_engine() {
    run_three_input_multi_metric_merge(
        false, // use_streaming_engine
        // Larger than total input size — forces num_outputs = 1.
        256 * 1024 * 1024,
        |staged, replaced, storage| async move {
            assert_three_input_three_metric_single_output_correct(&staged, &replaced, &storage)
                .await;
        },
    )
    .await;
}

/// Streaming engine, n=1 output. Same fixture as the in-memory variant;
/// must produce a row-content-equivalent output. Additionally asserts
/// `PEAK_BODY_COL_PAGE_CACHE_LEN > 0` to confirm the streaming engine
/// actually ran (the in-memory engine never writes to that atomic).
#[allow(
    clippy::await_holding_lock,
    reason = "see ms7_serial_lock rationale: std::sync::Mutex on a single-threaded tokio runtime"
)]
#[tokio::test]
async fn test_multi_metric_three_input_single_output_streaming_engine() {
    use quickwit_parquet_engine::merge::streaming::{
        PEAK_BODY_COL_PAGE_CACHE_LEN, ms7_serial_lock,
    };

    let _ms7_guard = ms7_serial_lock();
    PEAK_BODY_COL_PAGE_CACHE_LEN.store(0, Ordering::Relaxed);

    run_three_input_multi_metric_merge(
        true, // use_streaming_engine
        256 * 1024 * 1024,
        |staged, replaced, storage| async move {
            assert!(
                PEAK_BODY_COL_PAGE_CACHE_LEN.load(Ordering::Relaxed) > 0,
                "streaming engine did not write to PEAK_BODY_COL_PAGE_CACHE_LEN — \
                 routing may have silently fallen back to the in-memory engine",
            );
            assert_three_input_three_metric_single_output_correct(&staged, &replaced, &storage)
                .await;
        },
    )
    .await;
}

/// Run a merge pipeline over the canonical three-input fixture with
/// **prefix-aligned multi-row-group inputs** (`rg_partition_prefix_len = 1`,
/// one row group per metric_name). Drives the m:n bonus tests.
async fn run_three_input_prefix_aligned_merge<F, Fut>(
    use_streaming_engine: bool,
    target_split_size_bytes: u64,
    assertions: F,
) where
    F: for<'a> FnOnce(
        Arc<std::sync::Mutex<Vec<ParquetSplitMetadata>>>,
        Arc<std::sync::Mutex<Vec<String>>>,
        Arc<dyn Storage>,
    ) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    quickwit_common::setup_logging_for_tests();

    let universe = Universe::with_accelerated_time();
    let temp_dir = tempfile::tempdir().unwrap();
    let ram_storage: Arc<dyn Storage> = Arc::new(RamStorage::default());

    let (_paths, splits) =
        build_three_prefix_aligned_multi_metric_inputs(temp_dir.path(), &ram_storage).await;

    let capture = mount_capturing_metastore();
    let params = make_pipeline_params(
        &universe,
        capture.metastore.clone(),
        ram_storage.clone(),
        use_streaming_engine,
        target_split_size_bytes,
        // max_merge_ops = 1: outputs from the first (and only) merge land
        // at num_merge_ops = 1 and the planner refuses to merge them again,
        // pinning this test to exactly one merge regardless of how many
        // outputs the engine chose to produce.
        1,
    );

    let pipeline = ParquetMergePipeline::new(params, Some(splits), universe.spawn_ctx());
    let (_pipeline_mailbox, _pipeline_handle) = universe.spawn_builder().spawn(pipeline);

    wait_until_predicate(
        || {
            let publish_called = capture.publish_called.clone();
            async move { publish_called.load(Ordering::SeqCst) }
        },
        Duration::from_secs(60),
        Duration::from_millis(100),
    )
    .await
    .expect("timed out waiting for merge publish");

    assertions(capture.staged_metadata, capture.replaced_ids, ram_storage).await;

    universe.assert_quit().await;
}

/// **Bonus** test, in-memory engine: three multi-metric inputs each with
/// `rg_partition_prefix_len = 1` (one row group per metric_name), merged
/// with a small `target_split_size_bytes` that forces the executor to ask
/// the engine for `num_outputs > 1`. Exercises the previously-impossible
/// pipeline-level m:n merge path now that the executor's hardcoded
/// `num_outputs = 1` is gone. Verifies the multi-output contract:
/// sum-equals-total, internal monotonicity, inter-output disjointness on
/// `sorted_series`, and union-equals-full-set on metrics/services.
#[tokio::test]
async fn test_prefix_aligned_multi_metric_three_input_multi_output_in_memory_engine() {
    run_three_input_prefix_aligned_merge(
        false, // use_streaming_engine
        // Smaller than per-input size — guarantees num_outputs ≥ 2. The
        // engine clamps to available sorted_series boundaries (~60 in
        // this fixture: 3 metrics × 20 timeseries each), well above 2.
        500,
        |staged, replaced, storage| async move {
            assert_three_input_three_metric_multi_output_correct(
                &staged,
                &replaced,
                &storage,
                &["split-px", "split-py", "split-pz"],
            )
            .await;
        },
    )
    .await;
}

/// **Bonus** test, streaming engine: same fixture and contract as the
/// in-memory variant. Additionally asserts
/// `PEAK_BODY_COL_PAGE_CACHE_LEN > 0` to confirm the streaming engine
/// actually ran. With prefix-aligned inputs the streaming engine reads
/// each input's row groups (one per metric_name) through the prefix-aware
/// `StreamingParquetReader` path — distinct from the legacy
/// `LegacyInputAdapter` route, since these inputs do not require
/// promotion (`target_prefix_len_override` is `None` for regular merges).
#[allow(
    clippy::await_holding_lock,
    reason = "see ms7_serial_lock rationale: std::sync::Mutex on a single-threaded tokio runtime"
)]
#[tokio::test]
async fn test_prefix_aligned_multi_metric_three_input_multi_output_streaming_engine() {
    use quickwit_parquet_engine::merge::streaming::{
        PEAK_BODY_COL_PAGE_CACHE_LEN, ms7_serial_lock,
    };

    let _ms7_guard = ms7_serial_lock();
    PEAK_BODY_COL_PAGE_CACHE_LEN.store(0, Ordering::Relaxed);

    run_three_input_prefix_aligned_merge(
        true, // use_streaming_engine
        500,
        |staged, replaced, storage| async move {
            assert!(
                PEAK_BODY_COL_PAGE_CACHE_LEN.load(Ordering::Relaxed) > 0,
                "streaming engine did not write to PEAK_BODY_COL_PAGE_CACHE_LEN — \
                 routing may have silently fallen back to the in-memory engine",
            );
            assert_three_input_three_metric_multi_output_correct(
                &staged,
                &replaced,
                &storage,
                &["split-px", "split-py", "split-pz"],
            )
            .await;
        },
    )
    .await;
}
