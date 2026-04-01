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

//! Distributed DataFusion execution tests.
//!
//! These tests verify two key properties:
//!
//! 1. **Tasks, not shuffles**: When multiple splits exist and multiple searcher nodes
//!    are available, `DistributedPhysicalOptimizerRule` decomposes the plan into tasks
//!    (one `PartitionIsolatorExec` per split assigned to a worker) rather than using
//!    cross-worker shuffles (`NetworkShuffleExec`).  This is correct because each
//!    split is self-contained parquet — no repartitioning is needed.
//!
//! 2. **NULL columns for schema-widening**: When a query declares a column in the
//!    `CREATE EXTERNAL TABLE` schema that is absent from the underlying parquet file,
//!    DataFusion's `PhysicalExprAdapterFactory` returns NULL values for those rows.
//!    This is the mechanism that makes the "wide schema" pattern work: different splits
//!    can have different tag columns; the declared schema is the union; missing columns
//!    appear as NULLs.

use std::collections::HashSet;
use std::io::Cursor;

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::ipc::reader::StreamReader;
use quickwit_config::service::QuickwitService;
use quickwit_datafusion::test_utils::make_batch;
use quickwit_datafusion::sources::metrics::test_utils::make_batch_with_tags;
use quickwit_metastore::StageMetricsSplitsRequestExt;
use quickwit_parquet_engine::schema::ParquetSchema;
use quickwit_parquet_engine::split::{MetricsSplitMetadata, SplitId, TimeRange};
use quickwit_parquet_engine::storage::{ParquetWriter, ParquetWriterConfig};
use quickwit_proto::metastore::{
    CreateIndexRequest, MetastoreService, MetastoreServiceClient, PublishMetricsSplitsRequest,
    StageMetricsSplitsRequest,
};
use quickwit_metastore::CreateIndexRequestExt;
use quickwit_proto::types::IndexUid;

use crate::test_utils::{ClusterSandbox, ClusterSandboxBuilder};

/// Build a RecordBatch with ONLY the 4 required metrics columns (no tag columns).
///
/// Used to simulate a parquet file written before any tag columns were added,
/// or a file from a metric that happened to have no tags at ingest time.
fn make_narrow_batch(metric_name: &str, timestamps: &[u64], values: &[f64]) -> RecordBatch {
    use std::sync::Arc;
    use arrow::array::{DictionaryArray, Float64Array, Int32Array, StringArray, UInt64Array, UInt8Array};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema};

    let n = timestamps.len();
    let dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("metric_name", dict, false),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let keys = Int32Array::from(vec![0i32; n]);
    let vals = StringArray::from(vec![metric_name]);
    let metric_col = Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(vals)).unwrap());
    RecordBatch::try_new(
        schema,
        vec![
            metric_col as Arc<_>,
            Arc::new(UInt8Array::from(vec![0u8; n])),
            Arc::new(UInt64Array::from(timestamps.to_vec())),
            Arc::new(Float64Array::from(values.to_vec())),
        ],
    )
    .unwrap()
}

// ── Helpers ─────────────────────────────────────────────────────────

fn metastore_client(sandbox: &ClusterSandbox) -> MetastoreServiceClient {
    let (config, _) = sandbox
        .node_configs
        .iter()
        .find(|(_, svc)| svc.contains(&QuickwitService::Metastore))
        .unwrap();
    let addr = config.grpc_listen_addr;
    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_lazy();
    MetastoreServiceClient::from_channel(addr, channel, bytesize::ByteSize::mib(20), None)
}

/// The REST address of the primary searcher (first node with Searcher service).
fn searcher_rest_addr(sandbox: &ClusterSandbox) -> std::net::SocketAddr {
    sandbox
        .node_configs
        .iter()
        .find(|(_, svc)| svc.contains(&QuickwitService::Searcher))
        .unwrap()
        .0
        .rest_config
        .listen_addr
}

async fn create_metrics_index(
    metastore: &MetastoreServiceClient,
    index_id: &str,
    data_dir: &std::path::Path,
) -> IndexUid {
    let index_uri = format!("file://{}", data_dir.display());
    let index_config: quickwit_config::IndexConfig =
        serde_json::from_value(serde_json::json!({
            "version": "0.8",
            "index_id": index_id,
            "index_uri": index_uri,
            "doc_mapping": { "field_mappings": [] },
            "indexing_settings": {},
            "search_settings": {}
        }))
        .unwrap();
    let response = metastore
        .clone()
        .create_index(CreateIndexRequest::try_from_index_config(&index_config).unwrap())
        .await
        .unwrap();
    response.index_uid().clone()
}

async fn publish_split(
    metastore: &MetastoreServiceClient,
    index_uid: &IndexUid,
    data_dir: &std::path::Path,
    split_name: &str,
    batch: &RecordBatch,
) {
    let schema = ParquetSchema::from_arrow_schema(batch.schema());
    let parquet_bytes = ParquetWriter::new(schema, ParquetWriterConfig::default())
        .write_to_bytes(batch)
        .unwrap();
    let size_bytes = parquet_bytes.len() as u64;
    std::fs::write(data_dir.join(format!("{split_name}.parquet")), &parquet_bytes).unwrap();

    let batch_schema = batch.schema();
    let ts_idx = batch_schema.index_of("timestamp_secs").unwrap();
    let ts_col = batch
        .column(ts_idx)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap();
    let min_ts = (0..ts_col.len()).map(|i| ts_col.value(i)).min().unwrap_or(0);
    let max_ts = (0..ts_col.len()).map(|i| ts_col.value(i)).max().unwrap_or(0);

    let mn_idx = batch_schema.index_of("metric_name").unwrap();
    let dict = batch
        .column(mn_idx)
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>()
        .unwrap();
    let values = dict
        .values()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let metric_names: HashSet<String> =
        (0..values.len()).filter(|i| !values.is_null(*i)).map(|i| values.value(i).to_string()).collect();

    let mut builder = MetricsSplitMetadata::builder()
        .split_id(SplitId::new(split_name))
        .index_uid(index_uid.to_string())
        .time_range(TimeRange::new(min_ts, max_ts + 1))
        .num_rows(batch.num_rows() as u64)
        .size_bytes(size_bytes);
    for name in &metric_names {
        builder = builder.add_metric_name(name.clone());
    }

    metastore
        .clone()
        .stage_metrics_splits(
            StageMetricsSplitsRequest::try_from_splits_metadata(
                index_uid.clone(),
                &[builder.build()],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    metastore
        .clone()
        .publish_metrics_splits(PublishMetricsSplitsRequest {
            index_uid: Some(index_uid.clone().into()),
            staged_split_ids: vec![split_name.to_string()],
            replaced_split_ids: vec![],
            index_checkpoint_delta_json_opt: None,
            publish_token_opt: None,
        })
        .await
        .unwrap();
}

/// POST to the SQL endpoint and return decoded Arrow IPC batches.
async fn sql_query(rest_addr: std::net::SocketAddr, sql: &str) -> Vec<RecordBatch> {
    let response = reqwest::Client::new()
        .post(format!("http://{rest_addr}/api/v1/metrics/sql"))
        .header("content-type", "text/plain")
        .body(sql.to_string())
        .send()
        .await
        .unwrap_or_else(|e| panic!("request failed: {e}"));

    assert!(
        response.status().is_success(),
        "SQL failed ({}): {}",
        response.status(),
        response.text().await.unwrap_or_default()
    );

    let bytes = response.bytes().await.unwrap();
    if bytes.is_empty() {
        return vec![];
    }
    StreamReader::try_new(Cursor::new(&bytes), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

/// POST to the SQL endpoint with `?explain=true` and return the plan text.
async fn sql_explain(rest_addr: std::net::SocketAddr, sql: &str) -> String {
    let response = reqwest::Client::new()
        .post(format!("http://{rest_addr}/api/v1/metrics/sql?explain=true"))
        .header("content-type", "text/plain")
        .body(sql.to_string())
        .send()
        .await
        .unwrap_or_else(|e| panic!("explain request failed: {e}"));

    assert!(
        response.status().is_success(),
        "explain failed ({}): {}",
        response.status(),
        response.text().await.unwrap_or_default()
    );

    let bytes = response.bytes().await.unwrap();
    let batches: Vec<RecordBatch> = StreamReader::try_new(Cursor::new(&bytes), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_string()
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// ═══════════════════════════════════════════════════════════════════
// Test 1: Tasks, not shuffles
// ═══════════════════════════════════════════════════════════════════

/// Starts a 2-searcher cluster to verify that:
/// 1. The distributed optimizer fires (2 workers → task count is not collapsed to 1)
/// 2. The plan uses `PartitionIsolatorExec` (tasks) NOT `NetworkShuffleExec` (shuffles)
/// 3. The aggregation result is correct
///
/// Plan expected to contain (inner → outer):
/// ```
/// DistributedExec
///   NetworkCoalesceExec          ← coordinator merges partial aggregates
///     AggregateExec (partial)    ← runs on each worker
///       PartitionIsolatorExec    ← one per split, isolates each task's partition
///         DataSourceExec         ← parquet scan
/// ```
/// NOT present: `NetworkShuffleExec` (that would mean data is repartitioned across workers,
/// i.e. a hash-redistribute shuffle that is unnecessary for split-local aggregation).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_distributed_tasks_not_shuffles() {
    unsafe {
        std::env::set_var("QW_DISABLE_TELEMETRY", "1");
    }
    quickwit_common::setup_logging_for_tests();

    // Two searcher nodes so the distributed optimizer sees n_workers = 2.
    // Node 0: all services (metastore, indexer, searcher, ...)
    // Node 1: searcher only — the second worker
    let sandbox = ClusterSandboxBuilder::default()
        .add_node(QuickwitService::supported_services())
        .add_node([QuickwitService::Searcher])
        .build_and_start()
        .await;

    let data_dir = tempfile::tempdir().unwrap();
    let metastore = metastore_client(&sandbox);

    let index_uid = create_metrics_index(&metastore, "dist-test", data_dir.path()).await;

    // Write 4 splits — with 2 workers each worker should get 2 tasks
    let splits = [
        ("split_a", "cpu.usage", &[100u64, 200], &[0.1f64, 0.2]),
        ("split_b", "cpu.usage", &[300u64, 400], &[0.3f64, 0.4]),
        ("split_c", "memory.used", &[100u64, 200], &[1024.0f64, 2048.0]),
        ("split_d", "memory.used", &[300u64, 400], &[3072.0f64, 4096.0]),
    ];
    for (name, metric, ts, vals) in &splits {
        let batch = make_batch(metric, *ts, *vals, Some("web"));
        publish_split(&metastore, &index_uid, data_dir.path(), name, &batch).await;
    }

    let rest_addr = searcher_rest_addr(&sandbox);

    let ddl = r#"CREATE OR REPLACE EXTERNAL TABLE "dist-test" (
          metric_name    VARCHAR NOT NULL,
          metric_type    TINYINT,
          timestamp_secs BIGINT  NOT NULL,
          value          DOUBLE  NOT NULL,
          service        VARCHAR
        ) STORED AS metrics LOCATION 'dist-test'"#;
    let agg_sql = format!(
        "{ddl};\
         SELECT SUM(value) as total, COUNT(*) as cnt \
         FROM \"dist-test\""
    );

    // ── Verify plan shape ────────────────────────────────────────────
    let plan = sql_explain(rest_addr, &agg_sql).await;
    println!("=== Physical plan ===\n{plan}");

    // With 2 workers and 4 splits, the plan should be distributed
    assert!(
        plan.contains("DistributedExec") || plan.contains("PartitionIsolatorExec"),
        "expected distributed plan nodes, got:\n{plan}"
    );
    // Parquet scans should NOT need to be shuffled across workers
    assert!(
        !plan.contains("NetworkShuffleExec"),
        "expected no shuffle (data is already split-local), got:\n{plan}"
    );
    // The aggregation merge boundary comes from NetworkCoalesceExec
    if plan.contains("DistributedExec") {
        assert!(
            plan.contains("NetworkCoalesceExec"),
            "expected NetworkCoalesceExec for aggregate merge, got:\n{plan}"
        );
    }

    // ── Verify correctness ───────────────────────────────────────────
    let batches = sql_query(rest_addr, &agg_sql).await;
    assert_eq!(total_rows(&batches), 1);

    let total = batches[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    let expected_sum =
        0.1 + 0.2 + 0.3 + 0.4 + 1024.0 + 2048.0 + 3072.0 + 4096.0;
    assert!(
        (total - expected_sum).abs() < 1.0,
        "expected sum ~{expected_sum:.1}, got {total:.1}"
    );

    let cnt = batches[0]
        .column_by_name("cnt")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 8, "expected 8 rows across 4 splits");
}

// ═══════════════════════════════════════════════════════════════════
// Test 2: NULL columns for missing parquet fields
// ═══════════════════════════════════════════════════════════════════

/// Demonstrates that columns declared in the CREATE EXTERNAL TABLE schema
/// but absent from the underlying parquet file are returned as NULLs.
///
/// This is the "wide schema" mechanism: different splits have different tag
/// columns; the declared schema is the union; `PhysicalExprAdapterFactory`
/// fills missing columns with NULLs automatically.
///
/// Split A: only {metric_name, metric_type, timestamp_secs, value}
/// Split B: {metric_name, metric_type, timestamp_secs, value, service, env}
/// Query:   SELECT metric_name, service, env FROM index
///
/// Expected:
/// - Split A rows: service=NULL, env=NULL
/// - Split B rows: service='web', env='prod'
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_null_columns_for_missing_parquet_fields() {
    unsafe {
        std::env::set_var("QW_DISABLE_TELEMETRY", "1");
    }
    quickwit_common::setup_logging_for_tests();

    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let data_dir = tempfile::tempdir().unwrap();
    let metastore = metastore_client(&sandbox);

    let index_uid = create_metrics_index(&metastore, "null-cols-test", data_dir.path()).await;

    // Split A: ONLY 4 required columns — build directly from base schema, no tag columns
    let batch_a = make_narrow_batch("cpu.usage", &[100, 200], &[0.5, 0.8]);
    assert!(
        batch_a.schema().index_of("service").is_err(),
        "split_a should not have a service column"
    );
    publish_split(&metastore, &index_uid, data_dir.path(), "split_a_narrow", &batch_a).await;

    // Split B: 4 required + service + env
    let batch_b = make_batch_with_tags(
        "cpu.usage",
        &[300, 400],
        &[0.3, 0.6],
        Some("web"),   // service
        Some("prod"),  // env
        None, None, None,
    );
    assert!(batch_b.schema().index_of("service").is_ok());
    assert!(batch_b.schema().index_of("env").is_ok());

    publish_split(&metastore, &index_uid, data_dir.path(), "split_b_wide", &batch_b).await;

    let rest_addr = searcher_rest_addr(&sandbox);

    // DDL declares service + env even though split_a parquet file lacks them.
    // COUNT(col) counts non-NULL values — this tests the core NULL-fill behavior
    // without depending on the exact wire encoding of the tag columns.
    let sql = r#"
        CREATE OR REPLACE EXTERNAL TABLE "null-cols-test" (
          metric_name    VARCHAR NOT NULL,
          metric_type    TINYINT,
          timestamp_secs BIGINT  NOT NULL,
          value          DOUBLE  NOT NULL,
          service        VARCHAR,
          env            VARCHAR
        ) STORED AS metrics LOCATION 'null-cols-test';
        SELECT
          COUNT(*)          AS total_rows,
          COUNT(service)    AS rows_with_service,
          COUNT(env)        AS rows_with_env
        FROM "null-cols-test"
    "#;

    let batches = sql_query(rest_addr, sql).await;
    assert_eq!(total_rows(&batches), 1, "aggregate should return one row");

    let total = batches[0]
        .column_by_name("total_rows")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 4, "expected 4 total rows (2 per split)");

    let with_service = batches[0]
        .column_by_name("rows_with_service")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    // split_a: 2 rows with NULL service; split_b: 2 rows with service='web'
    assert_eq!(
        with_service, 2,
        "expected exactly 2 non-null service values (from split_b), \
         but COUNT(service)={with_service}. \
         If 0: PhysicalExprAdapterFactory is not reading the column from split_b. \
         If 4: split_a's absent column was not NULLed."
    );

    let with_env = batches[0]
        .column_by_name("rows_with_env")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        with_env, 2,
        "expected exactly 2 non-null env values (from split_b only)"
    );
}

/// Read the string value at position `i` from a column that is either a
/// `DictionaryArray<Int32Type>` (tag columns) or a `NullArray` (absent column
/// filled by PhysicalExprAdapterFactory).  Returns `None` for null entries.
fn dict_value_at(col: &dyn arrow::array::Array, i: usize) -> Option<String> {
    use arrow::array::Array;
    if col.is_null(i) {
        return None;
    }
    // Try DictionaryArray<Int32Type> first (normal tag column)
    if let Some(dict) = col
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>()
    {
        let keys = dict
            .keys()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        let vals = dict
            .values()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        if keys.is_null(i) {
            return None;
        }
        return Some(vals.value(keys.value(i) as usize).to_string());
    }
    // Try plain StringArray
    if let Some(sa) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
        return Some(sa.value(i).to_string());
    }
    None
}
