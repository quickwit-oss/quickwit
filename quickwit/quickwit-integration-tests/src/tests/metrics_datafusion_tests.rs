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

//! Full-stack integration tests for metrics DataFusion queries.
//!
//! Tests the complete path:
//! - REST POST `/api/v1/{index_id}/ingest-metrics` → parquet → metastore
//! - REST POST `/api/v1/metrics/sql` → DataFusion → Arrow IPC response
//!
//! Uses `ClusterSandbox` to start real Quickwit nodes.

use std::collections::HashSet;
use std::io::Cursor;

use arrow::array::{Array, Float64Array, RecordBatch};
use arrow::ipc::reader::StreamReader;
use quickwit_config::service::QuickwitService;
use quickwit_datafusion::test_utils::make_batch;
use quickwit_metastore::StageMetricsSplitsRequestExt;
use quickwit_parquet_engine::schema::ParquetSchema;
use quickwit_parquet_engine::split::{MetricsSplitMetadata, SplitId, TimeRange};
use quickwit_parquet_engine::storage::{ParquetWriter, ParquetWriterConfig};
use quickwit_proto::metastore::{
    CreateIndexRequest, MetastoreService, MetastoreServiceClient, PublishMetricsSplitsRequest,
    StageMetricsSplitsRequest,
};
use quickwit_proto::types::IndexUid;
use quickwit_metastore::CreateIndexRequestExt;

use crate::test_utils::{ClusterSandbox, ClusterSandboxBuilder};

// ── Setup ──────────────────────────────────────────────────────────

async fn start_metrics_sandbox() -> (ClusterSandbox, tempfile::TempDir) {
    // Must be set before the server starts.
    unsafe {
        std::env::set_var("QW_DISABLE_TELEMETRY", "1");
    }
    quickwit_common::setup_logging_for_tests();

    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let data_dir = tempfile::tempdir().unwrap();

    (sandbox, data_dir)
}

fn metastore_client(sandbox: &ClusterSandbox) -> MetastoreServiceClient {
    let node_config = sandbox
        .node_configs
        .iter()
        .find(|(_, services)| services.contains(&QuickwitService::Metastore))
        .unwrap();
    let addr = node_config.0.grpc_listen_addr;
    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_lazy();
    MetastoreServiceClient::from_channel(addr, channel, bytesize::ByteSize::mib(20), None)
}

fn searcher_rest_addr(sandbox: &ClusterSandbox) -> std::net::SocketAddr {
    let node_config = sandbox
        .node_configs
        .iter()
        .find(|(_, services)| services.contains(&QuickwitService::Searcher))
        .unwrap();
    node_config.0.rest_config.listen_addr
}

// ── Data helpers ───────────────────────────────────────────────────

/// Create a metrics index with file-backed storage.
async fn create_metrics_index(
    metastore: &MetastoreServiceClient,
    index_id: &str,
    data_dir: &std::path::Path,
) -> IndexUid {
    let index_uri = format!("file://{}", data_dir.display());
    let index_config_json = serde_json::json!({
        "version": "0.8",
        "index_id": index_id,
        "index_uri": index_uri,
        "doc_mapping": { "field_mappings": [] },
        "indexing_settings": {},
        "search_settings": {}
    });
    let response = metastore
        .clone()
        .create_index(
            CreateIndexRequest::try_from_index_config(
                &serde_json::from_value(index_config_json).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    response.index_uid().clone()
}

/// Write a parquet file to disk and stage+publish through the metastore.
///
/// Uses column-name lookup (no ParquetField enum) for OSS compatibility.
async fn write_and_publish_split(
    metastore: &MetastoreServiceClient,
    index_uid: &IndexUid,
    data_dir: &std::path::Path,
    split_name: &str,
    batch: &RecordBatch,
) {
    let schema = ParquetSchema::from_arrow_schema(batch.schema());
    let writer = ParquetWriter::new(schema, ParquetWriterConfig::default());
    let parquet_bytes = writer.write_to_bytes(batch).unwrap();
    let size_bytes = parquet_bytes.len() as u64;
    std::fs::write(data_dir.join(format!("{split_name}.parquet")), parquet_bytes).unwrap();

    // Extract timestamps by column name
    let schema = batch.schema();
    let ts_idx = schema.index_of("timestamp_secs").unwrap();
    let ts_col = batch
        .column(ts_idx)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap();
    let min_ts = (0..ts_col.len()).map(|i| ts_col.value(i)).min().unwrap_or(0);
    let max_ts = (0..ts_col.len()).map(|i| ts_col.value(i)).max().unwrap_or(0);

    // Extract metric names by column name
    let mn_idx = schema.index_of("metric_name").unwrap();
    let metric_col = batch.column(mn_idx);
    let dict = metric_col
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>()
        .unwrap();
    let values = dict
        .values()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let mut metric_names = HashSet::new();
    for i in 0..values.len() {
        if !values.is_null(i) {
            metric_names.insert(values.value(i).to_string());
        }
    }

    let mut builder = MetricsSplitMetadata::builder()
        .split_id(SplitId::new(split_name))
        .index_uid(index_uid.to_string())
        .time_range(TimeRange::new(min_ts, max_ts + 1))
        .num_rows(batch.num_rows() as u64)
        .size_bytes(size_bytes);
    for name in &metric_names {
        builder = builder.add_metric_name(name.clone());
    }
    let metadata = builder.build();

    let stage_request =
        StageMetricsSplitsRequest::try_from_splits_metadata(index_uid.clone(), &[metadata])
            .unwrap();
    metastore
        .clone()
        .stage_metrics_splits(stage_request)
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

/// Execute a DataFusion SQL query via the REST endpoint and decode Arrow IPC.
async fn metrics_sql(sandbox: &ClusterSandbox, sql: &str) -> Vec<RecordBatch> {
    let rest_addr = searcher_rest_addr(sandbox);
    let url = format!("http://{rest_addr}/api/v1/metrics/sql");

    let client = reqwest::Client::new();
    let response = client
        .post(&url)
        .header("content-type", "text/plain")
        .body(sql.to_string())
        .send()
        .await
        .unwrap_or_else(|e| panic!("metrics SQL request failed: {e}"));

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        panic!("metrics SQL query failed with status {status}: {body}");
    }

    let ipc_bytes = response.bytes().await.unwrap();
    if ipc_bytes.is_empty() {
        return vec![];
    }

    let cursor = Cursor::new(&ipc_bytes);
    let reader = StreamReader::try_new(cursor, None).expect("failed to read Arrow IPC");
    reader.collect::<Result<Vec<_>, _>>().unwrap()
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

/// Full production path: REST SQL → DataFusion session builder →
/// MetastoreIndexResolver → metastore RPC → StorageResolver →
/// QuickwitObjectStore → ParquetSource → Arrow IPC
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_metrics_sql_select_all() {
    let (sandbox, data_dir) = start_metrics_sandbox().await;
    let metastore = metastore_client(&sandbox);

    let index_uid =
        create_metrics_index(&metastore, "test-metrics-select", data_dir.path()).await;
    // make_batch produces OSS schema: metric_name, metric_type, timestamp_secs, value, service
    let batch = make_batch("cpu.usage", &[100, 200, 300], &[0.5, 0.8, 0.3], Some("web"));
    write_and_publish_split(&metastore, &index_uid, data_dir.path(), "split_1", &batch).await;

    let sql = r#"
        CREATE OR REPLACE EXTERNAL TABLE "test-metrics-select" (
          metric_name    VARCHAR NOT NULL,
          metric_type    TINYINT,
          timestamp_secs BIGINT  NOT NULL,
          value          DOUBLE  NOT NULL,
          service        VARCHAR
        ) STORED AS metrics LOCATION 'test-metrics-select';
        SELECT * FROM "test-metrics-select"
    "#;
    let batches = metrics_sql(&sandbox, sql).await;
    assert_eq!(total_rows(&batches), 3);
    assert_eq!(batches[0].num_columns(), 5);
}

/// Metric name pruning: only matching splits should be scanned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_metrics_sql_metric_name_pruning() {
    let (sandbox, data_dir) = start_metrics_sandbox().await;
    let metastore = metastore_client(&sandbox);

    let index_uid =
        create_metrics_index(&metastore, "test-metrics-prune", data_dir.path()).await;
    let batch_cpu = make_batch("cpu.usage", &[100, 200], &[0.5, 0.8], Some("web"));
    let batch_mem = make_batch("memory.used", &[100, 200], &[1024.0, 2048.0], Some("web"));

    write_and_publish_split(&metastore, &index_uid, data_dir.path(), "cpu", &batch_cpu).await;
    write_and_publish_split(&metastore, &index_uid, data_dir.path(), "mem", &batch_mem).await;

    let sql = r#"
        CREATE OR REPLACE EXTERNAL TABLE "test-metrics-prune" (
          metric_name    VARCHAR NOT NULL,
          metric_type    TINYINT,
          timestamp_secs BIGINT  NOT NULL,
          value          DOUBLE  NOT NULL,
          service        VARCHAR
        ) STORED AS metrics LOCATION 'test-metrics-prune';
        SELECT value FROM "test-metrics-prune" WHERE metric_name = 'cpu.usage'
    "#;
    let batches = metrics_sql(&sandbox, sql).await;
    assert_eq!(total_rows(&batches), 2);
}

/// Aggregation across multiple splits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_metrics_sql_aggregation() {
    let (sandbox, data_dir) = start_metrics_sandbox().await;
    let metastore = metastore_client(&sandbox);

    let index_uid =
        create_metrics_index(&metastore, "test-metrics-agg", data_dir.path()).await;
    let batch1 = make_batch("cpu.usage", &[100, 200], &[10.0, 20.0], Some("web"));
    let batch2 = make_batch("cpu.usage", &[300, 400], &[30.0, 40.0], Some("api"));

    write_and_publish_split(&metastore, &index_uid, data_dir.path(), "agg1", &batch1).await;
    write_and_publish_split(&metastore, &index_uid, data_dir.path(), "agg2", &batch2).await;

    let sql = r#"
        CREATE OR REPLACE EXTERNAL TABLE "test-metrics-agg" (
          metric_name    VARCHAR NOT NULL,
          metric_type    TINYINT,
          timestamp_secs BIGINT  NOT NULL,
          value          DOUBLE  NOT NULL,
          service        VARCHAR
        ) STORED AS metrics LOCATION 'test-metrics-agg';
        SELECT SUM(value) as total FROM "test-metrics-agg"
    "#;
    let batches = metrics_sql(&sandbox, sql).await;
    assert_eq!(total_rows(&batches), 1);
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert!((total - 100.0).abs() < 0.01, "expected 100.0, got {total}");
}

/// Time range pruning — exercises the CAST unwrapping fix in predicate.rs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_metrics_sql_time_range_pruning() {
    let (sandbox, data_dir) = start_metrics_sandbox().await;
    let metastore = metastore_client(&sandbox);

    let index_uid =
        create_metrics_index(&metastore, "test-metrics-time", data_dir.path()).await;
    let batch_early = make_batch("cpu.usage", &[100, 200, 300], &[0.1, 0.2, 0.3], Some("web"));
    let batch_late =
        make_batch("cpu.usage", &[1000, 1100, 1200], &[0.4, 0.5, 0.6], Some("web"));

    write_and_publish_split(&metastore, &index_uid, data_dir.path(), "early", &batch_early)
        .await;
    write_and_publish_split(&metastore, &index_uid, data_dir.path(), "late", &batch_late).await;

    let sql = r#"
        CREATE OR REPLACE EXTERNAL TABLE "test-metrics-time" (
          metric_name    VARCHAR NOT NULL,
          metric_type    TINYINT,
          timestamp_secs BIGINT  NOT NULL,
          value          DOUBLE  NOT NULL,
          service        VARCHAR
        ) STORED AS metrics LOCATION 'test-metrics-time';
        SELECT AVG(value) as avg_val FROM "test-metrics-time" WHERE timestamp_secs >= 1000
    "#;
    let batches = metrics_sql(&sandbox, sql).await;
    assert_eq!(total_rows(&batches), 1);
    let avg = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    let expected = (0.4 + 0.5 + 0.6) / 3.0;
    assert!((avg - expected).abs() < 0.01, "expected ~{expected}, got {avg}");
}

/// GROUP BY across splits using OSS bare column name `service`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_metrics_sql_group_by() {
    let (sandbox, data_dir) = start_metrics_sandbox().await;
    let metastore = metastore_client(&sandbox);

    let index_uid =
        create_metrics_index(&metastore, "test-metrics-group", data_dir.path()).await;
    let batch1 = make_batch("cpu.usage", &[100, 200, 300], &[0.5, 0.8, 0.3], Some("web"));
    let batch2 = make_batch("cpu.usage", &[400, 500, 600], &[0.4, 0.9, 0.2], Some("api"));
    let batch3 = make_batch("cpu.usage", &[700, 800, 900], &[0.1, 0.6, 0.7], Some("web"));

    write_and_publish_split(&metastore, &index_uid, data_dir.path(), "g1", &batch1).await;
    write_and_publish_split(&metastore, &index_uid, data_dir.path(), "g2", &batch2).await;
    write_and_publish_split(&metastore, &index_uid, data_dir.path(), "g3", &batch3).await;

    let sql = r#"
        CREATE OR REPLACE EXTERNAL TABLE "test-metrics-group" (
          metric_name    VARCHAR NOT NULL,
          metric_type    TINYINT,
          timestamp_secs BIGINT  NOT NULL,
          value          DOUBLE  NOT NULL,
          service        VARCHAR
        ) STORED AS metrics LOCATION 'test-metrics-group';
        SELECT service, COUNT(*) as cnt
        FROM "test-metrics-group"
        GROUP BY service
        ORDER BY service
    "#;
    let batches = metrics_sql(&sandbox, sql).await;
    assert_eq!(total_rows(&batches), 2, "expected 2 groups (api, web)");
}

/// REST ingest-metrics → SQL query (end-to-end).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_rest_ingest_then_sql_query() {
    let (sandbox, data_dir) = start_metrics_sandbox().await;
    let metastore = metastore_client(&sandbox);

    // Create a metrics index first
    create_metrics_index(&metastore, "metrics-ingest-test", data_dir.path()).await;

    let rest_addr = sandbox
        .node_configs
        .iter()
        .find(|(_, s)| s.contains(&QuickwitService::Indexer))
        .unwrap()
        .0
        .rest_config
        .listen_addr;

    // Ingest via REST endpoint using OSS field names (service, env — not tag_service, tag_env)
    let metrics_json = serde_json::json!([
        {"metric_name": "cpu.usage", "timestamp_secs": 1700000100, "value": 0.85, "service": "web", "env": "prod"},
        {"metric_name": "cpu.usage", "timestamp_secs": 1700000200, "value": 0.92, "service": "web", "env": "prod"},
        {"metric_name": "memory.used", "timestamp_secs": 1700000100, "value": 1024.0, "service": "db", "env": "staging"},
        {"metric_name": "cpu.usage", "timestamp_secs": 1700000300, "value": 0.45, "service": "api", "env": "prod"}
    ]);

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{rest_addr}/api/v1/metrics-ingest-test/ingest-metrics"))
        .json(&metrics_json)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "ingest failed: {} {}",
        response.status(),
        response.text().await.unwrap()
    );

    // Query via DataFusion SQL — count all rows
    let sql = r#"
        CREATE OR REPLACE EXTERNAL TABLE "metrics-ingest-test" (
          metric_name    VARCHAR NOT NULL,
          metric_type    TINYINT,
          timestamp_secs BIGINT  NOT NULL,
          value          DOUBLE  NOT NULL,
          service        VARCHAR,
          env            VARCHAR
        ) STORED AS metrics LOCATION 'metrics-ingest-test';
        SELECT COUNT(*) as cnt FROM "metrics-ingest-test"
    "#;
    let batches = metrics_sql(&sandbox, sql).await;
    assert_eq!(total_rows(&batches), 1);
    let cnt = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 4, "expected 4 ingested metrics");

    // Query with filter — only cpu.usage
    let sql = r#"
        SELECT value FROM "metrics-ingest-test"
        WHERE metric_name = 'cpu.usage'
        ORDER BY value
    "#;
    let batches = metrics_sql(&sandbox, sql).await;
    assert_eq!(total_rows(&batches), 3, "expected 3 cpu.usage rows");
}
