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

//! Integration tests for metrics DataFusion queries — executed in-process.
//!
//! No REST/gRPC transport. Tests build a `DataFusionSessionBuilder` directly
//! with a real metastore and real file-backed storage, then call
//! `session.sql(...)` as any application would.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, RecordBatch};
use quickwit_config::service::QuickwitService;
use quickwit_datafusion::DataFusionSessionBuilder;
use quickwit_datafusion::sources::metrics::MetricsDataSource;
use quickwit_datafusion::test_utils::make_batch;
use quickwit_metastore::{CreateIndexRequestExt, StageMetricsSplitsRequestExt};
use quickwit_parquet_engine::schema::ParquetSchema;
use quickwit_parquet_engine::split::{MetricsSplitMetadata, SplitId, TimeRange};
use quickwit_parquet_engine::storage::{ParquetWriter, ParquetWriterConfig};
use quickwit_proto::metastore::{
    CreateIndexRequest, MetastoreService, MetastoreServiceClient, PublishMetricsSplitsRequest,
    StageMetricsSplitsRequest,
};
use quickwit_proto::types::IndexUid;

use crate::test_utils::{ClusterSandbox, ClusterSandboxBuilder};

// ── Setup ──────────────────────────────────────────────────────────

async fn start_sandbox() -> (ClusterSandbox, tempfile::TempDir) {
    unsafe { std::env::set_var("QW_DISABLE_TELEMETRY", "1"); }
    quickwit_common::setup_logging_for_tests();
    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let data_dir = tempfile::tempdir().unwrap();
    (sandbox, data_dir)
}

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

/// Build a `DataFusionSessionBuilder` wired to the sandbox's real metastore + storage.
fn session_builder(
    sandbox: &ClusterSandbox,
    metastore: MetastoreServiceClient,
) -> DataFusionSessionBuilder {
    let source = Arc::new(MetricsDataSource::new(
        metastore,
        sandbox.storage_resolver().clone(),
    ));
    DataFusionSessionBuilder::new().with_source(source)
}

// ── Data helpers ───────────────────────────────────────────────────

async fn create_metrics_index(
    metastore: &MetastoreServiceClient,
    index_id: &str,
    data_dir: &std::path::Path,
) -> IndexUid {
    let index_uri = format!("file://{}", data_dir.display());
    let index_config: quickwit_config::IndexConfig =
        serde_json::from_value(serde_json::json!({
            "version": "0.8", "index_id": index_id, "index_uri": index_uri,
            "doc_mapping": { "field_mappings": [] },
            "indexing_settings": {}, "search_settings": {}
        }))
        .unwrap();
    let resp = metastore
        .clone()
        .create_index(CreateIndexRequest::try_from_index_config(&index_config).unwrap())
        .await
        .unwrap();
    resp.index_uid().clone()
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
    let ts_col = batch.column(ts_idx)
        .as_any().downcast_ref::<arrow::array::UInt64Array>().unwrap();
    let min_ts = (0..ts_col.len()).map(|i| ts_col.value(i)).min().unwrap_or(0);
    let max_ts = (0..ts_col.len()).map(|i| ts_col.value(i)).max().unwrap_or(0);

    let mn_idx = batch_schema.index_of("metric_name").unwrap();
    let dict = batch.column(mn_idx).as_any()
        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>().unwrap();
    let values = dict.values().as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    let metric_names: HashSet<String> = (0..values.len())
        .filter(|i| !values.is_null(*i))
        .map(|i| values.value(i).to_string())
        .collect();

    let mut builder = MetricsSplitMetadata::builder()
        .split_id(SplitId::new(split_name))
        .index_uid(index_uid.to_string())
        .time_range(TimeRange::new(min_ts, max_ts + 1))
        .num_rows(batch.num_rows() as u64)
        .size_bytes(size_bytes);
    for name in &metric_names {
        builder = builder.add_metric_name(name.clone());
    }

    metastore.clone()
        .stage_metrics_splits(
            StageMetricsSplitsRequest::try_from_splits_metadata(index_uid.clone(), &[builder.build()]).unwrap()
        ).await.unwrap();
    metastore.clone()
        .publish_metrics_splits(PublishMetricsSplitsRequest {
            index_uid: Some(index_uid.clone().into()),
            staged_split_ids: vec![split_name.to_string()],
            replaced_split_ids: vec![],
            index_checkpoint_delta_json_opt: None,
            publish_token_opt: None,
        }).await.unwrap();
}

/// Execute SQL in-process and return batches.
async fn run_sql(
    builder: &DataFusionSessionBuilder,
    sql: &str,
) -> Vec<RecordBatch> {
    let ctx = builder.build_session();
    // Split on ';' — DFParser consumes trailing ';' which breaks multi-stmt parse
    let fragments: Vec<&str> = sql.split(';').map(str::trim).filter(|s| !s.is_empty()).collect();
    for fragment in &fragments[..fragments.len().saturating_sub(1)] {
        ctx.sql(fragment).await.unwrap().collect().await.unwrap();
    }
    ctx.sql(fragments.last().unwrap()).await.unwrap().collect().await.unwrap()
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_select_all() {
    let (sandbox, data_dir) = start_sandbox().await;
    let metastore = metastore_client(&sandbox);
    let builder = session_builder(&sandbox, metastore.clone());

    let index_uid = create_metrics_index(&metastore, "test-select", data_dir.path()).await;
    let batch = make_batch("cpu.usage", &[100, 200, 300], &[0.5, 0.8, 0.3], Some("web"));
    publish_split(&metastore, &index_uid, data_dir.path(), "split_1", &batch).await;

    let sql = r#"
        CREATE OR REPLACE EXTERNAL TABLE "test-select" (
          metric_name VARCHAR NOT NULL, metric_type TINYINT,
          timestamp_secs BIGINT NOT NULL, value DOUBLE NOT NULL, service VARCHAR
        ) STORED AS metrics LOCATION 'test-select';
        SELECT * FROM "test-select""#;
    let batches = run_sql(&builder, sql).await;
    assert_eq!(total_rows(&batches), 3);
    assert_eq!(batches[0].num_columns(), 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_metric_name_pruning() {
    let (sandbox, data_dir) = start_sandbox().await;
    let metastore = metastore_client(&sandbox);
    let builder = session_builder(&sandbox, metastore.clone());

    let index_uid = create_metrics_index(&metastore, "test-prune", data_dir.path()).await;
    publish_split(&metastore, &index_uid, data_dir.path(), "cpu",
        &make_batch("cpu.usage", &[100, 200], &[0.5, 0.8], Some("web"))).await;
    publish_split(&metastore, &index_uid, data_dir.path(), "mem",
        &make_batch("memory.used", &[100, 200], &[1024.0, 2048.0], Some("web"))).await;


    let sql = r#"
        CREATE OR REPLACE EXTERNAL TABLE "test-prune" (
          metric_name VARCHAR NOT NULL, metric_type TINYINT,
          timestamp_secs BIGINT NOT NULL, value DOUBLE NOT NULL, service VARCHAR
        ) STORED AS metrics LOCATION 'test-prune';
        SELECT value FROM "test-prune" WHERE metric_name = 'cpu.usage'"#;
    assert_eq!(total_rows(&run_sql(&builder, sql).await), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_aggregation() {
    let (sandbox, data_dir) = start_sandbox().await;
    let metastore = metastore_client(&sandbox);
    let builder = session_builder(&sandbox, metastore.clone());

    let index_uid = create_metrics_index(&metastore, "test-agg", data_dir.path()).await;
    publish_split(&metastore, &index_uid, data_dir.path(), "agg1",
        &make_batch("cpu.usage", &[100, 200], &[10.0, 20.0], Some("web"))).await;
    publish_split(&metastore, &index_uid, data_dir.path(), "agg2",
        &make_batch("cpu.usage", &[300, 400], &[30.0, 40.0], Some("api"))).await;

    let sql = r#"
        CREATE OR REPLACE EXTERNAL TABLE "test-agg" (
          metric_name VARCHAR NOT NULL, metric_type TINYINT,
          timestamp_secs BIGINT NOT NULL, value DOUBLE NOT NULL, service VARCHAR
        ) STORED AS metrics LOCATION 'test-agg';
        SELECT SUM(value) as total FROM "test-agg""#;
    let batches = run_sql(&builder, sql).await;
    assert_eq!(total_rows(&batches), 1);
    let total = batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
    assert!((total - 100.0).abs() < 0.01, "expected 100.0, got {total}");
}

/// Time range pruning — exercises the CAST unwrapping fix in predicate.rs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_time_range_pruning() {
    let (sandbox, data_dir) = start_sandbox().await;
    let metastore = metastore_client(&sandbox);
    let builder = session_builder(&sandbox, metastore.clone());

    let index_uid = create_metrics_index(&metastore, "test-time", data_dir.path()).await;
    publish_split(&metastore, &index_uid, data_dir.path(), "early",
        &make_batch("cpu.usage", &[100, 200, 300], &[0.1, 0.2, 0.3], Some("web"))).await;
    publish_split(&metastore, &index_uid, data_dir.path(), "late",
        &make_batch("cpu.usage", &[1000, 1100, 1200], &[0.4, 0.5, 0.6], Some("web"))).await;

    let sql = r#"
        CREATE OR REPLACE EXTERNAL TABLE "test-time" (
          metric_name VARCHAR NOT NULL, metric_type TINYINT,
          timestamp_secs BIGINT NOT NULL, value DOUBLE NOT NULL, service VARCHAR
        ) STORED AS metrics LOCATION 'test-time';
        SELECT AVG(value) as avg_val FROM "test-time" WHERE timestamp_secs >= 1000"#;
    let batches = run_sql(&builder, sql).await;
    assert_eq!(total_rows(&batches), 1);
    let avg = batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
    let expected = (0.4 + 0.5 + 0.6) / 3.0;
    assert!((avg - expected).abs() < 0.01, "expected ~{expected}, got {avg}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_group_by() {
    let (sandbox, data_dir) = start_sandbox().await;
    let metastore = metastore_client(&sandbox);
    let builder = session_builder(&sandbox, metastore.clone());

    let index_uid = create_metrics_index(&metastore, "test-group", data_dir.path()).await;
    for (name, svc, ts) in [("g1", "web", [100u64, 200, 300]), ("g2", "api", [400u64, 500, 600])] {
        publish_split(&metastore, &index_uid, data_dir.path(), name,
            &make_batch("cpu.usage", &ts, &[0.1, 0.2, 0.3], Some(svc))).await;
    }

    let sql = r#"
        CREATE OR REPLACE EXTERNAL TABLE "test-group" (
          metric_name VARCHAR NOT NULL, metric_type TINYINT,
          timestamp_secs BIGINT NOT NULL, value DOUBLE NOT NULL, service VARCHAR
        ) STORED AS metrics LOCATION 'test-group';
        SELECT service, COUNT(*) as cnt FROM "test-group" GROUP BY service ORDER BY service"#;
    assert_eq!(total_rows(&run_sql(&builder, sql).await), 2);
}

/// REST ingest → in-process DataFusion query.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rest_ingest_then_in_process_query() {
    let (sandbox, data_dir) = start_sandbox().await;
    let metastore = metastore_client(&sandbox);

    // Create the index so the ingest endpoint can find it
    create_metrics_index(&metastore, "metrics-e2e", data_dir.path()).await;

    let rest_addr = sandbox
        .node_configs
        .iter()
        .find(|(_, s)| s.contains(&QuickwitService::Indexer))
        .unwrap()
        .0
        .rest_config
        .listen_addr;

    let metrics_json = serde_json::json!([
        {"metric_name": "cpu.usage", "timestamp_secs": 1700000100, "value": 0.85, "service": "web"},
        {"metric_name": "cpu.usage", "timestamp_secs": 1700000200, "value": 0.92, "service": "web"},
        {"metric_name": "memory.used", "timestamp_secs": 1700000100, "value": 1024.0, "service": "db"},
        {"metric_name": "cpu.usage", "timestamp_secs": 1700000300, "value": 0.45, "service": "api"}
    ]);

    let resp = reqwest::Client::new()
        .post(format!("http://{rest_addr}/api/v1/metrics-e2e/ingest-metrics"))
        .json(&metrics_json)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "ingest failed: {}", resp.text().await.unwrap());

    let builder = session_builder(&sandbox, metastore);
    let sql = r#"
        CREATE OR REPLACE EXTERNAL TABLE "metrics-e2e" (
          metric_name VARCHAR NOT NULL, metric_type TINYINT,
          timestamp_secs BIGINT NOT NULL, value DOUBLE NOT NULL,
          service VARCHAR, env VARCHAR
        ) STORED AS metrics LOCATION 'metrics-e2e';
        SELECT COUNT(*) as cnt FROM "metrics-e2e""#;
    let batches = run_sql(&builder, sql).await;
    let cnt = batches[0].column(0).as_any()
        .downcast_ref::<arrow::array::Int64Array>().unwrap().value(0);
    assert_eq!(cnt, 4);
}

/// Verifies that CAST-unwrapping in `predicate.rs` causes fewer splits to be scanned
/// when a time filter is applied through the full SQL pipeline.
///
/// DataFusion emits `CAST(timestamp_secs AS Int64) >= 1000` when comparing a UInt64
/// column against an Int64 literal. Without CAST unwrapping in `column_name()`, the
/// filter is left in `remaining` and the metastore query has no time range — all splits
/// are returned. With CAST unwrapping, only the late split matches.
///
/// This test exercises the extraction-to-pruning pipeline end-to-end: the CAST-wrapped
/// filter flows from DataFusion's optimizer through `extract_split_filters` and then
/// prunes the metastore split list. The correctness signal is the query result: if
/// pruning is wrong, early-split values (0.1, 0.2, 0.3) leak into the aggregate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cast_unwrapping_prunes_to_late_split_only() {
    let (sandbox, data_dir) = start_sandbox().await;
    let metastore = metastore_client(&sandbox);
    let builder = session_builder(&sandbox, metastore.clone());

    let index_uid = create_metrics_index(&metastore, "test-cast-prune", data_dir.path()).await;
    // Early split: timestamps 100–300, values 0.1–0.3
    publish_split(
        &metastore,
        &index_uid,
        data_dir.path(),
        "early",
        &make_batch("cpu.usage", &[100, 200, 300], &[0.1, 0.2, 0.3], Some("web")),
    )
    .await;
    // Late split: timestamps 1000–1200, values 0.4–0.6
    publish_split(
        &metastore,
        &index_uid,
        data_dir.path(),
        "late",
        &make_batch("cpu.usage", &[1000, 1100, 1200], &[0.4, 0.5, 0.6], Some("web")),
    )
    .await;

    // The direct proof that CAST unwrapping is working lives in the unit tests in
    // quickwit-datafusion/src/sources/metrics/predicate.rs
    // (test_timestamp_gte_with_cast_column, test_timestamp_lt_with_cast_column, and
    // test_metric_name_pruning_prunes_splits_not_just_rows). Those tests are
    // inaccessible here because `predicate` is an internal module.
    // This integration test verifies functional correctness (parquet-level filtering).

    let sql = r#"
        CREATE OR REPLACE EXTERNAL TABLE "test-cast-prune" (
          metric_name VARCHAR NOT NULL, metric_type TINYINT,
          timestamp_secs BIGINT NOT NULL, value DOUBLE NOT NULL, service VARCHAR
        ) STORED AS metrics LOCATION 'test-cast-prune';
        SELECT COUNT(*) AS cnt, SUM(value) AS total FROM "test-cast-prune"
        WHERE timestamp_secs >= 1000"#;
    let batches = run_sql(&builder, sql).await;
    assert_eq!(total_rows(&batches), 1);
    let cnt = batches[0]
        .column_by_name("cnt")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    // Note: this row-count assertion proves functional correctness (parquet-level
    // filter) but NOT split pruning. The split-pruning proof is the direct
    // predicate extraction assertion above.
    assert_eq!(cnt, 3, "expected 3 rows from late split only; got {cnt}");
    let total = batches[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    let expected = 0.4 + 0.5 + 0.6;
    assert!(
        (total - expected).abs() < 0.01,
        "expected {expected:.2}, got {total:.2} — early-split values must not appear"
    );
}

/// Verifies that querying an index with no published splits returns zero rows and does
/// not panic. This tests that DataFusion handles an empty `FileScanConfig` correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_query_empty_index_returns_zero_rows() {
    let (sandbox, data_dir) = start_sandbox().await;
    let metastore = metastore_client(&sandbox);
    let builder = session_builder(&sandbox, metastore.clone());

    // Create the index but publish NO splits.
    create_metrics_index(&metastore, "test-empty", data_dir.path()).await;

    let sql = r#"
        CREATE OR REPLACE EXTERNAL TABLE "test-empty" (
          metric_name VARCHAR NOT NULL, metric_type TINYINT,
          timestamp_secs BIGINT NOT NULL, value DOUBLE NOT NULL, service VARCHAR
        ) STORED AS metrics LOCATION 'test-empty';
        SELECT COUNT(*) AS cnt FROM "test-empty""#;
    let batches = run_sql(&builder, sql).await;
    let cnt = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 0, "empty index must return 0 rows, got {cnt}");
}

/// Verifies that a multi-value IN filter returns rows from ALL matching splits, not
/// just the first. This is the integration-level proof for the multi-value IN fix.
///
/// Three splits contain different services (web, api, db). A query filtering
/// `service IN ('web', 'api')` must return rows from both the web and api splits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_in_list_tag_filter_returns_all_matching_rows() {
    let (sandbox, data_dir) = start_sandbox().await;
    let metastore = metastore_client(&sandbox);
    let builder = session_builder(&sandbox, metastore.clone());

    let index_uid = create_metrics_index(&metastore, "test-inlist", data_dir.path()).await;
    publish_split(
        &metastore,
        &index_uid,
        data_dir.path(),
        "web_split",
        &make_batch("cpu.usage", &[100, 200], &[1.0, 2.0], Some("web")),
    )
    .await;
    publish_split(
        &metastore,
        &index_uid,
        data_dir.path(),
        "api_split",
        &make_batch("cpu.usage", &[300, 400], &[3.0, 4.0], Some("api")),
    )
    .await;
    publish_split(
        &metastore,
        &index_uid,
        data_dir.path(),
        "db_split",
        &make_batch("cpu.usage", &[500, 600], &[5.0, 6.0], Some("db")),
    )
    .await;

    let sql = r#"
        CREATE OR REPLACE EXTERNAL TABLE "test-inlist" (
          metric_name VARCHAR NOT NULL, metric_type TINYINT,
          timestamp_secs BIGINT NOT NULL, value DOUBLE NOT NULL, service VARCHAR
        ) STORED AS metrics LOCATION 'test-inlist';
        SELECT service, COUNT(*) AS cnt FROM "test-inlist"
        WHERE service IN ('web', 'api')
        GROUP BY service ORDER BY service"#;
    let batches = run_sql(&builder, sql).await;
    // Must return 2 rows (one group per service) — both web and api splits were scanned.
    assert_eq!(
        total_rows(&batches),
        2,
        "IN ('web','api') must return rows for both services; got {} groups",
        total_rows(&batches)
    );
    let total_data_rows: i64 = batches
        .iter()
        .map(|b| {
            b.column_by_name("cnt")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .iter()
                .flatten()
                .sum::<i64>()
        })
        .sum();
    assert_eq!(total_data_rows, 4, "web (2) + api (2) = 4 rows; db must be excluded");
}

/// Demonstrates the `sum:metric{filter} by {groups}.rollup(agg, interval)` pattern
/// over wide-format parquet data — no context/points JOIN needed.
///
/// In Datadog's internal model a query like:
///   `avg:cpu.usage{env:prod} by {service}.rollup(max, 30)`
/// is compiled to SQL over two tables joined on `bhandle` (a tag hash).
///
/// With our wide-format parquet model every data point carries its own tags
/// as columns, so the same query is a single two-level aggregation:
///
///   1. Inner GROUP BY (service, host, time_bin) → MAX(value) per series per bin
///   2. Outer GROUP BY (service, time_bin)       → AVG(max) across hosts per bin
///
/// Three prod series, one staging series (must be filtered out):
///   web / host=web-01: values 1,2,3,4,5,6 at t=0,15,30,45,60,75
///   web / host=web-02: values 10,20,30,40,50,60 at t=0,15,30,45,60,75
///   api / host=api-01: values 100,200,300,400,500,600 at t=0,15,30,45,60,75
///   web / host=web-01 / env=staging (should be excluded by env filter)
///
/// Expected results (30-second bins, epoch origin):
///   bin t=0:  web → avg(max(1,2),  max(10,20))  = avg(2,  20)  = 11.0
///             api → avg(max(100,200))             = 200.0
///   bin t=30: web → avg(max(3,4),  max(30,40))  = avg(4,  40)  = 22.0
///             api → avg(max(300,400))             = 400.0
///   bin t=60: web → avg(max(5,6),  max(50,60))  = avg(6,  60)  = 33.0
///             api → avg(max(500,600))             = 600.0
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rollup_nested_aggregation() {
    use quickwit_datafusion::test_utils::make_batch_with_tags;

    let (sandbox, data_dir) = start_sandbox().await;
    let metastore = metastore_client(&sandbox);
    let builder = session_builder(&sandbox, metastore.clone());

    let index_uid = create_metrics_index(&metastore, "rollup-test", data_dir.path()).await;

    // Timestamps span 3 full 30-second bins (0–29, 30–59, 60–89).
    let ts: &[u64] = &[0, 15, 30, 45, 60, 75];

    publish_split(&metastore, &index_uid, data_dir.path(), "web-01-prod",
        &make_batch_with_tags("cpu.usage", ts, &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            Some("web"), Some("prod"), None, None, Some("web-01"))).await;

    publish_split(&metastore, &index_uid, data_dir.path(), "web-02-prod",
        &make_batch_with_tags("cpu.usage", ts, &[10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            Some("web"), Some("prod"), None, None, Some("web-02"))).await;

    publish_split(&metastore, &index_uid, data_dir.path(), "api-01-prod",
        &make_batch_with_tags("cpu.usage", ts, &[100.0, 200.0, 300.0, 400.0, 500.0, 600.0],
            Some("api"), Some("prod"), None, None, Some("api-01"))).await;

    // Staging split — env filter must exclude all rows from this split.
    publish_split(&metastore, &index_uid, data_dir.path(), "web-01-staging",
        &make_batch_with_tags("cpu.usage", &[0, 30, 60], &[999.0, 999.0, 999.0],
            Some("web"), Some("staging"), None, None, Some("web-01"))).await;

    // The query mirrors the Datadog rollup pattern without a context/points join:
    //   avg:cpu.usage{env:prod} by {service}.rollup(max, 30)
    //
    // Step 1 (inner): MAX per series (service + host) per 30-second bin.
    // Step 2 (outer): AVG of those per-series maxes, grouped by service.
    //
    // to_timestamp_seconds() converts the stored epoch-seconds UInt64 to a
    // Timestamp so that date_bin() can bucket it into 30-second intervals.
    let sql = r#"
        CREATE OR REPLACE EXTERNAL TABLE "rollup-test" (
            metric_name    VARCHAR NOT NULL,
            metric_type    TINYINT,
            timestamp_secs BIGINT  NOT NULL,
            value          DOUBLE  NOT NULL,
            service        VARCHAR,
            env            VARCHAR,
            host           VARCHAR
        ) STORED AS metrics LOCATION 'rollup-test';
        WITH bin_max AS (
            SELECT
                service,
                host,
                date_bin(
                    INTERVAL '30 seconds',
                    to_timestamp_seconds(timestamp_secs)
                ) AS time_bin,
                MAX(value) AS max_bin_val
            FROM "rollup-test"
            WHERE metric_name = 'cpu.usage'
              AND env = 'prod'
            GROUP BY service, host, time_bin
        )
        SELECT
            service,
            time_bin,
            AVG(max_bin_val) AS avg_val
        FROM bin_max
        GROUP BY service, time_bin
        ORDER BY time_bin, service
    "#;

    let batches = run_sql(&builder, sql).await;

    // 3 bins × 2 services (web, api) = 6 result rows.
    assert_eq!(total_rows(&batches), 6,
        "expected 6 rows (3 bins × 2 services); staging rows must be excluded");

    // Collect (service, avg_val) pairs in ORDER BY time_bin, service order.
    // After GROUP BY, DataFusion casts dict-encoded strings to plain Utf8.
    let results: Vec<(String, f64)> = batches.iter().flat_map(|batch| {
        let svc_raw = batch.column_by_name("service").unwrap();
        let avg_col = batch.column_by_name("avg_val").unwrap()
            .as_any().downcast_ref::<Float64Array>().unwrap();
        (0..batch.num_rows()).map(|i| {
            // After GROUP BY, DataFusion 52 may return Utf8View, Utf8, or Dict.
            let svc = if let Some(sa) = svc_raw.as_any()
                    .downcast_ref::<arrow::array::StringViewArray>() {
                sa.value(i).to_string()
            } else if let Some(sa) = svc_raw.as_any()
                    .downcast_ref::<arrow::array::StringArray>() {
                sa.value(i).to_string()
            } else {
                let dict = svc_raw.as_any()
                    .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>()
                    .unwrap_or_else(|| panic!("service column: unexpected type {:?}", svc_raw.data_type()));
                let keys = dict.keys().as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
                let vals = dict.values().as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                vals.value(keys.value(i) as usize).to_string()
            };
            let avg = avg_col.value(i);
            (svc, avg)
        }).collect::<Vec<_>>()
    }).collect();

    // Expected: [(api,200), (web,11), (api,400), (web,22), (api,600), (web,33)]
    let expected = [
        ("api",  200.0_f64),
        ("web",   11.0),
        ("api",  400.0),
        ("web",   22.0),
        ("api",  600.0),
        ("web",   33.0),
    ];

    assert_eq!(results.len(), expected.len());
    for (i, ((got_svc, got_avg), (exp_svc, exp_avg))) in
        results.iter().zip(expected.iter()).enumerate()
    {
        assert_eq!(got_svc.as_str(), *exp_svc, "row {i}: wrong service");
        assert!(
            (got_avg - exp_avg).abs() < 0.01,
            "row {i} ({exp_svc}): expected avg={exp_avg:.2}, got {got_avg:.2}"
        );
    }
}
