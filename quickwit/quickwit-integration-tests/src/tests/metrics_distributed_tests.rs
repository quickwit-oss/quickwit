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

//! Distributed DataFusion execution tests — executed in-process.
//!
//! Test 1 (`test_distributed_tasks_not_shuffles`): builds a session with a
//! two-entry `SearcherPool` constructed from the 2-node sandbox addresses.
//! Verifies the physical plan contains `PartitionIsolatorExec` (one per split
//! assigned to a worker) and NOT `NetworkShuffleExec`.  Then executes the
//! query to verify correctness — workers are reached via the `WorkerService`
//! gRPC that `quickwit-serve/src/grpc.rs` registers on the same port.
//!
//! Test 2 (`test_null_columns_for_missing_parquet_fields`): verifies that
//! columns declared in the DDL schema but absent from a specific parquet file
//! are filled with NULLs by DataFusion's `PhysicalExprAdapterFactory`.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema};
use quickwit_config::service::QuickwitService;
use quickwit_datafusion::DataFusionSessionBuilder;
use quickwit_datafusion::sources::metrics::MetricsDataSource;
use quickwit_datafusion::test_utils::{make_batch, make_batch_with_tags};
use quickwit_metastore::StageMetricsSplitsRequestExt;
use quickwit_parquet_engine::split::{MetricsSplitMetadata, SplitId, TimeRange};
use quickwit_parquet_engine::storage::{ParquetWriter, ParquetWriterConfig};
use quickwit_proto::metastore::{
    CreateIndexRequest, MetastoreService, MetastoreServiceClient, PublishMetricsSplitsRequest,
    StageMetricsSplitsRequest,
};
use quickwit_metastore::CreateIndexRequestExt;
use quickwit_proto::types::IndexUid;
use quickwit_search::{SearcherPool, create_search_client_from_grpc_addr};

use crate::test_utils::{ClusterSandbox, ClusterSandboxBuilder};

// ── Helpers ──────────────────────────────────────────────────────────

fn metastore_client(sandbox: &ClusterSandbox) -> MetastoreServiceClient {
    let (config, _) = sandbox.node_configs.iter()
        .find(|(_, svc)| svc.contains(&QuickwitService::Metastore)).unwrap();
    let addr = config.grpc_listen_addr;
    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap().connect_lazy();
    MetastoreServiceClient::from_channel(addr, channel, bytesize::ByteSize::mib(20), None)
}

/// Build a RecordBatch with ONLY the 4 required columns — no tag columns.
fn make_narrow_batch(metric_name: &str, timestamps: &[u64], values: &[f64]) -> RecordBatch {
    let n = timestamps.len();
    let dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("metric_name", dict, false),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    use arrow::array::{DictionaryArray, Float64Array, Int32Array, StringArray, UInt64Array, UInt8Array};
    let keys = Int32Array::from(vec![0i32; n]);
    let vals = StringArray::from(vec![metric_name]);
    let metric_col = Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(vals)).unwrap());
    RecordBatch::try_new(schema, vec![
        metric_col as Arc<_>,
        Arc::new(UInt8Array::from(vec![0u8; n])),
        Arc::new(UInt64Array::from(timestamps.to_vec())),
        Arc::new(Float64Array::from(values.to_vec())),
    ]).unwrap()
}

async fn create_metrics_index(
    metastore: &MetastoreServiceClient,
    index_id: &str,
    data_dir: &std::path::Path,
) -> IndexUid {
    let index_uri = format!("file://{}", data_dir.display());
    let config: quickwit_config::IndexConfig = serde_json::from_value(serde_json::json!({
        "version": "0.8", "index_id": index_id, "index_uri": index_uri,
        "doc_mapping": {"field_mappings": []}, "indexing_settings": {}, "search_settings": {}
    })).unwrap();
    metastore.clone()
        .create_index(CreateIndexRequest::try_from_index_config(&config).unwrap())
        .await.unwrap().index_uid().clone()
}

async fn publish_split(
    metastore: &MetastoreServiceClient,
    index_uid: &IndexUid,
    data_dir: &std::path::Path,
    split_name: &str,
    batch: &RecordBatch,
) {
    let parquet_bytes = ParquetWriter::new(ParquetWriterConfig::default())
        .write_to_bytes(batch).unwrap();
    let size_bytes = parquet_bytes.len() as u64;
    std::fs::write(data_dir.join(format!("{split_name}.parquet")), &parquet_bytes).unwrap();

    let batch_schema = batch.schema();
    let ts_idx = batch_schema.index_of("timestamp_secs").unwrap();
    let ts_col = batch.column(ts_idx).as_any()
        .downcast_ref::<arrow::array::UInt64Array>().unwrap();
    let min_ts = (0..ts_col.len()).map(|i| ts_col.value(i)).min().unwrap_or(0);
    let max_ts = (0..ts_col.len()).map(|i| ts_col.value(i)).max().unwrap_or(0);

    let mn_idx = batch_schema.index_of("metric_name").unwrap();
    let dict = batch.column(mn_idx).as_any()
        .downcast_ref::<arrow::array::DictionaryArray<Int32Type>>().unwrap();
    let values = dict.values().as_any()
        .downcast_ref::<arrow::array::StringArray>().unwrap();
    let metric_names: HashSet<String> = (0..values.len())
        .filter(|i| !values.is_null(*i)).map(|i| values.value(i).to_string()).collect();

    let mut builder = MetricsSplitMetadata::builder()
        .split_id(SplitId::new(split_name))
        .index_uid(index_uid.to_string())
        .time_range(TimeRange::new(min_ts, max_ts + 1))
        .num_rows(batch.num_rows() as u64).size_bytes(size_bytes);
    for name in &metric_names { builder = builder.add_metric_name(name.clone()); }

    // Extract tag values for metastore split-level pruning
    for tag_col in &["service", "env", "datacenter", "region", "host"] {
        if let Ok(col_idx) = batch_schema.index_of(tag_col) {
            let col = batch.column(col_idx);
            if let Some(dict) = col.as_any().downcast_ref::<arrow::array::DictionaryArray<Int32Type>>() {
                let keys = dict.keys().as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
                let vals = dict.values().as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                let values: std::collections::HashSet<String> = (0..batch.num_rows())
                    .filter(|i| !keys.is_null(*i))
                    .map(|i| vals.value(keys.value(i) as usize).to_string())
                    .collect();
                for v in values { builder = builder.add_low_cardinality_tag(tag_col.to_string(), v); }
            }
        }
    }

    metastore.clone().stage_metrics_splits(
        StageMetricsSplitsRequest::try_from_splits_metadata(index_uid.clone(), &[builder.build()]).unwrap()
    ).await.unwrap();
    metastore.clone().publish_metrics_splits(PublishMetricsSplitsRequest {
        index_uid: Some(index_uid.clone().into()),
        staged_split_ids: vec![split_name.to_string()],
        replaced_split_ids: vec![],
        index_checkpoint_delta_json_opt: None,
        publish_token_opt: None,
    }).await.unwrap();
}

async fn run_sql(builder: &DataFusionSessionBuilder, sql: &str) -> Vec<RecordBatch> {
    let ctx = builder.build_session().unwrap();
    let fragments: Vec<&str> = sql.split(';').map(str::trim).filter(|s| !s.is_empty()).collect();
    for fragment in &fragments[..fragments.len().saturating_sub(1)] {
        ctx.sql(fragment).await.unwrap().collect().await.unwrap();
    }
    ctx.sql(fragments.last().unwrap()).await.unwrap().collect().await.unwrap()
}

// ═══════════════════════════════════════════════════════════════════
// Test 1: Tasks, not shuffles
// ═══════════════════════════════════════════════════════════════════

/// Builds a 2-searcher pool from the sandbox node gRPC addresses, which is
/// enough for `QuickwitWorkerResolver::get_urls()` to return 2 URLs so the
/// distributed optimizer fires.  Workers are reached via the `WorkerService`
/// registered by `grpc.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_distributed_tasks_not_shuffles() {
    unsafe { std::env::set_var("QW_DISABLE_TELEMETRY", "1"); std::env::set_var("QW_ENABLE_DATAFUSION_ENDPOINT", "true"); }
    quickwit_common::setup_logging_for_tests();

    let sandbox = ClusterSandboxBuilder::default()
        .add_node(QuickwitService::supported_services())
        .add_node([QuickwitService::Searcher])
        .build_and_start().await;

    let data_dir = tempfile::tempdir().unwrap();
    let metastore = metastore_client(&sandbox);

    let index_uid = create_metrics_index(&metastore, "dist-test", data_dir.path()).await;
    for (name, metric, ts, vals) in [
        ("split_a", "cpu.usage",    [100u64, 200], [0.1f64, 0.2]),
        ("split_b", "cpu.usage",    [300u64, 400], [0.3f64, 0.4]),
        ("split_c", "memory.used",  [100u64, 200], [1024.0f64, 2048.0]),
        ("split_d", "memory.used",  [300u64, 400], [3072.0f64, 4096.0]),
    ] {
        publish_split(&metastore, &index_uid, data_dir.path(), name,
            &make_batch(metric, &ts, &vals, Some("web"))).await;
    }

    // Build a SearcherPool with both searcher node addresses so the distributed
    // optimizer sees n_workers = 2 and decomposes the plan into tasks.
    let pool = SearcherPool::default();
    for (config, services) in &sandbox.node_configs {
        if services.contains(&QuickwitService::Searcher) {
            let addr = config.grpc_listen_addr;
            // Pool value is SearchServiceClient — only the key (addr) matters for
            // QuickwitWorkerResolver, which calls pool.keys() to get URLs.
            pool.insert(addr, create_search_client_from_grpc_addr(addr, bytesize::ByteSize::mib(20)));        }
    }

    let source = Arc::new(MetricsDataSource::new(
        metastore,
        sandbox.storage_resolver().clone(),
    ));
    let builder = DataFusionSessionBuilder::new()
        .with_source(source)
        .with_searcher_pool(pool);

    let ddl = r#"CREATE OR REPLACE EXTERNAL TABLE "dist-test" (
          metric_name VARCHAR NOT NULL, metric_type TINYINT,
          timestamp_secs BIGINT NOT NULL, value DOUBLE NOT NULL, service VARCHAR
        ) STORED AS metrics LOCATION 'dist-test'"#;
    let agg_sql = format!(
        "{ddl}; SELECT SUM(value) as total, COUNT(*) as cnt FROM \"dist-test\""
    );

    // ── Verify plan shape AND execute in the same session ────────────
    let ctx = builder.build_session().unwrap();
    let fragments: Vec<&str> = agg_sql.split(';').map(str::trim).filter(|s| !s.is_empty()).collect();
    ctx.sql(fragments[0]).await.unwrap().collect().await.unwrap(); // DDL
    let df = ctx.sql(fragments[1]).await.unwrap();
    // Inspect the physical plan before collecting so plan and execution are the same session.
    let plan = df.clone().create_physical_plan().await.unwrap();
    let plan_str = format!("{}", datafusion::physical_plan::displayable(plan.as_ref()).indent(true));
    println!("=== Physical plan ===\n{plan_str}");

    assert!(
        plan_str.contains("DistributedExec") && plan_str.contains("PartitionIsolatorExec"),
        "expected both DistributedExec and PartitionIsolatorExec in distributed plan:\n{plan_str}"
    );
    assert!(
        !plan_str.contains("NetworkShuffleExec"),
        "expected no shuffle (parquet scans are split-local):\n{plan_str}"
    );
    // With 4 splits across 2 workers there should be at least 1 PartitionIsolatorExec
    // (one per split partition assigned to a worker).
    let isolator_count = plan_str.matches("PartitionIsolatorExec").count();
    assert!(
        isolator_count >= 1,
        "expected at least 1 PartitionIsolatorExec, got {isolator_count}:\n{plan_str}"
    );

    // Execute in the SAME context that built the plan — guarantees plan and result agree.
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let total = batches[0].column_by_name("total").unwrap()
        .as_any().downcast_ref::<Float64Array>().unwrap().value(0);
    let expected = 0.1 + 0.2 + 0.3 + 0.4 + 1024.0 + 2048.0 + 3072.0 + 4096.0;
    assert!((total - expected).abs() < 1.0, "expected {expected:.1}, got {total:.1}");
    let cnt = batches[0].column_by_name("cnt").unwrap()
        .as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(cnt, 8);
}

// ═══════════════════════════════════════════════════════════════════
// Test 2: NULL columns for missing parquet fields
// ═══════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_null_columns_for_missing_parquet_fields() {
    unsafe { std::env::set_var("QW_DISABLE_TELEMETRY", "1"); std::env::set_var("QW_ENABLE_DATAFUSION_ENDPOINT", "true"); }
    quickwit_common::setup_logging_for_tests();

    let sandbox = ClusterSandboxBuilder::build_and_start_standalone().await;
    let data_dir = tempfile::tempdir().unwrap();
    let metastore = metastore_client(&sandbox);

    let index_uid = create_metrics_index(&metastore, "null-cols", data_dir.path()).await;

    // split_a: 4 required columns only, no service/env
    let batch_a = make_narrow_batch("cpu.usage", &[100, 200], &[0.5, 0.8]);
    assert!(batch_a.schema().index_of("service").is_err());
    publish_split(&metastore, &index_uid, data_dir.path(), "narrow", &batch_a).await;

    // split_b: 4 required + service + env
    let batch_b = make_batch_with_tags("cpu.usage", &[300, 400], &[0.3, 0.6],
        Some("web"), Some("prod"), None, None, None);
    publish_split(&metastore, &index_uid, data_dir.path(), "wide", &batch_b).await;

    let source = Arc::new(MetricsDataSource::new(metastore, sandbox.storage_resolver().clone()));
    let builder = DataFusionSessionBuilder::new().with_source(source);

    // COUNT(col) counts non-NULL values — tests the NULL-fill behavior
    let sql_str = r#"
        CREATE OR REPLACE EXTERNAL TABLE "null-cols" (
          metric_name VARCHAR NOT NULL, metric_type TINYINT,
          timestamp_secs BIGINT NOT NULL, value DOUBLE NOT NULL,
          service VARCHAR, env VARCHAR
        ) STORED AS metrics LOCATION 'null-cols';
        SELECT COUNT(*) AS total_rows, COUNT(service) AS rows_with_service,
               COUNT(env) AS rows_with_env FROM "null-cols""#;

    let batches = run_sql(&builder, sql_str).await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

    let total = batches[0].column_by_name("total_rows").unwrap()
        .as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(total, 4);

    let with_service = batches[0].column_by_name("rows_with_service").unwrap()
        .as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(with_service, 2,
        "split_a has no service col → NULLs; split_b has service='web' → 2 non-null");

    let with_env = batches[0].column_by_name("rows_with_env").unwrap()
        .as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(with_env, 2);
}
