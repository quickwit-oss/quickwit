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
//! Test 1 (`test_distributed_tasks_not_shuffles`): starts a real two-searcher
//! Quickwit cluster, queries the production DataFusion gRPC endpoint, and
//! verifies the physical plan contains a distributed stage with
//! `PartitionIsolatorExec` tasks and no `NetworkShuffleExec`. Then executes
//! the same query to verify correctness. Worker discovery flows through the
//! normal chitchat → `SearcherPool` path used by `quickwit-serve`.
//!
//! Test 2 (`test_null_columns_for_missing_parquet_fields`): verifies that
//! columns declared in the DDL schema but absent from a specific parquet file
//! are filled with NULLs by DataFusion's `PhysicalExprAdapterFactory`.

use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema};
use arrow::ipc::reader::StreamReader;
use futures_util::StreamExt;
use quickwit_config::service::QuickwitService;
use quickwit_datafusion::proto::ExecuteSqlRequest;
use quickwit_datafusion::proto::data_fusion_service_client::DataFusionServiceClient;
use quickwit_datafusion::sources::metrics::MetricsDataSource;
use quickwit_datafusion::test_utils::{make_batch, make_batch_with_tags};
use quickwit_datafusion::{DataFusionSessionBuilder, QuickwitObjectStoreRegistry};
use quickwit_metastore::CreateIndexRequestExt;
use quickwit_parquet_engine::timeseries_id::compute_timeseries_id;
use quickwit_proto::metastore::{CreateIndexRequest, MetastoreService, MetastoreServiceClient};
use quickwit_proto::types::IndexUid;

use crate::test_utils::{ClusterSandbox, ClusterSandboxBuilder, publish_split};

// ── Helpers ──────────────────────────────────────────────────────────

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

fn datafusion_client(
    sandbox: &ClusterSandbox,
) -> DataFusionServiceClient<tonic::transport::Channel> {
    let (config, _) = sandbox
        .node_configs
        .iter()
        .find(|(_, svc)| svc.contains(&QuickwitService::Searcher))
        .unwrap();
    let addr = config.grpc_listen_addr;
    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_lazy();
    DataFusionServiceClient::new(channel)
        .max_decoding_message_size(20 * 1024 * 1024)
        .max_encoding_message_size(20 * 1024 * 1024)
}

async fn execute_datafusion_sql(
    client: &mut DataFusionServiceClient<tonic::transport::Channel>,
    sql: impl Into<String>,
) -> Vec<RecordBatch> {
    let request = ExecuteSqlRequest {
        sql: sql.into(),
        properties: Default::default(),
    };
    let mut stream = client.execute_sql(request).await.unwrap().into_inner();
    let mut batches = Vec::new();
    while let Some(response) = stream.next().await {
        let ipc_bytes = response.unwrap().arrow_ipc_bytes;
        let reader = StreamReader::try_new(std::io::Cursor::new(ipc_bytes), None).unwrap();
        for batch in reader {
            batches.push(batch.unwrap());
        }
    }
    batches
}

fn explain_plan_text(batches: &[RecordBatch]) -> String {
    let mut plans = Vec::new();
    for batch in batches {
        let plan_col = batch.column_by_name("plan").unwrap();
        let plan = plan_col.as_any().downcast_ref::<StringArray>().unwrap();
        for row in 0..plan.len() {
            if plan.is_valid(row) {
                plans.push(plan.value(row).to_string());
            }
        }
    }
    plans.join("\n")
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
        Field::new("timeseries_id", DataType::Int64, false),
    ]));
    use arrow::array::{
        DictionaryArray, Float64Array, Int32Array, StringArray, UInt8Array, UInt64Array,
    };
    let timeseries_id = compute_timeseries_id(metric_name, 0, &std::collections::HashMap::new());
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
            Arc::new(Int64Array::from(vec![timeseries_id; n])),
        ],
    )
    .unwrap()
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
    }))
    .unwrap();
    metastore
        .clone()
        .create_index(CreateIndexRequest::try_from_index_config(&config).unwrap())
        .await
        .unwrap()
        .index_uid()
        .clone()
}

async fn run_sql(
    builder: &DataFusionSessionBuilder,
    sql: &str,
) -> Vec<datafusion::arrow::array::RecordBatch> {
    let ctx = builder.build_session().unwrap();
    let fragments: Vec<&str> = sql
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    for fragment in &fragments[..fragments.len().saturating_sub(1)] {
        ctx.sql(fragment).await.unwrap().collect().await.unwrap();
    }
    ctx.sql(fragments.last().unwrap())
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
}

// ═══════════════════════════════════════════════════════════════════
// Test 1: Tasks, not shuffles
// ═══════════════════════════════════════════════════════════════════

/// Uses the production DataFusion gRPC endpoint on a 2-searcher cluster.
///
/// This validates the real discovery path: chitchat emits cluster changes,
/// `setup_searcher` feeds them into `SearcherPool`, and
/// `QuickwitWorkerResolver` turns those searcher addresses into DataFusion
/// worker URLs. The plan assertion catches regressions where that path falls
/// back to single-node execution.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_distributed_tasks_not_shuffles() {
    unsafe {
        std::env::set_var("QW_DISABLE_TELEMETRY", "1");
        std::env::set_var("QW_ENABLE_DATAFUSION_ENDPOINT", "true");
    }
    quickwit_common::setup_logging_for_tests();

    let sandbox = ClusterSandboxBuilder::default()
        .add_node(QuickwitService::supported_services())
        .add_node([QuickwitService::Searcher])
        .build_and_start()
        .await;

    let data_dir = tempfile::tempdir().unwrap();
    let metastore = metastore_client(&sandbox);

    let index_uid = create_metrics_index(&metastore, "dist-test", data_dir.path()).await;
    for (name, metric, ts, vals) in [
        ("split_a", "cpu.usage", [100u64, 200], [0.1f64, 0.2]),
        ("split_b", "cpu.usage", [300u64, 400], [0.3f64, 0.4]),
        ("split_c", "memory.used", [100u64, 200], [1024.0f64, 2048.0]),
        ("split_d", "memory.used", [300u64, 400], [3072.0f64, 4096.0]),
    ] {
        publish_split(
            &metastore,
            &index_uid,
            data_dir.path(),
            name,
            &make_batch(metric, &ts, &vals, Some("web")),
        )
        .await;
    }

    let ddl = r#"CREATE OR REPLACE EXTERNAL TABLE "dist-test" (
          metric_name VARCHAR NOT NULL, metric_type TINYINT,
          timestamp_secs BIGINT NOT NULL, value DOUBLE NOT NULL, service VARCHAR
        ) STORED AS metrics LOCATION 'dist-test'"#;
    let query = r#"SELECT SUM(value) as total, COUNT(*) as cnt FROM "dist-test""#;

    let mut client = datafusion_client(&sandbox);
    let explain_batches =
        execute_datafusion_sql(&mut client, format!("{ddl}; EXPLAIN VERBOSE {query}")).await;
    let plan_str = explain_plan_text(&explain_batches);
    println!("=== DataFusion EXPLAIN output ===\n{plan_str}");

    assert!(
        plan_str.contains("DistributedExec") && plan_str.contains("PartitionIsolatorExec"),
        "expected both DistributedExec and PartitionIsolatorExec in distributed plan:\n{plan_str}"
    );
    assert!(
        plan_str.contains("[Stage 1] => NetworkCoalesceExec"),
        "expected a distributed stage in the plan:\n{plan_str}"
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

    let mut client = datafusion_client(&sandbox);
    let batches = execute_datafusion_sql(&mut client, format!("{ddl}; {query}")).await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let total = batches[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    let expected = 0.1 + 0.2 + 0.3 + 0.4 + 1024.0 + 2048.0 + 3072.0 + 4096.0;
    assert!(
        (total - expected).abs() < 1.0,
        "expected {expected:.1}, got {total:.1}"
    );
    let cnt = batches[0]
        .column_by_name("cnt")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 8);

    // Shut the cluster down cleanly so the test doesn't leak actors into the
    // tokio runtime — otherwise nextest logs "There are still running actors".
    sandbox.shutdown().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════
// Test 2: NULL columns for missing parquet fields
// ═══════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_null_columns_for_missing_parquet_fields() {
    unsafe {
        std::env::set_var("QW_DISABLE_TELEMETRY", "1");
        std::env::set_var("QW_ENABLE_DATAFUSION_ENDPOINT", "true");
    }
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
    let batch_b = make_batch_with_tags(
        "cpu.usage",
        &[300, 400],
        &[0.3, 0.6],
        Some("web"),
        Some("prod"),
        None,
        None,
        None,
    );
    publish_split(&metastore, &index_uid, data_dir.path(), "wide", &batch_b).await;

    let storage_resolver = quickwit_storage::StorageResolver::unconfigured();
    let source = Arc::new(MetricsDataSource::new(metastore));
    let schema_source = Arc::clone(&source);
    let registry = Arc::new(QuickwitObjectStoreRegistry::new(storage_resolver));
    let builder = DataFusionSessionBuilder::new()
        .with_object_store_registry(registry)
        .expect("install object store registry")
        .with_runtime_plugin(Arc::clone(&source) as Arc<_>)
        .with_substrait_consumer(source as Arc<_>)
        .with_schema_provider_factory("quickwit", "public", move || {
            schema_source.schema_provider()
        });

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

    let total = batches[0]
        .column_by_name("total_rows")
        .unwrap()
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 4);

    let with_service = batches[0]
        .column_by_name("rows_with_service")
        .unwrap()
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        with_service, 2,
        "split_a has no service col → NULLs; split_b has service='web' → 2 non-null"
    );

    let with_env = batches[0]
        .column_by_name("rows_with_env")
        .unwrap()
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(with_env, 2);

    sandbox.shutdown().await.unwrap();
}
