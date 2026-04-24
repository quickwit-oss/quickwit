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

//! Distributed DataFusion execution test — real 2-worker gRPC topology.
//!
//! Builds two in-process DataFusion worker gRPC servers on random localhost
//! ports and a `SearcherPool` that points at both. The coordinator's session
//! then runs the distributed optimizer against the 4-split index and we assert
//! the physical plan contains `DistributedExec` + `PartitionIsolatorExec` and
//! no `NetworkShuffleExec`. Then the query executes across the two workers and
//! we verify the result.
//!
//! This is the one test that genuinely needs more than a single process — every
//! other DataFusion integration test is single-node and lives alongside.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array};
use datafusion::arrow;
use quickwit_datafusion::service::split_sql_statements;
use quickwit_datafusion::sources::metrics::MetricsDataSource;
use quickwit_datafusion::test_utils::make_batch;
use quickwit_datafusion::{
    DataFusionSessionBuilder, QuickwitObjectStoreRegistry, QuickwitWorkerResolver,
};
use quickwit_search::{SearcherPool, create_search_client_from_grpc_addr};

mod common;

use common::sandbox::spawn_df_worker;
use common::{TestSandbox, create_metrics_index, publish_split};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_distributed_tasks_not_shuffles() {
    quickwit_common::setup_logging_for_tests();

    let sandbox = TestSandbox::start().await;
    let metastore = sandbox.metastore.clone();
    let data_dir = &sandbox.data_dir;

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

    let source = Arc::new(MetricsDataSource::new(metastore));

    // Pool starts empty; we populate it once the workers are bound. The resolver
    // reads pool keys lazily at query time, so this ordering is safe.
    let pool = SearcherPool::default();

    // Each builder gets its own `RuntimeEnv` wired with a custom
    // `QuickwitObjectStoreRegistry` — the registry lazily resolves any
    // URL `StorageResolver` understands the first time DataFusion reads
    // from it, so no startup registration is needed. Mirrors what
    // `build_datafusion_session_builder` does inside `quickwit-serve`.
    let make_builder = || -> Arc<DataFusionSessionBuilder> {
        let runtime_source = Arc::clone(&source);
        let substrait_source = Arc::clone(&source);
        let schema_source = Arc::clone(&source);
        let registry = Arc::new(QuickwitObjectStoreRegistry::new(
            sandbox.storage_resolver.clone(),
        ));
        Arc::new(
            DataFusionSessionBuilder::new()
                .with_object_store_registry(registry)
                .expect("install object store registry")
                .with_runtime_plugin(runtime_source as Arc<_>)
                .with_substrait_consumer(substrait_source as Arc<_>)
                .with_schema_provider_factory("quickwit", "public", move || {
                    schema_source.schema_provider()
                })
                .with_worker_resolver(QuickwitWorkerResolver::new(pool.clone())),
        )
    };

    let worker_a = spawn_df_worker(make_builder()).await;
    let worker_b = spawn_df_worker(make_builder()).await;

    // Populate the pool with the real worker gRPC addresses.
    // Pool value is a SearchServiceClient — only the key (addr) matters for
    // QuickwitWorkerResolver, which calls pool.keys() to get URLs.
    for addr in [worker_a.addr, worker_b.addr] {
        pool.insert(
            addr,
            create_search_client_from_grpc_addr(addr, bytesize::ByteSize::mib(20)),
        );
    }

    let builder = make_builder();

    let ddl = r#"CREATE OR REPLACE EXTERNAL TABLE "dist-test" (
          metric_name VARCHAR NOT NULL, metric_type TINYINT,
          timestamp_secs BIGINT NOT NULL, value DOUBLE NOT NULL, service VARCHAR
        ) STORED AS metrics LOCATION 'dist-test'"#;
    let agg_sql = format!("{ddl}; SELECT SUM(value) as total, COUNT(*) as cnt FROM \"dist-test\"");

    // Verify plan shape and execute in the same session so both reflect identical state.
    let ctx = builder.build_session().unwrap();
    let mut statements = split_sql_statements(&ctx, &agg_sql).unwrap();
    let query = statements.pop().unwrap();
    for statement in statements {
        ctx.sql(&statement).await.unwrap().collect().await.unwrap();
    }

    let df = ctx.sql(&query).await.unwrap();
    let plan = df.clone().create_physical_plan().await.unwrap();
    let plan_str = format!(
        "{}",
        datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
    );
    println!("=== Physical plan ===\n{plan_str}");

    assert!(
        plan_str.contains("DistributedExec") && plan_str.contains("PartitionIsolatorExec"),
        "expected DistributedExec + PartitionIsolatorExec in distributed plan:\n{plan_str}"
    );
    assert!(
        !plan_str.contains("NetworkShuffleExec"),
        "expected no shuffle (parquet scans are split-local):\n{plan_str}"
    );
    let isolator_count = plan_str.matches("PartitionIsolatorExec").count();
    assert!(
        isolator_count >= 1,
        "expected at least 1 PartitionIsolatorExec, got {isolator_count}:\n{plan_str}"
    );

    let batches = df.collect().await.unwrap();
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

    // Keep the worker handles alive until we've finished collecting results.
    drop(worker_a);
    drop(worker_b);
}
