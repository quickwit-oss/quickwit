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

//! Integration tests for DDSketch DataFusion queries.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Float64Array, RecordBatch};
use datafusion::arrow;
use quickwit_datafusion::service::split_sql_statements;
use quickwit_datafusion::sources::metrics::MetricsDataSource;
use quickwit_datafusion::{DataFusionSessionBuilder, QuickwitObjectStoreRegistry};
use quickwit_parquet_engine::ingest::{ArrowSketchBatchBuilder, SketchDataPoint};

mod common;
mod sketch_splits;

use common::{TestSandbox, create_metrics_index};
use sketch_splits::publish_sketch_split;

async fn start_sandbox() -> TestSandbox {
    quickwit_common::setup_logging_for_tests();
    TestSandbox::start().await
}

fn session_builder(sandbox: &TestSandbox) -> DataFusionSessionBuilder {
    let source = Arc::new(MetricsDataSource::new(sandbox.metastore.clone()));
    let schema_source = Arc::clone(&source);
    let registry = Arc::new(QuickwitObjectStoreRegistry::new(
        sandbox.storage_resolver.clone(),
    ));
    DataFusionSessionBuilder::new()
        .with_object_store_registry(registry)
        .expect("install object store registry")
        .with_runtime_plugin(Arc::clone(&source) as Arc<_>)
        .with_substrait_consumer(source as Arc<_>)
        .with_schema_provider_factory("quickwit", "public", move || {
            schema_source.schema_provider()
        })
}

async fn run_sql(builder: &DataFusionSessionBuilder, sql: &str) -> Vec<RecordBatch> {
    let ctx = builder.build_session().unwrap();
    let mut statements = split_sql_statements(&ctx, sql).unwrap();
    let last = statements.pop().unwrap();
    for statement in statements {
        ctx.sql(&statement).await.unwrap().collect().await.unwrap();
    }
    ctx.sql(&last).await.unwrap().collect().await.unwrap()
}

fn make_doc_example_batch() -> RecordBatch {
    let mut builder = ArrowSketchBatchBuilder::with_capacity(3);
    for (count, sum, max, keys, counts, host) in [
        (4, 50.0, 20.0, vec![1338, 1400], vec![3, 1], "a"),
        (3, 45.0, 18.0, vec![1338, 1450], vec![2, 1], "a"),
        (5, 70.0, 22.0, vec![1338, 1500], vec![4, 1], "b"),
    ] {
        let tags = HashMap::from([
            ("service".to_string(), "api".to_string()),
            ("host".to_string(), host.to_string()),
            ("region".to_string(), "us".to_string()),
        ]);
        builder.append(SketchDataPoint {
            metric_name: "req.latency".to_string(),
            timestamp_secs: 600,
            count,
            sum,
            min: 0.5,
            max,
            flags: 0,
            keys,
            counts,
            tags,
        });
    }
    builder.finish()
}

fn float_value(batch: &RecordBatch, name: &str) -> f64 {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0)
}

fn display_value(batch: &RecordBatch, name: &str) -> String {
    let col = batch.column_by_name(name).unwrap();
    arrow::util::display::array_value_to_string(col, 0).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sketch_merge_and_quantile_sql() {
    let sandbox = start_sandbox().await;
    let metastore = sandbox.metastore.clone();
    let data_dir = &sandbox.data_dir;
    let builder = session_builder(&sandbox);

    let index_uid = create_metrics_index(&metastore, "sketches-latency", data_dir.path()).await;
    let batch = make_doc_example_batch();
    publish_sketch_split(
        &metastore,
        &index_uid,
        data_dir.path(),
        "sketch_split_1",
        &batch,
    )
    .await;

    let sql = r#"
        SELECT
            SUM("count") AS total_count,
            SUM("sum") AS total_sum,
            MIN("min") AS global_min,
            MAX("max") AS global_max,
            dd_quantile(dd_sketch(keys, counts, "count", "min", "max", flags), 0.50) AS p50,
            dd_quantile(dd_sketch(keys, counts, "count", "min", "max", flags), 0.99) AS p99
        FROM "sketches-latency"
        WHERE metric_name = 'req.latency' AND timestamp_secs = 600
    "#;
    let batches = run_sql(&builder, sql).await;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    assert_eq!(display_value(&batches[0], "total_count"), "12");
    assert_eq!(float_value(&batches[0], "total_sum"), 165.0);
    assert_eq!(float_value(&batches[0], "global_min"), 0.5);
    assert_eq!(float_value(&batches[0], "global_max"), 22.0);

    let p50 = float_value(&batches[0], "p50");
    let p99 = float_value(&batches[0], "p99");
    assert!((p50 - 1.0).abs() < f64::EPSILON, "p50={p50}");
    assert!((p99 - 5.677260965422914).abs() < 1e-12, "p99={p99}");

    let multiple_sketch_states_sql = r#"
        SELECT
            dd_quantile(dd_sketch(keys, counts, "count", "min", "max", flags), 0.99) AS p99_all,
            dd_quantile(
                dd_sketch(keys, counts, "count", "min", "max", flags)
                    FILTER (WHERE "max" <= 20.0),
                0.99
            ) AS p99_filtered
        FROM "sketches-latency"
        WHERE metric_name = 'req.latency' AND timestamp_secs = 600
    "#;
    let multiple_state_batches = run_sql(&builder, multiple_sketch_states_sql).await;
    assert_eq!(multiple_state_batches.len(), 1);
    assert_eq!(multiple_state_batches[0].num_rows(), 1);
    let p99_all = float_value(&multiple_state_batches[0], "p99_all");
    let p99_filtered = float_value(&multiple_state_batches[0], "p99_filtered");
    assert!(
        (p99_all - 5.677260965422914).abs() < 1e-12,
        "p99_all={p99_all}"
    );
    assert!(
        (p99_filtered - 2.614988148096247).abs() < 1e-12,
        "p99_filtered={p99_filtered}"
    );

    let ddl_sql = r#"
        CREATE EXTERNAL TABLE "sketches_explicit" (
            metric_name VARCHAR NOT NULL,
            timestamp_secs BIGINT UNSIGNED NOT NULL,
            count BIGINT UNSIGNED NOT NULL,
            sum DOUBLE NOT NULL,
            min DOUBLE NOT NULL,
            max DOUBLE NOT NULL,
            flags INT UNSIGNED NOT NULL,
            keys ARRAY<SMALLINT> NOT NULL,
            counts ARRAY<BIGINT UNSIGNED> NOT NULL
        ) STORED AS sketches LOCATION 'sketches-latency';

        SELECT
            dd_quantile(dd_sketch(keys, counts, "count", "min", "max", flags), 0.50) AS p50
        FROM "sketches_explicit"
        WHERE metric_name = 'req.latency' AND timestamp_secs = 600
    "#;
    let ddl_batches = run_sql(&builder, ddl_sql).await;
    assert_eq!(ddl_batches.len(), 1);
    assert_eq!(ddl_batches[0].num_rows(), 1);
    let ddl_p50 = float_value(&ddl_batches[0], "p50");
    assert!((ddl_p50 - 1.0).abs() < f64::EPSILON, "p50={ddl_p50}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sketch_merge_and_quantile_substrait() {
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use datafusion_substrait::substrait::proto::extensions::simple_extension_declaration::MappingType;
    use prost::Message;

    let sandbox = start_sandbox().await;
    let metastore = sandbox.metastore.clone();
    let data_dir = &sandbox.data_dir;
    let builder = session_builder(&sandbox);

    let index_uid = create_metrics_index(&metastore, "datadog-sketches", data_dir.path()).await;
    let batch = make_doc_example_batch();
    publish_sketch_split(
        &metastore,
        &index_uid,
        data_dir.path(),
        "sketch_split_1",
        &batch,
    )
    .await;

    let ctx = builder.build_session().unwrap();
    ctx.sql(
        r#"
        CREATE OR REPLACE EXTERNAL TABLE "datadog-sketches" (
            metric_name VARCHAR NOT NULL,
            timestamp_secs BIGINT UNSIGNED NOT NULL,
            count BIGINT UNSIGNED NOT NULL,
            sum DOUBLE NOT NULL,
            min DOUBLE NOT NULL,
            max DOUBLE NOT NULL,
            flags INT UNSIGNED NOT NULL,
            keys ARRAY<SMALLINT> NOT NULL,
            counts ARRAY<BIGINT UNSIGNED> NOT NULL
        ) STORED AS sketches LOCATION 'datadog-sketches'
        "#,
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    let df = ctx
        .sql(
            r#"
            SELECT
                dd_quantile(dd_sketch(keys, counts, "count", "min", "max", flags), 0.50) AS p50
            FROM "datadog-sketches"
            WHERE metric_name = 'req.latency' AND timestamp_secs = 600
            "#,
        )
        .await
        .unwrap();
    let plan = df.into_optimized_plan().unwrap();
    let substrait_plan = to_substrait_plan(&plan, &ctx.state()).unwrap();
    let extension_functions: Vec<&str> = substrait_plan
        .extensions
        .iter()
        .filter_map(|extension| match &extension.mapping_type {
            Some(MappingType::ExtensionFunction(function)) => Some(function.name.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        extension_functions.contains(&"dd_sketch"),
        "Substrait plan must declare dd_sketch aggregate UDF; got {extension_functions:?}"
    );
    assert!(
        extension_functions.contains(&"dd_quantile"),
        "Substrait plan must declare dd_quantile scalar UDF; got {extension_functions:?}"
    );

    let plan_bytes = substrait_plan.encode_to_vec();
    let stream = builder.execute_substrait(&plan_bytes).await.unwrap();
    let batches = datafusion::physical_plan::common::collect(stream)
        .await
        .unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    let p50 = float_value(&batches[0], "p50");
    assert!((p50 - 1.0).abs() < f64::EPSILON, "p50={p50}");
}
