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

//! Verifies that columns declared in the DDL schema but absent from a specific
//! parquet file are filled with NULLs by DataFusion's `PhysicalExprAdapterFactory`.
//!
//! Single-node test: no workers, no `SearcherPool` — one parquet file has the
//! tag columns, the other doesn't, and `COUNT(col)` surfaces the difference.

use std::sync::Arc;

use arrow::array::{
    DictionaryArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt8Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema};
use quickwit_datafusion::service::split_sql_statements;
use quickwit_datafusion::sources::metrics::MetricsDataSource;
use quickwit_datafusion::test_utils::make_batch_with_tags;
use quickwit_datafusion::{DataFusionSessionBuilder, QuickwitObjectStoreRegistry};
use quickwit_parquet_engine::timeseries_id::compute_timeseries_id;

mod common;

use common::{TestSandbox, create_metrics_index, publish_split};

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

async fn run_sql(
    builder: &DataFusionSessionBuilder,
    sql: &str,
) -> Vec<datafusion::arrow::array::RecordBatch> {
    let ctx = builder.build_session().unwrap();
    let mut statements = split_sql_statements(&ctx, sql).unwrap();
    let last = statements.pop().unwrap();
    for statement in statements {
        ctx.sql(&statement).await.unwrap().collect().await.unwrap();
    }
    ctx.sql(&last).await.unwrap().collect().await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_null_columns_for_missing_parquet_fields() {
    quickwit_common::setup_logging_for_tests();

    let sandbox = TestSandbox::start().await;
    let metastore = sandbox.metastore.clone();
    let data_dir = &sandbox.data_dir;

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

    let source = Arc::new(MetricsDataSource::new(metastore));
    let schema_source = Arc::clone(&source);
    let registry = Arc::new(QuickwitObjectStoreRegistry::new(
        sandbox.storage_resolver.clone(),
    ));
    let builder = DataFusionSessionBuilder::new()
        .with_object_store_registry(registry)
        .expect("install object store registry")
        .with_runtime_plugin(Arc::clone(&source) as Arc<_>)
        .with_substrait_consumer(source as Arc<_>)
        .with_schema_provider_factory("quickwit", "public", move || {
            schema_source.schema_provider()
        });

    // COUNT(col) counts non-NULL values — tests the NULL-fill behavior.
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
}
