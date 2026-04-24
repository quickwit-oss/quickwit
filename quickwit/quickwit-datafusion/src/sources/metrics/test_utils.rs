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

//! Composable test utilities for quickwit-datafusion.
//!
//! Builds batches with the OSS dynamic schema (no fixed 14-column schema):
//! `metric_name`, `metric_type`, `timestamp_secs`, `value`, `service` (optional).
//!
//! Column names use the OSS convention — bare names without `tag_` prefix.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, DictionaryArray, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, UInt8Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema, SchemaRef};
use async_trait::async_trait;
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStoreExt, PutPayload};
use quickwit_parquet_engine::split::{ParquetSplitId, ParquetSplitMetadata, TimeRange};
use quickwit_parquet_engine::storage::{ParquetWriter, ParquetWriterConfig};
use quickwit_parquet_engine::table_config::TableConfig;
use quickwit_parquet_engine::timeseries_id::compute_timeseries_id;

use super::predicate::MetricsSplitQuery;
use super::table_provider::MetricsSplitProvider;

// ── Schema helpers ──────────────────────────────────────────────────

fn dict_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

/// Build the OSS dynamic schema for a batch with a `service` column.
///
/// Schema: metric_name (dict), metric_type (u8), timestamp_secs (u64),
///         value (f64), service (dict, nullable).
pub fn oss_schema_with_service() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new("metric_name", dict_type(), false),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("timeseries_id", DataType::Int64, false),
        Field::new("service", dict_type(), true),
    ]))
}

// ── Batch builders ──────────────────────────────────────────────────

/// Build a RecordBatch with the OSS dynamic schema (4 required + service).
///
/// Column names use bare names (no `tag_` prefix): `service`, not `tag_service`.
pub fn make_batch(
    metric_name: &str,
    timestamps: &[u64],
    values: &[f64],
    service: Option<&str>,
) -> RecordBatch {
    let n = timestamps.len();
    assert_eq!(n, values.len());
    let mut tags = std::collections::HashMap::new();
    if let Some(service) = service {
        tags.insert("service".to_string(), service.to_string());
    }
    let timeseries_id = compute_timeseries_id(metric_name, 0, &tags);

    let cols: Vec<ArrayRef> = vec![
        make_dict(n, metric_name),
        Arc::new(UInt8Array::from(vec![0u8; n])),
        Arc::new(UInt64Array::from(timestamps.to_vec())),
        Arc::new(Float64Array::from(values.to_vec())),
        Arc::new(Int64Array::from(vec![timeseries_id; n])),
        make_nullable_dict(n, service),
    ];

    RecordBatch::try_new(oss_schema_with_service(), cols).unwrap()
}

/// Build a RecordBatch with multiple OSS-style tag columns.
///
/// Mirrors the production `build_record_batch` behavior: a tag column is only
/// included in the schema when its value is `Some(_)`.  Passing `None` for a
/// tag omits the column entirely — `None` does NOT produce an all-null column.
///
/// This matches what `metrics_ingest_api::build_record_batch` produces, ensuring
/// tests exercise the same dynamic schema that real ingestion emits.
#[allow(clippy::too_many_arguments)]
pub fn make_batch_with_tags(
    metric_name: &str,
    timestamps: &[u64],
    values: &[f64],
    service: Option<&str>,
    env: Option<&str>,
    datacenter: Option<&str>,
    region: Option<&str>,
    host: Option<&str>,
) -> RecordBatch {
    let n = timestamps.len();
    assert_eq!(n, values.len());

    let mut fields = vec![
        Field::new("metric_name", dict_type(), false),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("timeseries_id", DataType::Int64, false),
    ];
    let mut tags = std::collections::HashMap::new();
    for (name, val) in [
        ("service", service),
        ("env", env),
        ("datacenter", datacenter),
        ("region", region),
        ("host", host),
    ] {
        if let Some(v) = val {
            tags.insert(name.to_string(), v.to_string());
        }
    }
    let timeseries_id = compute_timeseries_id(metric_name, 0, &tags);
    let mut cols: Vec<ArrayRef> = vec![
        make_dict(n, metric_name),
        Arc::new(UInt8Array::from(vec![0u8; n])),
        Arc::new(UInt64Array::from(timestamps.to_vec())),
        Arc::new(Float64Array::from(values.to_vec())),
        Arc::new(Int64Array::from(vec![timeseries_id; n])),
    ];

    // Only emit a column when the value is Some — matching production behavior.
    for (name, val) in [
        ("service", service),
        ("env", env),
        ("datacenter", datacenter),
        ("region", region),
        ("host", host),
    ] {
        if let Some(v) = val {
            fields.push(Field::new(name, dict_type(), true));
            cols.push(make_nullable_dict(n, Some(v)));
        }
    }

    let schema = Arc::new(ArrowSchema::new(fields));
    RecordBatch::try_new(schema, cols).unwrap()
}

fn make_dict(n: usize, value: &str) -> ArrayRef {
    let keys = Int32Array::from(vec![0i32; n]);
    let vals = StringArray::from(vec![value]);
    Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(vals)).unwrap())
}

fn make_nullable_dict(n: usize, value: Option<&str>) -> ArrayRef {
    match value {
        Some(v) => {
            let keys = Int32Array::from(vec![Some(0i32); n]);
            let vals = StringArray::from(vec![v]);
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(vals)).unwrap())
        }
        None => {
            let keys = Int32Array::from(vec![None::<i32>; n]);
            let vals = StringArray::from(vec![None::<&str>]);
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(vals)).unwrap())
        }
    }
}

// ── Split provider ──────────────────────────────────────────────────

/// In-memory split provider that applies real pruning logic.
///
/// Uses OSS tag key names (bare, no `tag_` prefix) for `get_tag_values`.
#[derive(Debug, Clone)]
pub struct TestSplitProvider {
    pub splits: Vec<ParquetSplitMetadata>,
}

impl TestSplitProvider {
    pub fn new(splits: Vec<ParquetSplitMetadata>) -> Self {
        Self { splits }
    }

    pub fn count_matching(&self, query: &MetricsSplitQuery) -> usize {
        futures::executor::block_on(self.list_splits(query))
            .unwrap()
            .len()
    }
}

#[async_trait]
impl MetricsSplitProvider for TestSplitProvider {
    async fn list_splits(&self, query: &MetricsSplitQuery) -> DFResult<Vec<ParquetSplitMetadata>> {
        let mut result = self.splits.clone();

        if let Some(ref names) = query.metric_names {
            result.retain(|s| names.iter().any(|n| s.metric_names.contains(n)));
        }
        if let Some(start) = query.time_range_start {
            result.retain(|s| s.time_range.end_secs > start);
        }
        if let Some(end) = query.time_range_end {
            result.retain(|s| s.time_range.start_secs < end);
        }

        Ok(result)
    }
}

// ── Testbed ─────────────────────────────────────────────────────────

/// Composable testbed for metrics DataFusion tests.
///
/// Writes real parquet files via `ParquetWriter` to an in-memory object store.
pub struct MetricsTestbed {
    pub object_store: Arc<InMemory>,
    pub splits: Vec<ParquetSplitMetadata>,
    split_counter: usize,
}

impl MetricsTestbed {
    pub fn new() -> Self {
        Self {
            object_store: Arc::new(InMemory::new()),
            splits: Vec::new(),
            split_counter: 0,
        }
    }

    pub async fn add_split(&mut self, batch: &RecordBatch) -> ParquetSplitMetadata {
        self.split_counter += 1;
        let split_id = format!("split_{}", self.split_counter);
        let metadata = write_split(&self.object_store, batch, &split_id).await;
        self.splits.push(metadata.clone());
        metadata
    }

    pub async fn add(
        &mut self,
        metric_name: &str,
        timestamps: &[u64],
        values: &[f64],
        service: Option<&str>,
    ) -> ParquetSplitMetadata {
        let batch = make_batch(metric_name, timestamps, values, service);
        self.add_split(&batch).await
    }

    pub fn split_provider(&self) -> Arc<TestSplitProvider> {
        Arc::new(TestSplitProvider::new(self.splits.clone()))
    }
}

impl Default for MetricsTestbed {
    fn default() -> Self {
        Self::new()
    }
}

// ── Plan helpers ────────────────────────────────────────────────────

pub async fn physical_plan_str(ctx: &SessionContext, sql: &str) -> String {
    let df = ctx.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    format!(
        "{}",
        datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
    )
}

pub async fn physical_plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
    let df = ctx.sql(sql).await.unwrap();
    df.create_physical_plan().await.unwrap()
}

pub async fn execute(
    ctx: &SessionContext,
    sql: &str,
) -> Vec<datafusion::arrow::array::RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

pub fn total_rows(batches: &[datafusion::arrow::array::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// ── Internal ────────────────────────────────────────────────────────

async fn write_split(
    store: &InMemory,
    batch: &RecordBatch,
    split_id: &str,
) -> ParquetSplitMetadata {
    let config = ParquetWriterConfig::default();
    let writer = ParquetWriter::new(config, &TableConfig::default()).unwrap();

    let (parquet_bytes, _) = writer.write_to_bytes(batch, None).unwrap();
    let size_bytes = parquet_bytes.len() as u64;

    store
        .put(
            &ObjectPath::from(format!("{split_id}.parquet").as_str()),
            PutPayload::from(bytes::Bytes::from(parquet_bytes)),
        )
        .await
        .unwrap();

    // Extract timestamps by column name (no ParquetField enum in OSS)
    let schema = batch.schema();
    let ts_idx = schema.index_of("timestamp_secs").unwrap();
    let timestamps: Vec<u64> = batch
        .column(ts_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .iter()
        .flatten()
        .collect();
    let min_ts = *timestamps.iter().min().unwrap_or(&0);
    let max_ts = *timestamps.iter().max().unwrap_or(&0);

    // Extract metric names by column name
    let mn_idx = schema.index_of("metric_name").unwrap();
    let metric_col = batch.column(mn_idx);
    let dict = metric_col
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .unwrap();
    let values = dict
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut metric_names = HashSet::new();
    for i in 0..values.len() {
        if !values.is_null(i) {
            metric_names.insert(values.value(i).to_string());
        }
    }

    let mut builder = ParquetSplitMetadata::metrics_builder()
        .split_id(ParquetSplitId::new(split_id))
        .index_uid("test-index:00000000000000000000000000")
        .time_range(TimeRange::new(min_ts, max_ts + 1))
        .num_rows(batch.num_rows() as u64)
        .size_bytes(size_bytes);
    for name in &metric_names {
        builder = builder.add_metric_name(name.clone());
    }
    builder.build()
}
