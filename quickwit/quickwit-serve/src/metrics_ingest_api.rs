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

//! REST endpoint for ingesting metrics as parquet splits.
//!
//! `POST /api/v1/{index_id}/ingest-metrics` accepts a JSON array of metric
//! data points, converts them to a `RecordBatch` with the OSS dynamic schema,
//! writes a parquet file via `ParquetWriter`, and stages+publishes the split
//! through the metastore.
//!
//! OSS schema uses bare column names without `tag_` prefix:
//!   `service`, `env`, `datacenter`, `region`, `host`

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, DictionaryArray, Float64Array, Int32Array, RecordBatch, StringArray, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema};
use quickwit_metastore::{IndexMetadataResponseExt, StageMetricsSplitsRequestExt};
use quickwit_parquet_engine::schema::ParquetSchema;
use quickwit_parquet_engine::split::{MetricsSplitMetadata, SplitId, TimeRange};
use quickwit_parquet_engine::storage::{ParquetWriter, ParquetWriterConfig};
use quickwit_proto::metastore::{
    IndexMetadataRequest, MetastoreService, MetastoreServiceClient, PublishMetricsSplitsRequest,
    StageMetricsSplitsRequest,
};
use quickwit_storage::StorageResolver;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use warp::Filter;

use crate::with_arg;

/// A single metric data point (OSS schema: bare tag column names).
#[derive(Deserialize)]
pub struct MetricDataPoint {
    pub metric_name: String,
    #[serde(default)]
    pub metric_type: Option<u8>,
    pub timestamp_secs: u64,
    pub value: f64,
    /// Bare column name — not `tag_service`
    #[serde(default)]
    pub service: Option<String>,
    /// Bare column name — not `tag_env`
    #[serde(default)]
    pub env: Option<String>,
    /// Bare column name — not `tag_datacenter`
    #[serde(default)]
    pub datacenter: Option<String>,
    /// Bare column name — not `tag_region`
    #[serde(default)]
    pub region: Option<String>,
    /// Bare column name — not `tag_host`
    #[serde(default)]
    pub host: Option<String>,
}

#[derive(Serialize)]
pub struct MetricsIngestResponse {
    pub num_metrics_ingested: usize,
    pub split_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum MetricsIngestError {
    #[error("failed to parse request body: {0}")]
    Parse(String),
    #[error("empty request body")]
    Empty,
    #[error("index error: {0}")]
    Index(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("metastore error: {0}")]
    Metastore(String),
    #[error("internal error: {0}")]
    Internal(String),
}

impl warp::reject::Reject for MetricsIngestError {}

impl MetricsIngestError {
    fn status_code(&self) -> hyper::StatusCode {
        match self {
            Self::Parse(_) | Self::Empty => hyper::StatusCode::BAD_REQUEST,
            _ => hyper::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl Serialize for MetricsIngestError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

pub(crate) fn metrics_ingest_handler(
    metastore_client: MetastoreServiceClient,
    storage_resolver: StorageResolver,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!(String / "ingest-metrics")
        .and(warp::post())
        .and(warp::body::bytes())
        .and(with_arg(metastore_client))
        .and(with_arg(storage_resolver))
        .then(
            |index_id: String,
             body: bytes::Bytes,
             metastore: MetastoreServiceClient,
             storage_resolver: StorageResolver| async move {
                match ingest_metrics(index_id, body, metastore, storage_resolver).await {
                    Ok(response) => warp::reply::with_status(
                        warp::reply::json(&response),
                        hyper::StatusCode::OK,
                    ),
                    Err(err) => warp::reply::with_status(
                        warp::reply::json(&serde_json::json!({"error": err.to_string()})),
                        err.status_code(),
                    ),
                }
            },
        )
        .boxed()
}

async fn ingest_metrics(
    index_id: String,
    body: bytes::Bytes,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
) -> Result<MetricsIngestResponse, MetricsIngestError> {
    let data_points: Vec<MetricDataPoint> =
        serde_json::from_slice(&body).map_err(|err| MetricsIngestError::Parse(err.to_string()))?;

    if data_points.is_empty() {
        return Err(MetricsIngestError::Empty);
    }

    let num_points = data_points.len();
    debug!(index_id, num_points, "ingesting metrics");

    let request = IndexMetadataRequest::for_index_id(index_id.clone());
    let response = metastore
        .clone()
        .index_metadata(request)
        .await
        .map_err(|err| MetricsIngestError::Index(err.to_string()))?;

    let index_metadata = response
        .deserialize_index_metadata()
        .map_err(|err| MetricsIngestError::Index(err.to_string()))?;

    let index_uid = index_metadata.index_uid.clone();
    let index_uri = &index_metadata.index_config.index_uri;

    let storage = storage_resolver
        .resolve(index_uri)
        .await
        .map_err(|err| MetricsIngestError::Storage(err.to_string()))?;

    // Build RecordBatch with the OSS dynamic schema
    let batch = build_record_batch(&data_points)
        .map_err(|err| MetricsIngestError::Storage(err.to_string()))?;

    let writer = ParquetWriter::new(ParquetSchema::new(), ParquetWriterConfig::default());
    let parquet_bytes = writer
        .write_to_bytes(&batch)
        .map_err(|err| MetricsIngestError::Storage(err.to_string()))?;

    let size_bytes = parquet_bytes.len() as u64;
    let split_id = format!(
        "metrics_{}",
        quickwit_proto::types::Ulid::new()
            .to_string()
            .to_lowercase()
    );
    let parquet_filename = format!("{split_id}.parquet");

    storage
        .put(Path::new(&parquet_filename), Box::new(parquet_bytes))
        .await
        .map_err(|err| MetricsIngestError::Storage(err.to_string()))?;

    let mut metadata =
        build_split_metadata(&data_points, &split_id, &index_uid.to_string(), size_bytes);
    metadata.finalize_tag_cardinality();

    let stage_request =
        StageMetricsSplitsRequest::try_from_splits_metadata(index_uid.clone(), &[metadata])
            .map_err(|err| MetricsIngestError::Metastore(err.to_string()))?;
    metastore
        .clone()
        .stage_metrics_splits(stage_request)
        .await
        .map_err(|err| MetricsIngestError::Metastore(err.to_string()))?;

    metastore
        .clone()
        .publish_metrics_splits(PublishMetricsSplitsRequest {
            index_uid: Some(index_uid.into()),
            staged_split_ids: vec![split_id.clone()],
            replaced_split_ids: vec![],
            index_checkpoint_delta_json_opt: None,
            publish_token_opt: None,
        })
        .await
        .map_err(|err| MetricsIngestError::Metastore(err.to_string()))?;

    info!(index_id, split_id, num_points, "metrics ingested");

    Ok(MetricsIngestResponse {
        num_metrics_ingested: num_points,
        split_id,
    })
}

/// Build a `RecordBatch` using the OSS dynamic schema.
///
/// Only columns that have at least one non-null value are included beyond
/// the 4 required fields. All tag columns use bare names (no `tag_` prefix).
fn build_record_batch(
    data_points: &[MetricDataPoint],
) -> Result<RecordBatch, MetricsIngestError> {
    let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

    let metric_names: Vec<&str> = data_points.iter().map(|d| d.metric_name.as_str()).collect();
    let metric_types: Vec<u8> = data_points
        .iter()
        .map(|d| d.metric_type.unwrap_or(0))
        .collect();
    let timestamps: Vec<u64> = data_points.iter().map(|d| d.timestamp_secs).collect();
    let values: Vec<f64> = data_points.iter().map(|d| d.value).collect();

    let mut fields = vec![
        Field::new("metric_name", dict_type.clone(), false),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
    ];
    let mut cols: Vec<ArrayRef> = vec![
        make_dict_from_strs(&metric_names)?,
        Arc::new(UInt8Array::from(metric_types)),
        Arc::new(UInt64Array::from(timestamps)),
        Arc::new(Float64Array::from(values)),
    ];

    // Add optional tag columns only if any value is present
    let service_vals: Vec<Option<&str>> =
        data_points.iter().map(|d| d.service.as_deref()).collect();
    if service_vals.iter().any(|v| v.is_some()) {
        fields.push(Field::new("service", dict_type.clone(), true));
        cols.push(make_nullable_dict_from_opts(&service_vals)?);
    }
    let env_vals: Vec<Option<&str>> = data_points.iter().map(|d| d.env.as_deref()).collect();
    if env_vals.iter().any(|v| v.is_some()) {
        fields.push(Field::new("env", dict_type.clone(), true));
        cols.push(make_nullable_dict_from_opts(&env_vals)?);
    }
    let dc_vals: Vec<Option<&str>> =
        data_points.iter().map(|d| d.datacenter.as_deref()).collect();
    if dc_vals.iter().any(|v| v.is_some()) {
        fields.push(Field::new("datacenter", dict_type.clone(), true));
        cols.push(make_nullable_dict_from_opts(&dc_vals)?);
    }
    let region_vals: Vec<Option<&str>> =
        data_points.iter().map(|d| d.region.as_deref()).collect();
    if region_vals.iter().any(|v| v.is_some()) {
        fields.push(Field::new("region", dict_type.clone(), true));
        cols.push(make_nullable_dict_from_opts(&region_vals)?);
    }
    let host_vals: Vec<Option<&str>> = data_points.iter().map(|d| d.host.as_deref()).collect();
    if host_vals.iter().any(|v| v.is_some()) {
        fields.push(Field::new("host", dict_type.clone(), true));
        cols.push(make_nullable_dict_from_opts(&host_vals)?);
    }

    let schema = Arc::new(ArrowSchema::new(fields));
    RecordBatch::try_new(schema, cols)
        .map_err(|e| MetricsIngestError::Internal(format!("column/schema mismatch: {e}")))
}

fn build_split_metadata(
    data_points: &[MetricDataPoint],
    split_id: &str,
    index_uid: &str,
    size_bytes: u64,
) -> MetricsSplitMetadata {
    let (min_ts, max_ts, metric_names) = data_points.iter().fold(
        (u64::MAX, 0u64, HashSet::new()),
        |(min, max, mut names), d| {
            names.insert(d.metric_name.clone());
            (min.min(d.timestamp_secs), max.max(d.timestamp_secs), names)
        },
    );
    let min_ts = if min_ts == u64::MAX { 0 } else { min_ts };

    let mut builder = MetricsSplitMetadata::builder()
        .split_id(SplitId::new(split_id))
        .index_uid(index_uid)
        .time_range(TimeRange::new(min_ts, max_ts + 1))
        .num_rows(data_points.len() as u64)
        .size_bytes(size_bytes);

    for name in &metric_names {
        builder = builder.add_metric_name(name.clone());
    }

    // Store tag values using bare key names (OSS convention)
    for dp in data_points {
        if let Some(ref v) = dp.service {
            builder = builder.add_low_cardinality_tag("service", v.clone());
        }
        if let Some(ref v) = dp.env {
            builder = builder.add_low_cardinality_tag("env", v.clone());
        }
        if let Some(ref v) = dp.datacenter {
            builder = builder.add_low_cardinality_tag("datacenter", v.clone());
        }
        if let Some(ref v) = dp.region {
            builder = builder.add_low_cardinality_tag("region", v.clone());
        }
        if let Some(ref v) = dp.host {
            builder = builder.add_low_cardinality_tag("host", v.clone());
        }
    }

    builder.build()
}

// ── Arrow helpers ───────────────────────────────────────────────────

fn make_dict_from_strs(values: &[&str]) -> Result<ArrayRef, MetricsIngestError> {
    let unique: Vec<&str> = {
        let mut seen = HashSet::new();
        values
            .iter()
            .copied()
            .filter(|v| seen.insert(*v))
            .collect()
    };
    let val_to_idx: std::collections::HashMap<&str, i32> = unique
        .iter()
        .enumerate()
        .map(|(i, v)| (*v, i as i32))
        .collect();
    let keys: Vec<i32> = values.iter().map(|v| val_to_idx[v]).collect();
    let dict_values = StringArray::from(unique);
    let array =
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(keys), Arc::new(dict_values))
            .map_err(|err| MetricsIngestError::Internal(err.to_string()))?;
    Ok(Arc::new(array))
}

fn make_nullable_dict_from_opts(
    values: &[Option<&str>],
) -> Result<ArrayRef, MetricsIngestError> {
    let unique: Vec<&str> = {
        let mut seen = HashSet::new();
        values
            .iter()
            .filter_map(|v| *v)
            .filter(|v| seen.insert(*v))
            .collect()
    };
    let val_to_idx: std::collections::HashMap<&str, i32> = unique
        .iter()
        .enumerate()
        .map(|(i, v)| (*v, i as i32))
        .collect();
    let keys: Vec<Option<i32>> = values.iter().map(|v| v.map(|s| val_to_idx[s])).collect();
    let dict_values = StringArray::from(unique);
    let array =
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(keys), Arc::new(dict_values))
            .map_err(|err| MetricsIngestError::Internal(err.to_string()))?;
    Ok(Arc::new(array))
}
