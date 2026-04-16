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

//! A Vector sink that batches metric events into Arrow IPC record batches
//! and POSTs them to an HTTP endpoint.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Builder, StringDictionaryBuilder, UInt8Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema as ArrowSchema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use parquet::variant::{VariantArrayBuilder, VariantBuilderExt, VariantType};
use serde::{Deserialize, Serialize};
use tracing::{debug, error};
use vector::config::{AcknowledgementsConfig, GenerateConfig, Input, SinkConfig, SinkContext};
use vector::event::{EventArray, EventStatus, Finalizable, Metric};
use vector::sinks::Healthcheck;
use vector::sinks::util::StreamSink;
use vector_lib::configurable::NamedComponent;
use vector_lib::sink::VectorSink;

use crate::transforms::preprocess_metric::{
    TAG_DATACENTER, TAG_ENV, TAG_HOST, TAG_REGION, TAG_SERVICE,
};

/// Maximum number of metrics to accumulate before flushing a batch.
const DEFAULT_BATCH_SIZE: usize = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MetricType {
    /// Gauge metric - instantaneous value
    Gauge = 0,
    /// Sum metric - cumulative or delta sum
    Sum = 1,
    /// Histogram metric (not yet fully supported)
    Histogram = 2,
    /// Exponential histogram metric (not yet fully supported)
    ExponentialHistogram = 3,
    /// Summary metric (not yet fully supported)
    Summary = 4,
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

pub fn metrics_arrow_schema() -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new(
            "metric_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("timestamp_secs", DataType::UInt64, false),
        Field::new("start_timestamp_secs", DataType::UInt64, true),
        Field::new("value", DataType::Float64, false),
        Field::new(
            "tag_service",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "tag_env",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "tag_datacenter",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "tag_region",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "tag_host",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "attributes",
            DataType::Struct(Fields::from(vec![
                Field::new("metadata", DataType::BinaryView, false),
                Field::new("value", DataType::BinaryView, false),
            ])),
            true,
        )
        .with_extension_type(VariantType),
        Field::new(
            "service_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new(
            "resource_attributes",
            DataType::Struct(Fields::from(vec![
                Field::new("metadata", DataType::BinaryView, false),
                Field::new("value", DataType::BinaryView, false),
            ])),
            true,
        )
        .with_extension_type(VariantType),
    ])
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Serializes metric events as Arrow IPC and POSTs them to an HTTP endpoint.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArrowIpcMetricsSinkConfig {
    /// Target HTTP endpoint that receives Arrow IPC batches.
    pub uri: String,
    /// Maximum number of metrics per Arrow IPC batch.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default, skip_serializing_if = "vector_lib::serde::is_default")]
    pub acknowledgements: AcknowledgementsConfig,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl NamedComponent for ArrowIpcMetricsSinkConfig {
    fn get_component_name(&self) -> &'static str {
        "arrow_ipc_metrics"
    }
}

impl GenerateConfig for ArrowIpcMetricsSinkConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(r#"uri = "http://localhost:7280/api/v1/datadog/byoc/metrics""#)
            .expect("config should be valid")
    }
}

#[async_trait]
#[typetag::serde(name = "arrow_ipc_metrics")]
impl SinkConfig for ArrowIpcMetricsSinkConfig {
    async fn build(&self, _cx: SinkContext) -> vector::Result<(VectorSink, Healthcheck)> {
        let sink = ArrowIpcMetricsSink {
            uri: self.uri.clone(),
            batch_size: self.batch_size,
            client: reqwest::Client::new(),
        };
        let healthcheck = futures::future::ok(()).boxed();
        Ok((VectorSink::Stream(Box::new(sink)), healthcheck))
    }

    fn input(&self) -> Input {
        Input::metric()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

struct ArrowIpcMetricsSink {
    uri: String,
    batch_size: usize,
    client: reqwest::Client,
}

#[async_trait]
impl StreamSink<EventArray> for ArrowIpcMetricsSink {
    async fn run(mut self: Box<Self>, mut input: BoxStream<'_, EventArray>) -> Result<(), ()> {
        let schema = Arc::new(metrics_arrow_schema());
        let mut pending_metrics: Vec<Metric> = Vec::with_capacity(self.batch_size);
        let mut pending_finalizers = Vec::new();

        while let Some(mut events) = input.next().await {
            let finalizers = events.take_finalizers();

            if let EventArray::Metrics(metrics) = events {
                for metric in metrics {
                    pending_metrics.push(metric);
                }
            }
            pending_finalizers.push(finalizers);

            if pending_metrics.len() >= self.batch_size {
                let status = self.flush(&schema, &mut pending_metrics).await;
                for pending_finalizer in pending_finalizers.drain(..) {
                    pending_finalizer.update_status(status);
                }
            }
        }
        // Flush remaining.
        if !pending_metrics.is_empty() {
            let status = self.flush(&schema, &mut pending_metrics).await;
            for pending_finalizer in pending_finalizers.drain(..) {
                pending_finalizer.update_status(status);
            }
        }
        Ok(())
    }
}

impl ArrowIpcMetricsSink {
    async fn flush(&self, schema: &Arc<ArrowSchema>, metrics: &mut Vec<Metric>) -> EventStatus {
        let batch_size = metrics.len();

        match build_record_batch(schema, metrics) {
            Ok(ipc_bytes) => match self
                .client
                .post(&self.uri)
                .header("content-type", "application/vnd.apache.arrow.stream")
                .body(ipc_bytes)
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {
                    debug!(batch_size, "flushed arrow ipc metrics batch");
                    EventStatus::Delivered
                }
                Ok(resp) => {
                    error!(
                        batch_size,
                        status = %resp.status(),
                        "arrow ipc metrics endpoint returned error"
                    );
                    EventStatus::Errored
                }
                Err(error) => {
                    error!(batch_size, %error, "failed to send arrow ipc metrics batch");
                    EventStatus::Errored
                }
            },
            Err(error) => {
                error!(batch_size, %error, "failed to build arrow record batch");
                EventStatus::Errored
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Arrow conversion
// ---------------------------------------------------------------------------

/// Well-known tag keys that have dedicated dictionary columns.
const KNOWN_TAGS: &[&str] = &[TAG_SERVICE, TAG_ENV, TAG_DATACENTER, TAG_REGION, TAG_HOST];

fn build_record_batch(
    schema: &Arc<ArrowSchema>,
    metrics: &mut Vec<Metric>,
) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let batch_size = metrics.len();

    let mut metric_name = StringDictionaryBuilder::<Int32Type>::new();
    let mut metric_type_col = UInt8Builder::with_capacity(batch_size);
    let mut metric_unit = arrow::array::StringBuilder::with_capacity(batch_size, batch_size * 8);
    let mut timestamp_secs = UInt64Builder::with_capacity(batch_size);
    let mut start_timestamp_secs = UInt64Builder::with_capacity(batch_size);
    let mut value_col = Float64Builder::with_capacity(batch_size);
    let mut tag_service = StringDictionaryBuilder::<Int32Type>::new();
    let mut tag_env = StringDictionaryBuilder::<Int32Type>::new();
    let mut tag_datacenter = StringDictionaryBuilder::<Int32Type>::new();
    let mut tag_region = StringDictionaryBuilder::<Int32Type>::new();
    let mut tag_host = StringDictionaryBuilder::<Int32Type>::new();
    let mut attributes = VariantArrayBuilder::new(batch_size);
    let mut service_name = StringDictionaryBuilder::<Int32Type>::new();
    let mut resource_attributes = VariantArrayBuilder::new(batch_size);

    for metric in metrics.drain(..) {
        metric_name.append_value(metric.name());
        metric_type_col.append_value(encode_metric_type(&metric));
        // Vector metrics don't carry a unit field.
        metric_unit.append_null();
        timestamp_secs.append_value(
            metric
                .timestamp()
                .map(|ts| ts.timestamp() as u64)
                .unwrap_or(0),
        );
        // Vector doesn't expose a start timestamp on Metric.
        start_timestamp_secs.append_null();
        value_col.append_value(extract_scalar_value(&metric));

        // Extract well-known tags.
        let tags = metric.tags();
        append_dict_tag(&mut tag_service, tags.and_then(|t| t.get(TAG_SERVICE)));
        append_dict_tag(&mut tag_env, tags.and_then(|t| t.get(TAG_ENV)));
        append_dict_tag(
            &mut tag_datacenter,
            tags.and_then(|t| t.get(TAG_DATACENTER)),
        );
        append_dict_tag(&mut tag_region, tags.and_then(|t| t.get(TAG_REGION)));
        append_dict_tag(&mut tag_host, tags.and_then(|t| t.get(TAG_HOST)));

        // service_name mirrors tag_service (non-nullable, defaults to "").
        let svc = tags.and_then(|t| t.get(TAG_SERVICE)).unwrap_or("");
        service_name.append_value(svc);

        // Remaining tags → attributes variant (JSON object).
        build_extra_tags_variant(&mut attributes, &metric);

        // Resource attributes: OTel resource.* tags that weren't promoted
        // to standard columns.
        build_resource_attrs_variant(&mut resource_attributes, &metric);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(metric_name.finish()) as ArrayRef,
            Arc::new(metric_type_col.finish()),
            Arc::new(metric_unit.finish()),
            Arc::new(timestamp_secs.finish()),
            Arc::new(start_timestamp_secs.finish()),
            Arc::new(value_col.finish()),
            Arc::new(tag_service.finish()),
            Arc::new(tag_env.finish()),
            Arc::new(tag_datacenter.finish()),
            Arc::new(tag_region.finish()),
            Arc::new(tag_host.finish()),
            ArrayRef::from(attributes.build()),
            Arc::new(service_name.finish()),
            ArrayRef::from(resource_attributes.build()),
        ],
    )?;

    let mut buf = Vec::with_capacity(4096);
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    Ok(buf)
}

fn append_dict_tag(builder: &mut StringDictionaryBuilder<Int32Type>, value: Option<&str>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn encode_metric_type(metric: &Metric) -> u8 {
    use vector::event::MetricValue;
    let metric_type = match metric.value() {
        MetricValue::Counter { .. } => MetricType::Sum,
        MetricValue::Gauge { .. } => MetricType::Gauge,
        MetricValue::AggregatedHistogram { .. } => MetricType::Histogram,
        MetricValue::AggregatedSummary { .. } => MetricType::Summary,
        MetricValue::Distribution { .. } => MetricType::Histogram,
        MetricValue::Sketch { .. } => MetricType::Histogram,
        MetricValue::Set { .. } => MetricType::Sum,
    };
    metric_type as u8
}

fn extract_scalar_value(metric: &Metric) -> f64 {
    use vector::event::MetricValue;
    match metric.value() {
        MetricValue::Counter { value } | MetricValue::Gauge { value } => *value,
        MetricValue::Set { values } => values.len() as f64,
        MetricValue::Distribution { samples, .. } => {
            samples.iter().map(|s| s.value * s.rate as f64).sum()
        }
        MetricValue::AggregatedHistogram { sum, .. } => *sum,
        MetricValue::AggregatedSummary { sum, .. } => *sum,
        MetricValue::Sketch { .. } => 0.0,
    }
}

/// Serializes non-well-known, non-resource tags into a Variant JSON object.
fn build_extra_tags_variant(builder: &mut VariantArrayBuilder, metric: &Metric) {
    let Some(tags) = metric.tags() else {
        builder.append_null();
        return;
    };
    let extras: Vec<(&str, &str)> = tags
        .iter_single()
        .filter(|(k, _)| !KNOWN_TAGS.contains(k) && !k.starts_with("resource."))
        .collect();
    if extras.is_empty() {
        builder.append_null();
        return;
    }
    let mut obj = builder.new_object();
    for (key, value) in extras {
        obj.insert(key, value);
    }
    obj.finish();
}

/// Serializes remaining `resource.*` tags into a Variant JSON object.
fn build_resource_attrs_variant(builder: &mut VariantArrayBuilder, metric: &Metric) {
    let Some(tags) = metric.tags() else {
        builder.append_null();
        return;
    };
    let resource_tags: Vec<(&str, &str)> = tags
        .iter_single()
        .filter(|(k, _)| k.starts_with("resource."))
        .collect();
    if resource_tags.is_empty() {
        builder.append_null();
        return;
    }
    let mut obj = builder.new_object();
    for (key, value) in resource_tags {
        obj.insert(key, value);
    }
    obj.finish();
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use vector::event::{Metric, MetricKind, MetricTags, MetricValue};

    use super::*;

    #[test]
    fn test_build_record_batch_counters() {
        let schema = Arc::new(metrics_arrow_schema());
        let mut tags = MetricTags::default();
        tags.insert("service".to_string(), "web".to_string());
        tags.insert("env".to_string(), "prod".to_string());
        tags.insert("host".to_string(), "h1".to_string());
        tags.insert("extra_tag".to_string(), "extra_val".to_string());

        let mut metrics = vec![
            Metric::new(
                "requests",
                MetricKind::Incremental,
                MetricValue::Counter { value: 42.0 },
            )
            .with_tags(Some(tags)),
            Metric::new(
                "cpu",
                MetricKind::Absolute,
                MetricValue::Gauge { value: 0.85 },
            ),
        ];
        let ipc_bytes = build_record_batch(&schema, &mut metrics).expect("should build batch");
        assert!(metrics.is_empty());
        assert!(!ipc_bytes.is_empty());

        let cursor = std::io::Cursor::new(ipc_bytes);
        let reader =
            arrow::ipc::reader::StreamReader::try_new(cursor, None).expect("should parse IPC");
        let batches: Vec<_> = reader.into_iter().collect::<Result<_, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 14);
    }

    #[test]
    fn test_encode_metric_type() {
        let counter = Metric::new(
            "c",
            MetricKind::Incremental,
            MetricValue::Counter { value: 1.0 },
        );
        assert_eq!(encode_metric_type(&counter), MetricType::Sum as u8);

        let gauge = Metric::new("g", MetricKind::Absolute, MetricValue::Gauge { value: 1.0 });
        assert_eq!(encode_metric_type(&gauge), MetricType::Gauge as u8);

        let set = Metric::new(
            "s",
            MetricKind::Incremental,
            MetricValue::Set {
                values: ["a".into()].into(),
            },
        );
        assert_eq!(encode_metric_type(&set), MetricType::Sum as u8);
    }

    #[test]
    fn test_extract_scalar_value() {
        let counter = Metric::new(
            "c",
            MetricKind::Incremental,
            MetricValue::Counter { value: 5.0 },
        );
        assert_eq!(extract_scalar_value(&counter), 5.0);

        let gauge = Metric::new(
            "g",
            MetricKind::Absolute,
            MetricValue::Gauge { value: -1.5 },
        );
        assert_eq!(extract_scalar_value(&gauge), -1.5);
    }

    #[test]
    fn test_schema_matches_expected_column_count() {
        let schema = metrics_arrow_schema();
        assert_eq!(schema.fields().len(), 14);
    }
}
