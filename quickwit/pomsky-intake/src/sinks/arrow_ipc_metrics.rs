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

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use quickwit_opentelemetry::otlp::{
    ArrowIpcError, ArrowMetricsBatchBuilder, MetricDataPoint, MetricType, record_batch_to_ipc,
};
use quickwit_parquet_engine::schema::REQUIRED_FIELDS;
use serde::{Deserialize, Serialize};
use tracing::{debug, error};
use vector::config::{AcknowledgementsConfig, GenerateConfig, Input, SinkConfig, SinkContext};
use vector::event::{EventArray, EventStatus, Finalizable, Metric};
use vector::sinks::Healthcheck;
use vector::sinks::util::StreamSink;
use vector_lib::configurable::NamedComponent;
use vector_lib::sink::VectorSink;

/// Maximum number of metrics to accumulate before flushing a batch.
const DEFAULT_BATCH_SIZE: usize = 1_000;

/// Default time-based flush interval in seconds.
const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 1;

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
    /// Maximum seconds to hold metrics before flushing, even if batch is not full.
    #[serde(default = "default_flush_interval_secs")]
    pub flush_interval_secs: u64,
    #[serde(default, skip_serializing_if = "vector_lib::serde::is_default")]
    pub acknowledgements: AcknowledgementsConfig,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_flush_interval_secs() -> u64 {
    DEFAULT_FLUSH_INTERVAL_SECS
}

impl NamedComponent for ArrowIpcMetricsSinkConfig {
    fn get_component_name(&self) -> &'static str {
        "arrow_ipc_metrics"
    }
}

impl GenerateConfig for ArrowIpcMetricsSinkConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(r#"uri = "http://localhost:7280/api/datadog/v1/byoc/metrics""#)
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
            flush_interval: Duration::from_secs(self.flush_interval_secs),
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
    flush_interval: Duration,
    client: reqwest::Client,
}

#[async_trait]
impl StreamSink<EventArray> for ArrowIpcMetricsSink {
    async fn run(mut self: Box<Self>, mut input: BoxStream<'_, EventArray>) -> Result<(), ()> {
        let mut pending_metrics: Vec<Metric> = Vec::with_capacity(self.batch_size);
        let mut pending_finalizers = Vec::new();
        let mut flush_timer = tokio::time::interval(self.flush_interval);

        loop {
            tokio::select! {
                biased;

                maybe_events = input.next() => {
                    let Some(mut events) = maybe_events else {
                        break;
                    };
                    let finalizers = events.take_finalizers();

                    if let EventArray::Metrics(metrics) = events {
                        for metric in metrics {
                            pending_metrics.push(metric);
                        }
                    }
                    pending_finalizers.push(finalizers);

                    if pending_metrics.len() >= self.batch_size {
                        let status = self.flush(&mut pending_metrics).await;
                        for pending_finalizer in pending_finalizers.drain(..) {
                            pending_finalizer.update_status(status);
                        }
                        flush_timer.reset();
                    }
                }

                _ = flush_timer.tick() => {
                    if !pending_metrics.is_empty() {
                        let status = self.flush(&mut pending_metrics).await;
                        for pending_finalizer in pending_finalizers.drain(..) {
                            pending_finalizer.update_status(status);
                        }
                    }
                }
            }
        }
        // Flush remaining on shutdown.
        if !pending_metrics.is_empty() {
            let status = self.flush(&mut pending_metrics).await;
            for pending_finalizer in pending_finalizers.drain(..) {
                pending_finalizer.update_status(status);
            }
        }
        Ok(())
    }
}

impl ArrowIpcMetricsSink {
    async fn flush(&self, metrics: &mut Vec<Metric>) -> EventStatus {
        let batch_size = metrics.len();

        match build_ipc_bytes(metrics) {
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
                error!(batch_size, %error, "failed to build arrow ipc batch");
                EventStatus::Errored
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Arrow conversion
// ---------------------------------------------------------------------------

fn vector_metric_to_data_point(metric: &Metric) -> MetricDataPoint {
    // TODO: this will silently drop tags sent by customers that are in REQUIRED_FIELDS.
    let tags: HashMap<String, String> = match metric.tags() {
        Some(tags) => tags
            .iter_single()
            .filter(|(k, _)| !REQUIRED_FIELDS.contains(k))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        None => HashMap::new(),
    };

    MetricDataPoint {
        metric_name: metric.name().to_string(),
        metric_type: encode_metric_type(metric),
        timestamp_secs: metric
            .timestamp()
            .map(|ts| ts.timestamp() as u64)
            .unwrap_or(0),
        value: extract_scalar_value(metric),
        tags,
    }
}

fn build_ipc_bytes(metrics: &mut Vec<Metric>) -> Result<Vec<u8>, ArrowIpcError> {
    let mut builder = ArrowMetricsBatchBuilder::with_capacity(metrics.len());
    for metric in metrics.drain(..) {
        builder.append(vector_metric_to_data_point(&metric));
    }
    let record_batch = builder.finish();
    record_batch_to_ipc(&record_batch)
}

fn encode_metric_type(metric: &Metric) -> MetricType {
    use vector::event::MetricValue;
    match metric.value() {
        MetricValue::Counter { .. } => MetricType::Sum,
        MetricValue::Gauge { .. } => MetricType::Gauge,
        MetricValue::AggregatedHistogram { .. } => MetricType::Histogram,
        MetricValue::AggregatedSummary { .. } => MetricType::Summary,
        MetricValue::Distribution { .. } => MetricType::Histogram,
        MetricValue::Sketch { .. } => MetricType::Histogram,
        MetricValue::Set { .. } => MetricType::Sum,
    }
}

fn extract_scalar_value(metric: &Metric) -> f64 {
    use vector::event::MetricValue;
    match metric.value() {
        MetricValue::Counter { value } | MetricValue::Gauge { value } => *value,
        MetricValue::Set { values } => values.len() as f64,
        MetricValue::Distribution { samples, .. } => samples
            .iter()
            .map(|sample| sample.value * sample.rate as f64)
            .sum(),
        MetricValue::AggregatedHistogram { sum, .. } => *sum,
        MetricValue::AggregatedSummary { sum, .. } => *sum,
        MetricValue::Sketch { .. } => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use arrow::ipc::reader::StreamReader;
    use vector::event::{Metric, MetricKind, MetricTags, MetricValue};

    use super::*;

    #[test]
    fn test_build_ipc_bytes_round_trip() {
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
        let ipc_bytes = build_ipc_bytes(&mut metrics).expect("should build ipc bytes");
        assert!(metrics.is_empty());
        assert!(!ipc_bytes.is_empty());

        let cursor = std::io::Cursor::new(ipc_bytes);
        let reader = StreamReader::try_new(cursor, None).expect("should parse IPC");
        let batches: Vec<_> = reader.into_iter().collect::<Result<_, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);

        // 5 fixed columns + 4 tag columns (env, extra_tag, host, service)
        let schema = batches[0].schema();
        assert_eq!(schema.fields().len(), 9);
    }

    #[test]
    fn test_tag_columns_use_bare_names() {
        let mut tags = MetricTags::default();
        tags.insert("service".to_string(), "api".to_string());
        tags.insert("env".to_string(), "staging".to_string());

        let mut metrics = vec![
            Metric::new(
                "latency",
                MetricKind::Absolute,
                MetricValue::Gauge { value: 1.5 },
            )
            .with_tags(Some(tags)),
        ];

        let ipc_bytes = build_ipc_bytes(&mut metrics).expect("should build ipc bytes");
        let cursor = std::io::Cursor::new(ipc_bytes);
        let reader = StreamReader::try_new(cursor, None).expect("should parse IPC");
        let batch = reader.into_iter().next().unwrap().unwrap();

        let schema = batch.schema();
        let field_names: Vec<&str> = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();

        // Bare names, not tag_service / tag_env.
        assert!(field_names.contains(&"service"));
        assert!(field_names.contains(&"env"));
        assert!(!field_names.contains(&"tag_service"));
        assert!(!field_names.contains(&"tag_env"));

        // Required columns present.
        assert!(field_names.contains(&"metric_name"));
        assert!(field_names.contains(&"metric_type"));
        assert!(field_names.contains(&"timestamp_secs"));
        assert!(field_names.contains(&"value"));
    }

    #[test]
    fn test_encode_metric_type() {
        let counter = Metric::new(
            "c",
            MetricKind::Incremental,
            MetricValue::Counter { value: 1.0 },
        );
        assert_eq!(encode_metric_type(&counter), MetricType::Sum);

        let gauge = Metric::new("g", MetricKind::Absolute, MetricValue::Gauge { value: 1.0 });
        assert_eq!(encode_metric_type(&gauge), MetricType::Gauge);

        let set = Metric::new(
            "s",
            MetricKind::Incremental,
            MetricValue::Set {
                values: ["a".into()].into(),
            },
        );
        assert_eq!(encode_metric_type(&set), MetricType::Sum);
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
    fn test_vector_metric_to_data_point() {
        let mut tags = MetricTags::default();
        tags.insert("service".to_string(), "web".to_string());
        tags.insert("env".to_string(), "prod".to_string());

        let metric = Metric::new(
            "http.requests",
            MetricKind::Incremental,
            MetricValue::Counter { value: 42.0 },
        )
        .with_tags(Some(tags));

        let dp = vector_metric_to_data_point(&metric);
        assert_eq!(dp.metric_name, "http.requests");
        assert_eq!(dp.metric_type, MetricType::Sum);
        assert_eq!(dp.value, 42.0);
        assert_eq!(
            dp.tags.get("service").map(|value| value.as_str()),
            Some("web")
        );
        assert_eq!(dp.tags.get("env").map(|value| value.as_str()), Some("prod"));
        assert_eq!(dp.tags.len(), 2);
    }
}
