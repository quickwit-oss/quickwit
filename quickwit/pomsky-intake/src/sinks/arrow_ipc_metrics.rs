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
//! and POSTs them to two HTTP endpoints — one for points (counters / gauges /
//! histograms / etc.) and one for DDSketches.
//!
//! Incoming `MetricValue::Sketch(MetricSketch::AgentDDSketch(_))` events are
//! routed to the sketches endpoint encoded with `ArrowSketchBatchBuilder`;
//! everything else flows through `ArrowMetricsBatchBuilder` to the points
//! endpoint. The two flushes run in parallel via `tokio::join!` and a failure
//! on either side fails the finalizer batch.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use quickwit_opentelemetry::otlp::{
    ArrowIpcError, ArrowMetricsBatchBuilder, MetricDataPoint, MetricType, record_batch_to_ipc,
};
use quickwit_parquet_engine::ingest::{ArrowSketchBatchBuilder, SketchDataPoint};
use quickwit_parquet_engine::schema::{REQUIRED_FIELDS, SketchParquetField};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, warn};
use vector::config::{AcknowledgementsConfig, GenerateConfig, Input, SinkConfig, SinkContext};
use vector::event::{EventArray, EventStatus, Finalizable, Metric, MetricValue};
use vector::sinks::Healthcheck;
use vector::sinks::util::StreamSink;
use vector_lib::configurable::NamedComponent;
use vector_lib::event::metric::MetricSketch;
use vector_lib::sink::VectorSink;

/// Maximum number of metrics to accumulate before flushing a batch.
const DEFAULT_BATCH_SIZE: usize = 1_000;

/// Default time-based flush interval in seconds.
const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 1;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Serializes metric events as Arrow IPC and POSTs them to two HTTP endpoints:
/// `metrics_uri` for points and `sketches_uri` for DDSketches.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArrowIpcMetricsSinkConfig {
    /// Target HTTP endpoint that receives Arrow IPC batches of points
    /// (counters, gauges, histograms, summaries, distributions, sets).
    pub metrics_uri: String,
    /// Target HTTP endpoint that receives Arrow IPC batches of DDSketches.
    pub sketches_uri: String,
    /// Maximum number of metrics per Arrow IPC batch. Counted across both
    /// the points buffer and the sketches buffer combined.
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
        toml::from_str(
            r#"
metrics_uri = "http://localhost:7280/api/datadog/v1/byoc/metrics"
sketches_uri = "http://localhost:7280/api/datadog/v1/byoc/sketches"
"#,
        )
        .expect("config should be valid")
    }
}

#[async_trait]
#[typetag::serde(name = "arrow_ipc_metrics")]
impl SinkConfig for ArrowIpcMetricsSinkConfig {
    async fn build(&self, _cx: SinkContext) -> vector::Result<(VectorSink, Healthcheck)> {
        let sink = ArrowIpcMetricsSink {
            metrics_uri: self.metrics_uri.clone(),
            sketches_uri: self.sketches_uri.clone(),
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
    metrics_uri: String,
    sketches_uri: String,
    batch_size: usize,
    flush_interval: Duration,
    client: reqwest::Client,
}

fn is_sketch_metric(metric: &Metric) -> bool {
    matches!(
        metric.value(),
        MetricValue::Sketch {
            sketch: MetricSketch::AgentDDSketch(_),
        }
    )
}

#[async_trait]
impl StreamSink<EventArray> for ArrowIpcMetricsSink {
    async fn run(mut self: Box<Self>, mut input: BoxStream<'_, EventArray>) -> Result<(), ()> {
        let mut pending_points: Vec<Metric> = Vec::with_capacity(self.batch_size);
        let mut pending_sketches: Vec<Metric> = Vec::with_capacity(self.batch_size);
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
                            if is_sketch_metric(&metric) {
                                pending_sketches.push(metric);
                            } else {
                                pending_points.push(metric);
                            }
                        }
                    }
                    pending_finalizers.push(finalizers);

                    if pending_points.len() + pending_sketches.len() >= self.batch_size {
                        let status = self.flush(&mut pending_points, &mut pending_sketches).await;
                        for pending_finalizer in pending_finalizers.drain(..) {
                            pending_finalizer.update_status(status);
                        }
                        flush_timer.reset();
                    }
                }

                _ = flush_timer.tick() => {
                    if !pending_points.is_empty() || !pending_sketches.is_empty() {
                        let status = self.flush(&mut pending_points, &mut pending_sketches).await;
                        for pending_finalizer in pending_finalizers.drain(..) {
                            pending_finalizer.update_status(status);
                        }
                    }
                }
            }
        }
        // Flush remaining on shutdown.
        if !pending_points.is_empty() || !pending_sketches.is_empty() {
            let status = self.flush(&mut pending_points, &mut pending_sketches).await;
            for pending_finalizer in pending_finalizers.drain(..) {
                pending_finalizer.update_status(status);
            }
        }
        Ok(())
    }
}

impl ArrowIpcMetricsSink {
    /// Flushes both buffers in parallel. Either side can be empty (in which
    /// case its arm short-circuits with `Delivered`). The combined status is
    /// `Errored` if either side failed.
    async fn flush(&self, points: &mut Vec<Metric>, sketches: &mut Vec<Metric>) -> EventStatus {
        let (points_status, sketches_status) =
            tokio::join!(self.flush_points(points), self.flush_sketches(sketches));
        if matches!(points_status, EventStatus::Errored)
            || matches!(sketches_status, EventStatus::Errored)
        {
            EventStatus::Errored
        } else {
            EventStatus::Delivered
        }
    }

    async fn flush_points(&self, metrics: &mut Vec<Metric>) -> EventStatus {
        if metrics.is_empty() {
            return EventStatus::Delivered;
        }
        let batch_size = metrics.len();
        match build_ipc_bytes(metrics) {
            Ok(ipc_bytes) => {
                self.post_ipc(&self.metrics_uri, ipc_bytes, "metrics", batch_size)
                    .await
            }
            Err(error) => {
                error!(batch_size, %error, "failed to build arrow ipc metrics batch");
                EventStatus::Errored
            }
        }
    }

    async fn flush_sketches(&self, metrics: &mut Vec<Metric>) -> EventStatus {
        if metrics.is_empty() {
            return EventStatus::Delivered;
        }
        let batch_size = metrics.len();
        match build_sketch_ipc_bytes(metrics) {
            Ok(Some(ipc_bytes)) => {
                self.post_ipc(&self.sketches_uri, ipc_bytes, "sketches", batch_size)
                    .await
            }
            // All sketches in the buffer were skipped (e.g. all empty); nothing
            // to send. Still acknowledge as delivered so finalizers complete.
            Ok(None) => {
                debug!(batch_size, "skipped empty arrow ipc sketches batch");
                EventStatus::Delivered
            }
            Err(error) => {
                error!(batch_size, %error, "failed to build arrow ipc sketches batch");
                EventStatus::Errored
            }
        }
    }

    async fn post_ipc(
        &self,
        uri: &str,
        ipc_bytes: Vec<u8>,
        kind: &'static str,
        batch_size: usize,
    ) -> EventStatus {
        match self
            .client
            .post(uri)
            .header("content-type", "application/vnd.apache.arrow.stream")
            .body(ipc_bytes)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                debug!(batch_size, kind, "flushed arrow ipc batch");
                EventStatus::Delivered
            }
            Ok(resp) => {
                error!(
                    batch_size,
                    kind,
                    status = %resp.status(),
                    "arrow ipc endpoint returned error"
                );
                EventStatus::Errored
            }
            Err(error) => {
                error!(batch_size, kind, %error, "failed to send arrow ipc batch");
                EventStatus::Errored
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Arrow conversion
// ---------------------------------------------------------------------------

fn vector_metric_to_data_point(metric: &Metric) -> MetricDataPoint {
    // Sketches are routed off the points path before reaching here; if one
    // slipped through, the points encoding would silently drop the bins.
    debug_assert!(
        !is_sketch_metric(metric),
        "vector_metric_to_data_point called on a sketch — sketches must be routed to \
         build_sketch_ipc_bytes",
    );
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

/// Builds an Arrow IPC batch from a buffer of sketch metrics.
///
/// Returns `Ok(None)` when every sketch in the buffer was skipped (empty /
/// malformed) — the caller should treat this as "nothing to send" rather than
/// posting an empty batch.
fn build_sketch_ipc_bytes(metrics: &mut Vec<Metric>) -> Result<Option<Vec<u8>>, ArrowIpcError> {
    let mut builder = ArrowSketchBatchBuilder::with_capacity(metrics.len());
    for metric in metrics.drain(..) {
        if let Some(data_point) = vector_sketch_to_data_point(&metric) {
            builder.append(data_point);
        }
    }
    if builder.is_empty() {
        return Ok(None);
    }
    let record_batch = builder.finish();
    record_batch_to_ipc(&record_batch).map(Some)
}

/// Converts a Vector sketch `Metric` into a `SketchDataPoint`. Returns `None`
/// (with a rate-limited warning) for empty / non-AgentDDSketch values so the
/// pipeline drops them rather than emitting bogus zero rows.
fn vector_sketch_to_data_point(metric: &Metric) -> Option<SketchDataPoint> {
    let MetricValue::Sketch {
        sketch: MetricSketch::AgentDDSketch(ddsketch),
    } = metric.value()
    else {
        // Caller guarantees this — `is_sketch_metric` filtered the buffer.
        debug_assert!(
            false,
            "vector_sketch_to_data_point called on a non-sketch metric"
        );
        return None;
    };

    if ddsketch.is_empty() {
        warn!(metric = metric.name(), "skipping empty agent ddsketch");
        return None;
    }

    let bin_map = ddsketch.bin_map();
    let (keys, counts_u16) = bin_map.into_parts();
    let counts: Vec<u64> = counts_u16.into_iter().map(u64::from).collect();

    // Tags reserved by the sketches Parquet schema — must not be passed
    // through as user tags or the dynamic-column logic in
    // `ArrowSketchBatchBuilder` will collide with the fixed columns.
    let reserved: Vec<&str> = SketchParquetField::all().iter().map(|f| f.name()).collect();
    let tags: HashMap<String, String> = match metric.tags() {
        Some(tags) => tags
            .iter_single()
            .filter(|(k, _)| !reserved.contains(k))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        None => HashMap::new(),
    };

    Some(SketchDataPoint {
        metric_name: metric.name().to_string(),
        timestamp_secs: metric
            .timestamp()
            .map(|ts| ts.timestamp() as u64)
            .unwrap_or(0),
        count: u64::from(ddsketch.count()),
        sum: ddsketch.sum().unwrap_or(0.0),
        min: ddsketch.min().unwrap_or(0.0),
        max: ddsketch.max().unwrap_or(0.0),
        flags: 0,
        keys,
        counts,
        tags,
    })
}

fn encode_metric_type(metric: &Metric) -> MetricType {
    match metric.value() {
        MetricValue::Counter { .. } => MetricType::Sum,
        MetricValue::Gauge { .. } => MetricType::Gauge,
        MetricValue::AggregatedHistogram { .. } => MetricType::Histogram,
        MetricValue::AggregatedSummary { .. } => MetricType::Summary,
        MetricValue::Distribution { .. } => MetricType::Histogram,
        MetricValue::Set { .. } => MetricType::Sum,
        // Sketches are routed off the points path; if one reaches here it's a bug.
        MetricValue::Sketch { .. } => {
            debug_assert!(false, "sketch reached encode_metric_type");
            MetricType::Histogram
        }
    }
}

fn extract_scalar_value(metric: &Metric) -> f64 {
    match metric.value() {
        MetricValue::Counter { value } | MetricValue::Gauge { value } => *value,
        MetricValue::Set { values } => values.len() as f64,
        MetricValue::Distribution { samples, .. } => samples
            .iter()
            .map(|sample| sample.value * sample.rate as f64)
            .sum(),
        MetricValue::AggregatedHistogram { sum, .. } => *sum,
        MetricValue::AggregatedSummary { sum, .. } => *sum,
        // Sketches are routed off the points path; if one reaches here it's a bug.
        MetricValue::Sketch { .. } => {
            debug_assert!(false, "sketch reached extract_scalar_value");
            0.0
        }
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

    fn make_sketch_metric(name: &str, keys: &[i16], counts: &[u16]) -> Metric {
        let count: u32 = counts.iter().map(|c| u32::from(*c)).sum();
        let sketch = vector_lib::metrics::AgentDDSketch::from_raw(
            count, 1.0, 100.0, 500.0, 50.0, keys, counts,
        )
        .expect("from_raw should succeed for non-empty sketch");
        Metric::new(name, MetricKind::Incremental, MetricValue::from(sketch))
    }

    #[test]
    fn test_is_sketch_metric_classifies_variants() {
        let counter = Metric::new(
            "c",
            MetricKind::Incremental,
            MetricValue::Counter { value: 1.0 },
        );
        let gauge = Metric::new("g", MetricKind::Absolute, MetricValue::Gauge { value: 2.0 });
        let sketch = make_sketch_metric("s", &[10, 20], &[3, 4]);

        assert!(!is_sketch_metric(&counter));
        assert!(!is_sketch_metric(&gauge));
        assert!(is_sketch_metric(&sketch));
    }

    #[test]
    fn test_vector_sketch_to_data_point_extracts_bins_and_scalars() {
        let mut tags = MetricTags::default();
        tags.insert("service".to_string(), "api".to_string());
        tags.insert("env".to_string(), "prod".to_string());

        let metric =
            make_sketch_metric("req.latency", &[100, 200, 300], &[5, 3, 2]).with_tags(Some(tags));

        let dp = vector_sketch_to_data_point(&metric).expect("non-empty sketch");
        assert_eq!(dp.metric_name, "req.latency");
        assert_eq!(dp.keys, vec![100, 200, 300]);
        assert_eq!(dp.counts, vec![5u64, 3, 2]);
        // count = sum(n) = 10
        assert_eq!(dp.count, 10);
        assert_eq!(dp.flags, 0);
        assert_eq!(
            dp.tags.get("service").map(|value| value.as_str()),
            Some("api")
        );
        assert_eq!(dp.tags.get("env").map(|value| value.as_str()), Some("prod"));
    }

    #[test]
    fn test_vector_sketch_to_data_point_filters_reserved_tags() {
        // Pick a tag name that the sketches Parquet schema reserves as a fixed
        // column. Any field returned by `SketchParquetField::all()` works; we
        // assert the helper drops it from the user tag map.
        let reserved_name = SketchParquetField::all()[0].name().to_string();

        let mut tags = MetricTags::default();
        tags.insert(reserved_name.clone(), "should-be-dropped".to_string());
        tags.insert("service".to_string(), "kept".to_string());

        let metric = make_sketch_metric("m", &[1], &[1]).with_tags(Some(tags));

        let dp = vector_sketch_to_data_point(&metric).expect("non-empty sketch");
        assert!(
            !dp.tags.contains_key(reserved_name.as_str()),
            "reserved tag {reserved_name} leaked into user tags",
        );
        assert_eq!(
            dp.tags.get("service").map(|value| value.as_str()),
            Some("kept")
        );
    }

    #[test]
    fn test_build_sketch_ipc_bytes_round_trip() {
        let mut tags = MetricTags::default();
        tags.insert("service".to_string(), "api".to_string());

        let mut metrics = vec![
            make_sketch_metric("a", &[10, 20], &[3, 1]).with_tags(Some(tags.clone())),
            make_sketch_metric("b", &[5], &[2]).with_tags(Some(tags)),
        ];
        let ipc_bytes = build_sketch_ipc_bytes(&mut metrics)
            .expect("should build")
            .expect("non-empty");
        assert!(metrics.is_empty());
        assert!(!ipc_bytes.is_empty());

        let cursor = std::io::Cursor::new(ipc_bytes);
        let reader = StreamReader::try_new(cursor, None).expect("should parse IPC");
        let batches: Vec<_> = reader.into_iter().collect::<Result<_, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);

        // ArrowSketchBatchBuilder emits 10 fixed columns + dynamic tag columns.
        let schema = batches[0].schema();
        let field_names: Vec<&str> = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        for required in [
            "metric_name",
            "timestamp_secs",
            "count",
            "sum",
            "min",
            "max",
            "flags",
            "keys",
            "counts",
            "timeseries_id",
        ] {
            assert!(
                field_names.contains(&required),
                "missing fixed column {required}",
            );
        }
        assert!(field_names.contains(&"service"));
    }

    #[test]
    fn test_build_sketch_ipc_bytes_empty_buffer_returns_none() {
        let mut metrics: Vec<Metric> = Vec::new();
        let result = build_sketch_ipc_bytes(&mut metrics).expect("should not error");
        assert!(result.is_none());
    }
}
