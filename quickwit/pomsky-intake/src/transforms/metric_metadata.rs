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

use std::pin::Pin;

use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use vector::config::{
    DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext, TransformOutput,
};
use vector::event::{Event, Metric, MetricValue};
use vector::schema::Definition;
use vector::transforms::{TaskTransform, Transform};
use vector_lib::config::clone_input_definitions;

// ---------------------------------------------------------------------------
// Serde default constants and functions (per D-01, CFG-03)
// ---------------------------------------------------------------------------

const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 15;
const DEFAULT_PERSIST_INTERVAL_SECS: u64 = 30;
const DEFAULT_BATCH_SIZE: usize = 200;
const DEFAULT_TTL_MIN_HOURS: u64 = 12;
const DEFAULT_TTL_MAX_HOURS: u64 = 36;
const DEFAULT_HTTP_TIMEOUT_SECS: u64 = 10;

fn default_flush_interval_secs() -> u64 {
    DEFAULT_FLUSH_INTERVAL_SECS
}

fn default_persist_interval_secs() -> u64 {
    DEFAULT_PERSIST_INTERVAL_SECS
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_ttl_min_hours() -> u64 {
    DEFAULT_TTL_MIN_HOURS
}

fn default_ttl_max_hours() -> u64 {
    DEFAULT_TTL_MAX_HOURS
}

fn default_http_timeout_secs() -> u64 {
    DEFAULT_HTTP_TIMEOUT_SECS
}

// ---------------------------------------------------------------------------
// Config (per D-01)
// ---------------------------------------------------------------------------

/// Configuration for the metric metadata transform.
///
/// All fields live in the Vector transform YAML config section per D-01.
/// No changes are made to `IntakeConfig` — the org_id, metadata_svc_url,
/// and operational parameters are declared here and deserialized by Vector.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MetricMetadataConfig {
    /// Organization identifier passed in HTTP requests to byoc-ingest-metadata-svc.
    pub org_id: String,
    /// Base URL of byoc-ingest-metadata-svc (e.g. "https://metadata.example.com").
    pub metadata_svc_url: String,
    /// How often (seconds) to flush pending metrics to the metadata service.
    #[serde(default = "default_flush_interval_secs")]
    pub flush_interval_secs: u64,
    /// How often (seconds) to persist the known-metrics set to disk.
    #[serde(default = "default_persist_interval_secs")]
    pub persist_interval_secs: u64,
    /// Maximum pending-metric count before an early flush is triggered.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Minimum TTL (hours) before a known metric expires and is re-submitted.
    #[serde(default = "default_ttl_min_hours")]
    pub ttl_min_hours: u64,
    /// Maximum TTL (hours) for randomized expiry of known metrics.
    #[serde(default = "default_ttl_max_hours")]
    pub ttl_max_hours: u64,
    /// HTTP request timeout (seconds) for calls to byoc-ingest-metadata-svc.
    #[serde(default = "default_http_timeout_secs")]
    pub http_timeout_secs: u64,
}

impl vector_lib::configurable::NamedComponent for MetricMetadataConfig {
    fn get_component_name(&self) -> &'static str {
        "metric_metadata"
    }
}

impl GenerateConfig for MetricMetadataConfig {
    fn generate_config() -> toml::Value {
        toml::Value::Table(Default::default())
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "metric_metadata")]
impl TransformConfig for MetricMetadataConfig {
    /// Validates that `DD_API_KEY` is present and constructs the transform.
    ///
    /// The API key is validated once at startup (D-02). If absent the pipeline
    /// fails to start with a descriptive error — there is no fallback or default
    /// key (T-01-01: spoofing mitigation).
    async fn build(&self, _context: &TransformContext) -> vector::Result<Transform> {
        // config is cloned once at build time; not in the hot path
        let api_key = std::env::var("DD_API_KEY").map_err(|_| {
            "DD_API_KEY environment variable is not set; \
             metric metadata transform cannot start without an API key"
        })?;
        Ok(Transform::event_task(MetricMetadataTransform {
            config: self.clone(),
            api_key,
        }))
    }

    fn input(&self) -> Input {
        Input::metric()
    }

    fn outputs(
        &self,
        _: &TransformContext,
        input_definitions: &[(OutputId, Definition)],
    ) -> Vec<TransformOutput> {
        vec![TransformOutput::new(
            DataType::Metric,
            clone_input_definitions(input_definitions),
        )]
    }

    // TaskTransform is inherently a single stream actor; concurrency setting
    // is only meaningful for FunctionTransform. Return false for clarity (D-11).
    fn enable_concurrency(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Transform (per D-11)
// ---------------------------------------------------------------------------

/// Metric metadata tracking transform.
///
/// Phase 1 skeleton: pass-through + metric type mapping.
/// Phase 2 will add in-memory state accumulation.
/// Phase 3 will add HTTP submission to byoc-ingest-metadata-svc.
///
/// NOTE: Debug is intentionally NOT derived — the `api_key` field must not
/// appear in log output (T-01-02: information disclosure mitigation).
pub struct MetricMetadataTransform {
    #[allow(dead_code)]
    config: MetricMetadataConfig,
    #[allow(dead_code)]
    api_key: String,
}

impl TaskTransform<Event> for MetricMetadataTransform {
    fn transform(
        self: Box<Self>,
        task: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>> {
        Box::pin(task.map(move |event| {
            if let Event::Metric(ref metric) = event {
                let _type_info = map_metric_type(metric);
                // Phase 2 will accumulate type_info into the known-metrics state.
            }
            event
        }))
    }
}

// ---------------------------------------------------------------------------
// Metric type mapping (per D-03, D-04, D-09, D-10)
// ---------------------------------------------------------------------------

/// SaaS-side representation of a metric type, serialized to the exact API
/// string values expected by byoc-ingest-metadata-svc (D-10).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetadataMetricType {
    Count,
    Rate,
    Gauge,
    // Explicit rename overrides rename_all; documents intent (D-10).
    #[serde(rename = "ddsketch")]
    DdSketch,
}

/// Pair of (metric_type, interval_seconds) sent to byoc-ingest-metadata-svc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricTypeInfo {
    pub metric_type: MetadataMetricType,
    /// Reporting interval in whole seconds. 0 for point-in-time types.
    pub interval: u32,
}

/// Maps a Vector `Metric` to the SaaS type representation (D-03, D-04).
///
/// Mapping rules:
/// - `Counter` with no `interval_ms`  → `count`   with `interval = 10`
/// - `Counter` with `interval_ms = N` → `rate`    with `interval = N / 1000`
/// - `Gauge`                          → `gauge`   with `interval = 0`
/// - `Sketch`                         → `ddsketch` with `interval = 0`
/// - Any other variant (Set, Distribution, …) → `gauge` with `interval = 0`
///   (conservative fallback; these are not expected from DD Agent / OTel sources)
///
/// Note: `interval_ms / 1000` is integer division — sub-second intervals
/// (< 1000 ms) round down to 0. This is intentional per D-04.
pub fn map_metric_type(metric: &Metric) -> MetricTypeInfo {
    match metric.value() {
        MetricValue::Counter { .. } => match metric.interval_ms() {
            None => MetricTypeInfo {
                metric_type: MetadataMetricType::Count,
                interval: 10,
            },
            Some(ms) => MetricTypeInfo {
                metric_type: MetadataMetricType::Rate,
                // Integer division is intentional per D-04: interval field is u32 seconds.
                interval: ms.get() / 1000,
            },
        },
        MetricValue::Gauge { .. } => MetricTypeInfo {
            metric_type: MetadataMetricType::Gauge,
            interval: 0,
        },
        MetricValue::Sketch { .. } => MetricTypeInfo {
            metric_type: MetadataMetricType::DdSketch,
            interval: 0,
        },
        // Other MetricValue variants (Set, Distribution, AggregatedHistogram,
        // AggregatedSummary) are not expected from Datadog Agent or OTel sources.
        // Default to Gauge with interval=0 as a conservative fallback.
        _ => MetricTypeInfo {
            metric_type: MetadataMetricType::Gauge,
            interval: 0,
        },
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use vector::event::{Metric, MetricKind, MetricValue};
    use vector_lib::metrics::AgentDDSketch;

    use super::*;

    // ----- Config deserialization -----

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
org_id: "test-org"
metadata_svc_url: "http://localhost:9999"
flush_interval_secs: 5
persist_interval_secs: 10
batch_size: 50
ttl_min_hours: 6
ttl_max_hours: 18
http_timeout_secs: 3
"#;
        let cfg: MetricMetadataConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.org_id, "test-org");
        assert_eq!(cfg.metadata_svc_url, "http://localhost:9999");
        assert_eq!(cfg.flush_interval_secs, 5);
        assert_eq!(cfg.persist_interval_secs, 10);
        assert_eq!(cfg.batch_size, 50);
        assert_eq!(cfg.ttl_min_hours, 6);
        assert_eq!(cfg.ttl_max_hours, 18);
        assert_eq!(cfg.http_timeout_secs, 3);
    }

    #[test]
    fn test_config_defaults() {
        let yaml = r#"
org_id: "test-org"
metadata_svc_url: "http://localhost:9999"
"#;
        let cfg: MetricMetadataConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.flush_interval_secs, 15);
        assert_eq!(cfg.persist_interval_secs, 30);
        assert_eq!(cfg.batch_size, 200);
        assert_eq!(cfg.ttl_min_hours, 12);
        assert_eq!(cfg.ttl_max_hours, 36);
        assert_eq!(cfg.http_timeout_secs, 10);
    }

    // ----- API key validation -----

    #[tokio::test]
    async fn test_build_fails_without_api_key() {
        let saved = std::env::var("DD_API_KEY").ok();
        // SAFETY: test is single-threaded in nextest isolation; env mutation is safe.
        unsafe {
            std::env::remove_var("DD_API_KEY");
        }

        let cfg = MetricMetadataConfig {
            org_id: "test-org".to_string(),
            metadata_svc_url: "http://localhost:9999".to_string(),
            flush_interval_secs: 15,
            persist_interval_secs: 30,
            batch_size: 200,
            ttl_min_hours: 12,
            ttl_max_hours: 36,
            http_timeout_secs: 10,
        };

        // Build a minimal context. TransformContext::default is not available;
        // we use `Default::default()` which relies on the derived impl.
        let ctx = TransformContext::default();
        let result = cfg.build(&ctx).await;

        // Restore the env var before any assert so failures don't pollute other tests.
        unsafe {
            match saved {
                Some(val) => std::env::set_var("DD_API_KEY", val),
                None => std::env::remove_var("DD_API_KEY"),
            }
        }

        // Transform doesn't impl Debug so we can't use unwrap_err/expect_err.
        match result {
            Ok(_) => panic!("build() should fail when DD_API_KEY is absent"),
            Err(err) => {
                assert!(
                    err.to_string().contains("DD_API_KEY"),
                    "expected error message to mention DD_API_KEY, got: {err}"
                );
            }
        }
    }

    // ----- Metric type mapping -----

    #[test]
    fn test_counter_without_interval_maps_to_count() {
        let metric = Metric::new("req", MetricKind::Incremental, MetricValue::Counter {
            value: 1.0,
        });
        let info = map_metric_type(&metric);
        assert_eq!(info.metric_type, MetadataMetricType::Count);
        assert_eq!(info.interval, 10);
    }

    #[test]
    fn test_counter_with_interval_ms_maps_to_rate() {
        let metric =
            Metric::new("req", MetricKind::Incremental, MetricValue::Counter { value: 1.0 })
                .with_interval_ms(NonZeroU32::new(10_000)); // 10_000 ms = 10 s
        let info = map_metric_type(&metric);
        assert_eq!(info.metric_type, MetadataMetricType::Rate);
        assert_eq!(info.interval, 10);
    }

    #[test]
    fn test_gauge_maps_to_gauge() {
        let metric = Metric::new("cpu", MetricKind::Absolute, MetricValue::Gauge { value: 0.5 });
        let info = map_metric_type(&metric);
        assert_eq!(info.metric_type, MetadataMetricType::Gauge);
        assert_eq!(info.interval, 0);
    }

    #[test]
    fn test_sketch_maps_to_ddsketch() {
        let sketch = AgentDDSketch::with_agent_defaults();
        let metric = Metric::new(
            "latency",
            MetricKind::Incremental,
            MetricValue::Sketch {
                sketch: vector::event::metric::MetricSketch::AgentDDSketch(sketch),
            },
        );
        let info = map_metric_type(&metric);
        assert_eq!(info.metric_type, MetadataMetricType::DdSketch);
        assert_eq!(info.interval, 0);
    }

    // ----- Serialization -----

    #[test]
    fn test_metric_type_serialization() {
        // Verify serde round-trip produces the exact API strings (D-10).
        let count: MetadataMetricType = serde_yaml::from_str("\"count\"").unwrap();
        assert_eq!(count, MetadataMetricType::Count);
        let serialized = serde_yaml::to_string(&MetadataMetricType::Count).unwrap();
        assert!(serialized.contains("count"), "expected 'count', got: {serialized}");

        let rate: MetadataMetricType = serde_yaml::from_str("\"rate\"").unwrap();
        assert_eq!(rate, MetadataMetricType::Rate);
        let serialized = serde_yaml::to_string(&MetadataMetricType::Rate).unwrap();
        assert!(serialized.contains("rate"), "expected 'rate', got: {serialized}");

        let gauge: MetadataMetricType = serde_yaml::from_str("\"gauge\"").unwrap();
        assert_eq!(gauge, MetadataMetricType::Gauge);
        let serialized = serde_yaml::to_string(&MetadataMetricType::Gauge).unwrap();
        assert!(serialized.contains("gauge"), "expected 'gauge', got: {serialized}");

        let ddsketch: MetadataMetricType = serde_yaml::from_str("\"ddsketch\"").unwrap();
        assert_eq!(ddsketch, MetadataMetricType::DdSketch);
        let serialized = serde_yaml::to_string(&MetadataMetricType::DdSketch).unwrap();
        assert!(serialized.contains("ddsketch"), "expected 'ddsketch', got: {serialized}");
    }

    // ----- Edge cases -----

    #[test]
    fn test_counter_with_sub_second_interval() {
        // 500 ms / 1000 = 0 (integer division). Documents intentional behavior per D-04.
        let metric =
            Metric::new("req", MetricKind::Incremental, MetricValue::Counter { value: 1.0 })
                .with_interval_ms(NonZeroU32::new(500));
        let info = map_metric_type(&metric);
        assert_eq!(info.metric_type, MetadataMetricType::Rate);
        assert_eq!(info.interval, 0, "sub-second interval rounds to 0 per D-04");
    }
}
