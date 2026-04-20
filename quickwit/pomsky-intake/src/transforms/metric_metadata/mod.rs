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
use vector::event::Event;
use vector::schema::Definition;
use vector::transforms::{TaskTransform, Transform};
use vector_lib::config::clone_input_definitions;

mod known_metrics;
pub mod types;

pub use known_metrics::KnownMetrics;
pub use types::{map_metric_type, MetadataMetricType, MetricTypeInfo};

// ---------------------------------------------------------------------------
// Serde default constants and functions (per D-01, CFG-03)
// ---------------------------------------------------------------------------

const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 15;
const DEFAULT_PERSIST_INTERVAL_SECS: u64 = 30;
const DEFAULT_BATCH_SIZE: usize = 200;
const DEFAULT_TTL_MIN_HOURS: u64 = 12;
const DEFAULT_TTL_MAX_HOURS: u64 = 36;
const DEFAULT_HTTP_TIMEOUT_SECS: u64 = 10;
const DEFAULT_PERSIST_FILE_PATH: &str = "/tmp/metric_metadata_known.csv";

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

fn default_persist_file_path() -> String {
    DEFAULT_PERSIST_FILE_PATH.to_string()
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
    /// Path to the CSV file for persisting the known-metrics set between restarts.
    #[serde(default = "default_persist_file_path")]
    pub persist_file_path: String,
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
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
persist_file_path: "/data/known.csv"
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
        assert_eq!(cfg.persist_file_path, "/data/known.csv");
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
        assert_eq!(cfg.persist_file_path, "/tmp/metric_metadata_known.csv");
    }

    #[test]
    fn test_config_deserialization_with_persist_path() {
        let yaml = r#"
org_id: "test-org"
metadata_svc_url: "http://localhost:9999"
persist_file_path: "/data/known.csv"
"#;
        let cfg: MetricMetadataConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.persist_file_path, "/data/known.csv");
    }

    #[test]
    fn test_config_defaults_persist_path() {
        let yaml = r#"
org_id: "test-org"
metadata_svc_url: "http://localhost:9999"
"#;
        let cfg: MetricMetadataConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.persist_file_path, "/tmp/metric_metadata_known.csv");
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
            persist_file_path: DEFAULT_PERSIST_FILE_PATH.to_string(),
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
}
