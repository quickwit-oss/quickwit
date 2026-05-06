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

use std::collections::HashMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tracing::warn;
use vector::config::{
    DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext, TransformOutput,
};
use vector::event::Event;
use vector::schema::Definition;
use vector::transforms::{TaskTransform, Transform};
use vector_lib::config::clone_input_definitions;

mod csv_persistence;
mod flush_client;
mod known_metrics;
pub mod types;

use flush_client::FlushClient;
pub use known_metrics::KnownMetrics;
pub use types::{MetadataMetricType, MetricTypeInfo, map_metric_type};

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
/// `api_key` and `metadata_svc_url` are sourced from `IntakeConfig` and
/// interpolated into the Vector transform YAML (the API key via
/// `${DD_API_KEY}` substitution against an in-memory map, not the process
/// env). Operational parameters (flush intervals, batch size, TTLs, etc.) are
/// declared here and deserialized by Vector.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MetricMetadataConfig {
    /// Datadog API key used to authenticate with byoc-ingest-metadata-svc.
    pub api_key: String,
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
    async fn build(&self, _context: &TransformContext) -> vector::Result<Transform> {
        // Fail-fast: validate persist_file_path parent directory exists and is
        // accessible -- misconfigured paths fail at startup, not silently at
        // the first persist tick.
        let persist_path = std::path::Path::new(&self.persist_file_path);
        if let Some(parent) = persist_path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::metadata(parent).map_err(|err| {
                format!(
                    "persist_file_path parent directory '{}' is not accessible: {err}; ensure the \
                     directory exists and is writable",
                    parent.display()
                )
            })?;
        }

        // Load known metrics from CSV on startup (PERSIST-03, D-04).
        let entries = csv_persistence::load_from_csv(persist_path)
            .map_err(|err| format!("failed to load known metrics CSV: {err}"))?;

        let mut known_metrics = KnownMetrics::new(self.ttl_min_hours, self.ttl_max_hours);
        known_metrics.load_entries(entries);

        let flush_client = FlushClient::new(
            self.api_key.clone(),
            self.metadata_svc_url.clone(),
            Duration::from_secs(self.http_timeout_secs),
        )
        .map_err(|err| format!("failed to build HTTP client: {err}"))?;

        Ok(Transform::event_task(MetricMetadataTransform {
            config: self.clone(),
            known_metrics,
            pending: HashMap::new(),
            flush_client,
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
/// Tracks which metric names have been submitted to the SaaS endpoint.
/// Per-event: classifies metrics as known/unknown, accumulates unknowns in
/// the pending list for later HTTP flush (Phase 3).
///
/// NOTE: Debug is intentionally NOT derived — the `api_key` field must not
/// appear in log output (T-01-02: information disclosure mitigation).
pub struct MetricMetadataTransform {
    config: MetricMetadataConfig,
    known_metrics: KnownMetrics,
    pending: HashMap<String, MetricTypeInfo>,
    flush_client: FlushClient,
}

impl TaskTransform<Event> for MetricMetadataTransform {
    fn transform(
        self: Box<Self>,
        task: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>> {
        let mut input = task;
        let mut known_metrics = self.known_metrics;
        let mut pending = self.pending;
        let flush_client = Arc::new(self.flush_client);
        let config = self.config;
        let persist_path = config.persist_file_path.clone();

        let mut flush_timer =
            tokio::time::interval(Duration::from_secs(config.flush_interval_secs));
        let mut persist_timer =
            tokio::time::interval(Duration::from_secs(config.persist_interval_secs));

        // Channel for receiving flush results from background tasks.
        // Decouples collecting unknown metrics from reporting them so that
        // slow HTTP calls do not stall the event pipeline (same pattern as
        // host-tags enrichment).
        let (flush_result_tx, mut flush_result_rx) =
            tokio::sync::mpsc::channel::<Result<Vec<String>, flush_client::FlushError>>(1);
        let mut flush_in_flight = false;

        Box::pin(async_stream::stream! {
            loop {
                tokio::select! {
                    biased;

                    // Highest priority: drain background flush results so
                    // known_metrics stays up-to-date for classification.
                    Some(result) = flush_result_rx.recv() => {
                        flush_in_flight = false;
                        match result {
                            Ok(succeeded) => {
                                for name in succeeded {
                                    known_metrics.insert(name);
                                }
                            }
                            Err(err) => {
                                warn!(error = %err, "metadata flush failed, will retry on next cycle");
                            }
                        }
                    }

                    maybe_event = input.next() => {
                        let Some(event) = maybe_event else {
                            // D-06: shutdown sequence
                            break;
                        };

                        // Per-event: classify metric and accumulate unknowns.
                        // Only allocate the name String when the metric is
                        // unknown — the common-case (known) path is alloc-free.
                        if let Event::Metric(ref metric) = event {
                            let metric_name = metric.name();
                            if !known_metrics.contains(metric_name) {
                                let type_info = map_metric_type(metric);
                                // HashMap dedup; last-seen-wins
                                pending.insert(metric_name.to_string(), type_info);
                            }
                        }

                        // D-04: batch_size trigger — spawn background flush
                        if pending.len() >= config.batch_size && !flush_in_flight {
                            let batch = std::mem::take(&mut pending);
                            let client = flush_client.clone();
                            let tx = flush_result_tx.clone();
                            flush_in_flight = true;
                            tokio::spawn(async move {
                                let result = client.flush_pending(&batch).await;
                                let _ = tx.send(result).await;
                            });
                            flush_timer.reset(); // D-05: avoid double-flush
                        }

                        yield event; // XFRM-01: pass-through unchanged
                    }

                    _ = flush_timer.tick(), if !flush_in_flight => {
                        if !pending.is_empty() {
                            let batch = std::mem::take(&mut pending);
                            let client = flush_client.clone();
                            let tx = flush_result_tx.clone();
                            flush_in_flight = true;
                            tokio::spawn(async move {
                                let result = client.flush_pending(&batch).await;
                                let _ = tx.send(result).await;
                            });
                        }
                    }

                    _ = persist_timer.tick() => {
                        known_metrics.prune_expired();
                        if let Err(err) = csv_persistence::save_to_csv(
                            Path::new(&persist_path),
                            known_metrics.iter(),
                        ) {
                            warn!(error = %err, "failed to persist known metrics");
                        }
                    }
                }
            }

            // D-06: post-loop shutdown sequence
            // Step 1: wait for any in-flight background flush to complete
            if flush_in_flight
                && let Some(result) = flush_result_rx.recv().await
            {
                match result {
                    Ok(succeeded) => {
                        for name in succeeded {
                            known_metrics.insert(name);
                        }
                    }
                    Err(err) => {
                        warn!(error = %err, "in-flight flush failed during shutdown");
                    }
                }
            }

            // Step 2: flush remaining pending metrics (shutdown — OK to await inline)
            if !pending.is_empty() {
                match flush_client.flush_pending(&pending).await {
                    Ok(succeeded) => {
                        for name in succeeded {
                            known_metrics.insert(name);
                        }
                    }
                    Err(err) => {
                        // D-07: log and proceed; metrics re-detected after restart
                        warn!(error = %err, "shutdown flush failed, pending metrics dropped");
                    }
                }
                pending.clear();
            }

            // Step 3: prune expired entries
            known_metrics.prune_expired();

            // Step 4: D-08: always persist CSV on shutdown
            if let Err(err) = csv_persistence::save_to_csv(
                Path::new(&persist_path),
                known_metrics.iter(),
            ) {
                warn!(error = %err, "failed to persist known metrics on shutdown");
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroU32;
    use std::time::Duration;

    use futures::stream;
    use vector::event::{Metric, MetricKind, MetricValue};

    use super::flush_client::FlushClient;
    use super::*;

    // Tests that mutate environment variables are serialized via
    // `#[serial_test::serial(env)]` rather than an in-process Mutex. This avoids
    // holding a sync lock across await points and integrates with serial_test's
    // process-wide ordering for env-mutating tests.

    // ----- Config deserialization -----

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
api_key: "test-api-key"
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
        assert_eq!(cfg.api_key, "test-api-key");
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
api_key: "test-api-key"
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
api_key: "test-api-key"
metadata_svc_url: "http://localhost:9999"
persist_file_path: "/data/known.csv"
"#;
        let cfg: MetricMetadataConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.persist_file_path, "/data/known.csv");
    }

    #[test]
    fn test_config_defaults_persist_path() {
        let yaml = r#"
api_key: "test-api-key"
metadata_svc_url: "http://localhost:9999"
"#;
        let cfg: MetricMetadataConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.persist_file_path, "/tmp/metric_metadata_known.csv");
    }

    // ----- Known/unknown classification (XFRM-02) -----

    #[test]
    fn test_unknown_metric_added_to_pending() {
        let known_metrics = KnownMetrics::new(12, 36);
        let mut pending: HashMap<String, MetricTypeInfo> = HashMap::new();

        // Simulate per-event logic for an unknown metric.
        let metric = Metric::new(
            "new.metric",
            MetricKind::Incremental,
            MetricValue::Counter { value: 1.0 },
        );
        let name = metric.name().to_string();
        if !known_metrics.contains(&name) {
            let type_info = map_metric_type(&metric);
            pending.insert(name, type_info);
        }

        assert_eq!(pending.len(), 1, "unknown metric should be in pending");
        assert!(
            pending.contains_key("new.metric"),
            "pending should contain 'new.metric'"
        );
        assert_eq!(pending["new.metric"].metric_type, MetadataMetricType::Count);
    }

    #[test]
    fn test_known_metric_not_added_to_pending() {
        let mut known_metrics = KnownMetrics::new(12, 36);
        known_metrics.insert("known.metric".to_string());
        let mut pending: HashMap<String, MetricTypeInfo> = HashMap::new();

        // Simulate per-event logic for a known metric.
        let metric = Metric::new(
            "known.metric",
            MetricKind::Incremental,
            MetricValue::Counter { value: 1.0 },
        );
        let name = metric.name().to_string();
        if !known_metrics.contains(&name) {
            let type_info = map_metric_type(&metric);
            pending.insert(name, type_info);
        }

        assert!(
            pending.is_empty(),
            "known metric should NOT be added to pending"
        );
    }

    #[test]
    fn test_pending_dedup_last_seen_wins() {
        let known_metrics = KnownMetrics::new(12, 36);
        let mut pending: HashMap<String, MetricTypeInfo> = HashMap::new();

        // First event: counter -> Count type
        let counter = Metric::new(
            "same.metric",
            MetricKind::Incremental,
            MetricValue::Counter { value: 1.0 },
        );
        let name = counter.name().to_string();
        if !known_metrics.contains(&name) {
            let type_info = map_metric_type(&counter);
            pending.insert(name, type_info);
        }

        // Second event: gauge -> Gauge type (last-seen-wins per D-07)
        let gauge = Metric::new(
            "same.metric",
            MetricKind::Absolute,
            MetricValue::Gauge { value: 42.0 },
        );
        let name = gauge.name().to_string();
        if !known_metrics.contains(&name) {
            let type_info = map_metric_type(&gauge);
            pending.insert(name, type_info);
        }

        assert_eq!(pending.len(), 1, "dedup should keep exactly one entry");
        assert_eq!(
            pending["same.metric"].metric_type,
            MetadataMetricType::Gauge,
            "last-seen-wins: gauge should overwrite count"
        );
    }

    // ----- Transform pass-through -----

    #[tokio::test]
    async fn test_transform_passes_events_through() {
        let transform = Box::new(MetricMetadataTransform {
            config: MetricMetadataConfig {
                api_key: "test-api-key".to_string(),
                metadata_svc_url: "http://localhost:9999".to_string(),
                flush_interval_secs: 15,
                persist_interval_secs: 30,
                batch_size: 200,
                ttl_min_hours: 12,
                ttl_max_hours: 36,
                http_timeout_secs: 10,
                persist_file_path: "/tmp/test.csv".to_string(),
            },
            known_metrics: KnownMetrics::new(12, 36),
            pending: HashMap::new(),
            flush_client: FlushClient::new(
                "test-api-key".to_string(),
                "http://localhost:9999".to_string(),
                Duration::from_secs(10),
            )
            .expect("test client build should succeed"),
        });

        let events: Vec<Event> = vec![
            Event::Metric(Metric::new(
                "cpu.user",
                MetricKind::Absolute,
                MetricValue::Gauge { value: 0.5 },
            )),
            Event::Metric(Metric::new(
                "mem.free",
                MetricKind::Incremental,
                MetricValue::Counter { value: 100.0 },
            )),
            Event::Metric(
                Metric::new(
                    "req.rate",
                    MetricKind::Incremental,
                    MetricValue::Counter { value: 42.0 },
                )
                .with_interval_ms(NonZeroU32::new(10_000)),
            ),
        ];

        let input: Pin<Box<dyn Stream<Item = Event> + Send>> =
            Box::pin(stream::iter(events.clone()));
        let output: Vec<Event> = transform.transform(input).collect().await;

        assert_eq!(output.len(), 3, "all events should pass through");
        for (idx, (out, inp)) in output.iter().zip(events.iter()).enumerate() {
            match (out, inp) {
                (Event::Metric(out_m), Event::Metric(in_m)) => {
                    assert_eq!(
                        out_m.name(),
                        in_m.name(),
                        "event {idx}: metric name mismatch"
                    );
                }
                _ => panic!("event {idx}: expected Metric variant"),
            }
        }
    }

    // ----- CSV loading on build (PERSIST-03) -----

    #[tokio::test]
    async fn test_build_loads_csv_on_startup() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let persist_path = dir.path().join("known.csv");

        // Write a CSV with 2 entries using far-future expiry timestamps.
        let future_ts = 9_999_999_999u64;
        {
            let mut file = std::fs::File::create(&persist_path).unwrap();
            writeln!(file, "metric_name,expiry_ts").unwrap();
            writeln!(file, "preloaded.metric,{future_ts}").unwrap();
            writeln!(file, "another.metric,{future_ts}").unwrap();
        }

        // Load CSV and build KnownMetrics directly to verify startup loading.
        let entries =
            csv_persistence::load_from_csv(&persist_path).expect("CSV load should succeed");
        assert_eq!(entries.len(), 2, "CSV should have 2 entries");

        let mut known_metrics = KnownMetrics::new(12, 36);
        known_metrics.load_entries(entries);

        // Verify that preloaded metrics are "known" and a new metric is not.
        assert!(
            known_metrics.contains("preloaded.metric"),
            "preloaded.metric should be known after CSV load"
        );
        assert!(
            known_metrics.contains("another.metric"),
            "another.metric should be known after CSV load"
        );
        assert!(
            !known_metrics.contains("brand.new.metric"),
            "brand.new.metric should NOT be known"
        );

        // Build a transform with the loaded known_metrics and verify the
        // per-event classification: preloaded metric skipped, new metric pending.
        let mut pending: HashMap<String, MetricTypeInfo> = HashMap::new();

        let preloaded = Metric::new(
            "preloaded.metric",
            MetricKind::Absolute,
            MetricValue::Gauge { value: 1.0 },
        );
        let name = preloaded.name().to_string();
        if !known_metrics.contains(&name) {
            pending.insert(name, map_metric_type(&preloaded));
        }

        let new_metric = Metric::new(
            "brand.new.metric",
            MetricKind::Absolute,
            MetricValue::Gauge { value: 2.0 },
        );
        let name = new_metric.name().to_string();
        if !known_metrics.contains(&name) {
            pending.insert(name, map_metric_type(&new_metric));
        }

        assert!(
            !pending.contains_key("preloaded.metric"),
            "preloaded metric should NOT be in pending"
        );
        assert!(
            pending.contains_key("brand.new.metric"),
            "new metric should be in pending"
        );
    }

    // ----- Build integration via TransformConfig (PERSIST-03) -----

    #[tokio::test]
    #[serial_test::serial(env)]
    async fn test_build_succeeds_with_valid_config() {
        let dir = tempfile::tempdir().unwrap();
        let persist_path = dir.path().join("known.csv");

        let saved = std::env::var("DD_API_KEY").ok();
        // SAFETY: test is single-threaded in nextest isolation; env mutation is safe.
        unsafe {
            std::env::set_var("DD_API_KEY", "test-api-key");
        }

        let cfg = MetricMetadataConfig {
            api_key: "test-api-key".to_string(),
            metadata_svc_url: "http://localhost:9999".to_string(),
            flush_interval_secs: 15,
            persist_interval_secs: 30,
            batch_size: 200,
            ttl_min_hours: 12,
            ttl_max_hours: 36,
            http_timeout_secs: 10,
            persist_file_path: persist_path.to_string_lossy().to_string(),
        };

        let ctx = TransformContext::default();
        let result = cfg.build(&ctx).await;

        unsafe {
            match saved {
                Some(val) => std::env::set_var("DD_API_KEY", val),
                None => std::env::remove_var("DD_API_KEY"),
            }
        }

        assert!(result.is_ok(), "build() should succeed with valid config");
    }

    // ----- Full lifecycle integration test (D-09, D-10) -----

    #[tokio::test]
    async fn test_full_transform_lifecycle() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;
        let dir = tempfile::tempdir().unwrap();
        let persist_path = dir.path().join("known.csv");

        // Mount wiremock: always responds with cpu.user and mem.free as succeeded.
        // The batch-size trigger (first 2 events) and the shutdown flush (3rd event)
        // both hit this mock. "req.rate" is submitted in the shutdown flush but is NOT
        // in the succeeded_metrics response, so it must NOT appear in the known set.
        Mock::given(method("POST"))
            .and(path("/api/unstable/byoc/ingest/metadata/metric-metadata"))
            .and(header("DD-API-KEY", "test-api-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "succeeded_metrics": ["cpu.user", "mem.free"]
            })))
            .expect(2) // batch-size flush + shutdown flush
            .mount(&mock_server)
            .await;

        let transform = Box::new(MetricMetadataTransform {
            config: MetricMetadataConfig {
                api_key: "test-api-key".to_string(),
                metadata_svc_url: mock_server.uri(),
                flush_interval_secs: 60, // long -- batch_size trigger fires, not timer
                persist_interval_secs: 60, // long -- only shutdown path persists
                batch_size: 2,           // triggers flush after 2 unknown metrics
                ttl_min_hours: 12,
                ttl_max_hours: 36,
                http_timeout_secs: 5,
                persist_file_path: persist_path.to_string_lossy().to_string(),
            },
            known_metrics: KnownMetrics::new(12, 36),
            pending: HashMap::new(),
            flush_client: FlushClient::new(
                "test-api-key".to_string(),
                mock_server.uri(),
                Duration::from_secs(5),
            )
            .expect("test client build should succeed"),
        });

        let events: Vec<Event> = vec![
            Event::Metric(Metric::new(
                "cpu.user",
                MetricKind::Absolute,
                MetricValue::Gauge { value: 0.5 },
            )),
            Event::Metric(Metric::new(
                "mem.free",
                MetricKind::Incremental,
                MetricValue::Counter { value: 100.0 },
            )),
            Event::Metric(
                Metric::new(
                    "req.rate",
                    MetricKind::Incremental,
                    MetricValue::Counter { value: 42.0 },
                )
                .with_interval_ms(NonZeroU32::new(10_000)),
            ),
        ];

        // Drive the transform through the real TaskTransform::transform() call (D-09).
        let input: Pin<Box<dyn Stream<Item = Event> + Send>> =
            Box::pin(stream::iter(events.clone()));
        let output: Vec<Event> = transform.transform(input).collect().await;

        // ---- Assertion (a): all events pass through unchanged (XFRM-01) ----
        assert_eq!(output.len(), 3, "all events should pass through");
        for (idx, (out, inp)) in output.iter().zip(events.iter()).enumerate() {
            match (out, inp) {
                (Event::Metric(out_m), Event::Metric(in_m)) => {
                    assert_eq!(
                        out_m.name(),
                        in_m.name(),
                        "event {idx}: metric name mismatch"
                    );
                }
                _ => panic!("event {idx}: expected Metric variant"),
            }
        }

        // ---- Assertion (b): wiremock received flush POST with correct headers ----
        let received = mock_server.received_requests().await.unwrap();
        assert!(
            !received.is_empty(),
            "expected at least 1 flush request, got 0"
        );
        for req in &received {
            assert_eq!(req.method.as_str(), "POST", "flush should be a POST");
            let api_key_header = req
                .headers
                .get("DD-API-KEY")
                .expect("request should have DD-API-KEY header");
            assert_eq!(
                api_key_header
                    .to_str()
                    .expect("header should be valid UTF-8"),
                "test-api-key",
                "DD-API-KEY header mismatch"
            );
            // Verify the request body contains records
            let body: serde_json::Value =
                serde_json::from_slice(&req.body).expect("body should be valid JSON");
            assert!(
                body["records"].is_array(),
                "body should have records array: {body}"
            );
        }

        // ---- Assertion (c): CSV file contains succeeded metrics ----
        let csv_entries = csv_persistence::load_from_csv(&persist_path)
            .expect("CSV load should succeed after shutdown");
        assert!(
            csv_entries.contains_key("cpu.user"),
            "CSV should contain cpu.user (succeeded)"
        );
        assert!(
            csv_entries.contains_key("mem.free"),
            "CSV should contain mem.free (succeeded)"
        );

        // Verify TTL: each entry's expiry should be in the future (fresh TTL)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        for (name, expiry) in &csv_entries {
            assert!(
                *expiry > now,
                "metric '{name}' expiry {expiry} should be > current time {now}"
            );
        }

        // ---- Assertion (d): known-set only has succeeded metrics ----
        // "req.rate" was submitted in the shutdown flush but was NOT in
        // succeeded_metrics, so it must NOT appear in the CSV.
        assert!(
            !csv_entries.contains_key("req.rate"),
            "req.rate should NOT be in CSV (not in succeeded_metrics)"
        );
    }

    // ----- Shutdown persistence test (D-06, D-08) -----

    #[tokio::test]
    async fn test_shutdown_persists_csv_even_without_flush() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;
        let dir = tempfile::tempdir().unwrap();
        let persist_path = dir.path().join("known.csv");

        // Mock responds with the single metric as succeeded
        Mock::given(method("POST"))
            .and(path("/api/unstable/byoc/ingest/metadata/metric-metadata"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "succeeded_metrics": ["solo.metric"]
            })))
            .expect(1) // only the shutdown flush
            .mount(&mock_server)
            .await;

        let transform = Box::new(MetricMetadataTransform {
            config: MetricMetadataConfig {
                api_key: "test-api-key".to_string(),
                metadata_svc_url: mock_server.uri(),
                flush_interval_secs: 60, // long -- below batch_size, no timer trigger
                persist_interval_secs: 60, // long -- only shutdown path persists
                batch_size: 200,         // high -- single event won't trigger batch flush
                ttl_min_hours: 12,
                ttl_max_hours: 36,
                http_timeout_secs: 5,
                persist_file_path: persist_path.to_string_lossy().to_string(),
            },
            known_metrics: KnownMetrics::new(12, 36),
            pending: HashMap::new(),
            flush_client: FlushClient::new(
                "test-api-key".to_string(),
                mock_server.uri(),
                Duration::from_secs(5),
            )
            .expect("test client build should succeed"),
        });

        let events: Vec<Event> = vec![Event::Metric(Metric::new(
            "solo.metric",
            MetricKind::Absolute,
            MetricValue::Gauge { value: 7.0 },
        ))];

        // Single event below batch_size -- flush happens only at shutdown (D-06)
        let input: Pin<Box<dyn Stream<Item = Event> + Send>> =
            Box::pin(stream::iter(events.clone()));
        let output: Vec<Event> = transform.transform(input).collect().await;

        // Event should pass through unchanged
        assert_eq!(output.len(), 1, "single event should pass through");

        // CSV should have been persisted on shutdown (D-08)
        let csv_entries = csv_persistence::load_from_csv(&persist_path)
            .expect("CSV load should succeed after shutdown");
        assert!(
            csv_entries.contains_key("solo.metric"),
            "CSV should contain solo.metric after shutdown flush"
        );

        // Wiremock received exactly 1 request (the shutdown flush)
        let received = mock_server.received_requests().await.unwrap();
        assert_eq!(
            received.len(),
            1,
            "expected exactly 1 shutdown flush request"
        );
    }

    // ----- Parent directory validation -----

    #[tokio::test]
    #[serial_test::serial(env)]
    async fn test_build_fails_with_missing_parent_directory() {
        let saved = std::env::var("DD_API_KEY").ok();
        // SAFETY: test is single-threaded in nextest isolation; env mutation is safe.
        unsafe {
            std::env::set_var("DD_API_KEY", "test-api-key");
        }

        let cfg = MetricMetadataConfig {
            api_key: "test-api-key".to_string(),
            metadata_svc_url: "http://localhost:9999".to_string(),
            flush_interval_secs: 15,
            persist_interval_secs: 30,
            batch_size: 200,
            ttl_min_hours: 12,
            ttl_max_hours: 36,
            http_timeout_secs: 10,
            persist_file_path: "/nonexistent_dir_abc123/known.csv".to_string(),
        };

        let ctx = TransformContext::default();
        let result = cfg.build(&ctx).await;

        unsafe {
            match saved {
                Some(val) => std::env::set_var("DD_API_KEY", val),
                None => std::env::remove_var("DD_API_KEY"),
            }
        }

        match result {
            Ok(_) => panic!("build() should fail with nonexistent parent directory"),
            Err(err) => {
                let msg = err.to_string();
                assert!(
                    msg.contains("persist_file_path parent directory"),
                    "error should mention persist_file_path parent directory, got: {msg}"
                );
                assert!(
                    msg.contains("not accessible"),
                    "error should mention 'not accessible', got: {msg}"
                );
            }
        }
    }
}
