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
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

use super::types::{MetadataMetricType, MetricTypeInfo};

// ---------------------------------------------------------------------------
// Wire format serde types (per D-01, matching Go UpsertMetricMetadataRequest)
// ---------------------------------------------------------------------------

/// Request body for POST to metric-metadata endpoint.
/// Field names match Go `UpsertMetricMetadataRequest` exactly.
#[derive(Serialize)]
struct UpsertRequest {
    records: Vec<UpsertRecord>,
}

/// Single record within the upsert request.
/// Maps from internal `MetricTypeInfo` + metric name.
#[derive(Serialize)]
struct UpsertRecord {
    metric_name: String,
    /// Serialized as lowercase string ("count", "rate", "gauge", "ddsketch").
    metric_type: MetadataMetricType,
    /// Reporting interval in seconds. Omitted when 0 to match Go `omitempty`.
    #[serde(skip_serializing_if = "is_zero")]
    interval: i64,
}

fn is_zero(val: &i64) -> bool {
    *val == 0
}

/// Response body from metric-metadata endpoint.
#[derive(Deserialize)]
struct UpsertResponse {
    /// Names of metrics successfully upserted. May be null, empty, or a subset.
    /// `Option<Vec<..>>` handles all three wire cases: missing field (`#[serde(default)]`
    /// → `None`), explicit `null` → `None`, and a present array → `Some(vec)`.
    #[serde(default)]
    succeeded_metrics: Option<Vec<String>>,
}

// ---------------------------------------------------------------------------
// Error type (thiserror enum per RESEARCH recommendation)
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum FlushError {
    #[error("http {status}: {body}")]
    HttpStatus { status: u16, body: String },
    #[error("timeout: metadata service did not respond")]
    Timeout,
    #[error("network error: {0}")]
    Network(String),
    #[error("response parse error: {0}")]
    ResponseParse(String),
}

// ---------------------------------------------------------------------------
// FlushClient (per D-03, D-04, D-05)
// ---------------------------------------------------------------------------

/// HTTP client for flushing pending metric metadata to the SaaS endpoint.
///
/// NOTE: Debug is intentionally NOT derived -- the `api_key` field must not
/// appear in log output (T-01-02: information disclosure mitigation).
pub struct FlushClient {
    client: reqwest::Client,
    api_key: String,
    metadata_svc_url: String,
}

impl FlushClient {
    pub fn new(
        api_key: String,
        metadata_svc_url: String,
        timeout: Duration,
    ) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder().timeout(timeout).build()?;
        Ok(Self {
            client,
            api_key,
            metadata_svc_url,
        })
    }

    pub async fn flush_pending(
        &self,
        pending: &HashMap<String, MetricTypeInfo>,
    ) -> Result<Vec<String>, FlushError> {
        let body = build_request_body(pending);
        let url = format!(
            "{}/api/unstable/byoc/ingest/metadata/metric-metadata",
            self.metadata_svc_url
        );

        let response = self
            .client
            .post(&url)
            .header("DD-API-KEY", &self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|err| {
                if err.is_timeout() {
                    FlushError::Timeout
                } else {
                    FlushError::Network(err.to_string())
                }
            })?;

        let status = response.status();
        if !status.is_success() {
            let body_text = response.text().await.unwrap_or_default();
            warn!(
                status = status.as_u16(),
                body = %body_text,
                "metadata flush failed"
            );
            return Err(FlushError::HttpStatus {
                status: status.as_u16(),
                body: body_text,
            });
        }

        let api_response: UpsertResponse = response
            .json()
            .await
            .map_err(|err| FlushError::ResponseParse(err.to_string()))?;

        Ok(api_response.succeeded_metrics.unwrap_or_default())
    }
}

fn build_request_body(pending: &HashMap<String, MetricTypeInfo>) -> UpsertRequest {
    let records = pending
        .iter()
        .map(|(name, info)| UpsertRecord {
            metric_name: name.clone(),
            metric_type: info.metric_type,
            interval: i64::from(info.interval),
        })
        .collect();
    UpsertRequest {
        records,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    /// Helper: build a FlushClient pointing at the given mock server URI.
    fn build_test_client(server_uri: &str) -> FlushClient {
        FlushClient::new(
            "test-key".to_string(),
            server_uri.to_string(),
            Duration::from_secs(5),
        )
        .expect("client build should succeed")
    }

    /// Helper: build a pending map with the given metric entries.
    fn pending_with(
        entries: &[(&str, MetadataMetricType, u32)],
    ) -> HashMap<String, MetricTypeInfo> {
        let mut map = HashMap::new();
        for (name, metric_type, interval) in entries {
            map.insert(
                name.to_string(),
                MetricTypeInfo {
                    metric_type: *metric_type,
                    interval: *interval,
                },
            );
        }
        map
    }

    // ----- Test 1: All metrics succeed -----

    #[tokio::test]
    async fn test_flush_all_succeeded() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/unstable/byoc/ingest/metadata/metric-metadata"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "succeeded_metrics": ["cpu.user", "mem.free"]
            })))
            .mount(&mock_server)
            .await;

        let client = build_test_client(&mock_server.uri());
        let pending = pending_with(&[
            ("cpu.user", MetadataMetricType::Gauge, 0),
            ("mem.free", MetadataMetricType::Gauge, 0),
        ]);

        let mut succeeded = client
            .flush_pending(&pending)
            .await
            .expect("flush should succeed");
        succeeded.sort();

        assert_eq!(succeeded, vec!["cpu.user", "mem.free"]);
    }

    // ----- Test 2: Partial success (D-10) -----

    #[tokio::test]
    async fn test_flush_partial_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/unstable/byoc/ingest/metadata/metric-metadata"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "succeeded_metrics": ["cpu.user"]
            })))
            .mount(&mock_server)
            .await;

        let client = build_test_client(&mock_server.uri());
        let pending = pending_with(&[
            ("cpu.user", MetadataMetricType::Gauge, 0),
            ("mem.free", MetadataMetricType::Gauge, 0),
        ]);

        let succeeded = client
            .flush_pending(&pending)
            .await
            .expect("flush should succeed");

        assert_eq!(succeeded, vec!["cpu.user".to_string()]);
        assert!(
            !succeeded.contains(&"mem.free".to_string()),
            "mem.free should NOT be in succeeded list"
        );
    }

    // ----- Test 3: Empty succeeded_metrics -----

    #[tokio::test]
    async fn test_flush_empty_response() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/unstable/byoc/ingest/metadata/metric-metadata"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"succeeded_metrics": []})),
            )
            .mount(&mock_server)
            .await;

        let client = build_test_client(&mock_server.uri());
        let pending = pending_with(&[("cpu.user", MetadataMetricType::Gauge, 0)]);

        let succeeded = client
            .flush_pending(&pending)
            .await
            .expect("flush should succeed");

        assert!(succeeded.is_empty(), "expected empty succeeded list");
    }

    // ----- Test 4: null succeeded_metrics -----

    #[tokio::test]
    async fn test_flush_null_succeeded_metrics() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/unstable/byoc/ingest/metadata/metric-metadata"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"succeeded_metrics": null})),
            )
            .mount(&mock_server)
            .await;

        let client = build_test_client(&mock_server.uri());
        let pending = pending_with(&[("cpu.user", MetadataMetricType::Gauge, 0)]);

        let succeeded = client
            .flush_pending(&pending)
            .await
            .expect("flush should succeed");

        assert!(
            succeeded.is_empty(),
            "null succeeded_metrics should be treated as empty"
        );
    }

    // ----- Test 5: Missing succeeded_metrics field entirely -----

    #[tokio::test]
    async fn test_flush_missing_succeeded_field() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/unstable/byoc/ingest/metadata/metric-metadata"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&mock_server)
            .await;

        let client = build_test_client(&mock_server.uri());
        let pending = pending_with(&[("cpu.user", MetadataMetricType::Gauge, 0)]);

        let succeeded = client
            .flush_pending(&pending)
            .await
            .expect("flush should succeed");

        assert!(
            succeeded.is_empty(),
            "missing succeeded_metrics field should be treated as empty"
        );
    }

    // ----- Test 6: HTTP 500 error (D-08) -----

    #[tokio::test]
    async fn test_flush_http_500_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/unstable/byoc/ingest/metadata/metric-metadata"))
            .respond_with(
                ResponseTemplate::new(500)
                    .set_body_json(serde_json::json!({"message": "internal error"})),
            )
            .mount(&mock_server)
            .await;

        let client = build_test_client(&mock_server.uri());
        let pending = pending_with(&[("cpu.user", MetadataMetricType::Gauge, 0)]);

        let err = client
            .flush_pending(&pending)
            .await
            .expect_err("flush should fail on 500");

        match err {
            FlushError::HttpStatus { status, .. } => {
                assert_eq!(status, 500);
            }
            other => panic!("expected HttpStatus error, got: {other}"),
        }
    }

    // ----- Test 7: HTTP 401 error -----

    #[tokio::test]
    async fn test_flush_http_401_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/unstable/byoc/ingest/metadata/metric-metadata"))
            .respond_with(
                ResponseTemplate::new(401)
                    .set_body_json(serde_json::json!({"message": "unauthorized"})),
            )
            .mount(&mock_server)
            .await;

        let client = build_test_client(&mock_server.uri());
        let pending = pending_with(&[("cpu.user", MetadataMetricType::Gauge, 0)]);

        let err = client
            .flush_pending(&pending)
            .await
            .expect_err("flush should fail on 401");

        match err {
            FlushError::HttpStatus { status, .. } => {
                assert_eq!(status, 401);
            }
            other => panic!("expected HttpStatus error, got: {other}"),
        }
    }

    // ----- Test 8: Request has correct headers -----

    #[tokio::test]
    async fn test_flush_request_has_correct_headers() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/unstable/byoc/ingest/metadata/metric-metadata"))
            .and(header("DD-API-KEY", "test-key"))
            .and(header("Content-Type", "application/json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "succeeded_metrics": ["cpu.user"]
            })))
            .mount(&mock_server)
            .await;

        let client = build_test_client(&mock_server.uri());
        let pending = pending_with(&[("cpu.user", MetadataMetricType::Gauge, 0)]);

        // If headers don't match, wiremock returns 404 which would cause an
        // HttpStatus error instead of success.
        let succeeded = client
            .flush_pending(&pending)
            .await
            .expect("flush should succeed with correct headers");

        assert_eq!(succeeded, vec!["cpu.user".to_string()]);
    }

    // ----- Test 9: Request body matches wire format (D-01) -----

    #[tokio::test]
    async fn test_flush_request_body_matches_wire_format() {
        let mock_server = MockServer::start().await;

        // Use a single metric for deterministic body assertion.
        Mock::given(method("POST"))
            .and(path("/api/unstable/byoc/ingest/metadata/metric-metadata"))
            .and(wiremock::matchers::body_json(serde_json::json!({
                "records": [
                    {
                        "metric_name": "system.cpu.user",
                        "metric_type": "rate",
                        "interval": 10
                    }
                ]
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "succeeded_metrics": ["system.cpu.user"]
            })))
            .mount(&mock_server)
            .await;

        let client = build_test_client(&mock_server.uri());
        let pending = pending_with(&[("system.cpu.user", MetadataMetricType::Rate, 10)]);

        let succeeded = client
            .flush_pending(&pending)
            .await
            .expect("flush should succeed with correct wire format");

        assert_eq!(succeeded, vec!["system.cpu.user".to_string()]);
    }

    // ----- Test 10: interval=0 omitted from JSON (Go omitempty semantics) -----

    #[tokio::test]
    async fn test_flush_interval_zero_omitted_in_json() {
        let mock_server = MockServer::start().await;

        // body_json matcher: interval field must NOT be present for gauge (interval=0).
        Mock::given(method("POST"))
            .and(path("/api/unstable/byoc/ingest/metadata/metric-metadata"))
            .and(wiremock::matchers::body_json(serde_json::json!({
                "records": [
                    {
                        "metric_name": "cpu.idle",
                        "metric_type": "gauge"
                    }
                ]
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "succeeded_metrics": ["cpu.idle"]
            })))
            .mount(&mock_server)
            .await;

        let client = build_test_client(&mock_server.uri());
        let pending = pending_with(&[("cpu.idle", MetadataMetricType::Gauge, 0)]);

        let succeeded = client
            .flush_pending(&pending)
            .await
            .expect("flush should succeed; interval=0 omitted matches wire format");

        assert_eq!(succeeded, vec!["cpu.idle".to_string()]);
    }

    // ----- Test 11: Timeout -----

    #[tokio::test]
    async fn test_flush_timeout() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/unstable/byoc/ingest/metadata/metric-metadata"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"succeeded_metrics": []}))
                    .set_delay(Duration::from_secs(2)),
            )
            .mount(&mock_server)
            .await;

        // Build client with very short timeout (1ms) so it times out.
        let client = FlushClient::new(
            "test-key".to_string(),
            mock_server.uri(),
            Duration::from_millis(1),
        )
        .expect("client build should succeed");

        let pending = pending_with(&[("cpu.user", MetadataMetricType::Gauge, 0)]);

        let err = client
            .flush_pending(&pending)
            .await
            .expect_err("flush should fail with timeout");

        match err {
            FlushError::Timeout => {} // expected
            other => panic!("expected Timeout error, got: {other}"),
        }
    }
}
