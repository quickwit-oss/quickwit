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

//! HTTP client for `/api/unstable/byoc/ingest/metadata/dual-shipped-metrics`.
//!
//! Wire format mirrors the Go `byoc-dualship-mgr/internal/client` package.

use std::time::Duration;

use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

use super::types::{Destination, MetricRecord};

const ENDPOINT_PATH: &str = "/api/unstable/byoc/ingest/metadata/dual-shipped-metrics";

#[derive(Serialize)]
struct FetchRequest {
    since_unix: i64,
}

#[derive(Deserialize)]
struct FetchResponse {
    #[serde(default)]
    metrics: Vec<RawMetric>,
}

#[derive(Deserialize)]
struct RawMetric {
    metric_name: String,
    destination: i32,
    last_updated_unix: i64,
}

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("timeout: dual-ship metadata service did not respond")]
    Timeout,
    #[error("network error: {0}")]
    Network(String),
    #[error("unauthorized: status {0}")]
    Unauthorized(u16),
    #[error("http {status}: {body}")]
    HttpStatus { status: u16, body: String },
    #[error("response parse error: {0}")]
    ResponseParse(String),
    #[error("unknown destination integer {0} for metric {1:?}")]
    UnknownDestination(i32, String),
}

/// HTTP fetcher for dual-ship metric routing.
///
/// `Debug` is intentionally not derived so the API key cannot leak into log
/// output (T-01-02 mitigation, same as `metric_metadata::FlushClient`).
pub struct DualShipFetcher {
    client: reqwest::Client,
    api_key: SecretString,
    metadata_svc_url: String,
}

impl DualShipFetcher {
    pub fn new(
        api_key: SecretString,
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

    pub async fn fetch(&self, since_unix: i64) -> Result<Vec<MetricRecord>, FetchError> {
        let url = format!(
            "{}{ENDPOINT_PATH}",
            self.metadata_svc_url.trim_end_matches('/')
        );

        let response = self
            .client
            .post(&url)
            .header("DD-API-KEY", self.api_key.expose_secret())
            .json(&FetchRequest { since_unix })
            .send()
            .await
            .map_err(|err| {
                if err.is_timeout() {
                    FetchError::Timeout
                } else {
                    FetchError::Network(err.to_string())
                }
            })?;

        let status = response.status();
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            return Err(FetchError::Unauthorized(status.as_u16()));
        }
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            warn!(status = status.as_u16(), %body, "dual-ship fetch failed");
            return Err(FetchError::HttpStatus {
                status: status.as_u16(),
                body,
            });
        }

        let body: FetchResponse = response
            .json()
            .await
            .map_err(|err| FetchError::ResponseParse(err.to_string()))?;

        let mut records = Vec::with_capacity(body.metrics.len());
        for raw in body.metrics {
            let destination = Destination::from_api_int(raw.destination).map_err(|_| {
                FetchError::UnknownDestination(raw.destination, raw.metric_name.clone())
            })?;
            records.push(MetricRecord {
                name: raw.metric_name,
                destination,
                last_updated_unix: raw.last_updated_unix,
            });
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn make_client(uri: &str, timeout: Duration) -> DualShipFetcher {
        DualShipFetcher::new(
            SecretString::from("test-api-key".to_string()),
            uri.to_string(),
            timeout,
        )
        .expect("client build should succeed")
    }

    #[tokio::test]
    async fn fetch_decodes_records_and_maps_destination() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path(ENDPOINT_PATH))
            .and(header("DD-API-KEY", "test-api-key"))
            .and(body_json(json!({"since_unix": 0})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "metrics": [
                    { "metric_name": "alpha", "destination": 1, "last_updated_unix": 100 },
                    { "metric_name": "bravo", "destination": 2, "last_updated_unix": 200 },
                    { "metric_name": "charlie", "destination": 3, "last_updated_unix": 300 },
                ]
            })))
            .mount(&server)
            .await;

        let client = make_client(&server.uri(), Duration::from_secs(2));
        let records = client.fetch(0).await.expect("fetch should succeed");
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].name, "alpha");
        assert_eq!(records[0].destination, Destination::Saas);
        assert_eq!(records[0].last_updated_unix, 100);
        assert_eq!(records[1].destination, Destination::Byoc);
        assert_eq!(records[2].destination, Destination::Dual);
    }

    #[tokio::test]
    async fn fetch_propagates_since_unix() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(ENDPOINT_PATH))
            .and(body_json(json!({"since_unix": 42})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"metrics": []})))
            .expect(1)
            .mount(&server)
            .await;

        let client = make_client(&server.uri(), Duration::from_secs(2));
        client.fetch(42).await.expect("fetch should succeed");
    }

    #[tokio::test]
    async fn fetch_returns_empty_when_metrics_field_missing() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(ENDPOINT_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .mount(&server)
            .await;

        let client = make_client(&server.uri(), Duration::from_secs(2));
        let records = client.fetch(0).await.expect("fetch should succeed");
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn fetch_maps_unauthorized() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(ENDPOINT_PATH))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        let client = make_client(&server.uri(), Duration::from_secs(2));
        let err = client.fetch(0).await.expect_err("fetch should fail on 401");
        assert!(matches!(err, FetchError::Unauthorized(401)));
    }

    #[tokio::test]
    async fn fetch_maps_forbidden_to_unauthorized() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(ENDPOINT_PATH))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        let client = make_client(&server.uri(), Duration::from_secs(2));
        let err = client.fetch(0).await.expect_err("fetch should fail on 403");
        assert!(matches!(err, FetchError::Unauthorized(403)));
    }

    #[tokio::test]
    async fn fetch_maps_500_to_http_status() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(ENDPOINT_PATH))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;

        let client = make_client(&server.uri(), Duration::from_secs(2));
        let err = client.fetch(0).await.expect_err("fetch should fail on 500");
        match err {
            FetchError::HttpStatus { status, body } => {
                assert_eq!(status, 500);
                assert!(body.contains("boom"));
            }
            other => panic!("expected HttpStatus, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fetch_maps_unknown_destination_int() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(ENDPOINT_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "metrics": [
                    { "metric_name": "alpha", "destination": 99, "last_updated_unix": 1 }
                ]
            })))
            .mount(&server)
            .await;

        let client = make_client(&server.uri(), Duration::from_secs(2));
        let err = client
            .fetch(0)
            .await
            .expect_err("fetch should reject unknown dest");
        match err {
            FetchError::UnknownDestination(99, name) => assert_eq!(name, "alpha"),
            other => panic!("expected UnknownDestination, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fetch_maps_timeout() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(ENDPOINT_PATH))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"metrics": []}))
                    .set_delay(Duration::from_secs(2)),
            )
            .mount(&server)
            .await;

        let client = make_client(&server.uri(), Duration::from_millis(1));
        let err = client.fetch(0).await.expect_err("fetch should time out");
        assert!(matches!(err, FetchError::Timeout));
    }
}
