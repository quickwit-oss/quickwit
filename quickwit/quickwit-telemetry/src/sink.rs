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

use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;
use reqwest::redirect::Policy;
use tokio::sync::mpsc::UnboundedSender;

use crate::payload::TelemetryPayload;

/// Telemetry ingest API URL
const DEFAULT_TELEMETRY_INGEST_API_URL: &str = "https://telemetry.quickwit.io/";

fn telemetry_ingest_api_url() -> String {
    if let Some(ingest_api_url) = std::env::var_os("TELEMETRY_INGEST_API") {
        ingest_api_url.to_string_lossy().to_string()
    } else {
        DEFAULT_TELEMETRY_INGEST_API_URL.to_string()
    }
}

#[async_trait]
pub trait Sink: Send + Sync + 'static {
    async fn send_payload(&self, payload: TelemetryPayload);
}
pub struct HttpClient {
    client: Client,
    endpoint: String,
}

impl HttpClient {
    pub fn try_new() -> Option<Self> {
        let client = Client::builder()
            .redirect(Policy::limited(3))
            .timeout(Duration::from_secs(10))
            .build()
            .ok()?;
        Some(HttpClient {
            client,
            endpoint: telemetry_ingest_api_url(),
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

#[async_trait]
impl Sink for UnboundedSender<TelemetryPayload> {
    async fn send_payload(&self, payload: TelemetryPayload) {
        let _ = self.send(payload);
    }
}

#[async_trait]
impl Sink for HttpClient {
    async fn send_payload(&self, payload: TelemetryPayload) {
        // Note that we swallow the error if any
        let _ = self.client.post(&self.endpoint).json(&payload).send().await;
    }
}
