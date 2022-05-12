// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::redirect::Policy;
use reqwest::Client;
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
