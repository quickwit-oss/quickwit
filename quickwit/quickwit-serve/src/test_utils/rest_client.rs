// Copyright (C) 2023 Quickwit, Inc.
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

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{bail, Context};
use hyper::client::HttpConnector;
use hyper::{Body, Request, Response, StatusCode};
use quickwit_cluster::ClusterSnapshot;
use quickwit_indexing::actors::IndexingServiceCounters;
use serde::de::DeserializeOwned;
use tokio_stream::StreamExt;

pub struct QuickwitRestClient {
    root_url: String,
    client: hyper::Client<HttpConnector, Body>,
}

impl QuickwitRestClient {
    pub fn new(addr: SocketAddr) -> Self {
        let client = hyper::Client::builder()
            .pool_idle_timeout(Duration::from_secs(30))
            .http2_only(true)
            .build_http();
        let root_url = format!("http://{addr}");
        Self { root_url, client }
    }

    pub async fn ingest_data(&self, index_id: &str, ndjson_doc: &str) -> anyhow::Result<()> {
        let uri = format!("{}/api/v1/{index_id}/ingest", self.root_url)
            .parse::<hyper::Uri>()
            .unwrap();
        let request = Request::builder()
            .uri(uri)
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(ndjson_doc.to_string()))
            .unwrap();
        let response = self
            .client
            .request(request)
            .await
            .context("Failed to emit request")?;
        if response.status() != StatusCode::OK {
            let body_bytes = hyper::body::to_bytes(response.into_body()).await.unwrap();
            let body_str = String::from_utf8_lossy(&body_bytes);
            bail!("error when creating index: {body_str}");
        }
        Ok(())
    }

    pub async fn create_index(&self, index_config_yaml: &str) -> anyhow::Result<()> {
        let uri = format!("{}/api/v1/indexes", self.root_url)
            .parse::<hyper::Uri>()
            .unwrap();
        let request = Request::builder()
            .uri(uri)
            .method("POST")
            .header("content-type", "application/yaml")
            .body(Body::from(index_config_yaml.to_string()))
            .unwrap();
        let response = self.client.request(request).await.unwrap();
        if response.status() == StatusCode::OK {
            return Ok(());
        }
        let body_bytes = hyper::body::to_bytes(response.into_body()).await.unwrap();
        let body_string = String::from_utf8(body_bytes.to_vec()).unwrap();
        Err(anyhow::anyhow!("error when creating index: {body_string}"))
    }

    pub async fn cluster_snapshot(&self) -> anyhow::Result<ClusterSnapshot> {
        let uri = format!("{}/api/v1/cluster", self.root_url)
            .parse::<hyper::Uri>()
            .unwrap();
        let response = self.client.get(uri).await?;
        let cluster_state = parse_body(response).await?;
        Ok(cluster_state)
    }

    pub async fn indexing_service_counters(&self) -> anyhow::Result<IndexingServiceCounters> {
        let uri = format!("{}/api/v1/indexing", self.root_url)
            .parse::<hyper::Uri>()
            .unwrap();
        let response = self.client.get(uri).await?;
        let indexing_service_counters = parse_body(response).await?;
        Ok(indexing_service_counters)
    }

    pub async fn is_live(&self) -> anyhow::Result<bool> {
        let uri = format!("{}/health/livez", self.root_url)
            .parse::<hyper::Uri>()
            .unwrap();
        let response = self.client.get(uri).await?;
        if response.status() == StatusCode::OK {
            return Ok(true);
        }
        Ok(false)
    }

    pub async fn is_ready(&self) -> anyhow::Result<bool> {
        let uri = format!("{}/health/readyz", self.root_url)
            .parse::<hyper::Uri>()
            .unwrap();
        let response = self.client.get(uri).await?;
        if response.status() == StatusCode::OK {
            return Ok(true);
        }
        Ok(false)
    }

    pub fn client(&self) -> hyper::Client<HttpConnector, Body> {
        self.client.clone()
    }

    pub fn root_url(&self) -> String {
        self.root_url.clone()
    }
}

async fn parse_body<T: DeserializeOwned>(mut response: Response<Body>) -> anyhow::Result<T> {
    if response.status() != StatusCode::OK {
        anyhow::bail!("Unexpected status {}", response.status());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.body_mut().next().await {
        body.extend_from_slice(&chunk?);
    }
    let deserialized_element: T = serde_json::from_slice(&body)?;
    Ok(deserialized_element)
}
