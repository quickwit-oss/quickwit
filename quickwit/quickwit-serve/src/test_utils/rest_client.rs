// Copyright (C) 2022 Quickwit, Inc.
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

use hyper::client::HttpConnector;
use hyper::{Body, Response, StatusCode};
use quickwit_cluster::ClusterSnapshot;
use quickwit_indexing::actors::IndexingServiceState;
use serde::de::DeserializeOwned;
use tokio_stream::StreamExt;

pub struct QuickwitRestClient {
    api_root: String,
    client: hyper::Client<HttpConnector, Body>,
}

impl QuickwitRestClient {
    pub fn new(addr: SocketAddr) -> Self {
        let client = hyper::Client::builder()
            .pool_idle_timeout(Duration::from_secs(30))
            .http2_only(true)
            .build_http();
        let api_root = format!("http://{}/api/v1", addr);
        Self { api_root, client }
    }

    pub async fn cluster_snapshot(&self) -> anyhow::Result<ClusterSnapshot> {
        let uri = format!("{}/cluster", self.api_root)
            .parse::<hyper::Uri>()
            .unwrap();
        let response = self.client.get(uri).await?;
        let cluster_state = parse_body(response).await?;
        Ok(cluster_state)
    }

    pub async fn indexing_service_state(&self) -> anyhow::Result<IndexingServiceState> {
        let uri = format!("{}/indexing", self.api_root)
            .parse::<hyper::Uri>()
            .unwrap();
        let response = self.client.get(uri).await?;
        let indexing_service_state = parse_body(response).await?;
        Ok(indexing_service_state)
    }

    pub async fn is_ready(&self) -> anyhow::Result<bool> {
        let uri = format!("{}/health/readyz", self.api_root)
            .parse::<hyper::Uri>()
            .unwrap();
        let response = self.client.get(uri).await?;
        if response.status() == StatusCode::OK {
            return Ok(true);
        }
        Ok(false)
    }
}

async fn parse_body<T: DeserializeOwned>(mut response: Response<Body>) -> anyhow::Result<T> {
    if response.status() != StatusCode::OK {
        anyhow::bail!("Unexepected status {}", response.status());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.body_mut().next().await {
        body.extend_from_slice(&chunk?);
    }
    let deserialized_element: T = serde_json::from_slice(&body)?;
    Ok(deserialized_element)
}
