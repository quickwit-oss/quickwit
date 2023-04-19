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

use hyper::client::HttpConnector;
use hyper::{Body, StatusCode};

/// A client for operations not supported by the official cluster rest client
pub struct TestClient {
    root_url: String,
    client: hyper::Client<HttpConnector, Body>,
}

impl TestClient {
    pub fn new(addr: SocketAddr) -> Self {
        let client = hyper::Client::builder()
            .pool_idle_timeout(Duration::from_secs(30))
            .http2_only(true)
            .build_http();
        let root_url = format!("http://{addr}");
        Self { root_url, client }
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
