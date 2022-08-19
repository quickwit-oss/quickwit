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


use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use hyper::client::HttpConnector;
use hyper::{Body, Uri};
use itertools::Itertools;
use quickwit_cluster::QuickwitService;
use quickwit_common::new_coolid;
use quickwit_common::uri::Uri as QuickwitUri;
use quickwit_config::QuickwitConfig;
use quickwit_control_plane::MetastoreService;
use quickwit_indexing::IndexingServiceState;
use quickwit_metastore::{quickwit_metastore_uri_resolver, IndexMetadata};
use quickwit_proto::tonic::transport::Endpoint;
use quickwit_search::{create_search_service_client, SearchServiceClient};
use rand::seq::SliceRandom;
use serde::__private::from_utf8_lossy;
use tempfile::TempDir;
use tokio_stream::StreamExt;

use crate::cluster_api::SerializedCluster;
use crate::serve_quickwit;

pub struct QuickwitRestClient {
    addr: SocketAddr,
    api_root: String,
    client: hyper::Client<HttpConnector, Body>,
}

impl QuickwitRestClient {
    pub fn new(addr: SocketAddr) -> Self {
        let client = hyper::Client::builder()
            .pool_idle_timeout(Duration::from_secs(30))
            .http2_only(true)
            .build_http();
        let api_root = format!("http://{}/api/v1", self.addr);
        Self { addr, api_root, client }
    }

    pub async fn cluster_state(&self) -> anyhow::Result<SerializedCluster> {
        let uri = format!("{}/cluster", self.api_root)
            .parse::<hyper::Uri>()
            .unwrap();
        let mut response = self.client.get(uri).await?;
        let mut body = Vec::new();
        while let Some(chunk) = response.body_mut().next().await {
            body.extend_from_slice(&chunk?);
        }
        let cluster_state: SerializedCluster = serde_json::from_slice(&body)?;
        Ok(cluster_state)
    }

    pub async fn indexing_service_state(&self) -> anyhow::Result<IndexingServiceState> {
        let uri = format!("http://{}/api/v1/indexing", self.addr)
            .parse::<hyper::Uri>()
            .unwrap();
        let mut response = self.client.get(uri).await?;
        let mut body = Vec::new();
        while let Some(chunk) = response.body_mut().next().await {
            body.extend_from_slice(&chunk?);
        }
        println!("indexing body {}", from_utf8_lossy(&body));
        let indexing_service_state: IndexingServiceState = serde_json::from_slice(&body)?;
        Ok(indexing_service_state)
    }
}