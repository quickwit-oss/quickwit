/*
 * Copyright (C) 2021 Quickwit Inc.
 *
 * Quickwit is offered under the AGPL v3.0 and as commercial software.
 * For commercial licensing, contact us at hello@quickwit.io.
 *
 * AGPL:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

use std::net::SocketAddr;

use http::Uri;
use tonic::transport::Endpoint;

use crate::SearchServiceClient;

#[derive(Debug, Clone)]
pub struct WrappedSearchServiceClient {
    pub client: SearchServiceClient,
    pub grpc_addr: SocketAddr,
}

impl WrappedSearchServiceClient {
    fn new(client: SearchServiceClient, grpc_addr: SocketAddr) -> Self {
        Self { client, grpc_addr }
    }

    /// Return the grpc_addr the underlying client connects to.
    pub fn grpc_addr(&self) -> SocketAddr {
        self.grpc_addr
    }

    /// Returns the unterlying client.
    pub fn client(&mut self) -> &mut SearchServiceClient {
        &mut self.client
    }
}

/// Create a SearchServiceClient with SocketAddr as an argument.
/// It will try to reconnect to the node automatically.
pub async fn create_search_service_client(
    grpc_addr: SocketAddr,
) -> anyhow::Result<WrappedSearchServiceClient> {
    let uri = Uri::builder()
        .scheme("http")
        .authority(grpc_addr.to_string().as_str())
        .path_and_query("/")
        .build()?;

    // Create a channel with connect_lazy to automatically reconnect to the node.
    let channel = Endpoint::from(uri).connect_lazy()?;

    let client = WrappedSearchServiceClient::new(SearchServiceClient::new(channel), grpc_addr);
    Ok(client)
}
