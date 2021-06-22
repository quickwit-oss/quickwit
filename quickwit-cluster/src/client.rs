//  Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::net::SocketAddr;

use http::Uri;
use tonic::transport::Channel;
use tonic::transport::Endpoint;

use quickwit_proto::search_service_client::SearchServiceClient;

/// Create a SearchServiceClient with SocketAddr as an argument.
/// It will try to reconnect to the node automatically.
pub async fn create_search_service_client(
    grpc_addr: SocketAddr,
) -> anyhow::Result<SearchServiceClient<Channel>> {
    let uri = Uri::builder()
        .scheme("http")
        .authority(grpc_addr.to_string().as_str())
        .path_and_query("/")
        .build()?;

    // Create a channel with connect_lazy to automatically reconnect to the node.
    let channel = Endpoint::from(uri).connect_lazy()?;

    let client = SearchServiceClient::new(channel);

    Ok(client)
}
