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

use hyper::service::Service;
use hyper::Uri;

/// Tries to connect at most 3 times to `SocketAddr`.
/// If not successful, returns an error.
/// This is a convenient function to wait before sending gRPC requests
/// to this `SocketAddr`.
pub async fn wait_for_server_ready(socket_addr: SocketAddr) -> anyhow::Result<()> {
    let mut num_attempts = 0;
    let max_num_attempts = 5;
    let uri = Uri::builder()
        .scheme("http")
        .authority(socket_addr.to_string().as_str())
        .path_and_query("/")
        .build()?;
    while num_attempts < max_num_attempts {
        tokio::time::sleep(Duration::from_millis(20 * (num_attempts + 1))).await;
        let mut http = hyper::client::connect::HttpConnector::new();
        match http.call(uri.clone()).await {
            Ok(_) => break,
            Err(_) => {
                println!(
                    "Failed to connect to `{}` failed, retrying {}/{}",
                    socket_addr,
                    num_attempts + 1,
                    max_num_attempts
                );
                num_attempts += 1;
            }
        }
    }
    if num_attempts == max_num_attempts {
        anyhow::bail!("Too many attempts to connect to `{}`", socket_addr);
    }
    Ok(())
}
