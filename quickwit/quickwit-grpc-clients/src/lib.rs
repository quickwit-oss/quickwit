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

pub mod balance_channel;
pub mod control_plane_client;
pub mod service_client_pool;

pub use balance_channel::create_balance_channel_from_watched_members;
pub use control_plane_client::ControlPlaneGrpcClient;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

// For tests.
pub async fn create_channel_from_duplex_stream(
    client: tokio::io::DuplexStream,
) -> anyhow::Result<Channel> {
    let mut client = Some(client);
    let channel = Endpoint::try_from("http://test.server")?
        .connect_with_connector(service_fn(move |_: Uri| {
            let client = client.take();
            async move {
                client.ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::Other, "Client already taken")
                })
            }
        }))
        .await?;
    Ok(channel)
}
