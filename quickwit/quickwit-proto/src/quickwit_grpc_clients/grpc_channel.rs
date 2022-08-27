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

use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::discover::Change;
use tower::timeout::Timeout;
use tracing::{debug, error, info};

use crate::quickwit_models::{ClusterMember, QuickwitService};

const CLIENT_TIMEOUT_DURATION: Duration = if cfg!(test) {
    Duration::from_millis(100)
} else {
    Duration::from_secs(5)
};

/// Create a balance [`Channel`] with endpoints dynamically updated from cluster
/// members and for a given [`QuickwitService`].
pub async fn create_balance_channel_and_udpate_from_members(
    mut members_watch_channel: WatchStream<Vec<ClusterMember>>,
    service: QuickwitService,
) -> anyhow::Result<Timeout<Channel>> {
    // Create a balance channel whose endpoint can be updated thanks to a sender.
    let (channel, channel_tx) = Channel::balance_channel(10);

    // A request send to to a channel with no endpoint will hang. To avoid a blocking request, a
    // timeout is added to the channel.
    // TODO: ideally, we want to implement our own `Channel::balance_channel` to
    // properly raise a timeout error.
    let timeout_channel = Timeout::new(channel, CLIENT_TIMEOUT_DURATION);

    let mut grpc_addresses_in_use = HashSet::new();

    // Watch for cluster members changes and dynamically update channel endpoint.
    tokio::spawn(async move {
        while let Some(new_members) = members_watch_channel.next().await {
            let new_grpc_addresses = get_metastore_grpc_addresses(&new_members, &service);
            update_channel_endpoints(&new_grpc_addresses, &grpc_addresses_in_use, &channel_tx)
                .await?; // <- Fails if the channel is closed. In this case we can stop the loop.
            grpc_addresses_in_use = new_grpc_addresses;
        }
        Result::<_, anyhow::Error>::Ok(())
    });

    Ok(timeout_channel)
}

/// Updates channel endpoints by:
/// - Sending `Change::Insert` grpc addresses not already present in `grpc_addresses_in_use`.
/// - Sending `Change::Remove` event on grpc addresses present in `grpc_addresses_in_use` but not in
///   `updated_members`.
async fn update_channel_endpoints(
    new_grpc_addresses: &HashSet<SocketAddr>,
    grpc_addresses_in_use: &HashSet<SocketAddr>,
    endpoint_channel_rx: &Sender<Change<SocketAddr, Endpoint>>,
) -> anyhow::Result<()> {
    if new_grpc_addresses.is_empty() {
        error!("No Metastore service is available in the cluster.");
    }

    for leaving_grpc_address in grpc_addresses_in_use.difference(new_grpc_addresses) {
        debug!(
            "Removing gRPC address `{}` from `MetastoreGrpcClient`.",
            leaving_grpc_address
        );
        endpoint_channel_rx
            .send(Change::Remove(*leaving_grpc_address))
            .await?;
    }

    for new_grpc_address in new_grpc_addresses.difference(grpc_addresses_in_use) {
        info!(
            "Adding gRPC address `{}` to `MetastoreGrpcClient`.",
            new_grpc_address
        );
        let new_grpc_uri_result = Uri::builder()
            .scheme("http")
            .authority(new_grpc_address.to_string().as_str())
            .path_and_query("/")
            .build();
        if let Ok(new_grpc_uri) = new_grpc_uri_result {
            let new_grpc_endpoint = Endpoint::from(new_grpc_uri);
            endpoint_channel_rx
                .send(Change::Insert(*new_grpc_address, new_grpc_endpoint))
                .await?;
        } else {
            error!(
                "Cannot build `Uri` from socket address `{}`, address ignored.",
                new_grpc_address
            );
        }
    }

    Ok(())
}

fn get_metastore_grpc_addresses(
    members: &[ClusterMember],
    service: &QuickwitService,
) -> HashSet<SocketAddr> {
    members
        .iter()
        .filter(|member| member.available_services.contains(service))
        .map(|member| member.grpc_advertise_addr)
        .collect()
}
