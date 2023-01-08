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
use std::ops::Sub;
use std::time::Duration;

use quickwit_cluster::ClusterMember;
use quickwit_config::service::QuickwitService;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::sync::watch::Receiver;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::discover::Change;
use tower::timeout::Timeout;
use tracing::{error, info};

const CLIENT_TIMEOUT_DURATION: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(100)
} else {
    Duration::from_secs(5)
};

/// Create a balance [`Channel`] with endpoints dynamically updated from cluster
/// members and for a given [`QuickwitService`].
/// Returns the balance channel along with a watcher containing the
/// number of registered endpoints in the channel.
/// A timeout wraps the balance channel as a channel with no endpoint will hang.
/// This avoids a request to block indefinitely
// TODO: ideally, we want to implement our own `Channel::balance_channel` to
// properly raise a timeout error.
pub async fn create_balance_channel_from_watched_members(
    mut members_watch_channel: WatchStream<Vec<ClusterMember>>,
    service: QuickwitService,
) -> anyhow::Result<(Timeout<Channel>, Receiver<usize>)> {
    // Create a balance channel whose endpoint can be updated thanks to a sender.
    let (channel, channel_tx) = Channel::balance_channel(10);

    let timeout_channel = Timeout::new(channel, CLIENT_TIMEOUT_DURATION);
    let (pool_size_tx, pool_size_rx) = watch::channel(0);

    // Watch for cluster members changes and dynamically update channel endpoint.
    tokio::spawn({
        let mut current_grpc_address_pool = HashSet::new();
        async move {
            while let Some(new_members) = members_watch_channel.next().await {
                let new_grpc_address_pool = filter_grpc_addresses(&new_members, &service);
                if let Err(error) = update_channel_endpoints(
                    &new_grpc_address_pool,
                    &current_grpc_address_pool,
                    &channel_tx,
                    &service,
                )
                .await
                {
                    // If it fails, just log an error and stop the loop.
                    error!("Failed to update balance channel endpoints: {error:?}");
                };
                current_grpc_address_pool = new_grpc_address_pool;
                // TODO: Expose number of metastore servers in the pool as a Prometheus metric.
                // The pool size channel can be closed if it's not used. Just ignore the send error.
                let _ = pool_size_tx.send(current_grpc_address_pool.len());
            }
            Result::<_, anyhow::Error>::Ok(())
        }
    });

    Ok((timeout_channel, pool_size_rx))
}

fn filter_grpc_addresses(
    members: &[ClusterMember],
    quickwit_service: &QuickwitService,
) -> HashSet<SocketAddr> {
    members
        .iter()
        .filter(|member| member.enabled_services.contains(quickwit_service))
        .map(|member| member.grpc_advertise_addr)
        .collect()
}

/// Updates channel endpoints by:
/// - Sending `Change::Insert` grpc addresses not already present in `grpc_addresses_in_use`.
/// - Sending `Change::Remove` event on grpc addresses present in `grpc_addresses_in_use` but not in
///   `updated_members`.
async fn update_channel_endpoints(
    new_grpc_address_pool: &HashSet<SocketAddr>,
    current_grpc_address_pool: &HashSet<SocketAddr>,
    channel_endpoint_tx: &Sender<Change<SocketAddr, Endpoint>>,
    quickwit_service: &QuickwitService,
) -> anyhow::Result<()> {
    if new_grpc_address_pool.is_empty() {
        error!("No metastore servers available in the cluster.");
    }
    let leaving_grpc_addresses = current_grpc_address_pool.sub(new_grpc_address_pool);
    if !leaving_grpc_addresses.is_empty() {
        info!(
            // TODO: Log node IDs along with the addresses.
            server_addresses=?leaving_grpc_addresses,
            "Removing `{quickwit_service}` nodes from client pool.",
        );
        for leaving_grpc_address in leaving_grpc_addresses {
            channel_endpoint_tx
                .send(Change::Remove(leaving_grpc_address))
                .await?;
        }
    }
    let new_grpc_addresses = new_grpc_address_pool.sub(current_grpc_address_pool);
    if !new_grpc_addresses.is_empty() {
        info!(
            // TODO: Log node IDs along with the addresses.
            server_addresses=?new_grpc_addresses,
            "Adding `{quickwit_service}` servers to client pool.",
        );
        for new_grpc_address in new_grpc_addresses {
            let new_grpc_uri = Uri::builder()
                .scheme("http")
                .authority(new_grpc_address.to_string())
                .path_and_query("/")
                .build()
                .expect("Failed to build URI. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.");
            let new_grpc_endpoint = Endpoint::from(new_grpc_uri);
            channel_endpoint_tx
                .send(Change::Insert(new_grpc_address, new_grpc_endpoint))
                .await?;
        }
    }
    Ok(())
}
