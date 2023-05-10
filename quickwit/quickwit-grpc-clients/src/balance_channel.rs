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

use std::time::Duration;

use futures::Stream;
use quickwit_cluster::ClusterChange;
use quickwit_config::service::QuickwitService;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::discover::Change;
use tower::timeout::Timeout;
use tracing::info;

const CLIENT_TIMEOUT_DURATION: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(100)
} else {
    Duration::from_secs(5)
};

pub async fn create_balance_channel_from_cluster_change_stream(
    service: QuickwitService,
    mut ready_nodes_change_stream: impl Stream<Item = ClusterChange> + Send + Unpin + 'static,
) -> Timeout<Channel> {
    let (channel, channel_tx) = Channel::balance_channel(100);
    let timeout_channel = Timeout::new(channel, CLIENT_TIMEOUT_DURATION);
    let future = async move {
        while let Some(cluster_change) = ready_nodes_change_stream.next().await {
            match cluster_change {
                ClusterChange::Add(node) if node.enabled_services().contains(&service) => {
                    let grpc_addr = node.grpc_advertise_addr();
                    let uri = Uri::builder()
                        .scheme("http")
                        .authority(grpc_addr.to_string())
                        .path_and_query("/")
                        .build()
                        .expect("");
                    let endpoint = Endpoint::from(uri).connect_timeout(Duration::from_secs(5));
                    let change = Change::Insert(grpc_addr, endpoint);
                    info!(node_id=%node.node_id(), grpc_addr=?grpc_addr, "Adding node to {} pool.", service);
                    if channel_tx.send(change).await.is_err() {
                        break;
                    }
                }
                ClusterChange::Remove(node) if node.enabled_services().contains(&service) => {
                    let grpc_addr = node.grpc_advertise_addr();
                    let change = Change::Remove(grpc_addr);
                    info!(node_id=%node.node_id(), grpc_addr=?grpc_addr, "Removing node from {} pool.", service);
                    if channel_tx.send(change).await.is_err() {
                        break;
                    }
                }
                _ => {}
            }
        }
    };
    tokio::spawn(future);
    timeout_channel
}
