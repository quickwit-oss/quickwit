// Copyright (C) 2024 Quickwit, Inc.
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

use std::collections::HashMap;
use std::iter::zip;
use std::mem;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use chitchat::transport::Transport;
use chitchat::{Chitchat, ChitchatId, VersionedValue};
use futures::Future;
use quickwit_common::pretty::PrettyDisplay;
use quickwit_proto::cluster::{
    ChitchatId as ProtoChitchatId, ClusterService, ClusterServiceClient, Digest,
    FetchClusterStateRequest, NodeDigest,
};
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::member::{GRPC_ADVERTISE_ADDR_KEY, READINESS_KEY, READINESS_VALUE_READY};
use crate::service::cluster_grpc_client;

const MAX_SAMPLES: usize = 5;
const MAX_GOSSIP_ROUNDS: usize = 3;

/// Bootstraps the cluster state by downloading it directly from a few selected nodes. It
/// first samples a few nodes from the peer seeds and then fetches the state from them via gRPC.
pub(crate) async fn spawn_bootstrap_cluster_state_task(
    cluster_id: String,
    transport: Arc<dyn Transport>,
    gossip_listen_addr: SocketAddr,
    chitchat: Arc<Mutex<Chitchat>>,
) {
    tokio::spawn(bootstrap_cluster_state(
        cluster_id,
        transport,
        gossip_listen_addr,
        chitchat,
        cluster_grpc_client,
    ));
}

async fn bootstrap_cluster_state<Factory, Fut>(
    cluster_id: String,
    transport: Arc<dyn Transport>,
    gossip_listen_addr: SocketAddr,
    chitchat: Arc<Mutex<Chitchat>>,
    grpc_client_factory: Factory,
) where
    Factory: Fn(SocketAddr) -> Fut,
    Fut: Future<Output = ClusterServiceClient>,
{
    let now = Instant::now();
    let mut chitchat_guard = chitchat.lock().await;

    let include_keys = vec![
        GRPC_ADVERTISE_ADDR_KEY.to_string(),
        READINESS_KEY.to_string(),
    ];
    let samples = match chitchat_guard
        .sample_from_seeds(
            &*transport,
            gossip_listen_addr,
            MAX_SAMPLES as u16,
            include_keys,
        )
        .await
    {
        Ok(samples) => samples,
        Err(error) => {
            error!(error=%error, "failed to sample nodes from peer seeds");
            return;
        }
    };
    let mut node_ids = Vec::with_capacity(MAX_SAMPLES);
    let mut grpc_advertise_addrs = Vec::with_capacity(MAX_SAMPLES);

    for (chitchat_id, node_state) in samples {
        if is_ready(&node_state) {
            if let Some(grpc_advertise_addr) = grpc_advertise_addr(&node_state) {
                node_ids.push(chitchat_id.node_id);
                grpc_advertise_addrs.push(grpc_advertise_addr);
            }
        }
    }
    if node_ids.is_empty() {
        info!("no nodes to bootstrap the cluster state from");
        return;
    }
    let mut digest = compute_digest(&chitchat_guard);
    drop(chitchat_guard);

    info!("bootstrapping cluster state from node(s) {node_ids:?}");

    let mut num_gossip_rounds = 0;

    for (node_id, grpc_advertise_addr) in zip(node_ids, grpc_advertise_addrs) {
        let mut cluster_client = grpc_client_factory(grpc_advertise_addr).await;

        let request = FetchClusterStateRequest {
            cluster_id: cluster_id.clone(),
            digest: Some(mem::take(&mut digest)),
        };
        let Ok(response) = cluster_client.fetch_cluster_state(request).await else {
            error!("failed to fetch cluster state from node `{node_id}`");
            continue;
        };
        num_gossip_rounds += 1;

        let mut chitchat_guard = chitchat.lock().await;

        for node_state in response.node_states {
            let proto_chitchat_id = node_state
                .chitchat_id
                .expect("`chitchat_id` should be a required field");
            let chitchat_id = ChitchatId {
                node_id: proto_chitchat_id.node_id.clone(),
                generation_id: proto_chitchat_id.generation_id,
                gossip_advertise_addr: proto_chitchat_id
                    .gossip_advertise_addr
                    .parse()
                    .expect("`gossip_advertise_addr` should be a valid socket address"),
            };
            let key_values = node_state.key_values.into_iter().map(|key_value| {
                (
                    key_value.key,
                    VersionedValue::new(key_value.value, key_value.version, key_value.is_tombstone),
                )
            });
            chitchat_guard.insert_or_update_node(&chitchat_id, key_values);
        }
        if num_gossip_rounds >= MAX_GOSSIP_ROUNDS {
            break;
        }
        digest = compute_digest(&chitchat_guard);
        drop(chitchat_guard);
    }
    info!(
        "bootstrapped cluster state in {}",
        now.elapsed().pretty_display()
    );
}

fn compute_digest(chitchat: &Chitchat) -> Digest {
    let node_digests: Vec<NodeDigest> = chitchat
        .node_states()
        .iter()
        .map(|(chitchat_id, node_state)| {
            let proto_chitchat_id = ProtoChitchatId {
                node_id: chitchat_id.node_id.clone(),
                generation_id: chitchat_id.generation_id,
                gossip_advertise_addr: chitchat_id.gossip_advertise_addr.to_string(),
            };
            let max_version = node_state.max_version();

            NodeDigest {
                chitchat_id: Some(proto_chitchat_id),
                max_version,
            }
        })
        .collect();
    Digest { node_digests }
}

fn grpc_advertise_addr(node_state: &HashMap<String, VersionedValue>) -> Option<SocketAddr> {
    node_state.get(GRPC_ADVERTISE_ADDR_KEY)?.value.parse().ok()
}

fn is_ready(node_state: &HashMap<String, VersionedValue>) -> bool {
    node_state
        .get(READINESS_KEY)
        .map(|value| value.value == READINESS_VALUE_READY)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use chitchat::transport::ChannelTransport;
    use quickwit_proto::cluster::MockClusterService;

    use super::*;
    use crate::create_cluster_for_test;

    #[tokio::test]
    async fn test_bootstrap_cluster_state() {
        let peer_seeds = vec![];
        let transport = Arc::new(ChannelTransport::default());
        let cluster = create_cluster_for_test(peer_seeds, &["indexer"], transport.clone(), true)
            .await
            .unwrap();
        let cluster_id = cluster.cluster_id().to_string();

        let grpc_client_factory = |_: SocketAddr| {
            Box::pin(async {
                let mut mock_cluster_service = MockClusterService::new();
                mock_cluster_service.expect_fetch_cluster_state().once();
                ClusterServiceClient::from(mock_cluster_service)
            })
        };

        let gossip_listen_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let chitchat = cluster.chitchat().await;

        bootstrap_cluster_state(
            cluster_id,
            transport,
            gossip_listen_addr,
            chitchat,
            grpc_client_factory,
        )
        .await;
        // TODO: Complete the test and add assertions.
    }
}
