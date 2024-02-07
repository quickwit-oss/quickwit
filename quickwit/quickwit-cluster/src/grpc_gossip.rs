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

use std::collections::BTreeMap;
use std::iter::zip;
use std::net::SocketAddr;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use chitchat::{Chitchat, ChitchatId, NodeState, VersionedValue};
use futures::Future;
use quickwit_common::pretty::PrettyDisplay;
use quickwit_proto::cluster::{ClusterService, ClusterServiceClient, FetchClusterStateRequest};
use rand::seq::IteratorRandom;
use tokio::sync::{watch, Mutex};
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;
use tracing::{error, info};

use crate::grpc_service::cluster_grpc_client;
use crate::member::NodeStateExt;
use crate::metrics::CLUSTER_METRICS;

const MAX_GOSSIP_ROUNDS: usize = 3;

/// select a few and then fetches the state from them via gRPC.
pub(crate) async fn spawn_catchup_callback_task(
    cluster_id: String,
    self_chitchat_id: ChitchatId,
    weak_chitchat: Weak<Mutex<Chitchat>>,
    live_nodes_rx: watch::Receiver<BTreeMap<ChitchatId, NodeState>>,
    mut catchup_callback_rx: watch::Receiver<()>,
) {
    let catchup_callback_future = async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.tick().await;

        loop {
            let Some(chitchat) = weak_chitchat.upgrade() else {
                return;
            };
            perform_grpc_gossip_rounds(
                cluster_id.clone(),
                &self_chitchat_id,
                chitchat,
                live_nodes_rx.clone(),
                cluster_grpc_client,
            )
            .await;

            interval.tick().await;

            if catchup_callback_rx.changed().await.is_err() {
                return;
            }
        }
    };
    tokio::spawn(catchup_callback_future);
}

async fn perform_grpc_gossip_rounds<Factory, Fut>(
    cluster_id: String,
    self_chitchat_id: &ChitchatId,
    chitchat: Arc<Mutex<Chitchat>>,
    live_nodes_rx: watch::Receiver<BTreeMap<ChitchatId, NodeState>>,
    grpc_client_factory: Factory,
) where
    Factory: Fn(SocketAddr) -> Fut,
    Fut: Future<Output = ClusterServiceClient>,
{
    wait_for_gossip_candidates(
        self_chitchat_id,
        live_nodes_rx.clone(),
        Duration::from_secs(10),
    )
    .await;

    let now = Instant::now();
    let (node_ids, grpc_advertise_addrs) =
        select_gossip_candidates(self_chitchat_id, live_nodes_rx);

    if node_ids.is_empty() {
        info!("no peer nodes to pull the cluster state from");
        return;
    }
    info!("pulling cluster state from node(s): {node_ids:?}");

    for (node_id, grpc_advertise_addr) in zip(node_ids, grpc_advertise_addrs) {
        let mut cluster_client = grpc_client_factory(grpc_advertise_addr).await;

        let request = FetchClusterStateRequest {
            cluster_id: cluster_id.clone(),
        };
        let Ok(response) = cluster_client.fetch_cluster_state(request).await else {
            error!("failed to fetch cluster state from node `{node_id}`");
            continue;
        };
        CLUSTER_METRICS.grpc_gossip_rounds_total.inc();

        let mut chitchat_guard = chitchat.lock().await;

        for proto_node_state in response.node_states {
            let proto_chitchat_id = proto_node_state
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
            if chitchat_id == *self_chitchat_id {
                continue;
            }
            let key_values = proto_node_state.key_values.into_iter().map(|key_value| {
                (
                    key_value.key,
                    VersionedValue::new(key_value.value, key_value.version, key_value.is_tombstone),
                )
            });
            chitchat_guard.reset_node_state(
                &chitchat_id,
                key_values,
                proto_node_state.max_version,
                proto_node_state.last_gc_version,
            );
        }
    }
    info!("pulled cluster state in {}", now.elapsed().pretty_display());
}

async fn wait_for_gossip_candidates(
    self_chitchat_id: &ChitchatId,
    live_nodes_rx: watch::Receiver<BTreeMap<ChitchatId, NodeState>>,
    timeout_after: Duration,
) {
    let live_nodes_stream = WatchStream::new(live_nodes_rx);
    let _ = tokio::time::timeout(
        timeout_after,
        live_nodes_stream
            .skip_while(|node_states| {
                node_states.len() < MAX_GOSSIP_ROUNDS
                    && node_states
                        .iter()
                        .filter(|(chitchat_id, node_state)| {
                            *chitchat_id != self_chitchat_id && is_candidate_for_gossip(node_state)
                        })
                        .count()
                        < MAX_GOSSIP_ROUNDS
            })
            .next(),
    )
    .await;
}

fn select_gossip_candidates(
    self_chitchat_id: &ChitchatId,
    live_nodes_rx: watch::Receiver<BTreeMap<ChitchatId, NodeState>>,
) -> (Vec<String>, Vec<SocketAddr>) {
    live_nodes_rx
        .borrow()
        .iter()
        .filter_map(|(chitchat_id, node_state)| {
            if chitchat_id != self_chitchat_id && node_state.is_ready() {
                if let Ok(grpc_advertise_addr) = node_state.grpc_advertise_addr() {
                    return Some((&chitchat_id.node_id, grpc_advertise_addr));
                }
            }
            None
        })
        .choose_multiple(&mut rand::thread_rng(), MAX_GOSSIP_ROUNDS)
        .into_iter()
        .map(|(node_id, grpc_advertise_addr)| (node_id.clone(), grpc_advertise_addr))
        .unzip()
}

fn is_candidate_for_gossip(node_state: &NodeState) -> bool {
    node_state.is_ready() && node_state.grpc_advertise_addr().is_ok()
}

#[cfg(test)]
mod tests {
    use chitchat::transport::ChannelTransport;
    use quickwit_proto::cluster::{
        ChitchatId as ProtoChitchatId, FetchClusterStateResponse, MockClusterService,
        NodeState as ProtoNodeState, VersionedKeyValue,
    };

    use super::*;
    use crate::create_cluster_for_test;
    use crate::member::{GRPC_ADVERTISE_ADDR_KEY, READINESS_KEY, READINESS_VALUE_READY};

    #[tokio::test]
    async fn test_perform_grpc_gossip_rounds() {
        let peer_seeds = Vec::new();
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(peer_seeds, &["indexer"], &transport, true)
            .await
            .unwrap();
        let cluster_id = cluster.cluster_id().to_string();
        let self_chitchat_id = cluster.self_chitchat_id();
        let chitchat = cluster.chitchat().await;

        let grpc_client_factory = |_: SocketAddr| {
            Box::pin(async {
                let mut mock_cluster_service = MockClusterService::new();
                mock_cluster_service
                    .expect_fetch_cluster_state()
                    .returning(|_request| {
                        let response = FetchClusterStateResponse {
                            node_states: vec![ProtoNodeState {
                                chitchat_id: Some(ProtoChitchatId {
                                    node_id: "node-4".to_string(),
                                    generation_id: 0,
                                    gossip_advertise_addr: "127.0.0.1:14000".to_string(),
                                }),
                                key_values: vec![VersionedKeyValue {
                                    key: "foo".to_string(),
                                    value: "bar".to_string(),
                                    version: 2,
                                    is_tombstone: false,
                                }],
                                max_version: 2,
                                last_gc_version: 1,
                            }],
                            ..Default::default()
                        };
                        Ok(response)
                    });
                ClusterServiceClient::from(mock_cluster_service)
            })
        };
        let live_nodes = BTreeMap::from_iter([
            {
                let chitchat_id = ChitchatId::for_local_test(11_000);
                let mut node_state = NodeState::for_test();

                node_state.set(GRPC_ADVERTISE_ADDR_KEY, "127.0.0.1:11001");
                node_state.set(READINESS_KEY, READINESS_VALUE_READY);
                (chitchat_id, node_state)
            },
            {
                let chitchat_id = ChitchatId::for_local_test(12_000);
                let mut node_state = NodeState::for_test();

                node_state.set(GRPC_ADVERTISE_ADDR_KEY, "127.0.0.1:12001");
                node_state.set(READINESS_KEY, READINESS_VALUE_READY);
                (chitchat_id, node_state)
            },
            {
                let chitchat_id = ChitchatId::for_local_test(13_000);
                let mut node_state = NodeState::for_test();

                node_state.set(GRPC_ADVERTISE_ADDR_KEY, "127.0.0.1:13001");
                node_state.set(READINESS_KEY, READINESS_VALUE_READY);
                (chitchat_id, node_state)
            },
        ]);
        let (_live_nodes_tx, live_nodes_rx) = watch::channel(live_nodes);

        perform_grpc_gossip_rounds(
            cluster_id,
            self_chitchat_id,
            chitchat.clone(),
            live_nodes_rx,
            grpc_client_factory,
        )
        .await;

        let chitchat_mutex_guard = chitchat.lock().await;
        let chitchat_id = ChitchatId {
            node_id: "node-4".to_string(),
            generation_id: 0,
            gossip_advertise_addr: "127.0.0.1:14000".parse().unwrap(),
        };
        let node_state = chitchat_mutex_guard.node_state(&chitchat_id).unwrap();
        assert_eq!(node_state.num_key_values(), 1);
        assert_eq!(node_state.get("foo").unwrap(), "bar");
        assert_eq!(node_state.max_version(), 2);
        assert_eq!(node_state.last_gc_version(), 1);
    }
}
