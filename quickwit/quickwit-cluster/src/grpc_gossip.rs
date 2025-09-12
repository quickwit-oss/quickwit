// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::iter::zip;
use std::net::SocketAddr;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use chitchat::{Chitchat, ChitchatId, NodeState, VersionedValue};
use futures::Future;
use quickwit_common::pretty::PrettyDisplay;
use quickwit_common::tower::ClientGrpcConfig;
use quickwit_proto::cluster::{ClusterService, ClusterServiceClient, FetchClusterStateRequest};
use rand::seq::IteratorRandom;
use tokio::sync::{Mutex, watch};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::WatchStream;
use tracing::{info, warn};

use crate::grpc_service::cluster_grpc_client;
use crate::member::NodeStateExt;
use crate::metrics::CLUSTER_METRICS;

const MAX_GOSSIP_PEERS: usize = 3;

/// select a few and then fetches the state from them via gRPC.
pub(crate) async fn spawn_catchup_callback_task(
    cluster_id: String,
    self_chitchat_id: ChitchatId,
    weak_chitchat: Weak<Mutex<Chitchat>>,
    live_nodes_rx: watch::Receiver<BTreeMap<ChitchatId, NodeState>>,
    mut catchup_callback_rx: watch::Receiver<()>,
    client_grpc_config: ClientGrpcConfig,
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
                |socket_addr| cluster_grpc_client(socket_addr, client_grpc_config.clone()),
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

async fn perform_grpc_gossip_rounds<ClusterServiceClientFactory, Fut>(
    cluster_id: String,
    self_chitchat_id: &ChitchatId,
    chitchat: Arc<Mutex<Chitchat>>,
    live_nodes_rx: watch::Receiver<BTreeMap<ChitchatId, NodeState>>,
    grpc_client_factory: ClusterServiceClientFactory,
) where
    ClusterServiceClientFactory: Fn(SocketAddr) -> Fut,
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
        let cluster_client = grpc_client_factory(grpc_advertise_addr).await;

        let request = FetchClusterStateRequest {
            cluster_id: cluster_id.clone(),
        };
        let Ok(response) = cluster_client.fetch_cluster_state(request).await else {
            warn!("failed to fetch cluster state from node `{node_id}`");
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
            let now = tokio::time::Instant::now();
            let key_values = proto_node_state.key_values.into_iter().map(|key_value| {
                let status: chitchat::DeletionStatus = match key_value.status() {
                    quickwit_proto::cluster::DeletionStatus::Set => chitchat::DeletionStatus::Set,
                    quickwit_proto::cluster::DeletionStatus::Deleted => {
                        chitchat::DeletionStatus::Deleted(now)
                    }
                    quickwit_proto::cluster::DeletionStatus::DeleteAfterTtl => {
                        chitchat::DeletionStatus::DeleteAfterTtl(now)
                    }
                };
                (
                    key_value.key,
                    VersionedValue {
                        value: key_value.value,
                        version: key_value.version,
                        status,
                    },
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
                node_states.len() < MAX_GOSSIP_PEERS
                    && node_states
                        .values()
                        .filter(|node_state| {
                            find_gossip_candidate_grpc_addr(self_chitchat_id, node_state).is_some()
                        })
                        .count()
                        < MAX_GOSSIP_PEERS
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
        .values()
        .filter_map(|node_state| {
            find_gossip_candidate_grpc_addr(self_chitchat_id, node_state)
                .map(|grpc_addr| (&node_state.chitchat_id().node_id, grpc_addr))
        })
        .choose_multiple(&mut rand::thread_rng(), MAX_GOSSIP_PEERS)
        .into_iter()
        .map(|(node_id, grpc_addr)| (node_id.clone(), grpc_addr))
        .unzip()
}

/// Returns the gRPC advertise address of the node if it is a gossip candidate.
fn find_gossip_candidate_grpc_addr(
    self_chitchat_id: &ChitchatId,
    node_state: &NodeState,
) -> Option<SocketAddr> {
    // Ignore self node, including previous generations, and nodes that are not ready.
    if self_chitchat_id.node_id == node_state.chitchat_id().node_id || !node_state.is_ready() {
        return None;
    }
    node_state.grpc_advertise_addr().ok()
}

#[cfg(test)]
mod tests {
    use chitchat::transport::ChannelTransport;
    use quickwit_proto::cluster::{
        ChitchatId as ProtoChitchatId, DeletionStatus, FetchClusterStateResponse,
        MockClusterService, NodeState as ProtoNodeState, VersionedKeyValue,
    };

    use super::*;
    use crate::change::tests::NodeStateBuilder;
    use crate::create_cluster_for_test;
    use crate::member::{GRPC_ADVERTISE_ADDR_KEY, READINESS_KEY, READINESS_VALUE_READY};

    #[tokio::test]
    async fn test_find_gossip_candidate_grpc_addr() {
        let gossip_advertise_addr: SocketAddr = "127.0.0.1:10000".parse().unwrap();
        let grpc_advertise_addr: SocketAddr = "127.0.0.1:10001".parse().unwrap();
        let self_chitchat_id =
            ChitchatId::new("test-node-foo".to_string(), 1, gossip_advertise_addr);

        let node_state = NodeStateBuilder::default()
            .with_readiness(true)
            .with_grpc_advertise_addr(grpc_advertise_addr)
            .build();
        let grpc_addr = find_gossip_candidate_grpc_addr(&self_chitchat_id, &node_state).unwrap();
        assert_eq!(grpc_addr, grpc_advertise_addr);

        let node_state = NodeStateBuilder::default()
            .with_readiness(false)
            .with_grpc_advertise_addr(grpc_advertise_addr)
            .build();
        let grpc_addr_opt = find_gossip_candidate_grpc_addr(&self_chitchat_id, &node_state);
        assert!(grpc_addr_opt.is_none());

        let node_state = NodeStateBuilder::default().with_readiness(false).build();
        let grpc_addr_opt = find_gossip_candidate_grpc_addr(&self_chitchat_id, &node_state);
        assert!(grpc_addr_opt.is_none());

        let self_chitchat_id = ChitchatId::new("test-node".to_string(), 1, gossip_advertise_addr);
        let node_state = NodeStateBuilder::default()
            .with_readiness(true)
            .with_grpc_advertise_addr(grpc_advertise_addr)
            .build();
        let grpc_addr_opt = find_gossip_candidate_grpc_addr(&self_chitchat_id, &node_state);
        assert!(grpc_addr_opt.is_none());
    }

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

                                    status: DeletionStatus::Set as i32,
                                }],
                                max_version: 2,
                                last_gc_version: 1,
                            }],
                            ..Default::default()
                        };
                        Ok(response)
                    });
                ClusterServiceClient::from_mock(mock_cluster_service)
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
