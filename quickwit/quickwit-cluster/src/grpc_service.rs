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

use std::net::SocketAddr;

use bytesize::ByteSize;
use itertools::Itertools;
use once_cell::sync::Lazy;
use quickwit_common::tower::{ClientGrpcConfig, GrpcMetricsLayer, make_channel};
use quickwit_proto::cluster::cluster_service_grpc_server::ClusterServiceGrpcServer;
use quickwit_proto::cluster::{
    ChitchatId as ProtoChitchatId, ClusterError, ClusterResult, ClusterService,
    ClusterServiceClient, ClusterServiceGrpcServerAdapter, FetchClusterStateRequest,
    FetchClusterStateResponse, NodeState as ProtoNodeState, VersionedKeyValue,
};
use tonic::async_trait;

use crate::Cluster;

const MAX_MESSAGE_SIZE: ByteSize = ByteSize::mib(64);

static CLUSTER_GRPC_CLIENT_METRICS_LAYER: Lazy<GrpcMetricsLayer> =
    Lazy::new(|| GrpcMetricsLayer::new("cluster", "client"));
static CLUSTER_GRPC_SERVER_METRICS_LAYER: Lazy<GrpcMetricsLayer> =
    Lazy::new(|| GrpcMetricsLayer::new("cluster", "server"));

pub(crate) async fn cluster_grpc_client(
    socket_addr: SocketAddr,
    client_grpc_config: ClientGrpcConfig,
) -> ClusterServiceClient {
    let channel = make_channel(socket_addr, client_grpc_config).await;

    ClusterServiceClient::tower()
        .stack_layer(CLUSTER_GRPC_CLIENT_METRICS_LAYER.clone())
        .build_from_channel(socket_addr, channel, MAX_MESSAGE_SIZE, None)
}

pub fn cluster_grpc_server(
    cluster: Cluster,
) -> ClusterServiceGrpcServer<ClusterServiceGrpcServerAdapter> {
    ClusterServiceClient::tower()
        .stack_layer(CLUSTER_GRPC_SERVER_METRICS_LAYER.clone())
        .build(cluster)
        .as_grpc_service(MAX_MESSAGE_SIZE)
}

#[async_trait]
impl ClusterService for Cluster {
    async fn fetch_cluster_state(
        &self,
        request: FetchClusterStateRequest,
    ) -> ClusterResult<FetchClusterStateResponse> {
        if request.cluster_id != self.cluster_id() {
            return Err(ClusterError::Internal("wrong cluster".to_string()));
        }
        let chitchat = self.chitchat().await;
        let chitchat_guard = chitchat.lock().await;

        let num_nodes = chitchat_guard.node_states().len();
        let mut proto_node_states = Vec::with_capacity(num_nodes);

        for (chitchat_id, node_state) in chitchat_guard.node_states() {
            let proto_chitchat_id = ProtoChitchatId {
                node_id: chitchat_id.node_id.clone(),
                generation_id: chitchat_id.generation_id,
                gossip_advertise_addr: chitchat_id.gossip_advertise_addr.to_string(),
            };

            let key_values: Vec<VersionedKeyValue> = node_state
                .key_values_including_deleted()
                .map(|(key, versioned_value)| {
                    let key_value_status_proto = match versioned_value.status {
                        chitchat::DeletionStatus::Set => {
                            quickwit_proto::cluster::DeletionStatus::Set
                        }
                        chitchat::DeletionStatus::Deleted(_) => {
                            quickwit_proto::cluster::DeletionStatus::Deleted
                        }
                        chitchat::DeletionStatus::DeleteAfterTtl(_) => {
                            quickwit_proto::cluster::DeletionStatus::DeleteAfterTtl
                        }
                    };
                    VersionedKeyValue {
                        key: key.to_string(),
                        value: versioned_value.value.clone(),
                        version: versioned_value.version,
                        status: key_value_status_proto as i32,
                    }
                })
                .sorted_unstable_by_key(|key_value| key_value.version)
                .collect();
            if key_values.is_empty() {
                continue;
            }
            let proto_node_state = ProtoNodeState {
                chitchat_id: Some(proto_chitchat_id),
                key_values,
                max_version: node_state.max_version(),
                last_gc_version: node_state.last_gc_version(),
            };
            proto_node_states.push(proto_node_state);
        }
        let response = FetchClusterStateResponse {
            cluster_id: request.cluster_id,
            node_states: proto_node_states,
        };
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use chitchat::transport::ChannelTransport;

    use super::*;
    use crate::create_cluster_for_test;
    use crate::member::{ENABLED_SERVICES_KEY, GRPC_ADVERTISE_ADDR_KEY, READINESS_KEY};

    #[tokio::test]
    async fn test_fetch_cluster_state() {
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();

        let cluster_id = cluster.cluster_id().to_string();
        let node_id = cluster.self_node_id().to_owned();

        cluster.set_self_key_value("foo", "bar").await;

        let fetch_cluster_state_request = FetchClusterStateRequest {
            cluster_id: cluster_id.clone(),
        };
        let mut fetch_cluster_state_response = cluster
            .fetch_cluster_state(fetch_cluster_state_request)
            .await
            .unwrap();
        assert_eq!(
            fetch_cluster_state_response.cluster_id,
            cluster.cluster_id()
        );
        assert_eq!(fetch_cluster_state_response.node_states.len(), 1);

        let node_state = &mut fetch_cluster_state_response.node_states[0];

        let chitchat_id = node_state.chitchat_id.clone().unwrap();
        assert_eq!(chitchat_id.node_id, node_id);
        assert_eq!(chitchat_id.generation_id, 1);

        node_state
            .key_values
            .sort_unstable_by(|left, right| left.key.cmp(&right.key));

        assert_eq!(node_state.key_values.len(), 4);
        assert_eq!(node_state.key_values[0].key, ENABLED_SERVICES_KEY);
        assert_eq!(node_state.key_values[0].value, "indexer");

        assert_eq!(node_state.key_values[1].key, "foo");
        assert_eq!(node_state.key_values[1].value, "bar");

        assert_eq!(node_state.key_values[2].key, GRPC_ADVERTISE_ADDR_KEY);

        assert_eq!(node_state.key_values[3].key, READINESS_KEY);
        assert_eq!(node_state.key_values[3].value, "READY");
    }
}
