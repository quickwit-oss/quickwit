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

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use bytesize::ByteSize;
use quickwit_actors::Mailbox;
use quickwit_cluster::Cluster;
use quickwit_config::NodeConfig;
use quickwit_config::service::QuickwitService;
use quickwit_control_plane::control_plane::{ControlPlane, GetDebugInfo};
use quickwit_ingest::{IngestRouter, Ingester};
use quickwit_proto::developer::{
    DeveloperError, DeveloperResult, DeveloperService, GetDebugInfoRequest, GetDebugInfoResponse,
};
use serde_json::json;

use crate::{BuildInfo, QuickwitServices, RuntimeInfo};

#[derive(Clone)]
pub(crate) struct DeveloperApiServer {
    node_config: Arc<NodeConfig>,
    cluster: Cluster,
    control_plane_mailbox_opt: Option<Mailbox<ControlPlane>>,
    ingest_router_opt: Option<IngestRouter>,
    ingester_opt: Option<Ingester>,
}

impl fmt::Debug for DeveloperApiServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeveloperApiServer").finish()
    }
}

impl DeveloperApiServer {
    pub const MAX_GRPC_MESSAGE_SIZE: ByteSize = ByteSize::mib(100);

    pub fn from_services(services: &QuickwitServices) -> Self {
        Self {
            node_config: services.node_config.clone(),
            cluster: services.cluster.clone(),
            control_plane_mailbox_opt: services.control_plane_server_opt.clone(),
            ingest_router_opt: services.ingest_router_opt.clone(),
            ingester_opt: services.ingester_opt.clone(),
        }
    }
}

#[async_trait]
impl DeveloperService for DeveloperApiServer {
    async fn get_debug_info(
        &self,
        request: GetDebugInfoRequest,
    ) -> DeveloperResult<GetDebugInfoResponse> {
        let roles: HashSet<QuickwitService> = request
            .roles
            .into_iter()
            .map(|role| role.parse())
            .collect::<anyhow::Result<_>>()
            .map_err(|error| DeveloperError::InvalidArgument(error.to_string()))?;

        let cluster_snapshot = self.cluster.snapshot().await;

        let mut debug_info = json!({
            "build_info": BuildInfo::get(),
            "runtime_info": RuntimeInfo::get(),
            "node_config": self.node_config,
            "cluster_membership_info": json!({
                "ready_nodes": cluster_snapshot.ready_nodes,
                "live_nodes": cluster_snapshot.live_nodes,
                "dead_nodes": cluster_snapshot.dead_nodes,
                "chitchat_state": cluster_snapshot.chitchat_state_snapshot.node_states,
            })
        });
        if let Some(control_plane_mailbox) = &self.control_plane_mailbox_opt
            && (roles.is_empty() || roles.contains(&QuickwitService::ControlPlane))
        {
            debug_info["control_plane"] = match control_plane_mailbox.ask(GetDebugInfo).await {
                Ok(debug_info) => debug_info,
                Err(error) => {
                    json!({"error": error.to_string()})
                }
            };
        }
        if let Some(ingest_router) = &self.ingest_router_opt {
            debug_info["ingest_router"] = ingest_router.debug_info().await;
        }
        if let Some(ingester) = &self.ingester_opt
            && (roles.is_empty() || roles.contains(&QuickwitService::Indexer))
        {
            debug_info["ingester"] = ingester.debug_info().await;
        };
        let debug_info_json = serde_json::to_vec(&debug_info).map_err(|error| {
            let message = format!("failed to JSON serialize debug info: {error}");
            DeveloperError::Internal(message)
        })?;
        let response = GetDebugInfoResponse {
            debug_info_json: Bytes::from(debug_info_json),
        };
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use quickwit_cluster::{ChannelTransport, create_cluster_for_test};
    use serde_json::Value as JsonValue;

    use super::*;

    #[tokio::test]
    async fn test_developer_api_server_get_debug_info() {
        let peer_seeds = Vec::new();
        let transport = ChannelTransport::default();
        let self_node_readiness = true;
        let cluster = create_cluster_for_test(
            peer_seeds,
            &["metastore", "control-plane", "indexer"],
            &transport,
            self_node_readiness,
        )
        .await
        .unwrap();

        let node_config = Arc::new(NodeConfig::for_test());

        let developer_api_server = DeveloperApiServer {
            node_config,
            cluster,
            control_plane_mailbox_opt: None,
            ingest_router_opt: None,
            ingester_opt: None,
        };
        let request = GetDebugInfoRequest { roles: Vec::new() };
        let response = developer_api_server.get_debug_info(request).await.unwrap();
        let debug_info: JsonValue = serde_json::from_slice(&response.debug_info_json).unwrap();

        assert!(debug_info["build_info"].is_object());
        assert!(debug_info["runtime_info"].is_object());
        assert!(debug_info["node_config"].is_object());
        assert!(debug_info["cluster_membership_info"].is_object());

        // TODO: Test control plane and ingester debug info.
    }
}
