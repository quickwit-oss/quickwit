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

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use glob::{MatchOptions, Pattern as GlobPattern};
use quickwit_cluster::Cluster;
use quickwit_config::service::QuickwitService;
use quickwit_proto::developer::{DeveloperService, DeveloperServiceClient, GetDebugInfoRequest};
use quickwit_proto::types::{NodeId, NodeIdRef};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tokio::time::timeout;
use tracing::error;
use warp::{Filter, Rejection, Reply};

use super::DeveloperApiServer;
use crate::with_arg;

#[derive(Deserialize)]
struct DebugInfoQueryParams {
    // Comma-separated list of case insensitive node ID glob patterns to restrict the debug
    // information to.
    node_ids: Option<String>,
    // Comma-separated list of roles to restrict the debug information to.
    roles: Option<String>,
}

#[utoipa::path(
    get,
    tag = "Debug",
    path = "/debug",
    responses(
        (status = 200, description = "Successfully fetched debug info."),
    ),
)]
/// Get debug information for the nodes in the cluster.
pub(super) fn debug_handler(
    cluster: Cluster,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path("debug")
        .and(warp::path::end())
        .and(with_arg(cluster))
        .and(warp::query::<DebugInfoQueryParams>())
        .then(get_node_debug_infos)
}

async fn get_node_debug_infos(
    cluster: Cluster,
    query_params: DebugInfoQueryParams,
) -> warp::reply::Response {
    let node_id_patterns = if let Some(node_ids) = &query_params.node_ids {
        match NodeIdGlobPatterns::try_from_comma_separated_patterns(node_ids) {
            Ok(node_id_patterns) => node_id_patterns,
            Err(error) => {
                return warp::reply::with_status(
                    format!(
                        "failed to parse node ID glob patterns `{}`: {error}",
                        query_params.node_ids.as_deref().unwrap_or("")
                    ),
                    warp::http::StatusCode::BAD_REQUEST,
                )
                .into_response()
            }
        }
    } else {
        NodeIdGlobPatterns::default()
    };
    let target_roles: HashSet<QuickwitService> = if let Some(roles) = query_params.roles {
        let target_roles_res = roles.split(',').map(|role| role.parse()).collect();

        match target_roles_res {
            Ok(target_roles) => target_roles,
            Err(error) => {
                return warp::reply::with_status(
                    format!("failed to parse roles `{roles}`: {error}"),
                    warp::http::StatusCode::BAD_REQUEST,
                )
                .into_response()
            }
        }
    } else {
        HashSet::new()
    };
    let ready_nodes = cluster.ready_nodes().await;
    let mut debug_infos: HashMap<NodeId, JsonValue> = HashMap::with_capacity(ready_nodes.len());

    let mut get_debug_info_futures = FuturesUnordered::new();

    for ready_node in ready_nodes {
        if node_id_patterns.matches(ready_node.node_id()) {
            let node_id = ready_node.node_id().to_owned();
            let client = DeveloperServiceClient::from_channel(
                ready_node.grpc_advertise_addr(),
                ready_node.channel(),
                DeveloperApiServer::MAX_GRPC_MESSAGE_SIZE,
            );
            let roles = target_roles.iter().map(|role| role.to_string()).collect();
            let request = GetDebugInfoRequest { roles };
            let get_debug_info_future = async move {
                let get_debug_info_res =
                    timeout(Duration::from_secs(5), client.get_debug_info(request)).await;
                (node_id, get_debug_info_res)
            };
            get_debug_info_futures.push(get_debug_info_future);
        }
    }
    while let Some(get_debug_info_res) = get_debug_info_futures.next().await {
        match get_debug_info_res {
            (node_id, Ok(Ok(debug_info_response))) => {
                match serde_json::from_slice(&debug_info_response.debug_info_json) {
                    Ok(debug_info) => {
                        debug_infos.insert(node_id, debug_info);
                    }
                    Err(error) => {
                        error!(%node_id, %error, "failed to parse JSON debug info from node");
                    }
                };
            }
            (node_id, Ok(Err(error))) => {
                error!(%node_id, %error, "failed to get debug info from node");
            }
            (node_id, Err(_elpased)) => {
                error!(%node_id, "get debug info request timed out");
            }
        }
    }
    warp::reply::json(&debug_infos).into_response()
}

#[derive(Debug)]
struct NodeIdGlobPatterns(HashSet<GlobPattern>, MatchOptions);

impl Default for NodeIdGlobPatterns {
    fn default() -> Self {
        let glob_patterns = HashSet::new();
        let match_options = MatchOptions {
            case_sensitive: false,
            ..Default::default()
        };
        Self(glob_patterns, match_options)
    }
}

impl NodeIdGlobPatterns {
    fn try_from_comma_separated_patterns(comma_separated_patterns: &str) -> anyhow::Result<Self> {
        let glob_patterns: HashSet<GlobPattern> = comma_separated_patterns
            .split(',')
            .filter(|pattern| !pattern.is_empty())
            .map(GlobPattern::new)
            .collect::<Result<_, _>>()?;
        let match_options = MatchOptions {
            case_sensitive: false,
            ..Default::default()
        };
        Ok(Self(glob_patterns, match_options))
    }

    fn matches(&self, node_id: &NodeIdRef) -> bool {
        if self.0.is_empty() {
            return true;
        }
        self.0
            .iter()
            .any(|pattern| pattern.matches_with(node_id.as_str(), self.1))
    }
}

#[cfg(test)]
mod tests {
    use quickwit_cluster::{create_cluster_for_test, ChannelTransport};

    use super::*;

    #[tokio::test]
    async fn test_developer_api_debug_handler() {
        let peer_seeds = Vec::new();
        let transport = ChannelTransport::default();
        let self_node_readiness = true;
        let cluster = create_cluster_for_test(
            peer_seeds,
            &["control-plane"],
            &transport,
            self_node_readiness,
        )
        .await
        .unwrap();

        let debug_handler = debug_handler(cluster);

        let response = warp::test::request()
            .path("/debug?roles=foo")
            .method("GET")
            .reply(&debug_handler)
            .await;
        assert_eq!(response.status(), 400);

        let response = warp::test::request()
            .path("/debug?node_ids=[")
            .method("GET")
            .reply(&debug_handler)
            .await;
        assert_eq!(response.status(), 400);

        let response = warp::test::request()
            .path("/debug")
            .method("GET")
            .reply(&debug_handler)
            .await;
        assert_eq!(response.status(), 200);

        // TODO: Refactor handler and test against mock developer service servers.
    }

    #[test]
    fn test_node_id_glob_patterns() {
        let node_id_patterns = NodeIdGlobPatterns::try_from_comma_separated_patterns("").unwrap();
        let node_id = NodeIdRef::from_str("node-1");
        assert!(node_id_patterns.matches(node_id));

        let node_id_patterns = NodeIdGlobPatterns::try_from_comma_separated_patterns(",").unwrap();
        let node_id = NodeIdRef::from_str("node-1");
        assert!(node_id_patterns.matches(node_id));

        let node_id_patterns = NodeIdGlobPatterns::try_from_comma_separated_patterns(
            "control-plane,,indexer-[1-2],searcher*",
        )
        .unwrap();

        let node_id = NodeIdRef::from_str("control-plane");
        assert!(node_id_patterns.matches(node_id));

        let node_id = NodeIdRef::from_str("indexer-1");
        assert!(node_id_patterns.matches(node_id));

        let node_id = NodeIdRef::from_str("Indexer-2");
        assert!(node_id_patterns.matches(node_id));

        let node_id = NodeIdRef::from_str("indexer-3");
        assert!(!node_id_patterns.matches(node_id));

        let node_id = NodeIdRef::from_str("searcher-1");
        assert!(node_id_patterns.matches(node_id));

        let node_id = NodeIdRef::from_str("janitor");
        assert!(!node_id_patterns.matches(node_id));
    }
}
