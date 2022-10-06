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

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use hyper::Uri;
use itertools::Itertools;
use quickwit_common::new_coolid;
use quickwit_common::rand::append_random_suffix;
use quickwit_common::uri::Uri as QuickwitUri;
use quickwit_config::service::QuickwitService;
use quickwit_config::{QuickwitConfig, SourceConfig};
use quickwit_metastore::{quickwit_metastore_uri_resolver, IndexMetadata};
use quickwit_proto::tonic::transport::Endpoint;
use quickwit_search::{create_search_service_client, SearchServiceClient};
use rand::seq::IteratorRandom;
use tempfile::TempDir;
use tracing::info;

use super::rest_client::QuickwitRestClient;
use crate::serve_quickwit;

/// Configuration of a node made of a [`QuickwitConfig`] and a
/// set of services.
#[derive(Clone)]
pub struct NodeConfig {
    pub quickwit_config: QuickwitConfig,
    pub services: HashSet<QuickwitService>,
}

/// Creates a Cluster Test environment.
///
/// The goal is to start several nodes and use the gRPC or REST clients to
/// test it.
///
/// WARNING: Currently, we cannot start an indexer in a different test as it will
/// will share the same `INGEST_API_SERVICE_INSTANCE`. The ingest API will be
/// dropped by the first running test and the other tests will fail.
pub struct ClusterSandbox {
    pub node_configs: Vec<NodeConfig>,
    pub grpc_search_clients: HashMap<SocketAddr, SearchServiceClient>,
    pub rest_client: QuickwitRestClient,
    pub index_id_for_test: String,
    _temp_dir: TempDir,
}

impl ClusterSandbox {
    // Starts one node that runs all the services.
    pub async fn start_standalone_node() -> anyhow::Result<Self> {
        let temp_dir = tempfile::tempdir()?;
        let services = HashSet::from_iter([QuickwitService::Searcher, QuickwitService::Metastore]);
        let node_configs = build_node_configs(temp_dir.path().to_path_buf(), &[services]);
        // There is exactly one node.
        let node_config = node_configs[0].clone();
        let node_config_clone = node_config.clone();
        // Creates an index before starting nodes as currently Quickwit does not support
        // dynamic creation/deletion of indexes/sources.
        let index_for_test = append_random_suffix("test-standalone-node-index");
        create_index_for_test(&index_for_test, &node_config.quickwit_config).await?;
        tokio::spawn(async move {
            let result = serve_quickwit(
                node_config_clone.quickwit_config,
                &node_config_clone.services,
            )
            .await;
            println!("Quickwit server terminated: {:?}", result);
            Result::<_, anyhow::Error>::Ok(())
        });
        wait_for_server_ready(node_config.quickwit_config.grpc_listen_addr).await?;
        let mut grpc_search_clients = HashMap::new();
        let search_client =
            create_search_service_client(node_config.quickwit_config.grpc_listen_addr).await?;
        grpc_search_clients.insert(node_config.quickwit_config.grpc_listen_addr, search_client);
        Ok(Self {
            node_configs,
            grpc_search_clients,
            rest_client: QuickwitRestClient::new(node_config.quickwit_config.rest_listen_addr),
            index_id_for_test: index_for_test,
            _temp_dir: temp_dir,
        })
    }

    // Starts nodes with corresponding services given by `nodes_services`.
    pub async fn start_cluster_nodes(
        nodes_services: &[HashSet<QuickwitService>],
    ) -> anyhow::Result<Self> {
        let temp_dir = tempfile::tempdir()?;
        let node_configs = build_node_configs(temp_dir.path().to_path_buf(), nodes_services);
        let index_for_test = append_random_suffix("test-multi-nodes-cluster-index");
        // Creates an index before starting nodes as currently Quickwit does not support
        // dynamic creation/deletion of indexes/sources.
        create_index_for_test(&index_for_test, &node_configs[0].quickwit_config).await?;
        for node_config in node_configs.iter() {
            let node_config_clone = node_config.clone();
            tokio::spawn(async move {
                let result = serve_quickwit(
                    node_config_clone.quickwit_config,
                    &node_config_clone.services,
                )
                .await;
                info!("Quickwit server terminated: {:?}", result);
                Result::<_, anyhow::Error>::Ok(())
            });
        }
        let indexer_config = node_configs
            .iter()
            .find(|node_config| node_config.services.contains(&QuickwitService::Indexer))
            .cloned()
            .unwrap();
        let mut grpc_search_clients = HashMap::new();
        for node_config in node_configs.iter() {
            if !node_config.services.contains(&QuickwitService::Searcher) {
                continue;
            }
            let search_client =
                create_search_service_client(node_config.quickwit_config.grpc_listen_addr).await?;
            grpc_search_clients.insert(search_client.grpc_addr(), search_client);
        }
        // Wait for a duration greater than chitchat GOSSIP_INTERVAL (50ms) so that the cluster is
        // formed.
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(Self {
            node_configs,
            grpc_search_clients,
            rest_client: QuickwitRestClient::new(indexer_config.quickwit_config.rest_listen_addr),
            index_id_for_test: index_for_test,
            _temp_dir: temp_dir,
        })
    }

    pub async fn wait_for_cluster_num_ready_nodes(
        &self,
        expected_num_alive_nodes: usize,
    ) -> anyhow::Result<()> {
        let mut num_attempts = 0;
        let max_num_attempts = 3;
        while num_attempts < max_num_attempts {
            tokio::time::sleep(Duration::from_millis(100 * (num_attempts + 1))).await;
            let cluster_snapshot = self.rest_client.cluster_snapshot().await?;
            if cluster_snapshot.ready_nodes.len() == expected_num_alive_nodes {
                return Ok(());
            }
            num_attempts += 1;
        }
        if num_attempts == max_num_attempts {
            anyhow::bail!("Too many attempts to get expected num members.");
        }
        Ok(())
    }

    pub fn get_random_search_client(&self) -> SearchServiceClient {
        let mut rng = rand::thread_rng();
        let selected_addr = self.grpc_search_clients.keys().choose(&mut rng).unwrap();
        self.grpc_search_clients.get(selected_addr).unwrap().clone()
    }
}

// Creates a `test-index` with default test metadata.
async fn create_index_for_test(
    index_id_for_test: &str,
    quickwit_config: &QuickwitConfig,
) -> anyhow::Result<()> {
    let index_uri = quickwit_config
        .default_index_root_uri
        .join(index_id_for_test)
        .unwrap();
    let index_meta = IndexMetadata::for_test(index_id_for_test, index_uri.as_str());
    let metastore_uri_resolver = quickwit_metastore_uri_resolver();
    let metastore = metastore_uri_resolver
        .resolve(&quickwit_config.metastore_uri)
        .await?;
    metastore.create_index(index_meta.clone()).await?;
    metastore
        .add_source(index_id_for_test, SourceConfig::ingest_api_default())
        .await?;
    Ok(())
}

/// Builds a list of [`NodeConfig`] given a list of Quickwit services.
/// Each element of `nodes_services` defines the services of a given node.
/// For each node, a `QuickwitConfig` is built with the right parameters
/// such that we will be able to run `quickwit_serve` on them and form
/// a quickwit cluster.
/// For each node, we set:
/// - `data_dir_path` defined by `root_data_dir/node_id`.
/// - `metastore_uri` defined by `root_data_dir/metastore`.
/// - `default_index_root_uri` defined by `root_data_dir/indexes`.
/// - `peers` defined by others nodes `gossip_advertise_addr`.
pub fn build_node_configs(
    root_data_dir: PathBuf,
    nodes_services: &[HashSet<QuickwitService>],
) -> Vec<NodeConfig> {
    let cluster_id = new_coolid("test-cluster");
    let mut node_configs = Vec::new();
    let mut peers: Vec<String> = Vec::new();
    let unique_dir_name = new_coolid("test-dir");
    for node_services in nodes_services.iter() {
        let mut config = QuickwitConfig::for_test();
        config.cluster_id = cluster_id.clone();
        config.data_dir_path = root_data_dir.join(&config.node_id);
        config.metastore_uri =
            QuickwitUri::try_new(&format!("ram:///{}/metastore", unique_dir_name)).unwrap();
        config.default_index_root_uri =
            QuickwitUri::try_new(&format!("ram:///{}/indexes", unique_dir_name)).unwrap();
        peers.push(config.gossip_advertise_addr.to_string());
        node_configs.push(NodeConfig {
            quickwit_config: config,
            services: node_services.clone(),
        });
    }
    for node_config in node_configs.iter_mut() {
        node_config.quickwit_config.peer_seeds = peers
            .clone()
            .into_iter()
            .filter(|seed| {
                *seed
                    != node_config
                        .quickwit_config
                        .gossip_advertise_addr
                        .to_string()
            })
            .collect_vec();
    }
    node_configs
}

/// Tries to connect at most 3 times to `SocketAddr`.
/// If not successful, returns an error.
/// This is a convenient function to wait before sending gRPC requests
/// to this `SocketAddr`.
async fn wait_for_server_ready(socket_addr: SocketAddr) -> anyhow::Result<()> {
    let mut num_attempts = 0;
    let max_num_attempts = 5;
    let uri = Uri::builder()
        .scheme("http")
        .authority(socket_addr.to_string().as_str())
        .path_and_query("/")
        .build()?;
    while num_attempts < max_num_attempts {
        tokio::time::sleep(Duration::from_millis(20 * (num_attempts + 1))).await;
        match Endpoint::from(uri.clone()).connect().await {
            Ok(_) => break,
            Err(_) => {
                println!(
                    "Failed to connect to `{}` failed, retrying {}/{}",
                    socket_addr,
                    num_attempts + 1,
                    max_num_attempts
                );
                num_attempts += 1;
            }
        }
    }
    if num_attempts == max_num_attempts {
        anyhow::bail!("Too many attempts to connect to `{}`", socket_addr);
    }
    Ok(())
}
