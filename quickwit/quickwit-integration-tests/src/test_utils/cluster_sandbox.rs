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

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use futures_util::future;
use itertools::Itertools;
use quickwit_actors::ActorExitStatus;
use quickwit_common::new_coolid;
use quickwit_common::runtimes::RuntimesConfig;
use quickwit_common::test_utils::{wait_for_server_ready, wait_until_predicate};
use quickwit_common::tower::BoxFutureInfaillible;
use quickwit_common::uri::Uri as QuickwitUri;
use quickwit_config::service::QuickwitService;
use quickwit_config::QuickwitConfig;
use quickwit_metastore::{MetastoreResolver, SplitState};
use quickwit_rest_client::models::IngestSource;
use quickwit_rest_client::rest_client::{
    CommitType, QuickwitClient, QuickwitClientBuilder, DEFAULT_BASE_URL,
};
use quickwit_serve::{serve_quickwit, ListSplitsQueryParams};
use quickwit_storage::StorageResolver;
use reqwest::Url;
use tempfile::TempDir;
use tokio::sync::watch::{self, Receiver, Sender};
use tokio::task::JoinHandle;
use tracing::debug;

/// Configuration of a node made of a [`QuickwitConfig`] and a
/// set of services.
#[derive(Clone)]
pub struct NodeConfig {
    pub quickwit_config: QuickwitConfig,
    pub services: HashSet<QuickwitService>,
}

struct ClusterShutdownTrigger {
    sender: Sender<()>,
    receiver: Receiver<()>,
}

impl ClusterShutdownTrigger {
    fn new() -> Self {
        let (sender, receiver) = watch::channel(());
        Self { sender, receiver }
    }

    fn shutdown_signal(&self) -> BoxFutureInfaillible<()> {
        let receiver = self.receiver.clone();
        Box::pin(async move {
            receiver.clone().changed().await.unwrap();
        })
    }

    fn shutdown(self) {
        self.sender.send(()).unwrap();
    }
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
    pub searcher_rest_client: QuickwitClient,
    pub indexer_rest_client: QuickwitClient,
    _temp_dir: TempDir,
    join_handles: Vec<JoinHandle<Result<HashMap<String, ActorExitStatus>, anyhow::Error>>>,
    shutdown_trigger: ClusterShutdownTrigger,
}

fn transport_url(addr: SocketAddr) -> Url {
    let mut url = Url::parse(DEFAULT_BASE_URL).unwrap();
    url.set_ip_host(addr.ip()).unwrap();
    url.set_port(Some(addr.port())).unwrap();
    url
}

#[macro_export]
macro_rules! ingest_json {
    ($($json:tt)+) => {
        quickwit_rest_client::models::IngestSource::Bytes(json!($($json)+).to_string().into())
    };
}

pub async fn ingest_with_retry(
    client: &QuickwitClient,
    index_id: &str,
    ingest_source: IngestSource,
    commit_type: CommitType,
) -> anyhow::Result<()> {
    wait_until_predicate(
        || {
            let commit_type_clone = commit_type;
            let ingest_source_clone = ingest_source.clone();
            async move {
                // Index one record.
                if let Err(err) = client
                    .ingest(index_id, ingest_source_clone, None, None, commit_type_clone)
                    .await
                {
                    debug!("Failed to index into {} due to error: {}", index_id, err);
                    false
                } else {
                    true
                }
            }
        },
        Duration::from_secs(10),
        Duration::from_millis(100),
    )
    .await?;
    Ok(())
}

impl ClusterSandbox {
    // Starts one node that runs all the services.
    pub async fn start_standalone_node() -> anyhow::Result<Self> {
        let temp_dir = tempfile::tempdir()?;
        let services = QuickwitService::supported_services();
        let node_configs = build_node_configs(temp_dir.path().to_path_buf(), &[services]);
        let storage_resolver = StorageResolver::unconfigured();
        let metastore_resolver = MetastoreResolver::unconfigured();
        // There is exactly one node.
        let node_config = node_configs[0].clone();
        let node_config_clone = node_config.clone();
        let runtimes_config = RuntimesConfig::light_for_tests();
        let shutdown_trigger = ClusterShutdownTrigger::new();
        let shutdown_signal = shutdown_trigger.shutdown_signal();
        let join_handles = vec![tokio::spawn(async move {
            let result = serve_quickwit(
                node_config_clone.quickwit_config,
                runtimes_config,
                storage_resolver,
                metastore_resolver,
                shutdown_signal,
            )
            .await?;
            Result::<_, anyhow::Error>::Ok(result)
        })];
        wait_for_server_ready(node_config.quickwit_config.grpc_listen_addr).await?;
        Ok(Self {
            node_configs,
            indexer_rest_client: QuickwitClientBuilder::new(transport_url(
                node_config.quickwit_config.rest_listen_addr,
            ))
            .build(),
            searcher_rest_client: QuickwitClientBuilder::new(transport_url(
                node_config.quickwit_config.rest_listen_addr,
            ))
            .build(),
            _temp_dir: temp_dir,
            join_handles,
            shutdown_trigger,
        })
    }

    // Starts nodes with corresponding services given by `nodes_services`.
    pub async fn start_cluster_nodes(
        nodes_services: &[HashSet<QuickwitService>],
    ) -> anyhow::Result<Self> {
        let temp_dir = tempfile::tempdir()?;
        let node_configs = build_node_configs(temp_dir.path().to_path_buf(), nodes_services);
        let runtimes_config = RuntimesConfig::light_for_tests();
        let storage_resolver = StorageResolver::unconfigured();
        let metastore_resolver = MetastoreResolver::unconfigured();
        let mut join_handles = Vec::new();
        let shutdown_trigger = ClusterShutdownTrigger::new();
        for node_config in node_configs.iter() {
            join_handles.push(tokio::spawn({
                let node_config = node_config.quickwit_config.clone();
                let storage_resolver = storage_resolver.clone();
                let metastore_resolver = metastore_resolver.clone();
                let shutdown_signal = shutdown_trigger.shutdown_signal();
                async move {
                    let result = serve_quickwit(
                        node_config,
                        runtimes_config,
                        storage_resolver,
                        metastore_resolver,
                        shutdown_signal,
                    )
                    .await?;
                    Result::<_, anyhow::Error>::Ok(result)
                }
            }));
        }
        let searcher_config = node_configs
            .iter()
            .find(|node_config| node_config.services.contains(&QuickwitService::Searcher))
            .cloned()
            .unwrap();
        let indexer_config = node_configs
            .iter()
            .find(|node_config| node_config.services.contains(&QuickwitService::Indexer))
            .cloned()
            .unwrap();
        // Wait for a duration greater than chitchat GOSSIP_INTERVAL (50ms) so that the cluster is
        // formed.
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(Self {
            node_configs,
            searcher_rest_client: QuickwitClientBuilder::new(transport_url(
                searcher_config.quickwit_config.rest_listen_addr,
            ))
            .build(),
            indexer_rest_client: QuickwitClientBuilder::new(transport_url(
                indexer_config.quickwit_config.rest_listen_addr,
            ))
            .build(),
            _temp_dir: temp_dir,
            join_handles,
            shutdown_trigger,
        })
    }

    pub async fn wait_for_cluster_num_ready_nodes(
        &self,
        expected_num_ready_nodes: usize,
    ) -> anyhow::Result<()> {
        wait_until_predicate(
            || async move {
                match self.indexer_rest_client.cluster().snapshot().await {
                    Ok(result) => {
                        if result.ready_nodes.len() != expected_num_ready_nodes {
                            debug!(
                                "wait_for_cluster_num_ready_nodes expected {} ready nodes, got {}",
                                expected_num_ready_nodes,
                                result.live_nodes.len()
                            );
                            false
                        } else {
                            true
                        }
                    }
                    Err(err) => {
                        debug!("wait_for_cluster_num_ready_nodes error {err}");
                        false
                    }
                }
            },
            Duration::from_secs(10),
            Duration::from_millis(100),
        )
        .await?;
        Ok(())
    }

    // Waits for the needed number of indexing pipeline to start.
    pub async fn wait_for_indexing_pipelines(
        &self,
        required_pipeline_num: usize,
    ) -> anyhow::Result<()> {
        wait_until_predicate(
            || async move {
                match self.indexer_rest_client.node_stats().indexing().await {
                    Ok(result) => {
                        if result.num_running_pipelines != required_pipeline_num {
                            debug!(
                                "wait_for_indexing_pipelines expected {} pipelines, got {}",
                                required_pipeline_num, result.num_running_pipelines
                            );
                            false
                        } else {
                            true
                        }
                    }
                    Err(err) => {
                        debug!("wait_for_cluster_num_ready_nodes error {err}");
                        false
                    }
                }
            },
            Duration::from_secs(10),
            Duration::from_millis(100),
        )
        .await?;
        Ok(())
    }

    // Waits for the needed number of indexing pipeline to start.
    pub async fn wait_for_splits(
        &self,
        index_id: &str,
        split_states: Option<Vec<SplitState>>,
        required_splits_num: usize,
    ) -> anyhow::Result<()> {
        wait_until_predicate(
            || {
                let splits_query_params = ListSplitsQueryParams {
                    split_states: split_states.clone(),
                    ..Default::default()
                };
                async move {
                    match self
                        .indexer_rest_client
                        .splits(index_id)
                        .list(splits_query_params)
                        .await
                    {
                        Ok(result) => {
                            if result.len() != required_splits_num {
                                debug!(
                                    "wait_for_splits expected {} splits, got {}",
                                    required_splits_num,
                                    result.len()
                                );
                                false
                            } else {
                                true
                            }
                        }
                        Err(err) => {
                            debug!("wait_for_splits error {err}");
                            false
                        }
                    }
                }
            },
            Duration::from_secs(10),
            Duration::from_millis(100),
        )
        .await?;
        Ok(())
    }

    pub async fn shutdown(self) -> Result<Vec<HashMap<String, ActorExitStatus>>, anyhow::Error> {
        // We need to drop rest clients first because reqwest can hold connections open
        // preventing rest server's graceful shutdown.
        drop(self.searcher_rest_client);
        drop(self.indexer_rest_client);
        self.shutdown_trigger.shutdown();
        let result = future::join_all(self.join_handles).await;
        let mut statuses = Vec::new();
        for node in result {
            statuses.push(node??);
        }
        Ok(statuses)
    }
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
    for (node_idx, node_services) in nodes_services.iter().enumerate() {
        let mut config = QuickwitConfig::for_test();
        config.enabled_services = node_services.clone();
        config.cluster_id = cluster_id.clone();
        config.node_id = format!("test-node-{node_idx}");
        config.data_dir_path = root_data_dir.join(&config.node_id);
        config.metastore_uri =
            QuickwitUri::from_str(&format!("ram:///{unique_dir_name}/metastore")).unwrap();
        config.default_index_root_uri =
            QuickwitUri::from_str(&format!("ram:///{unique_dir_name}/indexes")).unwrap();
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
