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
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Context;
use futures_util::future;
use itertools::Itertools;
use quickwit_actors::ActorExitStatus;
use quickwit_cli::tool::{local_ingest_docs_cli, LocalIngestDocsArgs};
use quickwit_common::new_coolid;
use quickwit_common::runtimes::RuntimesConfig;
use quickwit_common::test_utils::{wait_for_server_ready, wait_until_predicate};
use quickwit_common::tower::BoxFutureInfaillible;
use quickwit_common::uri::Uri as QuickwitUri;
use quickwit_config::service::QuickwitService;
use quickwit_config::NodeConfig;
use quickwit_metastore::{MetastoreResolver, SplitState};
use quickwit_proto::opentelemetry::proto::collector::logs::v1::logs_service_client::LogsServiceClient;
use quickwit_proto::opentelemetry::proto::collector::trace::v1::trace_service_client::TraceServiceClient;
use quickwit_proto::types::NodeId;
use quickwit_rest_client::models::IngestSource;
use quickwit_rest_client::rest_client::{
    CommitType, QuickwitClient, QuickwitClientBuilder, DEFAULT_BASE_URL,
};
use quickwit_serve::{serve_quickwit, ListSplitsQueryParams, SearchRequestQueryString};
use quickwit_storage::StorageResolver;
use reqwest::Url;
use serde_json::Value;
use tempfile::TempDir;
use tokio::sync::watch::{self, Receiver, Sender};
use tokio::task::JoinHandle;
use tonic::transport::channel;
use tracing::debug;

/// Configuration of a node made of a [`NodeConfig`] and a
/// set of services.
#[derive(Clone)]
pub struct TestNodeConfig {
    pub node_config: NodeConfig,
    pub services: HashSet<QuickwitService>,
}

type NodeJoinHandle = JoinHandle<Result<HashMap<String, ActorExitStatus>, anyhow::Error>>;

struct NodeShutdownHandle {
    sender: Sender<()>,
    receiver: Receiver<()>,
    node_services: HashSet<QuickwitService>,
    node_id: NodeId,
    join_handle_opt: Option<NodeJoinHandle>,
}

impl NodeShutdownHandle {
    fn new(node_id: NodeId, node_services: HashSet<QuickwitService>) -> Self {
        let (sender, receiver) = watch::channel(());
        Self {
            sender,
            receiver,
            node_id,
            node_services,
            join_handle_opt: None,
        }
    }

    fn shutdown_signal(&self) -> BoxFutureInfaillible<()> {
        let receiver = self.receiver.clone();
        Box::pin(async move {
            receiver.clone().changed().await.unwrap();
        })
    }

    fn set_node_join_handle(&mut self, join_handle: NodeJoinHandle) {
        self.join_handle_opt = Some(join_handle);
    }

    /// Initiate node shutdown and wait for it to complete
    async fn shutdown(self) -> anyhow::Result<HashMap<std::string::String, ActorExitStatus>> {
        self.sender.send(()).unwrap();
        self.join_handle_opt
            .expect("node join handle was not set before shutdown")
            .await
            .unwrap()
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
    pub node_configs: Vec<TestNodeConfig>,
    pub searcher_rest_client: QuickwitClient,
    pub indexer_rest_client: QuickwitClient,
    pub trace_client: TraceServiceClient<tonic::transport::Channel>,
    pub logs_client: LogsServiceClient<tonic::transport::Channel>,
    _temp_dir: TempDir,
    node_shutdown_handle: Vec<NodeShutdownHandle>,
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
        quickwit_rest_client::models::IngestSource::Str(json!($($json)+).to_string())
    };
}

pub(crate) async fn ingest_with_retry(
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
                    debug!(index=%index_id, err=%err, "failed to ingest");
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
    pub async fn start_cluster_with_configs(
        temp_dir: TempDir,
        node_configs: Vec<TestNodeConfig>,
    ) -> anyhow::Result<Self> {
        let runtimes_config = RuntimesConfig::light_for_tests();
        let storage_resolver = StorageResolver::unconfigured();
        let metastore_resolver = MetastoreResolver::unconfigured();
        let mut node_shutdown_handlers = Vec::new();
        for node_config in node_configs.iter() {
            let mut shutdown_handler = NodeShutdownHandle::new(
                node_config.node_config.node_id.clone(),
                node_config.services.clone(),
            );
            let shutdown_signal = shutdown_handler.shutdown_signal();
            let join_handle = tokio::spawn({
                let node_config = node_config.node_config.clone();
                let node_id = node_config.node_id.clone();
                let services = node_config.enabled_services.clone();
                let metastore_resolver = metastore_resolver.clone();
                let storage_resolver = storage_resolver.clone();

                async move {
                    let result = serve_quickwit(
                        node_config,
                        runtimes_config,
                        metastore_resolver,
                        storage_resolver,
                        shutdown_signal,
                        quickwit_serve::do_nothing_env_filter_reload_fn(),
                    )
                    .await?;
                    debug!("{} stopped successfully ({:?})", node_id, services);
                    Result::<_, anyhow::Error>::Ok(result)
                }
            });
            shutdown_handler.set_node_join_handle(join_handle);
            node_shutdown_handlers.push(shutdown_handler);
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
        if node_configs.len() == 1 {
            // We have only one node, so we can just wait for it to get started
            wait_for_server_ready(node_configs[0].node_config.grpc_listen_addr).await?;
        } else {
            // Wait for a duration greater than chitchat GOSSIP_INTERVAL (50ms) so that the cluster
            // is formed.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let channel = channel::Endpoint::from_str(&format!(
            "http://{}",
            indexer_config.node_config.grpc_listen_addr
        ))
        .unwrap()
        .connect_lazy();
        Ok(Self {
            node_configs,
            searcher_rest_client: QuickwitClientBuilder::new(transport_url(
                searcher_config.node_config.rest_config.listen_addr,
            ))
            .build(),
            indexer_rest_client: QuickwitClientBuilder::new(transport_url(
                indexer_config.node_config.rest_config.listen_addr,
            ))
            .build(),
            trace_client: TraceServiceClient::new(channel.clone()),
            logs_client: LogsServiceClient::new(channel),
            _temp_dir: temp_dir,
            node_shutdown_handle: node_shutdown_handlers,
        })
    }

    pub fn enable_ingest_v2(&mut self) {
        self.indexer_rest_client.enable_ingest_v2();
        self.searcher_rest_client.enable_ingest_v2();
    }

    // Starts one node that runs all the services and wait for it to be ready
    pub async fn start_standalone_node() -> anyhow::Result<Self> {
        let sandbox = Self::start_cluster_nodes(&[QuickwitService::supported_services()]).await?;
        sandbox.wait_for_cluster_num_ready_nodes(1).await?;
        Ok(sandbox)
    }

    pub async fn start_cluster_with_otlp_service(
        nodes_services: &[HashSet<QuickwitService>],
    ) -> anyhow::Result<Self> {
        let temp_dir = tempfile::tempdir()?;
        let mut node_configs = build_node_configs(temp_dir.path().to_path_buf(), nodes_services);
        // Set OTLP endpoint for indexers.
        for node_config in node_configs.iter_mut() {
            if node_config.services.contains(&QuickwitService::Indexer) {
                node_config.node_config.indexer_config.enable_otlp_endpoint = true;
            }
        }
        Self::start_cluster_with_configs(temp_dir, node_configs).await
    }

    // Starts nodes with corresponding services given by `nodes_services`.
    pub async fn start_cluster_nodes(
        nodes_services: &[HashSet<QuickwitService>],
    ) -> anyhow::Result<Self> {
        let temp_dir = tempfile::tempdir()?;
        let node_configs = build_node_configs(temp_dir.path().to_path_buf(), nodes_services);
        Self::start_cluster_with_configs(temp_dir, node_configs).await
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
        split_states_filter: Option<Vec<SplitState>>,
        required_splits_num: usize,
    ) -> anyhow::Result<()> {
        wait_until_predicate(
            || {
                let splits_query_params = ListSplitsQueryParams {
                    split_states: split_states_filter.clone(),
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
            Duration::from_millis(500),
        )
        .await?;
        Ok(())
    }

    pub async fn local_ingest(&self, index_id: &str, json_data: &[Value]) -> anyhow::Result<()> {
        let test_conf = self
            .node_configs
            .iter()
            .find(|config| config.services.contains(&QuickwitService::Indexer))
            .ok_or(anyhow::anyhow!("No indexer node found"))?;
        // NodeConfig cannot be serialized, we write our own simplified config
        let mut tmp_config_file = tempfile::Builder::new().suffix(".yaml").tempfile().unwrap();
        let node_config = format!(
            r#"
                version: 0.8
                metastore_uri: {}
                data_dir: {:?}
                "#,
            test_conf.node_config.metastore_uri, test_conf.node_config.data_dir_path
        );
        tmp_config_file.write_all(node_config.as_bytes())?;
        tmp_config_file.flush()?;

        let mut tmp_data_file = tempfile::NamedTempFile::new().unwrap();
        for line in json_data {
            serde_json::to_writer(&mut tmp_data_file, line)?;
            tmp_data_file.write_all(b"\n")?;
        }
        tmp_data_file.flush()?;

        local_ingest_docs_cli(LocalIngestDocsArgs {
            clear_cache: false,
            config_uri: QuickwitUri::from_str(tmp_config_file.path().to_str().unwrap())?,
            index_id: index_id.to_string(),
            input_format: quickwit_config::SourceInputFormat::Json,
            overwrite: false,
            vrl_script: None,
            input_path_opt: Some(QuickwitUri::from_str(
                tmp_data_file
                    .path()
                    .to_str()
                    .context("temp path could not be converted to URI")?,
            )?),
        })
        .await?;
        Ok(())
    }

    pub async fn assert_hit_count(&self, index_id: &str, query: &str, expected_num_hits: u64) {
        let search_response = self
            .searcher_rest_client
            .search(
                index_id,
                SearchRequestQueryString {
                    query: query.to_string(),
                    max_hits: 10,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        debug!(
            "search response for query {} on index {index_id}: {:?}",
            query, search_response
        );
        assert_eq!(
            search_response.num_hits, expected_num_hits,
            "unexpected num_hits for query {}",
            query
        );
    }

    /// Shutdown nodes that only provide the specified services
    pub async fn shutdown_services(
        &mut self,
        shutdown_services: &HashSet<QuickwitService>,
    ) -> Result<Vec<HashMap<String, ActorExitStatus>>, anyhow::Error> {
        // We need to drop rest clients first because reqwest can hold connections open
        // preventing rest server's graceful shutdown.
        let mut shutdown_futures = Vec::new();
        let mut shutdown_nodes = HashMap::new();
        let mut i = 0;
        while i < self.node_shutdown_handle.len() {
            let handler_services = &self.node_shutdown_handle[i].node_services;
            if handler_services.is_subset(shutdown_services) {
                let handler_to_shutdown = self.node_shutdown_handle.remove(i);
                shutdown_nodes.insert(
                    handler_to_shutdown.node_id.clone(),
                    handler_to_shutdown.node_services.clone(),
                );
                shutdown_futures.push(handler_to_shutdown.shutdown());
            } else {
                i += 1;
            }
        }
        debug!("shutting down {:?}", shutdown_nodes);
        let result = future::join_all(shutdown_futures).await;
        let mut statuses = Vec::new();
        for node in result {
            statuses.push(node?);
        }
        Ok(statuses)
    }

    pub async fn shutdown(
        mut self,
    ) -> Result<Vec<HashMap<String, ActorExitStatus>>, anyhow::Error> {
        self.shutdown_services(&QuickwitService::supported_services())
            .await
    }
}

/// Builds a list of [`NodeConfig`] given a list of Quickwit services.
/// Each element of `nodes_services` defines the services of a given node.
/// For each node, a `NodeConfig` is built with the right parameters
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
) -> Vec<TestNodeConfig> {
    let cluster_id = new_coolid("test-cluster");
    let mut node_configs = Vec::new();
    let mut peers: Vec<String> = Vec::new();
    let unique_dir_name = new_coolid("test-dir");
    for (node_idx, node_services) in nodes_services.iter().enumerate() {
        let mut config = NodeConfig::for_test();
        config.enabled_services.clone_from(node_services);
        config.cluster_id.clone_from(&cluster_id);
        config.node_id = NodeId::new(format!("test-node-{node_idx}"));
        config.data_dir_path = root_data_dir.join(config.node_id.as_str());
        config.metastore_uri =
            QuickwitUri::from_str(&format!("ram:///{unique_dir_name}/metastore")).unwrap();
        config.default_index_root_uri =
            QuickwitUri::from_str(&format!("ram:///{unique_dir_name}/indexes")).unwrap();
        peers.push(config.gossip_advertise_addr.to_string());
        node_configs.push(TestNodeConfig {
            node_config: config,
            services: node_services.clone(),
        });
    }
    for node_config in node_configs.iter_mut() {
        node_config.node_config.peer_seeds = peers
            .clone()
            .into_iter()
            .filter(|seed| *seed != node_config.node_config.gossip_advertise_addr.to_string())
            .collect_vec();
    }
    node_configs
}
