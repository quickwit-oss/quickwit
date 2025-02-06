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

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Context;
use futures_util::future;
use itertools::Itertools;
use quickwit_actors::ActorExitStatus;
use quickwit_cli::tool::{local_ingest_docs_cli, LocalIngestDocsArgs};
use quickwit_common::new_coolid;
use quickwit_common::runtimes::RuntimesConfig;
use quickwit_common::test_utils::wait_until_predicate;
use quickwit_common::uri::Uri as QuickwitUri;
use quickwit_config::service::QuickwitService;
use quickwit_config::NodeConfig;
use quickwit_metastore::{MetastoreResolver, SplitState};
use quickwit_proto::jaeger::storage::v1::span_reader_plugin_client::SpanReaderPluginClient;
use quickwit_proto::opentelemetry::proto::collector::logs::v1::logs_service_client::LogsServiceClient;
use quickwit_proto::opentelemetry::proto::collector::trace::v1::trace_service_client::TraceServiceClient;
use quickwit_proto::types::NodeId;
use quickwit_rest_client::models::{CumulatedIngestResponse, IngestSource};
use quickwit_rest_client::rest_client::{
    CommitType, QuickwitClient, QuickwitClientBuilder, DEFAULT_BASE_URL,
};
use quickwit_serve::tcp_listener::for_tests::TestTcpListenerResolver;
use quickwit_serve::{serve_quickwit, ListSplitsQueryParams, SearchRequestQueryString};
use quickwit_storage::StorageResolver;
use reqwest::Url;
use serde_json::Value;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tracing::debug;

use super::shutdown::NodeShutdownHandle;

pub struct TestNodeConfig {
    pub services: HashSet<QuickwitService>,
    pub enable_otlp: bool,
}

pub struct ClusterSandboxBuilder {
    temp_dir: TempDir,
    node_configs: Vec<TestNodeConfig>,
    use_legacy_ingest: bool,
}

impl Default for ClusterSandboxBuilder {
    fn default() -> Self {
        Self {
            temp_dir: tempfile::tempdir().unwrap(),
            node_configs: Vec::new(),
            use_legacy_ingest: false,
        }
    }
}

impl ClusterSandboxBuilder {
    pub fn add_node(mut self, services: impl IntoIterator<Item = QuickwitService>) -> Self {
        self.node_configs.push(TestNodeConfig {
            services: HashSet::from_iter(services),
            enable_otlp: false,
        });
        self
    }

    pub fn add_node_with_otlp(
        mut self,
        services: impl IntoIterator<Item = QuickwitService>,
    ) -> Self {
        self.node_configs.push(TestNodeConfig {
            services: HashSet::from_iter(services),
            enable_otlp: true,
        });
        self
    }

    pub fn use_legacy_ingest(mut self) -> Self {
        self.use_legacy_ingest = true;
        self
    }

    /// Builds a list of of [`NodeConfig`] from the node definitions added to
    /// builder. For each node, a [`NodeConfig`] is built with the right
    /// parameters such that we will be able to run `quickwit_serve` on them and
    /// form a Quickwit cluster. For each node, we set:
    /// - `data_dir_path` defined by `root_data_dir/node_id`.
    /// - `metastore_uri` defined by `root_data_dir/metastore`.
    /// - `default_index_root_uri` defined by `root_data_dir/indexes`.
    /// - `peers` defined by others nodes `gossip_advertise_addr`.
    async fn build_config(self) -> ResolvedClusterConfig {
        let root_data_dir = self.temp_dir.path().to_path_buf();
        let cluster_id = new_coolid("test-cluster");
        let mut resolved_node_configs = Vec::new();
        let mut peers: Vec<String> = Vec::new();
        let unique_dir_name = new_coolid("test-dir");
        let tcp_listener_resolver = TestTcpListenerResolver::default();
        for (node_idx, node_builder) in self.node_configs.iter().enumerate() {
            let socket: SocketAddr = ([127, 0, 0, 1], 0u16).into();
            let rest_tcp_listener = TcpListener::bind(socket).await.unwrap();
            let grpc_tcp_listener = TcpListener::bind(socket).await.unwrap();
            let mut config = NodeConfig::for_test_from_ports(
                rest_tcp_listener.local_addr().unwrap().port(),
                grpc_tcp_listener.local_addr().unwrap().port(),
            );
            tcp_listener_resolver.add_listener(rest_tcp_listener).await;
            tcp_listener_resolver.add_listener(grpc_tcp_listener).await;
            config.indexer_config.enable_otlp_endpoint = node_builder.enable_otlp;
            config.enabled_services.clone_from(&node_builder.services);
            config.jaeger_config.enable_endpoint = true;
            config.cluster_id.clone_from(&cluster_id);
            config.node_id = NodeId::new(format!("test-node-{node_idx}"));
            config.data_dir_path = root_data_dir.join(config.node_id.as_str());
            config.metastore_uri =
                QuickwitUri::from_str(&format!("ram:///{unique_dir_name}/metastore")).unwrap();
            config.default_index_root_uri =
                QuickwitUri::from_str(&format!("ram:///{unique_dir_name}/indexes")).unwrap();
            peers.push(config.gossip_advertise_addr.to_string());
            resolved_node_configs.push((config, node_builder.services.clone()));
        }
        for node_config in resolved_node_configs.iter_mut() {
            node_config.0.peer_seeds = peers
                .clone()
                .into_iter()
                .filter(|seed| *seed != node_config.0.gossip_advertise_addr.to_string())
                .collect_vec();
        }
        ResolvedClusterConfig {
            temp_dir: self.temp_dir,
            node_configs: resolved_node_configs,
            tcp_listener_resolver,
        }
    }

    /// Builds the cluster config, starts the nodes and waits for them to be ready
    pub async fn build_and_start(self) -> ClusterSandbox {
        self.build_config().await.start().await
    }

    pub async fn build_and_start_standalone() -> ClusterSandbox {
        ClusterSandboxBuilder::default()
            .add_node(QuickwitService::supported_services())
            .build_config()
            .await
            .start()
            .await
    }
}

/// Intermediate state where the ports of all the the test cluster nodes have
/// been reserved and the configurations have been generated.
struct ResolvedClusterConfig {
    temp_dir: TempDir,
    node_configs: Vec<(NodeConfig, HashSet<QuickwitService>)>,
    tcp_listener_resolver: TestTcpListenerResolver,
}

impl ResolvedClusterConfig {
    /// Start a cluster using this config and waits for the nodes to be ready
    pub async fn start(self) -> ClusterSandbox {
        let mut node_shutdown_handles = Vec::new();
        let runtimes_config = RuntimesConfig::light_for_tests();
        let storage_resolver = StorageResolver::unconfigured();
        let metastore_resolver = MetastoreResolver::unconfigured();
        let cluster_size = self.node_configs.len();
        for node_config in self.node_configs.iter() {
            let mut shutdown_handler =
                NodeShutdownHandle::new(node_config.0.node_id.clone(), node_config.1.clone());
            let shutdown_signal = shutdown_handler.shutdown_signal();
            let join_handle = tokio::spawn({
                let node_config = node_config.0.clone();
                let node_id = node_config.node_id.clone();
                let services = node_config.enabled_services.clone();
                let metastore_resolver = metastore_resolver.clone();
                let storage_resolver = storage_resolver.clone();
                let tcp_listener_resolver = self.tcp_listener_resolver.clone();

                async move {
                    let result = serve_quickwit(
                        node_config,
                        runtimes_config,
                        metastore_resolver,
                        storage_resolver,
                        tcp_listener_resolver,
                        shutdown_signal,
                        quickwit_serve::do_nothing_env_filter_reload_fn(),
                    )
                    .await?;
                    debug!("{node_id} stopped successfully ({:?})", services);
                    Result::<_, anyhow::Error>::Ok(result)
                }
            });
            shutdown_handler.set_node_join_handle(join_handle);
            node_shutdown_handles.push(shutdown_handler);
        }

        let sandbox = ClusterSandbox {
            node_configs: self.node_configs,
            _temp_dir: self.temp_dir,
            node_shutdown_handles,
        };
        sandbox
            .wait_for_cluster_num_ready_nodes(cluster_size)
            .await
            .unwrap();
        sandbox
    }
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

pub(crate) async fn ingest(
    client: &QuickwitClient,
    index_id: &str,
    ingest_source: IngestSource,
    commit_type: CommitType,
) -> anyhow::Result<CumulatedIngestResponse> {
    let resp = client
        .ingest(index_id, ingest_source, None, None, commit_type)
        .await?;
    Ok(resp)
}

/// A test environment where you can start a Quickwit cluster and use the gRPC
/// or REST clients to test it.
pub struct ClusterSandbox {
    pub node_configs: Vec<(NodeConfig, HashSet<QuickwitService>)>,
    _temp_dir: TempDir,
    node_shutdown_handles: Vec<NodeShutdownHandle>,
}

impl ClusterSandbox {
    fn find_node_for_service(&self, service: QuickwitService) -> NodeConfig {
        self.node_configs
            .iter()
            .find(|config| config.1.contains(&service))
            .unwrap_or_else(|| panic!("No {:?} node", service))
            .0
            .clone()
    }

    fn channel(&self, service: QuickwitService) -> tonic::transport::Channel {
        let node_config = self.find_node_for_service(service);
        let endpoint = format!("http://{}", node_config.grpc_listen_addr);
        tonic::transport::Channel::from_shared(endpoint)
            .unwrap()
            .connect_lazy()
    }

    /// Returns a client to one of the nodes that runs the specified service
    pub fn rest_client(&self, service: QuickwitService) -> QuickwitClient {
        let node_config = self.find_node_for_service(service);

        QuickwitClientBuilder::new(transport_url(node_config.rest_config.listen_addr)).build()
    }

    /// A client configured to ingest documents and return detailed parse failures.
    pub fn detailed_ingest_client(&self) -> QuickwitClient {
        let node_config = self.find_node_for_service(QuickwitService::Indexer);

        QuickwitClientBuilder::new(transport_url(node_config.rest_config.listen_addr))
            .detailed_response(true)
            .build()
    }

    // TODO(#5604)
    pub fn rest_client_legacy_indexer(&self) -> QuickwitClient {
        let node_config = self.find_node_for_service(QuickwitService::Indexer);

        QuickwitClientBuilder::new(transport_url(node_config.rest_config.listen_addr))
            .use_legacy_ingest(true)
            .build()
    }

    pub fn jaeger_client(&self) -> SpanReaderPluginClient<tonic::transport::Channel> {
        SpanReaderPluginClient::new(self.channel(QuickwitService::Searcher))
    }

    pub fn logs_client(&self) -> LogsServiceClient<tonic::transport::Channel> {
        LogsServiceClient::new(self.channel(QuickwitService::Indexer))
    }

    pub fn trace_client(&self) -> TraceServiceClient<tonic::transport::Channel> {
        TraceServiceClient::new(self.channel(QuickwitService::Indexer))
    }

    async fn wait_for_cluster_num_ready_nodes(
        &self,
        expected_num_ready_nodes: usize,
    ) -> anyhow::Result<()> {
        wait_until_predicate(
            || async move {
                match self
                    .rest_client(QuickwitService::Metastore)
                    .cluster()
                    .snapshot()
                    .await
                {
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

    /// Waits for the needed number of indexing pipeline to start.
    ///
    /// WARNING! does not work if multiple indexers are running
    pub async fn wait_for_indexing_pipelines(
        &self,
        required_pipeline_num: usize,
    ) -> anyhow::Result<()> {
        wait_until_predicate(
            || async move {
                match self
                    .rest_client(QuickwitService::Indexer)
                    .node_stats()
                    .indexing()
                    .await
                {
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
                        .rest_client(QuickwitService::Metastore)
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
            Duration::from_secs(15),
            Duration::from_millis(500),
        )
        .await?;
        Ok(())
    }

    pub async fn local_ingest(&self, index_id: &str, json_data: &[Value]) -> anyhow::Result<()> {
        let test_conf = self
            .node_configs
            .iter()
            .find(|config| config.1.contains(&QuickwitService::Indexer))
            .ok_or(anyhow::anyhow!("No indexer node found"))?;
        // NodeConfig cannot be serialized, we write our own simplified config
        let mut tmp_config_file = tempfile::Builder::new().suffix(".yaml").tempfile().unwrap();
        let node_config = format!(
            r#"
                version: 0.8
                metastore_uri: {}
                data_dir: {:?}
                "#,
            test_conf.0.metastore_uri, test_conf.0.data_dir_path
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
            .rest_client(QuickwitService::Searcher)
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
        shutdown_services: impl IntoIterator<Item = QuickwitService>,
    ) -> Result<Vec<HashMap<String, ActorExitStatus>>, anyhow::Error> {
        // We need to drop rest clients first because reqwest can hold connections open
        // preventing rest server's graceful shutdown.
        let mut indexer_shutdown_futures = Vec::new();
        let mut other_shutdown_futures = Vec::new();
        let mut shutdown_nodes = HashMap::new();
        let mut i = 0;
        let shutdown_services_map = HashSet::from_iter(shutdown_services);
        while i < self.node_shutdown_handles.len() {
            let handler_services = &self.node_shutdown_handles[i].node_services;
            if !handler_services.is_subset(&shutdown_services_map) {
                i += 1;
                continue;
            }
            let handler_to_shutdown = self.node_shutdown_handles.remove(i);
            shutdown_nodes.insert(
                handler_to_shutdown.node_id.clone(),
                handler_to_shutdown.node_services.clone(),
            );
            if handler_to_shutdown
                .node_services
                .contains(&QuickwitService::Indexer)
            {
                indexer_shutdown_futures.push(handler_to_shutdown.shutdown());
            } else {
                other_shutdown_futures.push(handler_to_shutdown.shutdown());
            }
        }
        debug!("shutting down {:?}", shutdown_nodes);
        // We must decommision the indexer nodes first and independently from the other nodes.
        let indexer_shutdown_results = future::join_all(indexer_shutdown_futures).await;
        let other_shutdown_results = future::join_all(other_shutdown_futures).await;
        let exit_statuses = indexer_shutdown_results
            .into_iter()
            .chain(other_shutdown_results)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(exit_statuses)
    }

    pub async fn shutdown(
        mut self,
    ) -> Result<Vec<HashMap<String, ActorExitStatus>>, anyhow::Error> {
        self.shutdown_services(QuickwitService::supported_services())
            .await
    }
}
