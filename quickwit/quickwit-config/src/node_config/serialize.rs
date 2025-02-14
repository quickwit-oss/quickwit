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

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::time::Duration;

use anyhow::{bail, Context};
use http::HeaderMap;
use quickwit_common::net::{find_private_ip, get_short_hostname, Host};
use quickwit_common::new_coolid;
use quickwit_common::uri::Uri;
use quickwit_proto::types::NodeId;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::{GrpcConfig, RestConfig};
use crate::config_value::ConfigValue;
use crate::qw_env_vars::*;
use crate::service::QuickwitService;
use crate::storage_config::StorageConfigs;
use crate::templating::render_config;
use crate::{
    validate_identifier, validate_node_id, ConfigFormat, IndexerConfig, IngestApiConfig,
    JaegerConfig, MetastoreConfigs, NodeConfig, SearcherConfig,
};

pub const DEFAULT_CLUSTER_ID: &str = "quickwit-default-cluster";

pub const DEFAULT_DATA_DIR_PATH: &str = "qwdata";

pub const DEFAULT_GOSSIP_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(25)
} else {
    Duration::from_secs(1)
};

// Default config values in the order they appear in [`NodeConfigBuilder`].
fn default_cluster_id() -> ConfigValue<String, QW_CLUSTER_ID> {
    ConfigValue::with_default(DEFAULT_CLUSTER_ID.to_string())
}

fn default_node_id() -> ConfigValue<String, QW_NODE_ID> {
    let node_id = match get_short_hostname() {
        Ok(short_hostname) => short_hostname,
        Err(error) => {
            let node_id = new_coolid("node");
            warn!(error=?error, "failed to determine hostname or hostname was invalid, falling back to random node ID `{}`", node_id);
            node_id
        }
    };
    ConfigValue::with_default(node_id)
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
struct List(Vec<String>);

impl FromStr for List {
    type Err = anyhow::Error;

    fn from_str(list_str: &str) -> Result<Self, Self::Err> {
        let list = list_str
            .split(',')
            .map(|elem| elem.trim().to_string())
            .filter(|elem| !elem.is_empty())
            .collect();
        Ok(List(list))
    }
}

fn default_enabled_services() -> ConfigValue<List, QW_ENABLED_SERVICES> {
    ConfigValue::with_default(List(
        QuickwitService::supported_services()
            .into_iter()
            .map(|service| service.to_string())
            .collect(),
    ))
}

fn default_listen_address() -> ConfigValue<String, QW_LISTEN_ADDRESS> {
    ConfigValue::with_default(Host::default().to_string())
}

fn default_rest_listen_port() -> u16 {
    7280
}

fn default_data_dir_uri() -> ConfigValue<Uri, QW_DATA_DIR> {
    ConfigValue::with_default(Uri::from_str(DEFAULT_DATA_DIR_PATH).unwrap())
}

/// Returns the default advertise host.
fn default_advertise_host(listen_ip: &IpAddr) -> anyhow::Result<Host> {
    if listen_ip.is_unspecified() {
        if let Some((interface_name, private_ip)) = find_private_ip() {
            info!(advertise_address=%private_ip, interface_name=%interface_name, "using sniffed advertise address `{private_ip}`");
            return Ok(Host::from(private_ip));
        }
        bail!("listen address `{listen_ip}` is unspecified and advertise address is not set");
    }
    info!(advertise_address=%listen_ip, "using listen address `{listen_ip}` as advertise address");
    Ok(Host::from(*listen_ip))
}

// Surprisingly, the default metastore and the index root uri are the same (if you exclude the
// polling_interval parameter). Indeed, this is a convenient setting for testing with a file backed
// metastore and indexes splits stored locally too.
// For a given index `index-id`, it means that we have the metastore file
// in  `./qwdata/indexes/{index-id}/metastore.json` and splits in
// dir `./qwdata/indexes/{index-id}/splits`.
fn default_metastore_uri(data_dir_uri: &Uri) -> Uri {
    data_dir_uri.join("indexes#polling_interval=30s").expect("Failed to create default metastore URI. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")
}

// See comment above.
fn default_index_root_uri(data_dir_uri: &Uri) -> Uri {
    data_dir_uri.join("indexes").expect("Failed to create default index root URI. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")
}

pub async fn load_node_config_with_env(
    config_format: ConfigFormat,
    config_content: &[u8],
    env_vars: &HashMap<String, String>,
) -> anyhow::Result<NodeConfig> {
    let rendered_config_content = render_config(config_content)?;
    let versioned_node_config: VersionedNodeConfig =
        config_format.parse(rendered_config_content.as_bytes())?;
    let node_config_builder: NodeConfigBuilder = versioned_node_config.into();
    let config = node_config_builder.build_and_validate(env_vars).await?;
    Ok(config)
}

#[derive(Debug, Deserialize)]
#[serde(tag = "version")]
enum VersionedNodeConfig {
    #[serde(rename = "0.9")]
    // Retro compatibility.
    #[serde(alias = "0.7", alias = "0.8")]
    V0_9(NodeConfigBuilder),
}

impl From<VersionedNodeConfig> for NodeConfigBuilder {
    fn from(versioned_node_config: VersionedNodeConfig) -> Self {
        match versioned_node_config {
            VersionedNodeConfig::V0_9(node_config_builder) => node_config_builder,
        }
    }
}

#[serde_with::serde_as]
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct NodeConfigBuilder {
    #[serde(default = "default_cluster_id")]
    cluster_id: ConfigValue<String, QW_CLUSTER_ID>,
    #[serde(default = "default_node_id")]
    node_id: ConfigValue<String, QW_NODE_ID>,
    #[serde(default = "default_enabled_services")]
    enabled_services: ConfigValue<List, QW_ENABLED_SERVICES>,
    #[serde(default = "default_listen_address")]
    listen_address: ConfigValue<String, QW_LISTEN_ADDRESS>,
    advertise_address: ConfigValue<String, QW_ADVERTISE_ADDRESS>,
    // Deprecated, use `rest.listen_port` instead.
    rest_listen_port: Option<u16>,
    gossip_listen_port: ConfigValue<u16, QW_GOSSIP_LISTEN_PORT>,
    grpc_listen_port: ConfigValue<u16, QW_GRPC_LISTEN_PORT>,
    gossip_interval_ms: ConfigValue<u32, QW_GOSSIP_INTERVAL_MS>,
    #[serde(default)]
    peer_seeds: ConfigValue<List, QW_PEER_SEEDS>,
    #[serde(rename = "data_dir")]
    #[serde(default = "default_data_dir_uri")]
    data_dir_uri: ConfigValue<Uri, QW_DATA_DIR>,
    metastore_uri: ConfigValue<Uri, QW_METASTORE_URI>,
    default_index_root_uri: ConfigValue<Uri, QW_DEFAULT_INDEX_ROOT_URI>,
    #[serde(rename = "rest")]
    #[serde(default)]
    rest_config_builder: RestConfigBuilder,
    #[serde(rename = "grpc")]
    #[serde(default)]
    grpc_config: GrpcConfig,
    #[serde(rename = "storage")]
    #[serde(default)]
    storage_configs: StorageConfigs,
    #[serde(rename = "metastore")]
    #[serde(default)]
    metastore_configs: MetastoreConfigs,
    #[serde(rename = "indexer")]
    #[serde(default)]
    indexer_config: IndexerConfig,
    #[serde(rename = "searcher")]
    #[serde(default)]
    searcher_config: SearcherConfig,
    #[serde(rename = "ingest_api")]
    #[serde(default)]
    ingest_api_config: IngestApiConfig,
    #[serde(rename = "jaeger")]
    #[serde(default)]
    jaeger_config: JaegerConfig,
}

impl NodeConfigBuilder {
    pub async fn build_and_validate(
        mut self,
        env_vars: &HashMap<String, String>,
    ) -> anyhow::Result<NodeConfig> {
        let node_id = self.node_id.resolve(env_vars).map(NodeId::new)?;

        let enabled_services = self
            .enabled_services
            .resolve(env_vars)?
            .0
            .into_iter()
            .map(|service| service.parse())
            .collect::<Result<_, _>>()?;

        let listen_address = self.listen_address.resolve(env_vars)?;
        let listen_host = listen_address.parse::<Host>()?;
        let listen_ip = listen_host.resolve().await?;

        if let Some(rest_listen_port) = self.rest_listen_port {
            if self.rest_config_builder.listen_port.is_some() {
                bail!(
                    "conflicting configuration values: please use only `rest.listen_port`, \
                     `rest_listen_port` is deprecated and should not be used alongside \
                     `rest.listen_port`. Update your configuration to use `rest.listen_port`."
                );
            }
            warn!("`rest_listen_port` is deprecated, use `rest.listen_port` instead");
            self.rest_config_builder.listen_port = Some(rest_listen_port);
        }

        let rest_config = self
            .rest_config_builder
            .build_and_validate(listen_ip, env_vars)?;

        self.grpc_config.validate()?;

        let gossip_listen_port = self
            .gossip_listen_port
            .resolve_optional(env_vars)?
            .unwrap_or(rest_config.listen_addr.port());
        let gossip_listen_addr = SocketAddr::new(listen_ip, gossip_listen_port);

        let grpc_listen_port = self
            .grpc_listen_port
            .resolve_optional(env_vars)?
            .unwrap_or(rest_config.listen_addr.port() + 1);
        let grpc_listen_addr = SocketAddr::new(listen_ip, grpc_listen_port);

        let advertise_address = self.advertise_address.resolve_optional(env_vars)?;
        let advertise_host = advertise_address
            .map(|addr| addr.parse::<Host>())
            .unwrap_or_else(|| default_advertise_host(&listen_ip))?;

        let advertise_ip = advertise_host.resolve().await?;
        let gossip_advertise_addr = SocketAddr::new(advertise_ip, gossip_listen_port);
        let grpc_advertise_addr = SocketAddr::new(advertise_ip, grpc_listen_port);

        let data_dir_uri = self.data_dir_uri.resolve(env_vars)?;
        let data_dir_path = data_dir_uri
            .filepath()
            .with_context(|| {
                format!(
                    "data dir must be located on the local file system. current location: \
                     `{data_dir_uri}`"
                )
            })?
            .to_path_buf();

        let metastore_uri = self
            .metastore_uri
            .resolve_optional(env_vars)?
            .unwrap_or_else(|| default_metastore_uri(&data_dir_uri));

        let default_index_root_uri = self
            .default_index_root_uri
            .resolve_optional(env_vars)?
            .unwrap_or_else(|| default_index_root_uri(&data_dir_uri));

        self.storage_configs.validate()?;
        self.storage_configs.apply_flavors();
        self.ingest_api_config.validate()?;
        self.searcher_config.validate()?;

        let gossip_interval = self
            .gossip_interval_ms
            .resolve_optional(env_vars)?
            .map(|gossip_interval_ms| Duration::from_millis(gossip_interval_ms as u64))
            .unwrap_or(DEFAULT_GOSSIP_INTERVAL);

        let node_config = NodeConfig {
            cluster_id: self.cluster_id.resolve(env_vars)?,
            node_id,
            enabled_services,
            gossip_listen_addr,
            grpc_listen_addr,
            gossip_advertise_addr,
            grpc_advertise_addr,
            gossip_interval,
            peer_seeds: self.peer_seeds.resolve(env_vars)?.0,
            data_dir_path,
            metastore_uri,
            default_index_root_uri,
            rest_config,
            grpc_config: self.grpc_config,
            metastore_configs: self.metastore_configs,
            storage_configs: self.storage_configs,
            indexer_config: self.indexer_config,
            searcher_config: self.searcher_config,
            ingest_api_config: self.ingest_api_config,
            jaeger_config: self.jaeger_config,
        };

        validate(&node_config)?;
        Ok(node_config)
    }
}

fn validate(node_config: &NodeConfig) -> anyhow::Result<()> {
    validate_identifier("cluster", &node_config.cluster_id)?;
    validate_node_id(&node_config.node_id)?;

    if node_config.cluster_id == DEFAULT_CLUSTER_ID {
        warn!("cluster ID is not set, falling back to default value `{DEFAULT_CLUSTER_ID}`",);
    }
    if node_config.peer_seeds.is_empty() {
        warn!("peer seeds are empty");
    }
    Ok(())
}

#[cfg(test)]
impl Default for NodeConfigBuilder {
    fn default() -> Self {
        Self {
            cluster_id: default_cluster_id(),
            node_id: default_node_id(),
            enabled_services: default_enabled_services(),
            listen_address: default_listen_address(),
            rest_listen_port: None,
            gossip_listen_port: ConfigValue::none(),
            grpc_listen_port: ConfigValue::none(),
            gossip_interval_ms: ConfigValue::none(),
            advertise_address: ConfigValue::none(),
            peer_seeds: ConfigValue::with_default(List::default()),
            data_dir_uri: default_data_dir_uri(),
            metastore_uri: ConfigValue::none(),
            default_index_root_uri: ConfigValue::none(),
            rest_config_builder: RestConfigBuilder::default(),
            grpc_config: GrpcConfig::default(),
            storage_configs: StorageConfigs::default(),
            metastore_configs: MetastoreConfigs::default(),
            indexer_config: IndexerConfig::default(),
            searcher_config: SearcherConfig::default(),
            ingest_api_config: IngestApiConfig::default(),
            jaeger_config: JaegerConfig::default(),
        }
    }
}

#[serde_with::serde_as]
#[derive(Debug, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
struct RestConfigBuilder {
    #[serde(default)]
    listen_port: Option<u16>,
    #[serde(default)]
    #[serde_as(deserialize_as = "serde_with::OneOrMany<_>")]
    pub cors_allow_origins: Vec<String>,
    #[serde(with = "http_serde::header_map")]
    #[serde(default)]
    pub extra_headers: HeaderMap,
}

impl RestConfigBuilder {
    fn build_and_validate(
        self,
        listen_ip: IpAddr,
        env_vars: &HashMap<String, String>,
    ) -> anyhow::Result<RestConfig> {
        let listen_port_from_config_or_default =
            self.listen_port.unwrap_or(default_rest_listen_port());
        let listen_port = ConfigValue::<u16, QW_REST_LISTEN_PORT>::with_default(
            listen_port_from_config_or_default,
        )
        .resolve(env_vars)?;
        let rest_config = RestConfig {
            listen_addr: SocketAddr::new(listen_ip, listen_port),
            cors_allow_origins: self.cors_allow_origins,
            extra_headers: self.extra_headers,
        };
        Ok(rest_config)
    }
}

#[cfg(any(test, feature = "testsuite"))]
pub fn node_config_for_tests_from_ports(
    rest_listen_port: u16,
    grpc_listen_port: u16,
) -> NodeConfig {
    let node_id = NodeId::new(default_node_id().unwrap());
    let enabled_services = QuickwitService::supported_services();
    let listen_address = Host::default();
    let rest_listen_addr = listen_address
        .with_port(rest_listen_port)
        .to_socket_addr()
        .expect("default host should be an IP address");
    let gossip_listen_addr = listen_address
        .with_port(rest_listen_port)
        .to_socket_addr()
        .expect("default host should be an IP address");
    let grpc_listen_addr = listen_address
        .with_port(grpc_listen_port)
        .to_socket_addr()
        .expect("default host should be an IP address");

    let data_dir_uri = default_data_dir_uri().unwrap();
    let data_dir_path = data_dir_uri
        .filepath()
        .expect("The default data dir should be valid directory path.")
        .to_path_buf();
    let metastore_uri = default_metastore_uri(&data_dir_uri);
    let default_index_root_uri = default_index_root_uri(&data_dir_uri);
    let rest_config = RestConfig {
        listen_addr: rest_listen_addr,
        cors_allow_origins: Vec::new(),
        extra_headers: HeaderMap::new(),
    };
    NodeConfig {
        cluster_id: default_cluster_id().unwrap(),
        node_id,
        enabled_services,
        gossip_advertise_addr: gossip_listen_addr,
        grpc_advertise_addr: grpc_listen_addr,
        gossip_listen_addr,
        grpc_listen_addr,
        gossip_interval: Duration::from_millis(25u64),
        peer_seeds: Vec::new(),
        data_dir_path,
        metastore_uri,
        default_index_root_uri,
        rest_config,
        grpc_config: GrpcConfig::default(),
        storage_configs: StorageConfigs::default(),
        metastore_configs: MetastoreConfigs::default(),
        indexer_config: IndexerConfig::default(),
        searcher_config: SearcherConfig::default(),
        ingest_api_config: IngestApiConfig::default(),
        jaeger_config: JaegerConfig::default(),
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::net::Ipv4Addr;
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::path::Path;

    use bytesize::ByteSize;
    use itertools::Itertools;

    use super::*;
    use crate::storage_config::StorageBackendFlavor;

    fn get_config_filepath(config_filename: &str) -> String {
        format!(
            "{}/resources/tests/node_config/{}",
            env!("CARGO_MANIFEST_DIR"),
            config_filename
        )
    }

    async fn test_node_config_parse_aux(config_format: ConfigFormat) -> anyhow::Result<()> {
        let config_filepath =
            get_config_filepath(&format!("quickwit.{config_format:?}").to_lowercase());
        let file = std::fs::read_to_string(&config_filepath).unwrap();
        let env_vars = HashMap::default();
        let config = load_node_config_with_env(config_format, file.as_bytes(), &env_vars).await?;
        assert_eq!(config.cluster_id, "quickwit-cluster");
        assert_eq!(config.enabled_services.len(), 2);

        assert!(config.is_service_enabled(QuickwitService::Janitor));
        assert!(config.is_service_enabled(QuickwitService::Metastore));

        assert_eq!(
            config.rest_config.listen_addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 1111)
        );
        assert_eq!(
            config.rest_config.extra_headers.get("x-header-1").unwrap(),
            "header-value-1"
        );
        assert_eq!(
            config.rest_config.extra_headers.get("x-header-2").unwrap(),
            "header-value-2"
        );
        assert_eq!(config.grpc_config.max_message_size, ByteSize::mb(10));

        assert_eq!(
            config.gossip_listen_addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 2222)
        );
        assert_eq!(
            config.grpc_listen_addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 3333)
        );
        assert_eq!(
            config.gossip_advertise_addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 0, 0, 12)), 2222)
        );
        assert_eq!(
            config.grpc_advertise_addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 0, 0, 12)), 3333)
        );
        assert_eq!(
            config.peer_seeds,
            vec![
                "quickwit-searcher-0.local".to_string(),
                "quickwit-searcher-1.local".to_string()
            ]
        );
        assert_eq!(config.data_dir_path, Path::new("/opt/quickwit/data"));
        assert_eq!(
            config.metastore_uri,
            "postgresql://username:password@host:port/db"
        );
        assert_eq!(config.default_index_root_uri, "s3://quickwit-indexes");

        let azure_storage_config = config.storage_configs.find_azure().unwrap();
        assert_eq!(
            azure_storage_config.account_name.as_ref().unwrap(),
            "quickwit-dev"
        );

        let s3_storage_config = config.storage_configs.find_s3().unwrap();
        assert_eq!(s3_storage_config.flavor.unwrap(), StorageBackendFlavor::Gcs);
        assert_eq!(
            s3_storage_config.endpoint.as_ref().unwrap(),
            "http://localhost:4566"
        );
        assert!(s3_storage_config.force_path_style_access);
        assert!(s3_storage_config.disable_multi_object_delete);
        assert!(s3_storage_config.disable_multipart_upload);

        let postgres_config = config.metastore_configs.find_postgres().unwrap();
        assert_eq!(postgres_config.min_connections, 1);
        assert_eq!(postgres_config.max_connections.get(), 12);
        assert_eq!(
            postgres_config.acquire_connection_timeout().unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(
            postgres_config.acquire_connection_timeout().unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(
            postgres_config.idle_connection_timeout_opt().unwrap(),
            Some(Duration::from_secs(1800))
        );
        assert_eq!(
            postgres_config.max_connection_lifetime_opt().unwrap(),
            Some(Duration::from_secs(3600))
        );

        assert_eq!(
            config.indexer_config,
            IndexerConfig {
                enable_otlp_endpoint: true,
                split_store_max_num_bytes: ByteSize::tb(1),
                split_store_max_num_splits: 10_000,
                max_concurrent_split_uploads: 8,
                merge_concurrency: NonZeroUsize::new(2).unwrap(),
                cpu_capacity: IndexerConfig::default_cpu_capacity(),
                enable_cooperative_indexing: false,
                max_merge_write_throughput: Some(ByteSize::mb(100)),
            }
        );
        assert_eq!(
            config.ingest_api_config,
            IngestApiConfig {
                replication_factor: 2,
                ..Default::default()
            }
        );
        assert_eq!(
            config.searcher_config,
            SearcherConfig {
                aggregation_memory_limit: ByteSize::gb(1),
                aggregation_bucket_limit: 500_000,
                fast_field_cache_capacity: ByteSize::gb(10),
                split_footer_cache_capacity: ByteSize::gb(1),
                partial_request_cache_capacity: ByteSize::mb(64),
                max_num_concurrent_split_searches: 150,
                max_num_concurrent_split_streams: 120,
                split_cache: None,
                request_timeout_secs: NonZeroU64::new(30).unwrap(),
                storage_timeout_policy: Some(crate::StorageTimeoutPolicy {
                    min_throughtput_bytes_per_secs: 100_000,
                    timeout_millis: 2_000,
                    max_num_retries: 2
                }),
                warmup_memory_budget: ByteSize::gb(100),
                warmup_single_split_initial_allocation: ByteSize::gb(1),
            }
        );
        assert_eq!(
            config.jaeger_config,
            JaegerConfig {
                enable_endpoint: true,
                lookback_period_hours: NonZeroU64::new(24).unwrap(),
                max_trace_duration_secs: NonZeroU64::new(600).unwrap(),
                max_fetch_spans: NonZeroU64::new(1_000).unwrap(),
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_node_config_parse_json() {
        test_node_config_parse_aux(ConfigFormat::Json)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_node_config_parse_toml() {
        test_node_config_parse_aux(ConfigFormat::Toml)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_node_config_parse_yaml() {
        test_node_config_parse_aux(ConfigFormat::Yaml)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_config_contains_wrong_values() {
        let config_filepath = get_config_filepath("quickwit.wrongkey.yaml");
        let config_str = std::fs::read_to_string(&config_filepath).unwrap();
        let parsing_error = super::load_node_config_with_env(
            ConfigFormat::Yaml,
            config_str.as_bytes(),
            &Default::default(),
        )
        .await
        .unwrap_err();
        assert!(format!("{parsing_error:?}")
            .contains("unknown field `max_num_concurrent_split_searches_with_typo`"));
    }

    #[tokio::test]
    async fn test_node_config_default_values_minimal() {
        let config_yaml = "version: 0.8";
        let config = load_node_config_with_env(
            ConfigFormat::Yaml,
            config_yaml.as_bytes(),
            &Default::default(),
        )
        .await
        .unwrap();
        assert_eq!(config.cluster_id, DEFAULT_CLUSTER_ID);
        assert_eq!(config.node_id, get_short_hostname().unwrap());
        assert_eq!(
            config.enabled_services,
            QuickwitService::supported_services()
        );
        assert_eq!(
            config.rest_config.listen_addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7280)
        );
        assert_eq!(
            config.gossip_listen_addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7280)
        );
        assert_eq!(
            config.grpc_listen_addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7281)
        );
        assert_eq!(
            config.data_dir_path.to_string_lossy(),
            format!("{}/qwdata", env::current_dir().unwrap().display())
        );
        assert_eq!(
            config.metastore_uri,
            format!(
                "file://{}/qwdata/indexes#polling_interval=30s",
                env::current_dir().unwrap().display()
            )
        );
        assert_eq!(
            config.default_index_root_uri,
            format!(
                "file://{}/qwdata/indexes",
                env::current_dir().unwrap().display()
            )
        );
        assert_eq!(config.ingest_api_config.replication_factor, 1);
    }

    #[tokio::test]
    async fn test_node_config_env_var_override() {
        let config_yaml = "version: 0.8";
        let mut env_vars = HashMap::new();
        env_vars.insert("QW_CLUSTER_ID".to_string(), "test-cluster".to_string());
        env_vars.insert("QW_NODE_ID".to_string(), "test-node".to_string());
        env_vars.insert(
            "QW_ENABLED_SERVICES".to_string(),
            "indexer,metastore".to_string(),
        );
        env_vars.insert("QW_LISTEN_ADDRESS".to_string(), "172.0.0.12".to_string());
        env_vars.insert("QW_ADVERTISE_ADDRESS".to_string(), "172.0.0.13".to_string());
        env_vars.insert("QW_REST_LISTEN_PORT".to_string(), "1234".to_string());
        env_vars.insert("QW_GOSSIP_LISTEN_PORT".to_string(), "5678".to_string());
        env_vars.insert("QW_GRPC_LISTEN_PORT".to_string(), "9012".to_string());
        env_vars.insert(
            "QW_PEER_SEEDS".to_string(),
            "test-peer-seed-0,test-peer-seed-1".to_string(),
        );
        env_vars.insert("QW_DATA_DIR".to_string(), "test-data-dir".to_string());
        env_vars.insert(
            "QW_METASTORE_URI".to_string(),
            "postgresql://test-user:test-password@test-host:4321/test-db".to_string(),
        );
        env_vars.insert(
            "QW_DEFAULT_INDEX_ROOT_URI".to_string(),
            "s3://quickwit-indexes/prod".to_string(),
        );
        let config =
            load_node_config_with_env(ConfigFormat::Yaml, config_yaml.as_bytes(), &env_vars)
                .await
                .unwrap();
        assert_eq!(config.cluster_id, "test-cluster");
        assert_eq!(config.node_id, "test-node");
        assert_eq!(config.enabled_services.len(), 2);
        assert_eq!(
            config
                .enabled_services
                .iter()
                .sorted_by_key(|service| service.as_str())
                .collect::<Vec<_>>(),
            &[&QuickwitService::Indexer, &QuickwitService::Metastore]
        );
        assert_eq!(
            config.rest_config.listen_addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 0, 0, 12)), 1234)
        );
        assert_eq!(
            config.gossip_listen_addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 0, 0, 12)), 5678)
        );
        assert_eq!(
            config.grpc_listen_addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 0, 0, 12)), 9012)
        );
        assert_eq!(
            config.gossip_advertise_addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 0, 0, 13)), 5678)
        );
        assert_eq!(
            config.grpc_advertise_addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 0, 0, 13)), 9012)
        );
        assert_eq!(
            config.peer_seeds,
            vec![
                "test-peer-seed-0".to_string(),
                "test-peer-seed-1".to_string()
            ]
        );
        assert_eq!(
            config.data_dir_path,
            env::current_dir().unwrap().join("test-data-dir")
        );
        assert_eq!(
            config.metastore_uri,
            "postgresql://test-user:test-password@test-host:4321/test-db"
        );
        assert_eq!(config.default_index_root_uri, "s3://quickwit-indexes/prod");
    }

    #[tokio::test]
    async fn test_quickwwit_config_default_values_storage() {
        let config_yaml = r#"
            version: 0.8
            node_id: "node-1"
            metastore_uri: postgres://username:password@host:port/db
        "#;
        let config = load_node_config_with_env(
            ConfigFormat::Yaml,
            config_yaml.as_bytes(),
            &Default::default(),
        )
        .await
        .unwrap();
        assert_eq!(config.cluster_id, DEFAULT_CLUSTER_ID);
        assert_eq!(config.node_id, "node-1");
        assert_eq!(
            config.metastore_uri,
            "postgresql://username:password@host:port/db"
        );
    }

    #[tokio::test]
    async fn test_node_config_config_default_values_default_indexer_searcher_config() {
        let config_yaml = r#"
            version: 0.8
            metastore_uri: postgres://username:password@host:port/db
            data_dir: /opt/quickwit/data
        "#;
        let config = load_node_config_with_env(
            ConfigFormat::Yaml,
            config_yaml.as_bytes(),
            &Default::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            config.metastore_uri,
            "postgresql://username:password@host:port/db"
        );
        assert_eq!(config.indexer_config, IndexerConfig::default());
        assert_eq!(config.searcher_config, SearcherConfig::default());
        assert_eq!(config.ingest_api_config, IngestApiConfig::default());
        assert_eq!(config.jaeger_config, JaegerConfig::default());
    }

    #[tokio::test]
    async fn test_node_config_validate() {
        let config_filepath = get_config_filepath("quickwit.toml");
        let file_content = std::fs::read_to_string(&config_filepath).unwrap();

        let data_dir_path = env::current_dir().unwrap();
        let mut env_vars = HashMap::new();
        env_vars.insert(
            "QW_DATA_DIR".to_string(),
            data_dir_path.to_string_lossy().to_string(),
        );
        load_node_config_with_env(ConfigFormat::Toml, file_content.as_bytes(), &env_vars)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_peer_socket_addrs() {
        {
            let node_config = NodeConfigBuilder {
                ..Default::default()
            }
            .build_and_validate(&HashMap::new())
            .await
            .unwrap();
            assert!(node_config.peer_seed_addrs().await.unwrap().is_empty());
        }
        {
            let node_config = NodeConfigBuilder {
                rest_config_builder: RestConfigBuilder {
                    listen_port: Some(1789),
                    ..Default::default()
                },
                peer_seeds: ConfigValue::for_test(List(vec![
                    "unresolvable.example.com".to_string(),
                    "localhost".to_string(),
                    "localhost:1337".to_string(),
                    "127.0.0.1".to_string(),
                    "127.0.0.1:1337".to_string(),
                ])),
                ..Default::default()
            }
            .build_and_validate(&HashMap::new())
            .await
            .unwrap();
            assert_eq!(
                node_config.peer_seed_addrs().await.unwrap(),
                vec![
                    "localhost:1789".to_string(),
                    "localhost:1337".to_string(),
                    "127.0.0.1:1789".to_string(),
                    "127.0.0.1:1337".to_string()
                ]
            );
        }
    }

    #[tokio::test]
    async fn test_socket_addr_ports() {
        {
            let node_config = NodeConfigBuilder {
                listen_address: default_listen_address(),
                ..Default::default()
            }
            .build_and_validate(&HashMap::new())
            .await
            .unwrap();
            assert_eq!(
                node_config.rest_config.listen_addr.to_string(),
                "127.0.0.1:7280"
            );
            assert_eq!(node_config.gossip_listen_addr.to_string(), "127.0.0.1:7280");
            assert_eq!(node_config.grpc_listen_addr.to_string(), "127.0.0.1:7281");
        }
        {
            let node_config = NodeConfigBuilder {
                listen_address: default_listen_address(),
                rest_config_builder: RestConfigBuilder {
                    listen_port: Some(1789),
                    ..Default::default()
                },
                ..Default::default()
            }
            .build_and_validate(&HashMap::new())
            .await
            .unwrap();
            assert_eq!(
                node_config.rest_config.listen_addr.to_string(),
                "127.0.0.1:1789"
            );
            assert_eq!(node_config.gossip_listen_addr.to_string(), "127.0.0.1:1789");
            assert_eq!(node_config.grpc_listen_addr.to_string(), "127.0.0.1:1790");
        }
        {
            let node_config = NodeConfigBuilder {
                listen_address: default_listen_address(),
                gossip_listen_port: ConfigValue::for_test(1889),
                grpc_listen_port: ConfigValue::for_test(1989),
                rest_config_builder: RestConfigBuilder {
                    listen_port: Some(1789),
                    ..Default::default()
                },
                ..Default::default()
            }
            .build_and_validate(&HashMap::new())
            .await
            .unwrap();
            assert_eq!(
                node_config.rest_config.listen_addr.to_string(),
                "127.0.0.1:1789"
            );
            assert_eq!(node_config.gossip_listen_addr.to_string(), "127.0.0.1:1889");
            assert_eq!(node_config.grpc_listen_addr.to_string(), "127.0.0.1:1989");
        }
    }

    #[tokio::test]
    async fn test_rest_deprecated_listen_port_config() {
        // This test should be removed once deprecated `rest_listen_port` field is removed.
        let node_config = NodeConfigBuilder {
            rest_listen_port: Some(1789),
            listen_address: default_listen_address(),
            rest_config_builder: RestConfigBuilder {
                listen_port: None,
                ..Default::default()
            },
            ..Default::default()
        }
        .build_and_validate(&HashMap::new())
        .await
        .unwrap();
        assert_eq!(
            node_config.rest_config.listen_addr.to_string(),
            "127.0.0.1:1789"
        );
        assert_eq!(node_config.gossip_listen_addr.to_string(), "127.0.0.1:1789");
        assert_eq!(node_config.grpc_listen_addr.to_string(), "127.0.0.1:1790");
    }

    #[tokio::test]
    async fn test_load_config_with_validation_error() {
        let config_filepath = get_config_filepath("quickwit.yaml");
        let file = std::fs::read_to_string(&config_filepath).unwrap();
        let error = NodeConfig::load(ConfigFormat::Yaml, file.as_bytes())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("data dir"));
    }

    #[tokio::test]
    async fn test_config_validates_uris() {
        {
            let config_yaml = r#"
            version: 0.8
            node_id: 1
            metastore_uri: ''
        "#;
            assert!(load_node_config_with_env(
                ConfigFormat::Yaml,
                config_yaml.as_bytes(),
                &Default::default()
            )
            .await
            .is_err());
        }
        {
            let config_yaml = r#"
            version: 0.8
            node_id: 1
            metastore_uri: postgres://username:password@host:port/db
            default_index_root_uri: ''
        "#;
            assert!(load_node_config_with_env(
                ConfigFormat::Yaml,
                config_yaml.as_bytes(),
                &Default::default()
            )
            .await
            .is_err());
        }
    }

    #[tokio::test]
    async fn test_node_config_data_dir_accepts_both_file_uris_and_file_paths() {
        {
            let config_yaml = r#"
                version: 0.8
                data_dir: /opt/quickwit/data
            "#;
            let config = load_node_config_with_env(
                ConfigFormat::Yaml,
                config_yaml.as_bytes(),
                &HashMap::default(),
            )
            .await
            .unwrap();
            assert_eq!(&config.data_dir_path, Path::new("/opt/quickwit/data"));
        }
        {
            let config_yaml = r#"
                version: 0.8
                data_dir: file:///opt/quickwit/data
            "#;
            let config = load_node_config_with_env(
                ConfigFormat::Yaml,
                config_yaml.as_bytes(),
                &HashMap::default(),
            )
            .await
            .unwrap();
            assert_eq!(&config.data_dir_path, Path::new("/opt/quickwit/data"));
        }
        {
            let config_yaml = r#"
                version: 0.8
                data_dir: s3://indexes/foo
            "#;
            let error = load_node_config_with_env(
                ConfigFormat::Yaml,
                config_yaml.as_bytes(),
                &HashMap::default(),
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("data dir must be located"));
        }
    }

    #[tokio::test]
    async fn test_config_invalid_when_both_listen_ports_params_are_configured() {
        let config_yaml = r#"
                version: 0.8
                rest_listen_port: 1789
                rest:
                  listen_port: 1789
            "#;
        let config = load_node_config_with_env(
            ConfigFormat::Yaml,
            config_yaml.as_bytes(),
            &HashMap::default(),
        )
        .await
        .unwrap_err();
        assert_eq!(
            &config.to_string(),
            "conflicting configuration values: please use only `rest.listen_port`, \
             `rest_listen_port` is deprecated and should not be used alongside \
             `rest.listen_port`. Update your configuration to use `rest.listen_port`."
        );
    }

    #[test]
    fn test_jaeger_config_rejects_null_values() {
        let jaeger_config_yaml = r#"
            enable_endpoint: true
            max_trace_duration_secs: 0
        "#;
        let error = serde_yaml::from_str::<JaegerConfig>(jaeger_config_yaml).unwrap_err();
        assert!(error
            .to_string()
            .contains("max_trace_duration_secs: invalid value: integer `0`"))
    }

    #[tokio::test]
    async fn test_rest_config_accepts_wildcard() {
        let rest_config_yaml = r#"
            version: 0.8
            rest:
              cors_allow_origins: '*'
        "#;
        let config = load_node_config_with_env(
            ConfigFormat::Yaml,
            rest_config_yaml.as_bytes(),
            &Default::default(),
        )
        .await
        .expect("Deserialize rest config");
        assert_eq!(config.rest_config.cors_allow_origins, ["*"]);
    }

    #[tokio::test]
    async fn test_rest_config_accepts_single_origin() {
        let rest_config_yaml = r#"
            version: 0.8
            rest:
              cors_allow_origins:
                - https://www.my-domain.com
        "#;
        let config = load_node_config_with_env(
            ConfigFormat::Yaml,
            rest_config_yaml.as_bytes(),
            &Default::default(),
        )
        .await
        .expect("Deserialize rest config");
        assert_eq!(
            config.rest_config.cors_allow_origins,
            ["https://www.my-domain.com"]
        );

        let rest_config_yaml = r#"
            version: 0.8
            rest:
              cors_allow_origins: http://192.168.0.108:7280
        "#;
        let config = load_node_config_with_env(
            ConfigFormat::Yaml,
            rest_config_yaml.as_bytes(),
            &Default::default(),
        )
        .await
        .expect("Deserialize rest config");
        assert_eq!(
            config.rest_config.cors_allow_origins,
            ["http://192.168.0.108:7280"]
        );
    }

    #[tokio::test]
    async fn test_rest_config_accepts_multi_origin() {
        let rest_config_yaml = r#"
            version: 0.8
            rest:
              cors_allow_origins:
                - https://www.my-domain.com
        "#;
        let config = load_node_config_with_env(
            ConfigFormat::Yaml,
            rest_config_yaml.as_bytes(),
            &Default::default(),
        )
        .await
        .expect("Deserialize rest config");
        assert_eq!(
            config.rest_config.cors_allow_origins,
            ["https://www.my-domain.com"]
        );

        let rest_config_yaml = r#"
            version: 0.8
            rest:
              cors_allow_origins:
                - https://www.my-domain.com
                - https://www.my-other-domain.com
        "#;
        let config = load_node_config_with_env(
            ConfigFormat::Yaml,
            rest_config_yaml.as_bytes(),
            &Default::default(),
        )
        .await
        .expect("Deserialize rest config");
        assert_eq!(
            config.rest_config.cors_allow_origins,
            [
                "https://www.my-domain.com",
                "https://www.my-other-domain.com"
            ]
        );

        let rest_config_yaml = r#"
            version: 0.8
            rest:
              rest_cors_allow_origins:
        "#;
        load_node_config_with_env(
            ConfigFormat::Yaml,
            rest_config_yaml.as_bytes(),
            &Default::default(),
        )
        .await
        .expect_err("Config should not allow empty origins.");

        let rest_config_yaml = r#"
            version: 0.8
            rest:
              cors_allow_origins:
                -
        "#;
        load_node_config_with_env(
            ConfigFormat::Yaml,
            rest_config_yaml.as_bytes(),
            &Default::default(),
        )
        .await
        .expect_err("Config should not allow empty origins.");
    }

    #[tokio::test]
    async fn test_node_config_validates_ingest_config() {
        let ingest_config = IngestApiConfig {
            replication_factor: 0,
            ..Default::default()
        };
        let error_message = ingest_config.validate().unwrap_err().to_string();
        assert!(error_message.contains("either 1 or 2, got `0`"));

        let ingest_config = IngestApiConfig {
            replication_factor: 3,
            ..Default::default()
        };
        let error_message = ingest_config.validate().unwrap_err().to_string();
        assert!(error_message.contains("either 1 or 2, got `3`"));

        let node_config_yaml = r#"
            version: 0.8
            ingest_api:
              replication_factor: 0
        "#;
        let error_message = load_node_config_with_env(
            ConfigFormat::Yaml,
            node_config_yaml.as_bytes(),
            &Default::default(),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error_message.contains("replication factor"));
    }
}
