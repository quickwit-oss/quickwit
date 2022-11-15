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
use std::env;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{bail, Context};
use byte_unit::Byte;
use quickwit_common::net::{find_private_ip, Host, HostAddr};
use quickwit_common::new_coolid;
use quickwit_common::uri::Uri;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config_value::ConfigValue;
use crate::qw_env_vars::*;
use crate::service::QuickwitService;
use crate::templating::render_config;
use crate::{validate_identifier, ConfigFormat};

pub const DEFAULT_QW_CONFIG_PATH: &str = "config/quickwit.yaml";

pub const DEFAULT_CLUSTER_ID: &str = "quickwit-default-cluster";

pub const DEFAULT_DATA_DIR_PATH: &str = "qwdata";

// Default config values in the order they appear in [`QuickwitConfigBuilder`].
fn default_cluster_id() -> ConfigValue<String, QW_CLUSTER_ID> {
    ConfigValue::with_default(DEFAULT_CLUSTER_ID.to_string())
}

fn default_node_id() -> ConfigValue<String, QW_NODE_ID> {
    ConfigValue::with_default(new_coolid("node"))
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

fn default_rest_listen_port() -> ConfigValue<u16, QW_REST_LISTEN_PORT> {
    ConfigValue::with_default(7280)
}

fn default_data_dir_uri() -> ConfigValue<Uri, QW_DATA_DIR> {
    ConfigValue::with_default(Uri::from_str(DEFAULT_DATA_DIR_PATH).unwrap())
}

/// Returns the default advertise host.
fn default_advertise_host(listen_ip: &IpAddr) -> anyhow::Result<Host> {
    if listen_ip.is_unspecified() {
        if let Some((interface_name, private_ip)) = find_private_ip() {
            info!(advertise_address=%private_ip, interface_name=%interface_name, "Using sniffed advertise address.");
            return Ok(Host::from(private_ip));
        }
        bail!("Listen address `{listen_ip}` is unspecified and advertise address is not set.");
    }
    info!(advertise_address=%listen_ip, "Using listen address as advertise address.");
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerConfig {
    #[doc(hidden)]
    #[serde(default = "IndexerConfig::default_enable_opentelemetry_otlp_service")]
    pub enable_opentelemetry_otlp_service: bool,
    #[serde(default = "IndexerConfig::default_split_store_max_num_bytes")]
    pub split_store_max_num_bytes: Byte,
    #[serde(default = "IndexerConfig::default_split_store_max_num_splits")]
    pub split_store_max_num_splits: usize,
    #[serde(default = "IndexerConfig::default_max_concurrent_split_uploads")]
    pub max_concurrent_split_uploads: usize,
}

impl IndexerConfig {
    fn default_enable_opentelemetry_otlp_service() -> bool {
        false
    }

    fn default_max_concurrent_split_uploads() -> usize {
        12
    }

    pub fn default_split_store_max_num_bytes() -> Byte {
        Byte::from_bytes(100_000_000_000) // 100G
    }

    pub fn default_split_store_max_num_splits() -> usize {
        1_000
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> anyhow::Result<Self> {
        let indexer_config = IndexerConfig {
            enable_opentelemetry_otlp_service: true,
            split_store_max_num_bytes: Byte::from_bytes(1_000_000),
            split_store_max_num_splits: 3,
            max_concurrent_split_uploads: 4,
        };
        Ok(indexer_config)
    }
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            enable_opentelemetry_otlp_service: Self::default_enable_opentelemetry_otlp_service(),
            split_store_max_num_bytes: Self::default_split_store_max_num_bytes(),
            split_store_max_num_splits: Self::default_split_store_max_num_splits(),
            max_concurrent_split_uploads: Self::default_max_concurrent_split_uploads(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearcherConfig {
    #[doc(hidden)]
    #[serde(default = "SearcherConfig::default_enable_jaeger_service")]
    pub enable_jaeger_service: bool,
    #[serde(default = "SearcherConfig::default_fast_field_cache_capacity")]
    pub fast_field_cache_capacity: Byte,
    #[serde(default = "SearcherConfig::default_split_footer_cache_capacity")]
    pub split_footer_cache_capacity: Byte,
    #[serde(default = "SearcherConfig::default_max_num_concurrent_split_searches")]
    pub max_num_concurrent_split_searches: usize,
    #[serde(default = "SearcherConfig::default_max_num_concurrent_split_streams")]
    pub max_num_concurrent_split_streams: usize,
}

impl SearcherConfig {
    fn default_enable_jaeger_service() -> bool {
        false
    }

    fn default_fast_field_cache_capacity() -> Byte {
        Byte::from_bytes(1_000_000_000) // 1G
    }

    fn default_split_footer_cache_capacity() -> Byte {
        Byte::from_bytes(500_000_000) // 500M
    }

    fn default_max_num_concurrent_split_searches() -> usize {
        100
    }

    fn default_max_num_concurrent_split_streams() -> usize {
        100
    }
}

impl Default for SearcherConfig {
    fn default() -> Self {
        Self {
            enable_jaeger_service: Self::default_enable_jaeger_service(),
            fast_field_cache_capacity: Self::default_fast_field_cache_capacity(),
            split_footer_cache_capacity: Self::default_split_footer_cache_capacity(),
            max_num_concurrent_split_streams: Self::default_max_num_concurrent_split_streams(),
            max_num_concurrent_split_searches: Self::default_max_num_concurrent_split_searches(),
        }
    }
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

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct QuickwitConfigBuilder {
    #[serde(default)]
    version: ConfigValue<String, QW_NONE>,
    #[serde(default = "default_cluster_id")]
    cluster_id: ConfigValue<String, QW_CLUSTER_ID>,
    #[serde(default = "default_node_id")]
    node_id: ConfigValue<String, QW_NODE_ID>,
    #[serde(default = "default_enabled_services")]
    enabled_services: ConfigValue<List, QW_ENABLED_SERVICES>,
    #[serde(default = "default_listen_address")]
    listen_address: ConfigValue<String, QW_LISTEN_ADDRESS>,
    advertise_address: ConfigValue<String, QW_ADVERTISE_ADDRESS>,
    #[serde(default = "default_rest_listen_port")]
    rest_listen_port: ConfigValue<u16, QW_REST_LISTEN_PORT>,
    gossip_listen_port: ConfigValue<u16, QW_GOSSIP_LISTEN_PORT>,
    grpc_listen_port: ConfigValue<u16, QW_GRPC_LISTEN_PORT>,
    #[serde(default)]
    peer_seeds: ConfigValue<List, QW_PEER_SEEDS>,
    #[serde(rename = "data_dir")]
    #[serde(default = "default_data_dir_uri")]
    data_dir_uri: ConfigValue<Uri, QW_DATA_DIR>,
    metastore_uri: ConfigValue<Uri, QW_METASTORE_URI>,
    default_index_root_uri: ConfigValue<Uri, QW_DEFAULT_INDEX_ROOT_URI>,
    #[serde(rename = "indexer")]
    #[serde(default)]
    indexer_config: IndexerConfig,
    #[serde(rename = "searcher")]
    #[serde(default)]
    searcher_config: SearcherConfig,
}

impl QuickwitConfigBuilder {
    fn from_uri(uri: &Uri, config_content: &[u8]) -> anyhow::Result<Self> {
        let config_format = ConfigFormat::sniff_from_uri(uri)?;
        let rendered_config = render_config(uri, config_content)?;
        config_format.parse(rendered_config.as_bytes())
    }

    pub async fn build(self, env_vars: &HashMap<String, String>) -> anyhow::Result<QuickwitConfig> {
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

        let rest_listen_port = self.rest_listen_port.resolve(env_vars)?;
        let rest_listen_addr = SocketAddr::new(listen_ip, rest_listen_port);

        let gossip_listen_port = self
            .gossip_listen_port
            .resolve_optional(env_vars)?
            .unwrap_or(rest_listen_port);
        let gossip_listen_addr = SocketAddr::new(listen_ip, gossip_listen_port);

        let grpc_listen_port = self
            .grpc_listen_port
            .resolve_optional(env_vars)?
            .unwrap_or(rest_listen_port + 1);
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
                    "Data dir must be located on the local file system. Current location: \
                     `{data_dir_uri}`."
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

        Ok(QuickwitConfig {
            version: self.version.resolve(env_vars)?,
            cluster_id: self.cluster_id.resolve(env_vars)?,
            node_id: self.node_id.resolve(env_vars)?,
            enabled_services,
            rest_listen_addr,
            gossip_listen_addr,
            grpc_listen_addr,
            gossip_advertise_addr,
            grpc_advertise_addr,
            peer_seeds: self.peer_seeds.resolve(env_vars)?.0,
            data_dir_path,
            metastore_uri,
            default_index_root_uri,
            indexer_config: self.indexer_config,
            searcher_config: self.searcher_config,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct QuickwitConfig {
    pub version: String,
    pub cluster_id: String,
    pub node_id: String,
    pub enabled_services: HashSet<QuickwitService>,
    pub rest_listen_addr: SocketAddr,
    pub gossip_listen_addr: SocketAddr,
    pub grpc_listen_addr: SocketAddr,
    pub gossip_advertise_addr: SocketAddr,
    pub grpc_advertise_addr: SocketAddr,
    pub peer_seeds: Vec<String>,
    pub data_dir_path: PathBuf,
    pub metastore_uri: Uri,
    pub default_index_root_uri: Uri,
    pub indexer_config: IndexerConfig,
    pub searcher_config: SearcherConfig,
}

impl QuickwitConfig {
    /// Parses and validates a [`QuickwitConfig`] from a given URI and config content.
    pub async fn load(config_uri: &Uri, config_content: &[u8]) -> anyhow::Result<Self> {
        let env_vars = env::vars().collect::<HashMap<_, _>>();
        let config = Self::load_with_env(config_uri, config_content, &env_vars).await?;
        Ok(config)
    }

    pub async fn load_with_env(
        config_uri: &Uri,
        config_content: &[u8],
        env_vars: &HashMap<String, String>,
    ) -> anyhow::Result<Self> {
        let config_builder = QuickwitConfigBuilder::from_uri(config_uri, config_content)?;
        let config = config_builder.build(env_vars).await?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        validate_identifier("Cluster ID", &self.cluster_id)?;
        validate_identifier("Node ID", &self.node_id)?;

        if self.cluster_id == DEFAULT_CLUSTER_ID {
            warn!(
                cluster_id=%DEFAULT_CLUSTER_ID,
                "Cluster ID is not set, falling back to default value."
            );
        }
        if self.peer_seeds.is_empty() {
            warn!("Peer seed list is empty.");
        }
        if !self.data_dir_path.exists() {
            bail!(
                "Data dir `{}` does not exist.",
                self.data_dir_path.display()
            );
        }
        Ok(())
    }

    /// Returns the list of peer seed addresses. The addresses MUST NOT be resolved. Otherwise, the
    /// DNS-based discovery mechanism implemented in Chitchat will not work correctly.
    pub async fn peer_seed_addrs(&self) -> anyhow::Result<Vec<String>> {
        let mut peer_seed_addrs = Vec::new();
        let default_gossip_port = self.gossip_listen_addr.port();

        // We want to pass non-resolved addresses to Chitchat but still want to resolve them for
        // validation purposes. Additionally, we need to append a default port if necessary and
        // finally return the addresses as strings, which is tricky for IPv6. We let the logic baked
        // in `HostAddr` handle this complexity.
        for peer_seed in &self.peer_seeds {
            let peer_seed_addr = HostAddr::parse_with_default_port(peer_seed, default_gossip_port)?;
            if let Err(error) = peer_seed_addr.resolve().await {
                warn!(peer_seed = %peer_seed_addr, error = ?error, "Failed to resolve peer seed address.");
                continue;
            }
            peer_seed_addrs.push(peer_seed_addr.to_string())
        }
        if !self.peer_seeds.is_empty() && peer_seed_addrs.is_empty() {
            bail!(
                "Failed to resolve any of the peer seed addresses: `{}`",
                self.peer_seeds.join(", ")
            )
        }
        Ok(peer_seed_addrs)
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> Self {
        let enabled_services = QuickwitService::supported_services();

        let listen_address = Host::default();
        let rest_listen_port = quickwit_common::net::find_available_tcp_port()
            .expect("The OS should almost always find an available port.");
        let rest_listen_addr = listen_address
            .with_port(rest_listen_port)
            .to_socket_addr()
            .expect("The default host should be an IP address.");
        let gossip_listen_addr = listen_address
            .with_port(rest_listen_port)
            .to_socket_addr()
            .expect("The default host should be an IP address.");
        let grpc_listen_port = quickwit_common::net::find_available_tcp_port()
            .expect("The OS should almost always find an available port.");
        let grpc_listen_addr = listen_address
            .with_port(grpc_listen_port)
            .to_socket_addr()
            .expect("The default host should be an IP address.");

        let data_dir_uri = default_data_dir_uri().unwrap();
        let data_dir_path = data_dir_uri
            .filepath()
            .expect("The default data dir should be valid directory path.")
            .to_path_buf();
        let metastore_uri = default_metastore_uri(&data_dir_uri);
        let default_index_root_uri = default_index_root_uri(&data_dir_uri);

        Self {
            version: "3".to_string(),
            cluster_id: default_cluster_id().unwrap(),
            node_id: default_node_id().unwrap(),
            enabled_services,
            gossip_advertise_addr: gossip_listen_addr,
            grpc_advertise_addr: grpc_listen_addr,
            rest_listen_addr,
            gossip_listen_addr,
            grpc_listen_addr,
            peer_seeds: Vec::new(),
            data_dir_path,
            metastore_uri,
            default_index_root_uri,
            indexer_config: IndexerConfig::default(),
            searcher_config: SearcherConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::path::Path;

    use itertools::Itertools;

    use super::*;

    impl Default for QuickwitConfigBuilder {
        fn default() -> Self {
            Self {
                version: ConfigValue::none(),
                cluster_id: default_cluster_id(),
                node_id: default_node_id(),
                enabled_services: default_enabled_services(),
                listen_address: default_listen_address(),
                rest_listen_port: default_rest_listen_port(),
                gossip_listen_port: ConfigValue::none(),
                grpc_listen_port: ConfigValue::none(),
                advertise_address: ConfigValue::none(),
                peer_seeds: ConfigValue::with_default(List::default()),
                data_dir_uri: default_data_dir_uri(),
                metastore_uri: ConfigValue::none(),
                default_index_root_uri: ConfigValue::none(),
                indexer_config: IndexerConfig::default(),
                searcher_config: SearcherConfig::default(),
            }
        }
    }

    fn get_config_filepath(config_filename: &str) -> String {
        format!(
            "{}/resources/tests/config/{}",
            env!("CARGO_MANIFEST_DIR"),
            config_filename
        )
    }

    #[track_caller]
    async fn test_quickwit_config_parse_aux(config_format: ConfigFormat) -> anyhow::Result<()> {
        let config_filepath =
            get_config_filepath(&format!("quickwit.{config_format:?}").to_lowercase());
        let config_uri = Uri::from_str(&config_filepath)?;
        let file = std::fs::read_to_string(&config_filepath).unwrap();
        let config = QuickwitConfigBuilder::from_uri(&config_uri, file.as_bytes())?
            .build(&HashMap::new())
            .await?;
        assert_eq!(config.version, "3");
        assert_eq!(config.cluster_id, "quickwit-cluster");

        assert_eq!(config.enabled_services.len(), 2);

        assert!(config.enabled_services.contains(&QuickwitService::Janitor));
        assert!(config
            .enabled_services
            .contains(&QuickwitService::Metastore));

        assert_eq!(
            config.rest_listen_addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 1111)
        );
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
            "postgres://username:password@host:port/db"
        );
        assert_eq!(config.default_index_root_uri, "s3://quickwit-indexes");
        assert_eq!(
            config.indexer_config,
            IndexerConfig {
                enable_opentelemetry_otlp_service: false,
                split_store_max_num_bytes: Byte::from_str("1T").unwrap(),
                split_store_max_num_splits: 10_000,
                max_concurrent_split_uploads: 8,
            }
        );
        assert_eq!(
            config.searcher_config,
            SearcherConfig {
                enable_jaeger_service: false,
                fast_field_cache_capacity: Byte::from_str("10G").unwrap(),
                split_footer_cache_capacity: Byte::from_str("1G").unwrap(),
                max_num_concurrent_split_searches: 150,
                max_num_concurrent_split_streams: 120,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_quickwit_config_parse_json() {
        test_quickwit_config_parse_aux(ConfigFormat::Json)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_quickwit_config_parse_toml() {
        test_quickwit_config_parse_aux(ConfigFormat::Toml)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_quickwit_config_parse_yaml() {
        test_quickwit_config_parse_aux(ConfigFormat::Toml)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_config_contains_wrong_values() {
        let config_filepath = get_config_filepath("quickwit.wrongkey.yaml");
        let config_uri = Uri::from_str(&config_filepath).unwrap();
        let config_str = std::fs::read_to_string(&config_filepath).unwrap();
        let parsing_error =
            QuickwitConfigBuilder::from_uri(&config_uri, config_str.as_bytes()).unwrap_err();
        assert!(format!("{parsing_error:?}")
            .contains("unknown field `max_num_concurrent_split_searchs`"));
    }

    #[test]
    fn test_indexer_config_default_values() {
        let indexer_config = serde_yaml::from_str::<IndexerConfig>("{}").unwrap();
        assert_eq!(indexer_config, IndexerConfig::default());
    }

    #[test]
    fn test_searcher_config_default_values() {
        let searcher_config = serde_yaml::from_str::<SearcherConfig>("{}").unwrap();
        assert_eq!(searcher_config, SearcherConfig::default());
    }

    #[tokio::test]
    async fn test_quickwit_config_default_values_minimal() {
        let config_yaml = "version: 3";
        let config_builder = serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
        let config = config_builder.build(&HashMap::new()).await.unwrap();
        assert_eq!(config.version, "3");
        assert_eq!(config.cluster_id, DEFAULT_CLUSTER_ID);
        assert!(config.node_id.starts_with("node-"));
        assert_eq!(
            config.enabled_services,
            QuickwitService::supported_services()
        );
        assert_eq!(
            config.rest_listen_addr,
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
    }

    #[tokio::test]
    async fn test_quickwit_config_env_var_override() {
        let config_yaml = "version: 3";
        let config_builder = serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
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
            "postgres://test-user:test-password@test-host:4321/test-db".to_string(),
        );
        env_vars.insert(
            "QW_DEFAULT_INDEX_ROOT_URI".to_string(),
            "s3://quickwit-indexes/prod".to_string(),
        );
        let config = config_builder.build(&env_vars).await.unwrap();
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
            config.rest_listen_addr,
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
            "postgres://test-user:test-password@test-host:4321/test-db"
        );
        assert_eq!(config.default_index_root_uri, "s3://quickwit-indexes/prod");
    }

    #[tokio::test]
    async fn test_quickwwit_config_default_values_storage() {
        let config_yaml = r#"
            version: 3
            node_id: 1
            metastore_uri: postgres://username:password@host:port/db
        "#;
        let config_builder = serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
        let config = config_builder.build(&HashMap::new()).await.unwrap();
        assert_eq!(config.version, "3");
        assert_eq!(config.cluster_id, DEFAULT_CLUSTER_ID);
        assert_eq!(config.node_id, "1");
        assert_eq!(
            config.metastore_uri,
            "postgres://username:password@host:port/db"
        );
    }

    #[tokio::test]
    async fn test_quickwit_config_config_default_values_default_indexer_searcher_config() {
        let config_yaml = r#"
            version: 3
            metastore_uri: postgres://username:password@host:port/db
            data_dir: /opt/quickwit/data
        "#;
        let config_builder = serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
        let config = config_builder.build(&HashMap::new()).await.unwrap();
        assert_eq!(config.version, "3");
        assert_eq!(
            config.metastore_uri,
            "postgres://username:password@host:port/db"
        );
        assert_eq!(config.indexer_config, IndexerConfig::default());
        assert_eq!(config.searcher_config, SearcherConfig::default());
    }

    #[tokio::test]
    async fn test_quickwit_config_validate() {
        let config_filepath = get_config_filepath("quickwit.toml");
        let config_uri = Uri::from_str(&config_filepath).unwrap();
        let file_content = std::fs::read_to_string(&config_filepath).unwrap();

        let data_dir_path = env::current_dir().unwrap();
        let mut env_vars = HashMap::new();
        env_vars.insert(
            "QW_DATA_DIR".to_string(),
            data_dir_path.to_string_lossy().to_string(),
        );
        let config = QuickwitConfig::load_with_env(&config_uri, file_content.as_bytes(), &env_vars)
            .await
            .unwrap();
        assert!(config.validate().is_ok());
    }

    #[tokio::test]
    async fn test_peer_socket_addrs() {
        {
            let quickwit_config = QuickwitConfigBuilder {
                version: ConfigValue::for_test("3".to_string()),
                rest_listen_port: ConfigValue::for_test(1789),
                ..Default::default()
            }
            .build(&HashMap::new())
            .await
            .unwrap();
            assert!(quickwit_config.peer_seed_addrs().await.unwrap().is_empty());
        }
        {
            let quickwit_config = QuickwitConfigBuilder {
                version: ConfigValue::for_test("3".to_string()),
                rest_listen_port: ConfigValue::for_test(1789),
                peer_seeds: ConfigValue::for_test(List(vec!["unresolvable-host".to_string()])),
                ..Default::default()
            }
            .build(&HashMap::new())
            .await
            .unwrap();
            assert!(quickwit_config.peer_seed_addrs().await.is_err());
        }
        {
            let quickwit_config = QuickwitConfigBuilder {
                version: ConfigValue::for_test("3".to_string()),
                rest_listen_port: ConfigValue::for_test(1789),
                peer_seeds: ConfigValue::for_test(List(vec![
                    "unresolvable-host".to_string(),
                    "localhost".to_string(),
                    "localhost:1337".to_string(),
                    "127.0.0.1".to_string(),
                    "127.0.0.1:1337".to_string(),
                ])),
                ..Default::default()
            }
            .build(&HashMap::new())
            .await
            .unwrap();
            assert_eq!(
                quickwit_config.peer_seed_addrs().await.unwrap(),
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
            let quickwit_config = QuickwitConfigBuilder {
                version: ConfigValue::for_test("3".to_string()),
                listen_address: default_listen_address(),
                ..Default::default()
            }
            .build(&HashMap::new())
            .await
            .unwrap();
            assert_eq!(
                quickwit_config.rest_listen_addr.to_string(),
                "127.0.0.1:7280"
            );
            assert_eq!(
                quickwit_config.gossip_listen_addr.to_string(),
                "127.0.0.1:7280"
            );
            assert_eq!(
                quickwit_config.grpc_listen_addr.to_string(),
                "127.0.0.1:7281"
            );
        }
        {
            let quickwit_config = QuickwitConfigBuilder {
                version: ConfigValue::for_test("3".to_string()),
                listen_address: default_listen_address(),
                rest_listen_port: ConfigValue::for_test(1789),
                ..Default::default()
            }
            .build(&HashMap::new())
            .await
            .unwrap();
            assert_eq!(
                quickwit_config.rest_listen_addr.to_string(),
                "127.0.0.1:1789"
            );
            assert_eq!(
                quickwit_config.gossip_listen_addr.to_string(),
                "127.0.0.1:1789"
            );
            assert_eq!(
                quickwit_config.grpc_listen_addr.to_string(),
                "127.0.0.1:1790"
            );
        }
        {
            let quickwit_config = QuickwitConfigBuilder {
                version: ConfigValue::for_test("3".to_string()),
                listen_address: default_listen_address(),
                rest_listen_port: ConfigValue::for_test(1789),
                gossip_listen_port: ConfigValue::for_test(1889),
                grpc_listen_port: ConfigValue::for_test(1989),
                ..Default::default()
            }
            .build(&HashMap::new())
            .await
            .unwrap();
            assert_eq!(
                quickwit_config.rest_listen_addr.to_string(),
                "127.0.0.1:1789"
            );
            assert_eq!(
                quickwit_config.gossip_listen_addr.to_string(),
                "127.0.0.1:1889"
            );
            assert_eq!(
                quickwit_config.grpc_listen_addr.to_string(),
                "127.0.0.1:1989"
            );
        }
    }

    #[tokio::test]
    async fn test_load_config_with_validation_error() {
        let config_filepath = get_config_filepath("quickwit.yaml");
        let config_uri = Uri::from_str(&config_filepath).unwrap();
        let file = std::fs::read_to_string(&config_filepath).unwrap();
        let config = QuickwitConfig::load(&config_uri, file.as_bytes())
            .await
            .unwrap_err();
        assert!(config.to_string().contains("Data dir"));
    }

    #[tokio::test]
    async fn test_config_validates_uris() {
        {
            let config_yaml = r#"
            version: 3
            node_id: 1
            metastore_uri: ''
        "#;
            serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap_err();
        }
        {
            let config_yaml = r#"
            version: 3
            node_id: 1
            metastore_uri: postgres://username:password@host:port/db
            default_index_root_uri: ''
        "#;
            serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap_err();
        }
    }

    #[tokio::test]
    async fn test_quickwit_config_data_dir_accepts_both_file_uris_and_file_paths() {
        {
            let config_yaml = r#"
                version: 3
                data_dir: /opt/quickwit/data
            "#;
            let config_builder =
                serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
            let config = config_builder.build(&HashMap::new()).await.unwrap();
            assert_eq!(config.data_dir_path, PathBuf::from("/opt/quickwit/data"));
        }
        {
            let config_yaml = r#"
                version: 3
                data_dir: file:///opt/quickwit/data
            "#;
            let config_builder =
                serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
            let config = config_builder.build(&HashMap::new()).await.unwrap();
            assert_eq!(config.data_dir_path, PathBuf::from("/opt/quickwit/data"));
        }
        {
            let config_yaml = r#"
                version: 3
                data_dir: s3://indexes/foo
            "#;
            let config_builder =
                serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
            let error = config_builder.build(&HashMap::new()).await.unwrap_err();
            assert!(error.to_string().contains("Data dir must be located"));
        }
    }
}
