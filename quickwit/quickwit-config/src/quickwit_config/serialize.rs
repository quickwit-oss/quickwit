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

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

use anyhow::{bail, Context};
use quickwit_common::net::{find_private_ip, Host};
use quickwit_common::new_coolid;
use quickwit_common::uri::Uri;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config_value::ConfigValue;
use crate::qw_env_vars::*;
use crate::service::QuickwitService;
use crate::templating::render_config;
use crate::{validate_identifier, ConfigFormat, IndexerConfig, QuickwitConfig, SearcherConfig};

pub const DEFAULT_CLUSTER_ID: &str = "quickwit-default-cluster";

pub const DEFAULT_DATA_DIR_PATH: &str = "qwdata";

// Default config values in the order they appear in [`QuickwitConfigBuilder`].
fn default_cluster_id() -> ConfigValue<String, QW_CLUSTER_ID> {
    ConfigValue::with_default(DEFAULT_CLUSTER_ID.to_string())
}

fn default_node_id() -> ConfigValue<String, QW_NODE_ID> {
    ConfigValue::with_default(new_coolid("node"))
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

pub async fn load_quickwit_config_with_env(
    config_format: ConfigFormat,
    config_content: &[u8],
    env_vars: &HashMap<String, String>,
) -> anyhow::Result<QuickwitConfig> {
    let rendered_config_content = render_config(config_content)?;
    let versioned_quickwit_config: VersionedQuickwitConfig =
        config_format.parse(rendered_config_content.as_bytes())?;
    let quickwit_config_builder: QuickwitConfigBuilder = versioned_quickwit_config.into();
    let config = quickwit_config_builder.build_and_validate(env_vars).await?;
    Ok(config)
}

#[derive(Debug, Deserialize)]
#[serde(tag = "version")]
enum VersionedQuickwitConfig {
    #[serde(rename = "0.4")]
    V0_4(QuickwitConfigBuilder),
}

impl From<VersionedQuickwitConfig> for QuickwitConfigBuilder {
    fn from(versioned_quickwit_config: VersionedQuickwitConfig) -> Self {
        match versioned_quickwit_config {
            VersionedQuickwitConfig::V0_4(quickwit_config_builder) => quickwit_config_builder,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct QuickwitConfigBuilder {
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
    pub async fn build_and_validate(
        self,
        env_vars: &HashMap<String, String>,
    ) -> anyhow::Result<QuickwitConfig> {
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

        let quickwit_config = QuickwitConfig {
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
        };

        validate(&quickwit_config)?;
        Ok(quickwit_config)
    }
}

fn validate(quickwit_config: &QuickwitConfig) -> anyhow::Result<()> {
    validate_identifier("Cluster ID", &quickwit_config.cluster_id)?;
    validate_identifier("Node ID", &quickwit_config.node_id)?;
    if quickwit_config.cluster_id == DEFAULT_CLUSTER_ID {
        warn!(
            cluster_id=%DEFAULT_CLUSTER_ID,
            "Cluster ID is not set, falling back to default value."
        );
    }
    if quickwit_config.peer_seeds.is_empty() {
        warn!("Peer seed list is empty.");
    }
    Ok(())
}

#[cfg(test)]
impl Default for QuickwitConfigBuilder {
    fn default() -> Self {
        Self {
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

#[cfg(any(test, feature = "testsuite"))]
pub fn quickwit_config_for_test() -> QuickwitConfig {
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

    QuickwitConfig {
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

#[cfg(test)]
mod tests {
    use std::env;
    use std::net::Ipv4Addr;
    use std::path::Path;

    use byte_unit::Byte;
    use itertools::Itertools;

    use super::*;

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
        let file = std::fs::read_to_string(&config_filepath).unwrap();
        let env_vars = HashMap::default();
        let config =
            load_quickwit_config_with_env(config_format, file.as_bytes(), &env_vars).await?;
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
        let config_str = std::fs::read_to_string(&config_filepath).unwrap();
        let parsing_error = super::load_quickwit_config_with_env(
            ConfigFormat::Yaml,
            config_str.as_bytes(),
            &Default::default(),
        )
        .await
        .unwrap_err();
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
        let config_yaml = "version: 0.4";
        let config = load_quickwit_config_with_env(
            ConfigFormat::Yaml,
            config_yaml.as_bytes(),
            &Default::default(),
        )
        .await
        .unwrap();
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
        let config_yaml = "version: 0.4";
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
        let config =
            load_quickwit_config_with_env(ConfigFormat::Yaml, config_yaml.as_bytes(), &env_vars)
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
            version: 0.4
            node_id: "node1"
            metastore_uri: postgres://username:password@host:port/db
        "#;
        let config = load_quickwit_config_with_env(
            ConfigFormat::Yaml,
            config_yaml.as_bytes(),
            &Default::default(),
        )
        .await
        .unwrap();
        assert_eq!(config.cluster_id, DEFAULT_CLUSTER_ID);
        assert_eq!(config.node_id, "node1");
        assert_eq!(
            config.metastore_uri,
            "postgres://username:password@host:port/db"
        );
    }

    #[tokio::test]
    async fn test_quickwit_config_config_default_values_default_indexer_searcher_config() {
        let config_yaml = r#"
            version: 0.4
            metastore_uri: postgres://username:password@host:port/db
            data_dir: /opt/quickwit/data
        "#;
        let config = load_quickwit_config_with_env(
            ConfigFormat::Yaml,
            config_yaml.as_bytes(),
            &Default::default(),
        )
        .await
        .unwrap();
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
        let file_content = std::fs::read_to_string(&config_filepath).unwrap();

        let data_dir_path = env::current_dir().unwrap();
        let mut env_vars = HashMap::new();
        env_vars.insert(
            "QW_DATA_DIR".to_string(),
            data_dir_path.to_string_lossy().to_string(),
        );
        assert!(load_quickwit_config_with_env(
            ConfigFormat::Toml,
            file_content.as_bytes(),
            &env_vars,
        )
        .await
        .is_ok());
    }

    #[tokio::test]
    async fn test_peer_socket_addrs() {
        {
            let quickwit_config = QuickwitConfigBuilder {
                rest_listen_port: ConfigValue::for_test(1789),
                ..Default::default()
            }
            .build_and_validate(&HashMap::new())
            .await
            .unwrap();
            assert!(quickwit_config.peer_seed_addrs().await.unwrap().is_empty());
        }
        {
            let quickwit_config = QuickwitConfigBuilder {
                rest_listen_port: ConfigValue::for_test(1789),
                peer_seeds: ConfigValue::for_test(List(vec!["unresolvable-host".to_string()])),
                ..Default::default()
            }
            .build_and_validate(&HashMap::new())
            .await
            .unwrap();
            assert!(quickwit_config.peer_seed_addrs().await.is_err());
        }
        {
            let quickwit_config = QuickwitConfigBuilder {
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
            .build_and_validate(&HashMap::new())
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
                listen_address: default_listen_address(),
                ..Default::default()
            }
            .build_and_validate(&HashMap::new())
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
                listen_address: default_listen_address(),
                rest_listen_port: ConfigValue::for_test(1789),
                ..Default::default()
            }
            .build_and_validate(&HashMap::new())
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
                listen_address: default_listen_address(),
                rest_listen_port: ConfigValue::for_test(1789),
                gossip_listen_port: ConfigValue::for_test(1889),
                grpc_listen_port: ConfigValue::for_test(1989),
                ..Default::default()
            }
            .build_and_validate(&HashMap::new())
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
        let file = std::fs::read_to_string(&config_filepath).unwrap();
        let config = QuickwitConfig::load(ConfigFormat::Yaml, file.as_bytes())
            .await
            .unwrap_err();
        assert!(config.to_string().contains("Data dir"));
    }

    #[tokio::test]
    async fn test_config_validates_uris() {
        {
            let config_yaml = r#"
            version: 0.4
            node_id: 1
            metastore_uri: ''
        "#;
            assert!(load_quickwit_config_with_env(
                ConfigFormat::Yaml,
                config_yaml.as_bytes(),
                &Default::default()
            )
            .await
            .is_err());
        }
        {
            let config_yaml = r#"
            version: 0.4
            node_id: 1
            metastore_uri: postgres://username:password@host:port/db
            default_index_root_uri: ''
        "#;
            assert!(load_quickwit_config_with_env(
                ConfigFormat::Yaml,
                config_yaml.as_bytes(),
                &Default::default()
            )
            .await
            .is_err());
        }
    }

    #[tokio::test]
    async fn test_quickwit_config_data_dir_accepts_both_file_uris_and_file_paths() {
        {
            let config_yaml = r#"
                version: 0.4
                data_dir: /opt/quickwit/data
            "#;
            let config = load_quickwit_config_with_env(
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
                version: 0.4
                data_dir: file:///opt/quickwit/data
            "#;
            let config = load_quickwit_config_with_env(
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
                version: 0.4
                data_dir: s3://indexes/foo
            "#;
            let error = load_quickwit_config_with_env(
                ConfigFormat::Yaml,
                config_yaml.as_bytes(),
                &HashMap::default(),
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("Data dir must be located"));
        }
    }
}
