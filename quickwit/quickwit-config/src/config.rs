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
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::{env, fmt};

use anyhow::{bail, Context};
use byte_unit::Byte;
use derivative::Derivative;
use json_comments::StripComments;
use quickwit_common::net::{find_private_ip, Host, HostAddr};
use quickwit_common::new_coolid;
use quickwit_common::uri::{Extension, Uri};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config_value::{ConfigValue, ConfigValueBuilder};
use crate::qw_env_vars::*;
use crate::validate_identifier;

pub const DEFAULT_QW_CONFIG_PATH: &str = "./config/quickwit.yaml";

const DEFAULT_DATA_DIR_PATH: &str = "./qwdata";

const DEFAULT_CLUSTER_ID: &str = "quickwit-default-cluster";

fn default_cluster_id() -> ConfigValueBuilder<String, QW_CLUSTER_ID> {
    ConfigValueBuilder::with_default(DEFAULT_CLUSTER_ID.to_string())
}

fn default_node_id() -> ConfigValueBuilder<String, QW_NODE_ID> {
    ConfigValueBuilder::with_default(new_coolid("node"))
}

fn default_listen_address() -> ConfigValueBuilder<String, QW_LISTEN_ADDRESS> {
    ConfigValueBuilder::with_default(Host::default().to_string())
}

fn default_rest_listen_port() -> ConfigValueBuilder<u16, QW_REST_LISTEN_PORT> {
    ConfigValueBuilder::with_default(7280)
}

// Surprisingly, the default metastore and the index root uri are the same (if you exclude the
// polling_interval parameter). Indeed, this is a convenient setting for testing with a file backed
// metastore and indexes splits stored locally too.
// For a given index `index-id`, it means that we have the metastore file
// in  `./qwdata/indexes/{index-id}/metastore.json` and splits in
// dir `./qwdata/indexes/{index-id}/splits`.
fn default_metastore_uri(data_dir_path: &Path) -> Uri {
    Uri::try_new(&data_dir_path.join("indexes#polling_interval=30s").to_string_lossy())
        .expect("Failed to create default metastore URI. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")
}

// See comment above.
fn default_index_root_uri(data_dir_path: &Path) -> Uri {
    Uri::try_new(&data_dir_path.join("indexes").to_string_lossy())
        .expect("Failed to create default index_root URI. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")
}

fn default_data_dir_path() -> ConfigValueBuilder<PathBuf, QW_DATA_DIR> {
    ConfigValueBuilder::with_default(PathBuf::from(DEFAULT_DATA_DIR_PATH))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerConfig {
    #[serde(default = "IndexerConfig::default_split_store_max_num_bytes")]
    pub split_store_max_num_bytes: Byte,
    #[serde(default = "IndexerConfig::default_split_store_max_num_splits")]
    pub split_store_max_num_splits: usize,
}

impl IndexerConfig {
    fn default_split_store_max_num_bytes() -> Byte {
        Byte::from_bytes(100_000_000_000) // 100G
    }

    fn default_split_store_max_num_splits() -> usize {
        1_000
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> anyhow::Result<Self> {
        let indexer_config = IndexerConfig {
            split_store_max_num_bytes: Byte::from_bytes(1_000_000),
            split_store_max_num_splits: 3,
        };
        Ok(indexer_config)
    }
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            split_store_max_num_bytes: Self::default_split_store_max_num_bytes(),
            split_store_max_num_splits: Self::default_split_store_max_num_splits(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearcherConfig {
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
            fast_field_cache_capacity: Self::default_fast_field_cache_capacity(),
            split_footer_cache_capacity: Self::default_split_footer_cache_capacity(),
            max_num_concurrent_split_streams: Self::default_max_num_concurrent_split_streams(),
            max_num_concurrent_split_searches: Self::default_max_num_concurrent_split_searches(),
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
#[derive(Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct QuickwitConfigBuilder {
    version: ConfigValueBuilder<usize, QW_NONE>,
    #[serde(default = "default_cluster_id")]
    cluster_id: ConfigValueBuilder<String, QW_CLUSTER_ID>,
    #[serde(default = "default_node_id")]
    node_id: ConfigValueBuilder<String, QW_NODE_ID>,
    #[serde(default = "default_listen_address")]
    listen_address: ConfigValueBuilder<String, QW_LISTEN_ADDRESS>,
    advertise_address: Option<ConfigValueBuilder<String, QW_ADVERTISE_ADDRESS>>,
    #[serde(default = "default_rest_listen_port")]
    rest_listen_port: ConfigValueBuilder<u16, QW_REST_LISTEN_PORT>,
    gossip_listen_port: Option<ConfigValueBuilder<u16, QW_GOSSIP_LISTEN_PORT>>,
    grpc_listen_port: Option<ConfigValueBuilder<u16, QW_GRPC_LISTEN_PORT>>,
    #[serde(default)]
    peer_seeds: Vec<String>,
    #[serde(default)]
    #[derivative(Debug(format_with = "redact_uri"))]
    metastore_uri: Option<String>,
    #[serde(default)]
    #[derivative(Debug(format_with = "redact_uri"))]
    default_index_root_uri: Option<String>,
    #[serde(default = "default_data_dir_path")]
    #[serde(rename = "data_dir")]
    data_dir_path: ConfigValueBuilder<PathBuf, QW_DATA_DIR>,
    #[serde(rename = "indexer")]
    #[serde(default)]
    indexer_config: IndexerConfig,
    #[serde(rename = "searcher")]
    #[serde(default)]
    searcher_config: SearcherConfig,
}

impl QuickwitConfigBuilder {
    async fn load(uri: &Uri, config_content: &[u8]) -> anyhow::Result<Self> {
        let parser_fn = match uri.extension() {
            Some(Extension::Json) => Self::from_json,
            Some(Extension::Toml) => Self::from_toml,
            Some(Extension::Yaml) => Self::from_yaml,
            Some(Extension::Unknown(extension)) => bail!(
                "Failed to read quickwit config file `{}`: file extension `.{}` is not supported. \
                 Supported file formats and extensions are JSON (.json), TOML (.toml), and YAML \
                 (.yaml or .yml).",
                uri,
                extension
            ),
            None => bail!(
                "Failed to read config file `{}`: file extension is missing. Supported file \
                 formats and extensions are JSON (.json), TOML (.toml), and YAML (.yaml or .yml).",
                uri
            ),
        };
        parser_fn(config_content)
    }

    fn from_json(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_reader(StripComments::new(bytes))
            .context("Failed to parse JSON config file.")
    }

    fn from_toml(bytes: &[u8]) -> anyhow::Result<Self> {
        toml::from_slice(bytes).context("Failed to parse TOML config file.")
    }

    fn from_yaml(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_yaml::from_slice(bytes).context("Failed to parse YAML config file.")
    }

    fn advertise_host(
        &self,
        env_vars: &HashMap<String, String>,
        listen_host: &Host,
    ) -> anyhow::Result<ConfigValue<Host>> {
        unimplemented!()
    }

    fn metastore_uri(&self, data_dir_path: &Path) -> anyhow::Result<Uri> {
        if let Some(uri) = &self.metastore_uri {
            Uri::try_new(uri).with_context(|| format!("Failed to parse metastore URI `{uri}`."))
        } else {
            Ok(default_metastore_uri(data_dir_path))
        }
    }

    fn default_index_root_uri(&self, data_dir_path: &Path) -> anyhow::Result<Uri> {
        if let Some(uri) = &self.default_index_root_uri {
            Uri::try_new(uri)
                .with_context(|| format!("Failed to parse default index root URI `{uri}`."))
        } else {
            Ok(default_index_root_uri(data_dir_path))
        }
    }

    pub async fn build(self) -> anyhow::Result<QuickwitConfig> {
        let env_vars = env::vars().collect::<HashMap<_, _>>();

        let listen_host = self
            .listen_address
            .build(&env_vars)?
            .try_map(|listen_address| listen_address.parse::<Host>())?;
        let listen_ip = listen_host.resolve().await?;

        let rest_listen_port = self.rest_listen_port.build(&env_vars)?;
        let rest_listen_addr = SocketAddr::new(listen_ip.clone(), *rest_listen_port);

        let gossip_listen_port = self
            .gossip_listen_port
            .map(|cvb| cvb.build(&env_vars))
            .transpose()?
            .unwrap_or(ConfigValue::with_default(*rest_listen_port));
        let gossip_listen_addr = SocketAddr::new(listen_ip.clone(), *gossip_listen_port);

        let grpc_listen_port = self
            .grpc_listen_port
            .map(|cvb| cvb.build(&env_vars))
            .transpose()?
            .unwrap_or(ConfigValue::with_default(*rest_listen_port + 1));
        let grpc_listen_addr = SocketAddr::new(listen_ip, *grpc_listen_port);

        let advertise_host: ConfigValue<Host> = self.advertise_host(&env_vars, &*listen_host)?;
        let advertise_ip = advertise_host.resolve().await?;
        let gossip_advertise_addr = SocketAddr::new(advertise_ip.clone(), *gossip_listen_port);
        let grpc_advertise_addr = SocketAddr::new(advertise_ip, *grpc_listen_port);

        let data_dir_path = self.data_dir_path.build(&env_vars)?;

        Ok(QuickwitConfig {
            version: self.version.build(&env_vars)?,
            cluster_id: self.cluster_id.build(&env_vars)?,
            node_id: self.node_id.build(&env_vars)?,
            listen_host,
            rest_listen_port,
            rest_listen_addr,
            gossip_listen_port,
            gossip_listen_addr,
            grpc_listen_port,
            grpc_listen_addr,
            advertise_host,
            gossip_advertise_addr,
            grpc_advertise_addr,
            peer_seeds: self.peer_seeds,
            data_dir_path,
            // metastore_uri: self.metastore_uri(&*data_dir_path)?,
            // default_index_root_uri: self.default_index_root_uri(&*&data_dir_path)?,
            indexer_config: self.indexer_config,
            searcher_config: self.searcher_config,
        })
    }
}

fn redact_uri(
    uri_opt: &Option<String>,
    formatter: &mut std::fmt::Formatter,
) -> Result<(), std::fmt::Error> {
    match uri_opt.as_ref().map(|uri| Uri::try_new(uri)) {
        Some(Ok(uri)) => {
            formatter.write_str("Some(")?;
            formatter.write_str(&uri.as_redacted_str())?;
            formatter.write_str(")")?;
        }
        Some(Err(_)) => formatter.write_str("Some(***redacted***)")?,
        None => formatter.write_str("None")?,
    };
    Ok(())
}

#[derive(Clone, Serialize)]
pub struct QuickwitConfig {
    pub version: ConfigValue<usize>,
    pub cluster_id: ConfigValue<String>,
    pub node_id: ConfigValue<String>,
    pub listen_host: ConfigValue<Host>,
    pub rest_listen_port: ConfigValue<u16>,
    pub rest_listen_addr: SocketAddr,
    pub gossip_listen_port: ConfigValue<u16>,
    pub gossip_listen_addr: SocketAddr,
    pub grpc_listen_port: ConfigValue<u16>,
    pub grpc_listen_addr: SocketAddr,
    pub advertise_host: ConfigValue<Host>,
    pub gossip_advertise_addr: SocketAddr,
    pub grpc_advertise_addr: SocketAddr,
    pub peer_seeds: Vec<String>,
    pub data_dir_path: ConfigValue<PathBuf>,
    // pub metastore_uri: ConfigValue<Uri>,
    // pub default_index_root_uri: ConfigValue<Uri>,
    pub indexer_config: IndexerConfig,
    pub searcher_config: SearcherConfig,
}

impl QuickwitConfig {
    /// Parses and validates a [`QuickwitConfig`] from a given URI and config content.
    pub async fn load(uri: &Uri, config_content: &[u8]) -> anyhow::Result<Self> {
        let config_builder = QuickwitConfigBuilder::load(uri, config_content).await?;
        let config = config_builder.build().await?;
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
        let data_dir_uri = Uri::try_new(&self.data_dir_path.to_string_lossy())?;

        if !data_dir_uri.protocol().is_file() {
            bail!(
                "Data dir must be located on local file system. Current location: `{data_dir_uri}`"
            )
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

        let data_dir_path = default_data_dir_path().for_test();
        // let metastore_uri = default_metastore_uri(&data_dir_path);
        // let default_index_root_uri = default_index_root_uri(&data_dir_path);

        Self {
            version: ConfigValue::default(),
            cluster_id: default_cluster_id().for_test(),
            node_id: default_node_id().for_test(),
            gossip_advertise_addr: gossip_listen_addr,
            grpc_advertise_addr: grpc_listen_addr,
            rest_listen_addr,
            gossip_listen_addr,
            grpc_listen_addr,
            peer_seeds: Vec::new(),
            data_dir_path,
            // metastore_uri,
            // default_index_root_uri,
            indexer_config: IndexerConfig::default(),
            searcher_config: SearcherConfig::default(),
        }
    }
}

impl fmt::Debug for QuickwitConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuickwitConfig")
            .field("version", &self.version)
            .field("cluster_id", &self.cluster_id)
            .field("node_id", &self.node_id)
            .field("listen_address", &self.listen_host)
            .field("advertise_address", &self.advertise_host)
            .field("rest_listen_port", &self.rest_listen_port)
            .field("gossip_listen_port", &self.gossip_listen_port)
            .field("grpc_listen_port", &self.grpc_listen_port)
            .field("peer_seeds", &self.peer_seeds)
            .field("data_dir", &self.data_dir_path)
            // .field("metastore_uri", &self.metastore_uri)
            // .field("default_index_root_uri", &self.default_index_root_uri)
            .field("indexer_config", &self.indexer_config)
            .field("searcher_config", &self.searcher_config)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::*;

    impl Default for QuickwitConfigBuilder {
        fn default() -> Self {
            Self {
                version: ConfigValueBuilder::default(),
                cluster_id: default_cluster_id(),
                node_id: default_node_id(),
                listen_address: default_listen_address(),
                advertise_address: None,
                rest_listen_port: default_rest_listen_port(),
                gossip_listen_port: None,
                grpc_listen_port: None,
                peer_seeds: Vec::new(),
                metastore_uri: None,
                default_index_root_uri: None,
                data_dir_path: default_data_dir_path(),
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

    macro_rules! test_parser {
        ($test_function_name:ident, $file_extension:expr) => {
            #[tokio::test]
            async fn $test_function_name() -> anyhow::Result<()> {
                let config_filepath =
                    get_config_filepath(&format!("quickwit.{}", stringify!($file_extension)));
                let config_uri = Uri::try_new(&config_filepath)?;
                let file = std::fs::read_to_string(&config_filepath).unwrap();
                let config = QuickwitConfigBuilder::from_uri(&config_uri, file.as_bytes())
                    .await?
                    .build()
                    .await?;
                assert_eq!(config.version, 0);
                assert_eq!(config.cluster_id, "quickwit-cluster");
                assert_eq!(
                    config.rest_listen_addr,
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 1111)
                );
                assert_eq!(
                    config.peer_seeds,
                    vec![
                        "quickwit-searcher-0.local".to_string(),
                        "quickwit-searcher-1.local".to_string()
                    ]
                );
                assert_eq!(
                    config.metastore_uri,
                    "postgres://username:password@host:port/db"
                );

                assert_eq!(
                    config.indexer_config,
                    IndexerConfig {
                        split_store_max_num_bytes: Byte::from_str("1T").unwrap(),
                        split_store_max_num_splits: 10_000,
                    }
                );

                assert_eq!(
                    config.searcher_config,
                    SearcherConfig {
                        fast_field_cache_capacity: Byte::from_str("10G").unwrap(),
                        split_footer_cache_capacity: Byte::from_str("1G").unwrap(),
                        max_num_concurrent_split_searches: 150,
                        max_num_concurrent_split_streams: 120,
                    }
                );

                Ok(())
            }
        };
    }

    test_parser!(test_config_from_json, json);
    test_parser!(test_config_from_toml, toml);
    test_parser!(test_config_from_yaml, yaml);

    #[tokio::test]
    async fn test_config_contains_wrong_values() {
        let config_filepath = get_config_filepath("quickwit.wrongkey.yaml");
        let config_uri = Uri::try_new(&config_filepath).unwrap();
        let config_str = std::fs::read_to_string(&config_filepath).unwrap();
        let parsing_error = QuickwitConfigBuilder::load(&config_uri, config_str.as_bytes())
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
        let config_yaml = "version: 0";
        let config_builder = serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
        let config = config_builder.build().await.unwrap();
        assert_eq!(config.version, 0);
        assert!(config.node_id.starts_with("node-"));
        assert_eq!(
            config.metastore_uri,
            format!(
                "file://{}/qwdata/indexes#polling_interval=30s",
                env::current_dir().unwrap().display()
            )
        );
        assert_eq!(config.data_dir_path.to_string_lossy(), "./qwdata");
    }

    #[tokio::test]
    async fn test_quickwwit_config_default_values_storage() {
        let config_yaml = r#"
            version: 0
            node_id: 1
            metastore_uri: postgres://username:password@host:port/db
        "#;
        let config_builder = serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
        let config = config_builder.build().await.unwrap();
        assert_eq!(config.version, 0);
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
            version: 0
            metastore_uri: postgres://username:password@host:port/db
            data_dir: /opt/quickwit/data
        "#;
        let config_builder = serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
        let config = config_builder.build().await.unwrap();
        assert_eq!(config.version, 0);
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
        let config_uri = Uri::try_new(&config_filepath).unwrap();
        let config_content = std::fs::read_to_string(&config_filepath).unwrap();
        let config = QuickwitConfig::load(&config_uri, config_content.as_bytes())
            .await
            .unwrap();
        assert!(config.validate().is_ok());
    }

    #[tokio::test]
    async fn test_peer_socket_addrs() {
        {
            let quickwit_config = QuickwitConfigBuilder {
                rest_listen_port: ConfigValueBuilder::with_default(1789),
                ..Default::default()
            }
            .build()
            .await
            .unwrap();
            assert!(quickwit_config.peer_seed_addrs().await.unwrap().is_empty());
        }
        {
            let quickwit_config = QuickwitConfigBuilder {
                rest_listen_port: ConfigValueBuilder::with_default(1789),
                peer_seeds: vec!["unresolvable-host".to_string()],
                ..Default::default()
            }
            .build()
            .await
            .unwrap();
            assert!(quickwit_config.peer_seed_addrs().await.is_err());
        }
        {
            let quickwit_config = QuickwitConfigBuilder {
                rest_listen_port: ConfigValueBuilder::with_default(1789),
                peer_seeds: vec![
                    "unresolvable-host".to_string(),
                    "localhost".to_string(),
                    "localhost:1337".to_string(),
                    "127.0.0.1".to_string(),
                    "127.0.0.1:1337".to_string(),
                ],
                ..Default::default()
            }
            .build()
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
                listen_address: ConfigValueBuilder::with_default(Ipv4Addr::LOCALHOST.to_string()),
                ..Default::default()
            }
            .build()
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
                listen_address: ConfigValueBuilder::with_default(Ipv4Addr::LOCALHOST.to_string()),
                rest_listen_port: ConfigValueBuilder::with_default(1789),
                ..Default::default()
            }
            .build()
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
                listen_address: ConfigValueBuilder::with_default(Ipv4Addr::LOCALHOST.to_string()),
                rest_listen_port: ConfigValueBuilder::with_default(1789),
                gossip_listen_port: Some(ConfigValueBuilder::with_default(1889)),
                grpc_listen_port: Some(ConfigValueBuilder::with_default(1989)),
                ..Default::default()
            }
            .build()
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
        let config_uri = Uri::try_new(&config_filepath).unwrap();
        let config_content = std::fs::read_to_string(&config_filepath).unwrap();
        let config = QuickwitConfig::load(&config_uri, config_content.as_bytes())
            .await
            .unwrap_err();
        assert!(config.to_string().contains("Data dir"));
    }

    #[tokio::test]
    async fn test_config_validates_uris() {
        {
            let config_yaml = r#"
            version: 0
            node_id: 1
            metastore_uri: ''
        "#;
            let config_builder =
                serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
            config_builder.build().await.unwrap_err();
        }
        {
            let config_yaml = r#"
            version: 0
            node_id: 1
            metastore_uri: postgres://username:password@host:port/db
            default_index_root_uri: ''
        "#;
            let config_builder =
                serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
            config_builder.build().await.unwrap_err();
        }
    }
}
