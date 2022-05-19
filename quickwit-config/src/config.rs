// Copyright (C) 2021 Quickwit, Inc.
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

use std::env;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use byte_unit::Byte;
use json_comments::StripComments;
use once_cell::sync::OnceCell;
use quickwit_common::net::{get_socket_addr, parse_socket_addr_with_default_port};
use quickwit_common::new_coolid;
use quickwit_common::uri::{Extension, Uri, FILE_PROTOCOL};
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};
use tracing::{info, warn};

use crate::validate_identifier;

pub const DEFAULT_QW_CONFIG_PATH: &str = "./config/quickwit.yaml";

const DEFAULT_DATA_DIR_PATH: &str = "./qwdata";

const DEFAULT_CLUSTER_ID: &str = "quickwit-default-cluster";

fn default_data_dir_path() -> PathBuf {
    PathBuf::from(DEFAULT_DATA_DIR_PATH)
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

fn default_cluster_id() -> String {
    DEFAULT_CLUSTER_ID.to_string()
}

fn default_node_id() -> String {
    new_coolid("node")
}

fn default_listen_address() -> String {
    "127.0.0.1".to_string()
}

fn default_rest_listen_port() -> u16 {
    7280
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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

    #[doc(hidden)]
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

pub static SEARCHER_CONFIG_INSTANCE: once_cell::sync::OnceCell<SearcherConfig> = OnceCell::new();

pub fn get_searcher_config_instance() -> &'static SearcherConfig {
    SEARCHER_CONFIG_INSTANCE.get_or_init(SearcherConfig::default)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SearcherConfig {
    #[serde(default = "SearcherConfig::default_fast_field_cache_capacity")]
    pub fast_field_cache_capacity: Byte,
    #[serde(default = "SearcherConfig::default_split_footer_cache_capacity")]
    pub split_footer_cache_capacity: Byte,
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
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct S3Config {
    pub region: Option<String>,
    pub endpoint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageConfig {
    #[serde(rename = "s3")]
    pub s3_config: S3Config,
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QuickwitConfig {
    pub version: usize,
    #[serde(default = "default_cluster_id")]
    pub cluster_id: String,
    #[serde(default = "default_node_id")]
    pub node_id: String,
    #[serde(default = "default_listen_address")]
    pub listen_address: String,
    #[serde(default = "default_rest_listen_port")]
    pub rest_listen_port: u16,
    pub gossip_listen_port: Option<u16>,
    pub grpc_listen_port: Option<u16>,
    #[serde(default)]
    pub peer_seeds: Vec<String>,
    #[serde(default)]
    #[serde(deserialize_with = "deser_valid_uri")]
    metastore_uri: Option<Uri>,
    #[serde(default)]
    #[serde(deserialize_with = "deser_valid_uri")]
    default_index_root_uri: Option<Uri>,
    #[serde(default = "default_data_dir_path")]
    #[serde(rename = "data_dir")]
    pub data_dir_path: PathBuf,
    #[serde(rename = "indexer")]
    #[serde(default)]
    pub indexer_config: IndexerConfig,
    #[serde(rename = "searcher")]
    #[serde(default)]
    pub searcher_config: SearcherConfig,
    #[serde(rename = "storage")]
    pub storage_config: Option<StorageConfig>,
}

impl QuickwitConfig {
    /// Parses and validates a [`QuickwitConfig`] from a given URI and config content.
    pub async fn load(
        uri: &Uri,
        config_content: &[u8],
        data_dir_path_opt: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        let mut config = QuickwitConfig::from_uri(uri, config_content).await?;
        if let Some(data_dir_path) = data_dir_path_opt {
            info!(
                data_dir_path = %data_dir_path.display(),
                "Setting data dir path from CLI args or environment variable",
            );
            config.data_dir_path = data_dir_path;
        }
        config.validate()?;
        Ok(config)
    }

    async fn from_uri(uri: &Uri, config_content: &[u8]) -> anyhow::Result<Self> {
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

    pub fn validate(&self) -> anyhow::Result<()> {
        validate_identifier("Cluster ID", &self.cluster_id)?;
        validate_identifier("Node ID", &self.node_id)?;

        if self.cluster_id == DEFAULT_CLUSTER_ID {
            warn!(
                cluster_id = DEFAULT_CLUSTER_ID,
                "Cluster ID is not set, falling back to default value."
            );
        }
        if self.peer_seeds.is_empty() {
            warn!("Seed list is empty.");
        }
        let data_dir_uri = Uri::try_new(&self.data_dir_path.to_string_lossy())?;

        if data_dir_uri.protocol() != FILE_PROTOCOL {
            bail!("Data dir must be located on local file system")
        }
        if !self.data_dir_path.exists() {
            bail!(
                "Data dir `{}` does not exist.",
                self.data_dir_path.display()
            );
        }
        Ok(())
    }

    pub fn rest_socket_addr(&self) -> anyhow::Result<SocketAddr> {
        get_socket_addr(&(self.listen_address.as_str(), self.rest_listen_port))
    }

    /// Returns the grpc listen port
    /// extracted from config if specified or computed from `rest_listen_port + 1`.
    fn grpc_listen_port(&self) -> u16 {
        match self.grpc_listen_port {
            Some(grpc_listen_port) => grpc_listen_port,
            None => self.rest_listen_port + 1,
        }
    }

    /// Returns the gossip listen port
    /// extracted from config if specified or the same as `rest_listen_port`.
    fn gossip_listen_port(&self) -> u16 {
        match self.gossip_listen_port {
            Some(grpc_listen_port) => grpc_listen_port,
            // By default, we use the same port number as the rest port but this is UDP.
            None => self.rest_listen_port,
        }
    }

    /// Returns the grpc socket address.
    pub fn grpc_socket_addr(&self) -> anyhow::Result<SocketAddr> {
        get_socket_addr(&(self.listen_address.as_str(), self.grpc_listen_port()))
    }

    /// Returns the gossip socket address.
    pub fn gossip_socket_addr(&self) -> anyhow::Result<SocketAddr> {
        get_socket_addr(&(self.listen_address.as_str(), self.gossip_listen_port()))
    }

    /// The node gossip_public_address should ideally be specified via config;
    /// environment variable interpolation if necessary.
    /// right now we just try to fetch from environment variable otherwise fallback
    /// to listen_address.
    pub fn gossip_public_addr(&self) -> anyhow::Result<SocketAddr> {
        match env::var("QW_GOSSIP_PUBLIC_ADDRESS") {
            Ok(addr) => Ok(addr.parse::<SocketAddr>()?),
            Err(_) => self.gossip_socket_addr(),
        }
    }

    pub fn seed_socket_addrs(&self) -> anyhow::Result<Vec<SocketAddr>> {
        // If no port is given, we assume a seed is using the same port as ourself.
        let default_gossip_port = self.gossip_socket_addr()?.port();
        let seed_socket_addrs: Vec<SocketAddr> = self
            .peer_seeds
            .iter()
            .flat_map(|seed| {
                parse_socket_addr_with_default_port(seed, default_gossip_port)
                    .map_err(|_| warn!(address = %seed, "Failed to resolve seed address."))
            })
            .collect();

        if !self.peer_seeds.is_empty() && seed_socket_addrs.is_empty() {
            bail!(
                "Failed to resolve any of the seed addresses: `{}`",
                self.peer_seeds.join(", ")
            )
        }
        Ok(seed_socket_addrs)
    }

    #[doc(hidden)]
    pub fn for_test() -> anyhow::Result<Self> {
        use quickwit_common::net::find_available_tcp_port;

        let indexer_config = Self {
            rest_listen_port: find_available_tcp_port()?,
            ..Default::default()
        };
        Ok(indexer_config)
    }
}

impl QuickwitConfig {
    pub fn metastore_uri(&self) -> Uri {
        self.metastore_uri
            .as_ref()
            .cloned()
            .unwrap_or_else(|| default_metastore_uri(&self.data_dir_path))
    }

    pub fn default_index_root_uri(&self) -> Uri {
        self.default_index_root_uri
            .as_ref()
            .cloned()
            .unwrap_or_else(|| default_index_root_uri(&self.data_dir_path))
    }
}

impl Default for QuickwitConfig {
    fn default() -> Self {
        Self {
            version: 0,
            listen_address: default_listen_address(),
            rest_listen_port: default_rest_listen_port(),
            gossip_listen_port: None,
            grpc_listen_port: None,
            peer_seeds: Vec::new(),
            cluster_id: default_cluster_id(),
            node_id: default_node_id(),
            metastore_uri: None,
            default_index_root_uri: None,
            data_dir_path: PathBuf::from(DEFAULT_DATA_DIR_PATH),
            indexer_config: IndexerConfig::default(),
            searcher_config: SearcherConfig::default(),
            storage_config: None,
        }
    }
}

impl std::fmt::Debug for QuickwitConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter
            .debug_struct("QuickwitConfig")
            .field("version", &self.version)
            .field("node_id", &self.node_id)
            .field("cluster_id", &self.cluster_id)
            .field("listen_address", &self.listen_address)
            .field("rest_listen_port", &self.rest_listen_port)
            .field("gossip_listen_port", &self.gossip_listen_port())
            .field("grpc_listen_port", &self.grpc_listen_port())
            .field("peer_seeds", &self.peer_seeds)
            .field("data_dir_path", &self.data_dir_path)
            .field("metastore_uri", &self.metastore_uri())
            .field("default_index_root_uri", &self.default_index_root_uri())
            .field("indexer_config", &self.indexer_config)
            .field("searcher_config", &self.searcher_config)
            .field("storage_config", &self.storage_config)
            .finish()
    }
}

/// Deserializes and validates a [`Uri`].
pub(super) fn deser_valid_uri<'de, D>(deserializer: D) -> Result<Option<Uri>, D::Error>
where D: Deserializer<'de> {
    let uri_opt: Option<String> = Deserialize::deserialize(deserializer)?;
    uri_opt
        .map(|uri| Uri::try_new(&uri))
        .transpose()
        .map_err(D::Error::custom)
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

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
                let config = QuickwitConfig::from_uri(&config_uri, file.as_bytes()).await?;
                assert_eq!(config.version, 0);
                assert_eq!(config.cluster_id, "quickwit-cluster");
                assert_eq!(config.listen_address, "0.0.0.0".to_string());
                assert_eq!(config.rest_listen_port, 1111);
                assert_eq!(
                    config.peer_seeds,
                    vec![
                        "quickwit-searcher-0.local".to_string(),
                        "quickwit-searcher-1.local".to_string()
                    ]
                );
                assert_eq!(
                    config.metastore_uri(),
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
                        max_num_concurrent_split_streams: 120,
                    }
                );

                assert_eq!(
                    config.storage_config.unwrap().s3_config,
                    S3Config {
                        region: Some("us-east-1".to_string()),
                        endpoint: Some("https://s3.us-east-1.amazonaws.com".to_string()),
                    }
                );
                Ok(())
            }
        };
    }

    test_parser!(test_config_from_json, json);
    test_parser!(test_config_from_toml, toml);
    test_parser!(test_config_from_yaml, yaml);

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

    #[test]
    fn test_quickwit_config_default_values_minimal() {
        let minimal_config_yaml = "version: 0";
        let config = serde_yaml::from_str::<QuickwitConfig>(minimal_config_yaml).unwrap();
        assert_eq!(config.version, 0);
        assert!(config.node_id.starts_with("node-"));
        assert_eq!(
            config.metastore_uri(),
            format!(
                "file://{}/qwdata/indexes#polling_interval=30s",
                env::current_dir().unwrap().display()
            )
        );
        assert_eq!(config.data_dir_path.to_string_lossy(), "./qwdata");
    }

    #[test]
    fn test_quickwwit_config_default_values_storage() {
        let config_yaml = r#"
            version: 0
            node_id: 1
            metastore_uri: postgres://username:password@host:port/db
        "#;
        let config = serde_yaml::from_str::<QuickwitConfig>(config_yaml).unwrap();
        assert_eq!(config.version, 0);
        assert_eq!(config.cluster_id, DEFAULT_CLUSTER_ID);
        assert_eq!(config.node_id, "1");
        assert_eq!(
            config.metastore_uri(),
            "postgres://username:password@host:port/db"
        );
        assert!(config.storage_config.is_none());
    }

    #[test]
    fn test_quickwit_config_config_default_values_default_indexer_searcher_config() {
        let config_yaml = r#"
            version: 0
            metastore_uri: postgres://username:password@host:port/db
            data_dir: /opt/quickwit/data
        "#;
        let config = serde_yaml::from_str::<QuickwitConfig>(config_yaml).unwrap();
        assert_eq!(config.version, 0);
        assert_eq!(
            config.metastore_uri(),
            "postgres://username:password@host:port/db"
        );
        assert_eq!(config.indexer_config, IndexerConfig::default());
        assert_eq!(config.searcher_config, SearcherConfig::default());
    }

    #[tokio::test]
    async fn test_quickwit_config_validate() {
        let config_filepath = get_config_filepath("quickwit.toml");
        let file_content = std::fs::read_to_string(&config_filepath).unwrap();

        let config_uri = Uri::try_new(&config_filepath).unwrap();
        let mut quickwit_config = QuickwitConfig::from_uri(&config_uri, file_content.as_bytes())
            .await
            .unwrap();
        quickwit_config.data_dir_path = env::current_dir().unwrap();
        assert!(quickwit_config.validate().is_ok());
    }

    #[test]
    fn test_peer_socket_addrs() {
        {
            let quickwit_config = QuickwitConfig {
                rest_listen_port: 1789,
                ..Default::default()
            };
            assert!(quickwit_config.seed_socket_addrs().unwrap().is_empty());
        }
        {
            let quickwit_config = QuickwitConfig {
                rest_listen_port: 1789,
                peer_seeds: vec!["unresolvable-host".to_string()],
                ..Default::default()
            };
            assert!(quickwit_config.seed_socket_addrs().is_err());
        }
        {
            let quickwit_config = QuickwitConfig {
                rest_listen_port: 1789,
                peer_seeds: vec!["unresolvable-host".to_string(), "127.0.0.1".to_string()],
                ..Default::default()
            };
            assert_eq!(
                quickwit_config.seed_socket_addrs().unwrap(),
                vec!["127.0.0.1:1789".parse().unwrap()]
            );
        }
    }

    #[test]
    fn test_socket_addr_ports() {
        {
            let quickwit_config = QuickwitConfig {
                listen_address: "127.0.0.1".to_string(),
                ..Default::default()
            };
            assert_eq!(
                quickwit_config.rest_socket_addr().unwrap().to_string(),
                "127.0.0.1:7280"
            );
            assert_eq!(
                quickwit_config.gossip_socket_addr().unwrap().to_string(),
                "127.0.0.1:7280"
            );
            assert_eq!(
                quickwit_config.grpc_socket_addr().unwrap().to_string(),
                "127.0.0.1:7281"
            );
        }
        {
            let quickwit_config = QuickwitConfig {
                listen_address: "127.0.0.1".to_string(),
                rest_listen_port: 1789,
                ..Default::default()
            };
            assert_eq!(
                quickwit_config.rest_socket_addr().unwrap().to_string(),
                "127.0.0.1:1789"
            );
            assert_eq!(
                quickwit_config.gossip_socket_addr().unwrap().to_string(),
                "127.0.0.1:1789"
            );
            assert_eq!(
                quickwit_config.grpc_socket_addr().unwrap().to_string(),
                "127.0.0.1:1790"
            );
        }
        {
            let quickwit_config = QuickwitConfig {
                listen_address: "127.0.0.1".to_string(),
                rest_listen_port: 1789,
                gossip_listen_port: Some(1889),
                grpc_listen_port: Some(1989),
                ..Default::default()
            };
            assert_eq!(
                quickwit_config.rest_socket_addr().unwrap().to_string(),
                "127.0.0.1:1789"
            );
            assert_eq!(
                quickwit_config.gossip_socket_addr().unwrap().to_string(),
                "127.0.0.1:1889"
            );
            assert_eq!(
                quickwit_config.grpc_socket_addr().unwrap().to_string(),
                "127.0.0.1:1989"
            );
        }
    }

    #[tokio::test]
    async fn test_load_config_with_validation_error() {
        let config_filepath = get_config_filepath("quickwit.yaml");
        let config_uri = Uri::try_new(&config_filepath).unwrap();
        let file = std::fs::read_to_string(&config_filepath).unwrap();
        let config = QuickwitConfig::load(&config_uri, file.as_bytes(), None)
            .await
            .unwrap_err();
        assert!(config.to_string().contains("Data dir"));
    }

    #[test]
    fn test_config_validates_uris() {
        {
            let config_yaml = r#"
            version: 0
            node_id: 1
            metastore_uri: ''
        "#;
            serde_yaml::from_str::<QuickwitConfig>(config_yaml).unwrap_err();
        }
        {
            let config_yaml = r#"
            version: 0
            node_id: 1
            metastore_uri: postgres://username:password@host:port/db
            default_index_root_uri: ''
        "#;
            serde_yaml::from_str::<QuickwitConfig>(config_yaml).unwrap_err();
        }
    }
}
