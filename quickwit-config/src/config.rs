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

use std::ffi::OsStr;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use byte_unit::Byte;
use json_comments::StripComments;
use once_cell::sync::OnceCell;
use quickwit_common::net::{get_socket_addr, parse_socket_addr_with_default_port};
use quickwit_common::new_coolid;
use quickwit_common::uri::Uri;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

static DEFAULT_DATA_DIR_PATH: &str = "./qwdata";

fn default_data_dir_path() -> PathBuf {
    PathBuf::from(DEFAULT_DATA_DIR_PATH)
}

// Paradoxically the default metastore and the index root uri are the same.
// Indeed, this is a convenient setting for testing with a file backed metastore
// and indexes splits stored locally too.
// For a given index `index-id`, it means that we have the metastore file
// in  `./qwdata/indexes/{index-id}/metastore.json` and splits in
// dir `./qwdata/indexes/{index-id}/splits`.
fn default_metastore_and_index_root_uri() -> String {
    Uri::try_new(&default_data_dir_path().join("indexes").to_string_lossy())
        .expect("Default data dir `./qwdata` value is invalid.")
        .as_ref()
        .to_string()
}

fn default_node_id() -> String {
    new_coolid("node")
}

fn default_listen_address() -> String {
    "127.0.0.1".to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IndexerConfig {
    #[serde(default = "default_listen_address")]
    pub rest_listen_address: String,
    #[serde(default = "IndexerConfig::default_rest_listen_port")]
    pub rest_listen_port: u16,
    #[serde(default = "IndexerConfig::default_split_store_max_num_bytes")]
    pub split_store_max_num_bytes: Byte,
    #[serde(default = "IndexerConfig::default_split_store_max_num_splits")]
    pub split_store_max_num_splits: usize,
}

impl IndexerConfig {
    fn default_rest_listen_port() -> u16 {
        7180
    }

    fn default_split_store_max_num_bytes() -> Byte {
        Byte::from_bytes(100_000_000_000) // 100G
    }

    fn default_split_store_max_num_splits() -> usize {
        1_000
    }

    #[doc(hidden)]
    pub fn for_test() -> anyhow::Result<Self> {
        use quickwit_common::net::find_available_port;

        let indexer_config = IndexerConfig {
            rest_listen_port: find_available_port()?,
            split_store_max_num_bytes: Byte::from_bytes(1_000_000),
            split_store_max_num_splits: 3,
            ..Default::default()
        };
        Ok(indexer_config)
    }
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            rest_listen_address: default_listen_address(),
            rest_listen_port: Self::default_rest_listen_port(),
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
    #[serde(default = "default_listen_address")]
    pub rest_listen_address: String,
    #[serde(default = "SearcherConfig::default_rest_listen_port")]
    pub rest_listen_port: u16,
    #[serde(default)]
    pub peer_seeds: Vec<String>,
    #[serde(default = "SearcherConfig::default_fast_field_cache_capacity")]
    pub fast_field_cache_capacity: Byte,
    #[serde(default = "SearcherConfig::default_split_footer_cache_capacity")]
    pub split_footer_cache_capacity: Byte,
    #[serde(default = "SearcherConfig::default_max_num_concurrent_split_streams")]
    pub max_num_concurrent_split_streams: usize,
}

impl SearcherConfig {
    fn default_rest_listen_port() -> u16 {
        7280
    }

    fn default_fast_field_cache_capacity() -> Byte {
        Byte::from_bytes(1_000_000_000) // 1G
    }

    fn default_split_footer_cache_capacity() -> Byte {
        Byte::from_bytes(500_000_000) // 500M
    }

    fn default_max_num_concurrent_split_streams() -> usize {
        100
    }

    pub fn rest_socket_addr(&self) -> anyhow::Result<SocketAddr> {
        get_socket_addr(&(self.rest_listen_address.as_str(), self.rest_listen_port))
    }

    pub fn grpc_socket_addr(&self) -> anyhow::Result<SocketAddr> {
        get_socket_addr(&(self.rest_listen_address.as_str(), self.rest_listen_port + 1))
    }

    pub fn gossip_socket_addr(&self) -> anyhow::Result<SocketAddr> {
        // We use the same port number as the rest port but this is UDP
        get_socket_addr(&(self.rest_listen_address.as_str(), self.rest_listen_port))
    }

    pub fn peer_socket_addrs(&self) -> anyhow::Result<Vec<SocketAddr>> {
        // If no port is given, we assume a peer is using the same port as ourself.
        let default_gossip_port = self.gossip_socket_addr()?.port();
        let peer_socket_addrs: Vec<SocketAddr> = self
            .peer_seeds
            .iter()
            .flat_map(|peer_seed| {
                parse_socket_addr_with_default_port(peer_seed, default_gossip_port).map_err(
                    |_| warn!(address = %peer_seed, "Failed to resolve peer seed address."),
                )
            })
            .collect();

        if !self.peer_seeds.is_empty() && peer_socket_addrs.is_empty() {
            bail!(
                "Failed to resolve any of the peer seed addresses: `{}`",
                self.peer_seeds.join(", ")
            )
        }
        Ok(peer_socket_addrs)
    }
}

impl SearcherConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.peer_seeds.is_empty() {
            warn!("Peer seed list is empty.")
        }
        Ok(())
    }
}

impl Default for SearcherConfig {
    fn default() -> Self {
        Self {
            rest_listen_address: default_listen_address(),
            rest_listen_port: Self::default_rest_listen_port(),
            peer_seeds: Vec::new(),
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QuickwitConfig {
    pub version: usize,
    #[serde(default = "default_node_id")]
    pub node_id: String,
    #[serde(default = "default_metastore_and_index_root_uri")]
    pub metastore_uri: String,
    #[serde(default = "default_metastore_and_index_root_uri")]
    pub default_index_root_uri: String,
    #[serde(default = "default_data_dir_path")]
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
    // Parses quickwit config from the given config content and validates.
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
        let parser_fn = match Path::new(uri.as_ref()).extension().and_then(OsStr::to_str) {
            Some("json") => Self::from_json,
            Some("toml") => Self::from_toml,
            Some("yaml") | Some("yml") => Self::from_yaml,
            Some(extension) => bail!(
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
            .context("Failed to parse JSON server config file.")
    }

    fn from_toml(bytes: &[u8]) -> anyhow::Result<Self> {
        toml::from_slice(bytes).context("Failed to parse TOML server config file.")
    }

    fn from_yaml(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_yaml::from_slice(bytes).context("Failed to parse YAML server config file.")
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.data_dir_path.exists() {
            bail!(
                "Data dir `{}` does not exist.",
                self.data_dir_path.display()
            );
        }
        self.searcher_config.validate()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::path::PathBuf;

    use super::*;

    fn get_resource_path(resource_filename: &str) -> String {
        format!(
            "{}/resources/tests/config/{}",
            env!("CARGO_MANIFEST_DIR"),
            resource_filename
        )
    }

    fn set_data_dir_path(config: &mut QuickwitConfig, path: PathBuf) {
        config.data_dir_path = path;
    }

    macro_rules! test_parser {
        ($test_function_name:ident, $file_extension:expr) => {
            #[tokio::test]
            async fn $test_function_name() -> anyhow::Result<()> {
                let config_filepath =
                    get_resource_path(&format!("quickwit.{}", stringify!($file_extension)));
                let config_uri = Uri::try_new(&config_filepath)?;
                let file = std::fs::read_to_string(&config_filepath).unwrap();
                let config = QuickwitConfig::from_uri(&config_uri, file.as_bytes()).await?;
                assert_eq!(config.version, 0);
                assert_eq!(
                    config.metastore_uri,
                    "postgres://username:password@host:port/db"
                );

                assert_eq!(
                    config.indexer_config,
                    IndexerConfig {
                        rest_listen_address: "0.0.0.0".to_string(),
                        rest_listen_port: 1111,
                        split_store_max_num_bytes: Byte::from_str("1T").unwrap(),
                        split_store_max_num_splits: 10_000,
                    }
                );

                assert_eq!(
                    config.searcher_config,
                    SearcherConfig {
                        rest_listen_address: "0.0.0.0".to_string(),
                        rest_listen_port: 11111,
                        peer_seeds: vec![
                            "quickwit-searcher-0.local".to_string(),
                            "quickwit-searcher-1.local".to_string()
                        ],
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
    fn test_config_default_values() {
        {
            let config_yaml = r#"
            version: 0
            node_id: 1
            metastore_uri: postgres://username:password@host:port/db
        "#;
            let config = serde_yaml::from_str::<QuickwitConfig>(config_yaml).unwrap();
            assert_eq!(config.version, 0);
            assert_eq!(config.node_id, "1");
            assert_eq!(
                config.metastore_uri,
                "postgres://username:password@host:port/db"
            );
            assert!(config.storage_config.is_none());
        }
        {
            let config_yaml = r#"
            version: 0
            metastore_uri: postgres://username:password@host:port/db
            data_dir_path: /opt/quickwit/data
        "#;
            let config = serde_yaml::from_str::<QuickwitConfig>(config_yaml).unwrap();
            assert_eq!(config.version, 0);
            assert_eq!(
                config.metastore_uri,
                "postgres://username:password@host:port/db"
            );
            assert_eq!(
                config.indexer_config,
                IndexerConfig {
                    ..Default::default()
                }
            );
            assert_eq!(
                config.searcher_config,
                SearcherConfig {
                    ..Default::default()
                }
            );
        }
        {
            let minimal_config_yaml = "version: 0";
            let config = serde_yaml::from_str::<QuickwitConfig>(minimal_config_yaml).unwrap();
            assert_eq!(config.version, 0);
            assert!(config.node_id.starts_with("node-"));
            assert_eq!(
                config.metastore_uri,
                format!(
                    "file://{}/qwdata/indexes",
                    env::current_dir().unwrap().display()
                )
            );
            assert_eq!(config.data_dir_path.to_string_lossy(), "./qwdata");
        }
    }

    #[test]
    fn test_searcher_config_validate() {
        {
            let searcher_config = SearcherConfig {
                ..Default::default()
            };
            assert!(searcher_config.validate().is_ok());
        }
    }

    #[tokio::test]
    async fn test_quickwit_config_validate() {
        let config_filepath = get_resource_path("quickwit.toml");
        let file_content = std::fs::read_to_string(&config_filepath).unwrap();

        let config_uri = Uri::try_new(&config_filepath).unwrap();
        let mut quickwit_config = QuickwitConfig::from_uri(&config_uri, file_content.as_bytes())
            .await
            .unwrap();
        set_data_dir_path(&mut quickwit_config, env::current_dir().unwrap());
        assert!(quickwit_config.validate().is_ok());
    }

    #[test]
    fn test_peer_socket_addrs() {
        {
            let searcher_config = SearcherConfig {
                rest_listen_port: 1789,
                peer_seeds: Vec::new(),
                ..Default::default()
            };
            assert!(searcher_config.peer_socket_addrs().unwrap().is_empty());
        }
        {
            let searcher_config = SearcherConfig {
                rest_listen_port: 1789,
                peer_seeds: vec!["unresolvable-host".to_string()],
                ..Default::default()
            };
            assert!(searcher_config.peer_socket_addrs().is_err());
        }
        {
            let searcher_config = SearcherConfig {
                rest_listen_port: 1789,
                peer_seeds: vec!["unresolvable-host".to_string(), "127.0.0.1".to_string()],
                ..Default::default()
            };
            assert_eq!(
                searcher_config.peer_socket_addrs().unwrap(),
                vec!["127.0.0.1:1789".parse().unwrap()]
            );
        }
    }

    #[tokio::test]
    async fn test_load_config_with_validation_error() {
        let config_path = get_resource_path("quickwit.yaml");
        let config_uri = Uri::try_new(&config_path).unwrap();
        let file = std::fs::read_to_string(&config_path).unwrap();
        let config = QuickwitConfig::load(&config_uri, file.as_bytes(), None)
            .await
            .unwrap_err();
        assert!(config.to_string().contains("Data dir"));
    }
}
