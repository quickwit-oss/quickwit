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
use quickwit_common::net::socket_addr_from_str;
use quickwit_storage::load_file;
use serde::{Deserialize, Serialize};
use tracing::warn;

fn default_data_dir_path() -> PathBuf {
    PathBuf::from("/var/lib/quickwit/data")
}

fn default_listen_address() -> String {
    "127.0.0.1".to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IndexerConfig {
    #[serde(default = "default_data_dir_path")]
    pub data_dir_path: PathBuf,
    #[serde(default = "default_listen_address")]
    pub rest_listen_address: String,
    #[serde(default = "IndexerConfig::default_rest_listen_port")]
    pub rest_listen_port: u16,
    #[serde(default = "default_listen_address")]
    pub grpc_listen_address: String,
    #[serde(default = "IndexerConfig::default_grpc_listen_port")]
    pub grpc_listen_port: u16,
    #[serde(default = "IndexerConfig::default_split_store_max_num_bytes")]
    pub split_store_max_num_bytes: Byte,
    #[serde(default = "IndexerConfig::default_split_store_max_num_splits")]
    pub split_store_max_num_splits: usize,
}

impl IndexerConfig {
    fn default_rest_listen_port() -> u16 {
        7180
    }

    fn default_grpc_listen_port() -> u16 {
        7181
    }

    fn default_split_store_max_num_bytes() -> Byte {
        Byte::from_bytes(100_000_000_000) // 100G
    }

    fn default_split_store_max_num_splits() -> usize {
        1_000
    }

    pub fn for_test() -> anyhow::Result<(Self, tempfile::TempDir)> {
        use quickwit_common::net::find_available_port;

        let temp_dir = tempfile::tempdir()?;
        let indexer_config = IndexerConfig {
            data_dir_path: temp_dir.path().to_path_buf(),
            rest_listen_port: find_available_port()?,
            grpc_listen_port: find_available_port()?,
            split_store_max_num_bytes: Byte::from_bytes(50_000_000),
            split_store_max_num_splits: 100,
            ..Default::default()
        };
        Ok((indexer_config, temp_dir))
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.data_dir_path.exists() {
            bail!(
                "Data dir `{}` does not exist.",
                self.data_dir_path.display()
            );
        }
        Ok(())
    }
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            data_dir_path: default_data_dir_path(),
            rest_listen_address: default_listen_address(),
            rest_listen_port: Self::default_rest_listen_port(),
            grpc_listen_address: default_listen_address(),
            grpc_listen_port: Self::default_grpc_listen_port(),
            split_store_max_num_bytes: Self::default_split_store_max_num_bytes(),
            split_store_max_num_splits: Self::default_split_store_max_num_splits(),
        }
    }
}

// TODO: caching
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct SearcherConfig {
    #[serde(default = "default_data_dir_path")]
    pub data_dir_path: PathBuf,
    #[serde(default = "SearcherConfig::default_host_key_path")]
    pub host_key_path: PathBuf,
    #[serde(default = "default_listen_address")]
    pub rest_listen_address: String,
    #[serde(default = "SearcherConfig::default_rest_listen_port")]
    pub rest_listen_port: u16,
    #[serde(default = "default_listen_address")]
    pub grpc_listen_address: String,
    #[serde(default = "SearcherConfig::default_grpc_listen_port")]
    pub grpc_listen_port: u16,
    #[serde(default = "default_listen_address")]
    pub discovery_listen_address: String,
    #[serde(default = "SearcherConfig::default_discovery_listen_port")]
    pub discovery_listen_port: u16,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub peer_seeds: Vec<String>,
    #[serde(default = "SearcherConfig::default_fast_field_cache_capacity")]
    pub fast_field_cache_capacity: Byte,
    #[serde(default = "SearcherConfig::default_split_footer_cache_capacity")]
    pub split_footer_cache_capacity: Byte,
}

impl SearcherConfig {
    fn default_host_key_path() -> PathBuf {
        PathBuf::from("/var/lib/quickwit/host_key")
    }

    fn default_rest_listen_port() -> u16 {
        7280
    }

    fn default_grpc_listen_port() -> u16 {
        7281
    }

    fn default_discovery_listen_port() -> u16 {
        7282
    }

    fn default_fast_field_cache_capacity() -> Byte {
        Byte::from_bytes(1_000_000_000) // 1G
    }

    fn default_split_footer_cache_capacity() -> Byte {
        Byte::from_bytes(500_000_000) // 500M
    }

    pub fn rest_socket_addr(&self) -> anyhow::Result<SocketAddr> {
        socket_addr_from_str(format!(
            "{}:{}",
            self.rest_listen_address, self.rest_listen_port
        ))
    }

    pub fn grpc_socket_addr(&self) -> anyhow::Result<SocketAddr> {
        socket_addr_from_str(format!(
            "{}:{}",
            self.grpc_listen_address, self.grpc_listen_port
        ))
    }

    pub fn discovery_socket_addr(&self) -> anyhow::Result<SocketAddr> {
        socket_addr_from_str(format!(
            "{}:{}",
            self.discovery_listen_address, self.discovery_listen_port
        ))
    }

    pub fn peer_socket_addrs(&self) -> anyhow::Result<Vec<SocketAddr>> {
        self.peer_seeds.iter().map(socket_addr_from_str).collect()
    }
}

impl SearcherConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.data_dir_path.exists() {
            bail!(
                "Data dir `{}` does not exist.",
                self.data_dir_path.display()
            );
        }
        if self.peer_seeds.is_empty() {
            warn!("Peer seed list is empty.")
        }
        Ok(())
    }
}

impl Default for SearcherConfig {
    fn default() -> Self {
        Self {
            data_dir_path: default_data_dir_path(),
            host_key_path: Self::default_host_key_path(),
            rest_listen_address: default_listen_address(),
            rest_listen_port: Self::default_rest_listen_port(),
            grpc_listen_address: default_listen_address(),
            grpc_listen_port: Self::default_grpc_listen_port(),
            discovery_listen_address: default_listen_address(),
            discovery_listen_port: Self::default_discovery_listen_port(),
            peer_seeds: Vec::new(),
            fast_field_cache_capacity: Self::default_fast_field_cache_capacity(),
            split_footer_cache_capacity: Self::default_split_footer_cache_capacity(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct S3Config {
    pub region: Option<String>,
    pub endpoint: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct StorageConfig {
    #[serde(rename = "s3")]
    pub s3_config: S3Config,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ServerConfig {
    pub version: usize,
    pub metastore_uri: String,
    #[serde(rename = "indexer")]
    pub indexer_config: Option<IndexerConfig>,
    #[serde(rename = "searcher")]
    pub searcher_config: Option<SearcherConfig>,
    #[serde(rename = "storage")]
    pub storage_config: Option<StorageConfig>,
}

impl ServerConfig {
    pub async fn from_file(path: &str) -> anyhow::Result<Self> {
        let parser_fn = match Path::new(path).extension().and_then(OsStr::to_str) {
            Some("json") => Self::from_json,
            Some("toml") => Self::from_toml,
            Some("yaml") | Some("yml") => Self::from_yaml,
            Some(extension) => bail!(
                "Failed to read server config file: file extension `.{}` is not supported. \
                 Supported file formats and extensions are JSON (.json), TOML (.toml), and YAML \
                 (.yaml or .yml).",
                extension
            ),
            None => bail!(
                "Failed to read server config file: file extension is missing. Supported file \
                 formats and extensions are JSON (.json), TOML (.toml), and YAML (.yaml or .yml)."
            ),
        };
        let file_content = load_file(path).await?;
        parser_fn(file_content.as_slice())
    }

    pub fn from_json(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_reader(StripComments::new(bytes))
            .context("Failed to parse JSON server config file.")
    }

    pub fn from_toml(bytes: &[u8]) -> anyhow::Result<Self> {
        toml::from_slice(bytes).context("Failed to parse TOML server config file.")
    }

    pub fn from_yaml(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_yaml::from_slice(bytes).context("Failed to parse YAML server config file.")
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if let Some(indexer_config) = &self.indexer_config {
            indexer_config.validate()?;
        }
        if let Some(searcher_config) = &self.searcher_config {
            searcher_config.validate()?;
        }
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
            "{}/resources/tests/server_config/{}",
            env!("CARGO_MANIFEST_DIR"),
            resource_filename
        )
    }

    fn set_data_dir_path(server_config: &mut ServerConfig, path: PathBuf) {
        if let Some(indexer_config) = &mut server_config.indexer_config {
            indexer_config.data_dir_path = path.clone();
        }
        if let Some(searcher_config) = &mut server_config.searcher_config {
            searcher_config.data_dir_path = path;
        }
    }

    macro_rules! test_parser {
        ($test_function_name:ident, $file_extension:expr) => {
            #[tokio::test]
            async fn $test_function_name() -> anyhow::Result<()> {
                let server_config_filepath =
                    get_resource_path(&format!("server.{}", stringify!($file_extension)));
                let server_config = ServerConfig::from_file(&server_config_filepath).await?;
                assert_eq!(server_config.version, 0);
                assert_eq!(
                    server_config.metastore_uri,
                    "postgres://username:password@host:port/db"
                );

                assert_eq!(
                    server_config.indexer_config.unwrap(),
                    IndexerConfig {
                        data_dir_path: PathBuf::from("/opt/quickwit/data"),
                        rest_listen_address: "0.0.0.0".to_string(),
                        rest_listen_port: 1111,
                        grpc_listen_address: "0.0.0.0".to_string(),
                        grpc_listen_port: 2222,
                        split_store_max_num_bytes: Byte::from_str("1T").unwrap(),
                        split_store_max_num_splits: 10_000,
                    }
                );

                assert_eq!(
                    server_config.searcher_config.unwrap(),
                    SearcherConfig {
                        data_dir_path: PathBuf::from("/opt/quickwit/data"),
                        host_key_path: PathBuf::from("/opt/quickwit/host_key"),
                        rest_listen_address: "0.0.0.0".to_string(),
                        rest_listen_port: 11111,
                        grpc_listen_address: "0.0.0.0".to_string(),
                        grpc_listen_port: 22222,
                        discovery_listen_address: "0.0.0.0".to_string(),
                        discovery_listen_port: 33333,
                        peer_seeds: vec![
                            "quickwit-searcher-0.local:33333".to_string(),
                            "quickwit-searcher-1.local:33333".to_string()
                        ],
                        fast_field_cache_capacity: Byte::from_str("10G").unwrap(),
                        split_footer_cache_capacity: Byte::from_str("1G").unwrap(),
                    }
                );

                assert_eq!(
                    server_config.storage_config.unwrap().s3_config,
                    S3Config {
                        region: Some("us-east-1".to_string()),
                        endpoint: Some("https://s3.us-east-1.amazonaws.com".to_string()),
                    }
                );
                Ok(())
            }
        };
    }

    test_parser!(test_server_config_from_json, json);
    test_parser!(test_server_config_from_toml, toml);
    test_parser!(test_server_config_from_yaml, yaml);

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
    fn test_server_config_default_values() {
        {
            let server_config_yaml = r#"
            version: 0
            metastore_uri: postgres://username:password@host:port/db
        "#;
            let server_config = serde_yaml::from_str::<ServerConfig>(server_config_yaml).unwrap();
            assert_eq!(server_config.version, 0);
            assert_eq!(
                server_config.metastore_uri,
                "postgres://username:password@host:port/db"
            );
            assert!(server_config.indexer_config.is_none());
            assert!(server_config.searcher_config.is_none());
            assert!(server_config.storage_config.is_none());
        }
        {
            let server_config_yaml = r#"
            version: 0
            metastore_uri: postgres://username:password@host:port/db
 
            indexer:
              data_dir_path: /opt/quickwit/data

            searcher:
              data_dir_path: /opt/quickwit/data
              host_key_path: /opt/quickwit/host_key
        "#;
            let server_config = serde_yaml::from_str::<ServerConfig>(server_config_yaml).unwrap();
            assert_eq!(server_config.version, 0);
            assert_eq!(
                server_config.metastore_uri,
                "postgres://username:password@host:port/db"
            );
            assert_eq!(
                server_config.indexer_config.unwrap(),
                IndexerConfig {
                    data_dir_path: PathBuf::from("/opt/quickwit/data"),
                    ..Default::default()
                }
            );
            assert_eq!(
                server_config.searcher_config.unwrap(),
                SearcherConfig {
                    data_dir_path: PathBuf::from("/opt/quickwit/data"),
                    host_key_path: PathBuf::from("/opt/quickwit/host_key"),
                    ..Default::default()
                }
            );
        }
    }

    #[test]
    fn test_indexer_config_validate() {
        {
            let indexer_config = IndexerConfig {
                data_dir_path: "/data/dir/does/not/exist".into(),
                ..Default::default()
            };
            assert!(indexer_config.validate().is_err());
        }
        {
            let indexer_config = IndexerConfig {
                data_dir_path: env::current_dir().unwrap(),
                ..Default::default()
            };
            assert!(indexer_config.validate().is_ok());
        }
    }

    #[test]
    fn test_searcher_config_validate() {
        {
            let searcher_config = SearcherConfig {
                data_dir_path: "/data/dir/does/not/exist".into(),
                ..Default::default()
            };
            assert!(searcher_config.validate().is_err());
        }
        {
            let searcher_config = SearcherConfig {
                data_dir_path: env::current_dir().unwrap(),
                ..Default::default()
            };
            assert!(searcher_config.validate().is_ok());
        }
    }

    #[tokio::test]
    async fn test_server_config_validate() {
        let server_config_filepath = get_resource_path("server.toml");
        let mut server_config = ServerConfig::from_file(&server_config_filepath)
            .await
            .unwrap();
        set_data_dir_path(&mut server_config, env::current_dir().unwrap());
        assert!(server_config.validate().is_ok());
    }
}
