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
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use byte_unit::Byte;
use serde::{Deserialize, Serialize};
use tracing::warn;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct IndexerConfig {
    pub data_dir_path: PathBuf,
    pub rest_listen_port: u16,
    pub grpc_listen_port: u16,
    #[serde(default = "default_split_store_max_num_bytes")]
    pub split_store_max_num_bytes: Byte,
    #[serde(default = "default_split_store_max_num_files")]
    pub split_store_max_num_files: usize,
}

fn default_split_store_max_num_bytes() -> Byte {
    Byte::from_bytes(100_000_000_000) // 100G
}

fn default_split_store_max_num_files() -> usize {
    1_000
}

impl IndexerConfig {
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

// TODO: caching
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct SearcherConfig {
    pub data_dir_path: PathBuf,
    pub rest_listen_port: u16,
    pub grpc_listen_port: u16,
    pub discovery_listen_port: u16,
    #[serde(default)]
    pub peer_seeds: Vec<String>,
    #[serde(default = "default_fast_field_cache_capacity")]
    pub fast_field_cache_capacity: Byte,
    #[serde(default = "default_split_footer_cache_capacity")]
    pub split_footer_cache_capacity: Byte,
}

fn default_fast_field_cache_capacity() -> Byte {
    Byte::from_bytes(1_000_000_000) // 1G
}

fn default_split_footer_cache_capacity() -> Byte {
    Byte::from_bytes(500_000_000) // 500M
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
    pub metastore_uri: String,
    #[serde(rename = "indexer")]
    pub indexer_config: Option<IndexerConfig>,
    #[serde(rename = "searcher")]
    pub searcher_config: Option<SearcherConfig>,
    #[serde(rename = "storage")]
    pub storage_config: Option<StorageConfig>,
}

impl ServerConfig {
    // TODO: asyncify?
    pub fn from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let parser_fn = match path.as_ref().extension().and_then(OsStr::to_str) {
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
        let file_content = std::fs::read_to_string(path)?;
        parser_fn(file_content.as_bytes())
    }

    pub fn from_json(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice::<ServerConfig>(bytes)
            .context("Failed to parse JSON server config file.")
    }

    pub fn from_toml(bytes: &[u8]) -> anyhow::Result<Self> {
        toml::from_slice::<ServerConfig>(bytes).context("Failed to parse TOML server config file.")
    }

    pub fn from_yaml(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_yaml::from_slice::<ServerConfig>(bytes)
            .context("Failed to parse YAML server config file.")
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

    fn get_resource_path(relative_resource_path: &str) -> PathBuf {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/tests/server_config/");
        path.push(relative_resource_path);
        path
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
            #[test]
            fn $test_function_name() -> anyhow::Result<()> {
                let server_config_filepath =
                    get_resource_path(&format!("server.{}", stringify!($file_extension)));
                let server_config = ServerConfig::from_file(server_config_filepath)?;
                assert_eq!(
                    server_config.metastore_uri,
                    "postgres://username:password@host:port/db"
                );

                assert_eq!(
                    server_config.indexer_config.unwrap(),
                    IndexerConfig {
                        data_dir_path: "/opt/quickwit/data".into(),
                        rest_listen_port: 7180,
                        grpc_listen_port: 7181,
                        split_store_max_num_bytes: Byte::from_str("1T").unwrap(),
                        split_store_max_num_files: 1_000,
                    }
                );

                assert_eq!(
                    server_config.searcher_config.unwrap(),
                    SearcherConfig {
                        data_dir_path: "/opt/quickwit/data".into(),
                        rest_listen_port: 7380,
                        grpc_listen_port: 7381,
                        discovery_listen_port: 7382,
                        peer_seeds: vec![
                            "quickwit-searcher-0.local".to_string(),
                            "quickwit-searcher-1.local".to_string()
                        ],
                        fast_field_cache_capacity: Byte::from_str("5G").unwrap(),
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

    test_parser!(test_from_json, json);
    test_parser!(test_from_toml, toml);
    test_parser!(test_from_yaml, yaml);

    #[test]
    fn test_indexer_config_default_values() {
        let indexer_config_yaml = r#"
            data_dir_path: /opt/quickwit/data
            rest_listen_port: 7180
            grpc_listen_port: 7181
            "#;
        let indexer_config = serde_yaml::from_str::<IndexerConfig>(indexer_config_yaml).unwrap();
        assert_eq!(
            indexer_config.split_store_max_num_bytes,
            Byte::from_str("100G").unwrap()
        );
        assert_eq!(indexer_config.split_store_max_num_files, 1_000);
    }

    #[test]
    fn test_searcher_config_default_values() {
        let searcher_config_yaml = r#"
            data_dir_path: /opt/quickwit/data
            rest_listen_port: 7380
            grpc_listen_port: 7381
            discovery_listen_port: 7382
            "#;
        let searcher_config = serde_yaml::from_str::<SearcherConfig>(searcher_config_yaml).unwrap();
        assert!(searcher_config.peer_seeds.is_empty());
        assert_eq!(
            searcher_config.fast_field_cache_capacity,
            Byte::from_str("1G").unwrap()
        );
        assert_eq!(
            searcher_config.split_footer_cache_capacity,
            Byte::from_str("500M").unwrap()
        );
    }

    #[test]
    fn test_indexer_config_validate() {
        {
            let indexer_config = IndexerConfig {
                data_dir_path: "/data/dir/does/not/exist".into(),
                rest_listen_port: 7180,
                grpc_listen_port: 7181,
                split_store_max_num_bytes: Byte::from_bytes(1),
                split_store_max_num_files: 1,
            };
            assert!(indexer_config.validate().is_err());
        }
        {
            let indexer_config = IndexerConfig {
                data_dir_path: env::current_dir().unwrap(),
                rest_listen_port: 7180,
                grpc_listen_port: 7181,
                split_store_max_num_bytes: Byte::from_bytes(1),
                split_store_max_num_files: 1,
            };
            assert!(indexer_config.validate().is_ok());
        }
    }

    #[test]
    fn test_searcher_config_validate() {
        {
            let searcher_config = SearcherConfig {
                data_dir_path: "/data/dir/does/not/exist".into(),
                rest_listen_port: 7380,
                grpc_listen_port: 7381,
                discovery_listen_port: 7382,
                peer_seeds: vec!["quickwit-searcher-0.local".to_string()],
                fast_field_cache_capacity: Byte::from_bytes(1),
                split_footer_cache_capacity: Byte::from_bytes(1),
            };
            assert!(searcher_config.validate().is_err());
        }
        {
            let searcher_config = SearcherConfig {
                data_dir_path: env::current_dir().unwrap(),
                rest_listen_port: 7380,
                grpc_listen_port: 7381,
                discovery_listen_port: 7382,
                peer_seeds: vec!["quickwit-searcher-0.local".to_string()],
                fast_field_cache_capacity: Byte::from_bytes(1),
                split_footer_cache_capacity: Byte::from_bytes(1),
            };
            assert!(searcher_config.validate().is_ok());
        }
    }

    #[test]
    fn test_server_config_validate() {
        let mut server_config = ServerConfig::from_file(get_resource_path("server.toml")).unwrap();
        set_data_dir_path(&mut server_config, env::current_dir().unwrap());
        assert!(server_config.validate().is_ok());
    }
}
