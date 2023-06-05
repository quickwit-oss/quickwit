// Copyright (C) 2023 Quickwit, Inc.
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

use std::num::NonZeroUsize;
use std::ops::Deref;

use anyhow::ensure;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, EnumMap};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetastoreBackend {
    File,
    #[serde(alias = "pg", alias = "postgres")]
    PostgreSQL,
}

/// Holds the metastore configurations defined in the `metastore` section of node config files.
///
/// ```yaml
/// metastore:
///   file:
///     polling_interval: 30s
///
///   postgres:
///     max_num_connections: 12
/// ```
#[serde_as]
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetastoreConfigs(#[serde_as(as = "EnumMap")] Vec<MetastoreConfig>);

impl MetastoreConfigs {
    pub fn redact(&mut self) {
        for metastore_config in self.0.iter_mut() {
            metastore_config.redact();
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let backends: Vec<MetastoreBackend> = self
            .0
            .iter()
            .map(|metastore_config| metastore_config.backend())
            .sorted()
            .collect();

        for (left, right) in backends.iter().zip(backends.iter().skip(1)) {
            ensure!(
                left != right,
                "{left:?} metastore config is defined multiple times."
            );
        }
        Ok(())
    }

    pub fn find_file(&self) -> Option<&FileMetastoreConfig> {
        self.0
            .iter()
            .find_map(|metastore_config| match metastore_config {
                MetastoreConfig::File(file_metastore_config) => Some(file_metastore_config),
                _ => None,
            })
    }

    pub fn find_postgres(&self) -> Option<&PostgresMetastoreConfig> {
        self.0
            .iter()
            .find_map(|metastore_config| match metastore_config {
                MetastoreConfig::PostgreSQL(postgres_metastore_config) => {
                    Some(postgres_metastore_config)
                }
                _ => None,
            })
    }
}

impl Deref for MetastoreConfigs {
    type Target = Vec<MetastoreConfig>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetastoreConfig {
    File(FileMetastoreConfig),
    #[serde(alias = "pg", alias = "postgres")]
    PostgreSQL(PostgresMetastoreConfig),
}

impl MetastoreConfig {
    pub fn backend(&self) -> MetastoreBackend {
        match self {
            Self::File(_) => MetastoreBackend::File,
            Self::PostgreSQL(_) => MetastoreBackend::PostgreSQL,
        }
    }

    pub fn as_file(&self) -> Option<&FileMetastoreConfig> {
        match self {
            Self::File(file_metastore_config) => Some(file_metastore_config),
            _ => None,
        }
    }

    pub fn as_postgres(&self) -> Option<&PostgresMetastoreConfig> {
        match self {
            Self::PostgreSQL(postgres_metastore_config) => Some(postgres_metastore_config),
            _ => None,
        }
    }

    pub fn redact(&mut self) {
        // TODO: Implement this method when we end up storing secrets in the
        // metastore config.
    }
}

impl From<FileMetastoreConfig> for MetastoreConfig {
    fn from(file_metastore_config: FileMetastoreConfig) -> Self {
        Self::File(file_metastore_config)
    }
}

impl From<PostgresMetastoreConfig> for MetastoreConfig {
    fn from(postgres_metastore_config: PostgresMetastoreConfig) -> Self {
        Self::PostgreSQL(postgres_metastore_config)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresMetastoreConfig {
    #[serde(default = "PostgresMetastoreConfig::default_max_num_connections")]
    pub max_num_connections: NonZeroUsize,
}

impl Default for PostgresMetastoreConfig {
    fn default() -> Self {
        Self {
            max_num_connections: Self::default_max_num_connections(),
        }
    }
}

impl PostgresMetastoreConfig {
    pub fn default_max_num_connections() -> NonZeroUsize {
        NonZeroUsize::new(10).expect("10 is always non-zero.")
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMetastoreConfig;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metastore_configs_serde() {
        let metastore_configs_yaml = "";
        let metastore_configs: MetastoreConfigs =
            serde_yaml::from_str(metastore_configs_yaml).unwrap();
        assert!(metastore_configs.is_empty());

        let metastore_configs_yaml = r#"
                postgres:
                    max_num_connections: 12
            "#;
        let metastore_configs: MetastoreConfigs =
            serde_yaml::from_str(metastore_configs_yaml).unwrap();

        let expected_metastore_configs = MetastoreConfigs(vec![PostgresMetastoreConfig {
            max_num_connections: NonZeroUsize::new(12).expect("12 is always non-zero."),
        }
        .into()]);
        assert_eq!(metastore_configs, expected_metastore_configs);
    }

    #[test]
    fn test_metastore_configs_validate() {
        let metastore_configs = MetastoreConfigs(vec![
            PostgresMetastoreConfig {
                max_num_connections: NonZeroUsize::new(12).expect("12 is always non-zero."),
            }
            .into(),
            PostgresMetastoreConfig {
                max_num_connections: NonZeroUsize::new(12).expect("12 is always non-zero."),
            }
            .into(),
        ]);
        metastore_configs.validate().unwrap_err();
    }

    #[test]
    fn test_pg_metastore_config_serde() {
        {
            let pg_metastore_config_yaml = r#"
                max_num_connections: 12
            "#;
            let pg_metastore_config: PostgresMetastoreConfig =
                serde_yaml::from_str(pg_metastore_config_yaml).unwrap();

            let expected_pg_metastore_config = PostgresMetastoreConfig {
                max_num_connections: NonZeroUsize::new(12).expect("12 is always non-zero."),
            };
            assert_eq!(pg_metastore_config, expected_pg_metastore_config);
        }
    }
}
