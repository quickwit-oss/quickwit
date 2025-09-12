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

use std::num::NonZeroUsize;
use std::ops::Deref;
use std::time::Duration;

use anyhow::{Context, ensure};
use humantime::parse_duration;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_with::{EnumMap, serde_as};

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
///     max_connections: 12
/// ```
#[serde_as]
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetastoreConfigs(#[serde_as(as = "EnumMap")] Vec<MetastoreConfig>);

impl MetastoreConfigs {
    pub fn redact(&mut self) {
        for metastore_config in &mut self.0 {
            metastore_config.redact();
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        for metastore_config in &self.0 {
            metastore_config.validate()?;
        }
        let backends: Vec<MetastoreBackend> = self
            .0
            .iter()
            .map(|metastore_config| metastore_config.backend())
            .sorted()
            .collect();

        for (left, right) in backends.iter().zip(backends.iter().skip(1)) {
            ensure!(
                left != right,
                "{left:?} metastore config is defined multiple times"
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

    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::File(file_metastore_config) => file_metastore_config.validate()?,
            Self::PostgreSQL(postgres_metastore_config) => postgres_metastore_config.validate()?,
        }
        Ok(())
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
    #[serde(default = "PostgresMetastoreConfig::default_min_connections")]
    pub min_connections: usize,
    #[serde(
        alias = "max_num_connections",
        default = "PostgresMetastoreConfig::default_max_connections"
    )]
    pub max_connections: NonZeroUsize,
    #[serde(default = "PostgresMetastoreConfig::default_acquire_connection_timeout")]
    pub acquire_connection_timeout: String,
    #[serde(default = "PostgresMetastoreConfig::default_idle_connection_timeout")]
    pub idle_connection_timeout: String,
    #[serde(default = "PostgresMetastoreConfig::default_max_connection_lifetime")]
    pub max_connection_lifetime: String,
}

impl Default for PostgresMetastoreConfig {
    fn default() -> Self {
        Self {
            min_connections: Self::default_min_connections(),
            max_connections: Self::default_max_connections(),
            acquire_connection_timeout: Self::default_acquire_connection_timeout(),
            idle_connection_timeout: Self::default_idle_connection_timeout(),
            max_connection_lifetime: Self::default_max_connection_lifetime(),
        }
    }
}

impl PostgresMetastoreConfig {
    pub fn default_min_connections() -> usize {
        0
    }

    pub fn default_max_connections() -> NonZeroUsize {
        NonZeroUsize::new(10).unwrap()
    }

    pub fn default_acquire_connection_timeout() -> String {
        "10s".to_string()
    }

    pub fn default_idle_connection_timeout() -> String {
        "10min".to_string()
    }

    pub fn default_max_connection_lifetime() -> String {
        "30min".to_string()
    }

    pub fn acquire_connection_timeout(&self) -> anyhow::Result<Duration> {
        parse_duration(&self.acquire_connection_timeout).with_context(|| {
            format!(
                "failed to parse `acquire_connection_timeout` value `{}`",
                self.acquire_connection_timeout
            )
        })
    }

    pub fn idle_connection_timeout_opt(&self) -> anyhow::Result<Option<Duration>> {
        if self.idle_connection_timeout.is_empty() || self.idle_connection_timeout == "0" {
            return Ok(None);
        }
        let idle_connection_timeout =
            parse_duration(&self.idle_connection_timeout).with_context(|| {
                format!(
                    "failed to parse `idle_connection_timeout` value `{}`",
                    self.idle_connection_timeout
                )
            })?;
        if idle_connection_timeout.is_zero() {
            Ok(None)
        } else {
            Ok(Some(idle_connection_timeout))
        }
    }

    pub fn max_connection_lifetime_opt(&self) -> anyhow::Result<Option<Duration>> {
        if self.max_connection_lifetime.is_empty() || self.max_connection_lifetime == "0" {
            return Ok(None);
        }
        let max_connection_lifetime =
            parse_duration(&self.max_connection_lifetime).with_context(|| {
                format!(
                    "failed to parse `max_connection_lifetime` value `{}`",
                    self.max_connection_lifetime
                )
            })?;
        if max_connection_lifetime.is_zero() {
            Ok(None)
        } else {
            Ok(Some(max_connection_lifetime))
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.min_connections <= self.max_connections.get(),
            "`min_connections` must be less than or equal to `max_connections`"
        );
        self.acquire_connection_timeout()?;
        self.idle_connection_timeout_opt()?;
        self.max_connection_lifetime_opt()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMetastoreConfig;

impl FileMetastoreConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

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
                    max_connections: 12
            "#;
        let metastore_configs: MetastoreConfigs =
            serde_yaml::from_str(metastore_configs_yaml).unwrap();

        let expected_metastore_configs = MetastoreConfigs(vec![
            PostgresMetastoreConfig {
                max_connections: NonZeroUsize::new(12).unwrap(),
                ..Default::default()
            }
            .into(),
        ]);
        assert_eq!(metastore_configs, expected_metastore_configs);
    }

    #[test]
    fn test_metastore_configs_validate() {
        let metastore_configs = MetastoreConfigs(vec![
            PostgresMetastoreConfig {
                max_connections: NonZeroUsize::new(12).unwrap(),
                ..Default::default()
            }
            .into(),
            PostgresMetastoreConfig {
                max_connections: NonZeroUsize::new(12).unwrap(),
                ..Default::default()
            }
            .into(),
        ]);
        let error = metastore_configs.validate().unwrap_err();
        assert!(error.to_string().contains("defined multiple times"));

        let metastore_configs = MetastoreConfigs(vec![
            PostgresMetastoreConfig {
                acquire_connection_timeout: "15".to_string(),
                ..Default::default()
            }
            .into(),
        ]);
        let error = metastore_configs.validate().unwrap_err();
        assert!(error.to_string().contains("`acquire_connection_timeout`"));
    }

    #[test]
    fn test_pg_metastore_config_serde() {
        {
            let pg_metastore_config_yaml = "";
            let pg_metastore_config: PostgresMetastoreConfig =
                serde_yaml::from_str(pg_metastore_config_yaml).unwrap();
            assert_eq!(pg_metastore_config, PostgresMetastoreConfig::default());
        }
        {
            let pg_metastore_config_yaml = r#"
                max_connections: 12
            "#;
            let pg_metastore_config: PostgresMetastoreConfig =
                serde_yaml::from_str(pg_metastore_config_yaml).unwrap();

            let expected_pg_metastore_config = PostgresMetastoreConfig {
                max_connections: NonZeroUsize::new(12).unwrap(),
                ..Default::default()
            };
            assert_eq!(pg_metastore_config, expected_pg_metastore_config);
        }
        {
            let pg_metastore_config_yaml = r#"
                min_connections: 6
                max_connections: 12
                acquire_connection_timeout: 500ms
                idle_connection_timeout: 1h
                max_connection_lifetime: 1d
            "#;
            let pg_metastore_config: PostgresMetastoreConfig =
                serde_yaml::from_str(pg_metastore_config_yaml).unwrap();

            let expected_pg_metastore_config = PostgresMetastoreConfig {
                min_connections: 6,
                max_connections: NonZeroUsize::new(12).unwrap(),
                acquire_connection_timeout: "500ms".to_string(),
                idle_connection_timeout: "1h".to_string(),
                max_connection_lifetime: "1d".to_string(),
            };
            assert_eq!(pg_metastore_config, expected_pg_metastore_config);
            assert_eq!(
                pg_metastore_config.acquire_connection_timeout().unwrap(),
                Duration::from_millis(500)
            );
            assert_eq!(
                pg_metastore_config.idle_connection_timeout_opt().unwrap(),
                Some(Duration::from_secs(3600))
            );
            assert_eq!(
                pg_metastore_config.max_connection_lifetime_opt().unwrap(),
                Some(Duration::from_secs(24 * 3600))
            );
        }
        {
            let pg_metastore_config_yaml = r#"
                min_connections: 6
                max_connections: 12
                acquire_connection_timeout: 15s
                idle_connection_timeout: ""
                max_connection_lifetime: 0
            "#;
            let pg_metastore_config: PostgresMetastoreConfig =
                serde_yaml::from_str(pg_metastore_config_yaml).unwrap();

            let expected_pg_metastore_config = PostgresMetastoreConfig {
                min_connections: 6,
                max_connections: NonZeroUsize::new(12).unwrap(),
                acquire_connection_timeout: "15s".to_string(),
                idle_connection_timeout: "".to_string(),
                max_connection_lifetime: "0".to_string(),
            };
            assert_eq!(pg_metastore_config, expected_pg_metastore_config);
            assert_eq!(
                pg_metastore_config.acquire_connection_timeout().unwrap(),
                Duration::from_secs(15)
            );
            assert!(
                pg_metastore_config
                    .idle_connection_timeout_opt()
                    .unwrap()
                    .is_none()
            );
            assert!(
                pg_metastore_config
                    .max_connection_lifetime_opt()
                    .unwrap()
                    .is_none(),
            );
        }
    }
}
