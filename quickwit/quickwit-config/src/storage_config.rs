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

use std::ops::Deref;
use std::{env, fmt};

use anyhow::ensure;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, EnumMap};

/// Lists the storage backends supported by Quickwit.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageBackend {
    /// Azure Blob Storage
    Azure,
    /// Local file system
    File,
    /// In-memory storage, for testing purposes
    Ram,
    /// Amazon S3 or S3-compatible storage
    S3,
}

/// Holds the storage configurations defined in the `storage` section of node config files.
///
/// ```yaml
/// storage:
///   azure:
///     account: test-account
///
///   s3:
///     endpoint: http://localhost:4566
/// ```
#[serde_as]
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageConfigs(#[serde_as(as = "EnumMap")] Vec<StorageConfig>);

impl StorageConfigs {
    pub fn redact(&mut self) {
        for storage_config in self.0.iter_mut() {
            storage_config.redact();
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let backends: Vec<StorageBackend> = self
            .0
            .iter()
            .map(|storage_config| storage_config.backend())
            .sorted()
            .collect();

        for (left, right) in backends.iter().zip(backends.iter().skip(1)) {
            ensure!(
                left != right,
                "{left:?} storage config is defined multiple times.",
            );
        }
        Ok(())
    }

    pub fn find_azure(&self) -> Option<&AzureStorageConfig> {
        self.0
            .iter()
            .find_map(|storage_config| match storage_config {
                StorageConfig::Azure(azure_storage_config) => Some(azure_storage_config),
                _ => None,
            })
    }

    pub fn find_file(&self) -> Option<&FileStorageConfig> {
        self.0
            .iter()
            .find_map(|storage_config| match storage_config {
                StorageConfig::File(file_storage_config) => Some(file_storage_config),
                _ => None,
            })
    }

    pub fn find_ram(&self) -> Option<&RamStorageConfig> {
        self.0
            .iter()
            .find_map(|storage_config| match storage_config {
                StorageConfig::Ram(ram_storage_config) => Some(ram_storage_config),
                _ => None,
            })
    }

    pub fn find_s3(&self) -> Option<&S3StorageConfig> {
        self.0
            .iter()
            .find_map(|storage_config| match storage_config {
                StorageConfig::S3(s3_storage_config) => Some(s3_storage_config),
                _ => None,
            })
    }
}

impl Deref for StorageConfigs {
    type Target = Vec<StorageConfig>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageConfig {
    Azure(AzureStorageConfig),
    File(FileStorageConfig),
    Ram(RamStorageConfig),
    S3(S3StorageConfig),
}

impl StorageConfig {
    pub fn redact(&mut self) {
        match self {
            Self::Azure(azure_storage_config) => azure_storage_config.redact(),
            Self::File(_) | Self::Ram(_) => {}
            Self::S3(s3_storage_config) => s3_storage_config.redact(),
        }
    }

    pub fn as_azure(&self) -> Option<&AzureStorageConfig> {
        match self {
            Self::Azure(azure_storage_config) => Some(azure_storage_config),
            _ => None,
        }
    }

    pub fn as_file(&self) -> Option<&FileStorageConfig> {
        match self {
            Self::File(file_storage_config) => Some(file_storage_config),
            _ => None,
        }
    }

    pub fn as_ram(&self) -> Option<&RamStorageConfig> {
        match self {
            Self::Ram(ram_storage_config) => Some(ram_storage_config),
            _ => None,
        }
    }

    pub fn as_s3(&self) -> Option<&S3StorageConfig> {
        match self {
            Self::S3(s3_storage_config) => Some(s3_storage_config),
            _ => None,
        }
    }
}

impl From<AzureStorageConfig> for StorageConfig {
    fn from(azure_storage_config: AzureStorageConfig) -> Self {
        Self::Azure(azure_storage_config)
    }
}

impl From<FileStorageConfig> for StorageConfig {
    fn from(file_storage_config: FileStorageConfig) -> Self {
        Self::File(file_storage_config)
    }
}

impl From<RamStorageConfig> for StorageConfig {
    fn from(ram_storage_config: RamStorageConfig) -> Self {
        Self::Ram(ram_storage_config)
    }
}

impl From<S3StorageConfig> for StorageConfig {
    fn from(s3_storage_config: S3StorageConfig) -> Self {
        Self::S3(s3_storage_config)
    }
}

impl StorageConfig {
    pub fn backend(&self) -> StorageBackend {
        match self {
            Self::Azure(_) => StorageBackend::Azure,
            Self::File(_) => StorageBackend::File,
            Self::Ram(_) => StorageBackend::Ram,
            Self::S3(_) => StorageBackend::S3,
        }
    }
}

#[derive(Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AzureStorageConfig {
    #[serde(default)]
    #[serde(rename = "account")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_name: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_key: Option<String>,
}

impl AzureStorageConfig {
    pub const AZURE_STORAGE_ACCOUNT_ENV_VAR: &'static str = "QW_AZURE_STORAGE_ACCOUNT";

    pub const AZURE_STORAGE_ACCESS_KEY_ENV_VAR: &'static str = "QW_AZURE_STORAGE_ACCESS_KEY";

    /// Redacts the access key.
    pub fn redact(&mut self) {
        if let Some(access_key) = self.access_key.as_mut() {
            *access_key = "***redacted***".to_string();
        }
    }

    /// Attempts to find the account name in the environment variable `QW_AZURE_STORAGE_ACCOUNT` or
    /// the config.
    pub fn resolve_account_name(&self) -> Option<String> {
        env::var(Self::AZURE_STORAGE_ACCOUNT_ENV_VAR)
            .ok()
            .or_else(|| self.account_name.clone())
    }

    /// Attempts to find the access key in the environment variable `QW_AZURE_STORAGE_ACCESS_KEY` or
    /// the config.
    pub fn resolve_access_key(&self) -> Option<String> {
        env::var(Self::AZURE_STORAGE_ACCESS_KEY_ENV_VAR)
            .ok()
            .or_else(|| self.access_key.clone())
    }
}

impl fmt::Debug for AzureStorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AzureStorageConfig")
            .field("account_name", &self.account_name)
            .field(
                "access_key",
                &self.access_key.as_ref().map(|_| "***redacted***"),
            )
            .finish()
    }
}

#[derive(Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3StorageConfig {
    #[serde(default)]
    pub access_key_id: Option<String>,
    #[serde(default)]
    pub secret_access_key: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub force_path_style_access: bool,
    #[serde(default)]
    pub disable_multi_object_delete_requests: bool,
}

impl S3StorageConfig {
    pub fn redact(&mut self) {
        if let Some(secret_access_key) = self.secret_access_key.as_mut() {
            *secret_access_key = "***redacted***".to_string();
        }
    }

    pub fn endpoint(&self) -> Option<String> {
        env::var("QW_S3_ENDPOINT")
            .ok()
            .or_else(|| self.endpoint.clone())
    }

    pub fn force_path_style_access(&self) -> Option<bool> {
        Some(env::var("QW_S3_FORCE_PATH_STYLE_ACCESS").is_ok() || self.force_path_style_access)
    }
}

impl fmt::Debug for S3StorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3StorageConfig")
            .field("access_key_id", &self.access_key_id)
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| "***redacted***"),
            )
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("force_path_style_access", &self.force_path_style_access)
            .field(
                "disable_multi_object_delete_requests",
                &self.disable_multi_object_delete_requests,
            )
            .finish()
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileStorageConfig;

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RamStorageConfig;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_configs_serde() {
        let storage_configs_yaml = "";
        let storage_configs: StorageConfigs = serde_yaml::from_str(storage_configs_yaml).unwrap();
        assert!(storage_configs.is_empty());

        let storage_configs_yaml = r#"
                azure:
                    account: test-account
                s3:
                    endpoint: http://localhost:4566
            "#;
        let storage_configs: StorageConfigs = serde_yaml::from_str(storage_configs_yaml).unwrap();

        let expected_storage_configs = StorageConfigs(vec![
            AzureStorageConfig {
                account_name: Some("test-account".to_string()),
                ..Default::default()
            }
            .into(),
            S3StorageConfig {
                endpoint: Some("http://localhost:4566".to_string()),
                ..Default::default()
            }
            .into(),
        ]);
        assert_eq!(storage_configs, expected_storage_configs);
    }

    #[test]
    fn test_storage_configs_validate() {
        let storage_configs = StorageConfigs(vec![
            AzureStorageConfig {
                account_name: Some("test-account".to_string()),
                ..Default::default()
            }
            .into(),
            AzureStorageConfig {
                account_name: Some("prod-account".to_string()),
                ..Default::default()
            }
            .into(),
        ]);
        storage_configs.validate().unwrap_err();
    }

    #[test]
    fn test_storage_configs_redact() {
        let mut storage_configs = StorageConfigs(vec![
            AzureStorageConfig {
                access_key: Some("test-azure-access-key".to_string()),
                ..Default::default()
            }
            .into(),
            S3StorageConfig {
                secret_access_key: Some("test-s3-secret-access-key".to_string()),
                ..Default::default()
            }
            .into(),
        ]);
        storage_configs.redact();

        assert_eq!(
            storage_configs
                .find_azure()
                .unwrap()
                .access_key
                .as_ref()
                .unwrap(),
            "***redacted***"
        );
        assert_eq!(
            storage_configs
                .find_s3()
                .unwrap()
                .secret_access_key
                .as_ref()
                .unwrap(),
            "***redacted***"
        );
    }

    #[test]
    fn test_storage_azure_config_serde() {
        {
            let azure_storage_config_yaml = r#"
                account: test-account
            "#;
            let azure_storage_config: AzureStorageConfig =
                serde_yaml::from_str(azure_storage_config_yaml).unwrap();

            let expected_azure_config = AzureStorageConfig {
                account_name: Some("test-account".to_string()),
                ..Default::default()
            };
            assert_eq!(azure_storage_config, expected_azure_config);
        }
        {
            let azure_storage_config_yaml = r#"
                account: test-account
                access_key: test-access-key
            "#;
            let azure_storage_config: AzureStorageConfig =
                serde_yaml::from_str(azure_storage_config_yaml).unwrap();

            let expected_azure_config = AzureStorageConfig {
                account_name: Some("test-account".to_string()),
                access_key: Some("test-access-key".to_string()),
            };
            assert_eq!(azure_storage_config, expected_azure_config);
        }
    }

    #[test]
    fn test_storage_s3_config_serde() {
        {
            let s3_storage_config_yaml = r#"
                endpoint: http://localhost:4566
            "#;
            let s3_storage_config: S3StorageConfig =
                serde_yaml::from_str(s3_storage_config_yaml).unwrap();

            let expected_s3_config = S3StorageConfig {
                endpoint: Some("http://localhost:4566".to_string()),
                ..Default::default()
            };
            assert_eq!(s3_storage_config, expected_s3_config);
        }
        {
            let s3_storage_config_yaml = r#"
                region: us-east-1
                endpoint: http://localhost:4566
                force_path_style_access: true
                disable_multi_object_delete_requests: true
            "#;
            let s3_storage_config: S3StorageConfig =
                serde_yaml::from_str(s3_storage_config_yaml).unwrap();

            let expected_s3_config = S3StorageConfig {
                region: Some("us-east-1".to_string()),
                endpoint: Some("http://localhost:4566".to_string()),
                force_path_style_access: true,
                disable_multi_object_delete_requests: true,
                ..Default::default()
            };
            assert_eq!(s3_storage_config, expected_s3_config);
        }
    }
}
