// Copyright (C) 2024 Quickwit, Inc.
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

use anyhow::{ensure, Context};
use quickwit_common::uri::Uri;
use quickwit_proto::types::IndexId;
use serde::{Deserialize, Serialize};
use tracing::info;

use super::validate_index_config;
use crate::{
    validate_identifier, ConfigFormat, DocMapping, IndexConfig, IndexingSettings, RetentionPolicy,
    SearchSettings,
};

/// Alias for the latest serialization format.
type IndexConfigForSerialization = IndexConfigV0_8;

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "version")]
pub(crate) enum VersionedIndexConfig {
    #[serde(rename = "0.8")]
    // Retro compatibility
    #[serde(alias = "0.7")]
    V0_8(IndexConfigV0_8),
}

impl From<VersionedIndexConfig> for IndexConfigForSerialization {
    fn from(versioned_config: VersionedIndexConfig) -> IndexConfigForSerialization {
        match versioned_config {
            VersionedIndexConfig::V0_8(v0_8) => v0_8,
        }
    }
}

/// Parses and validates an [`IndexConfig`] as supplied by a user with a given [`ConfigFormat`],
/// config content and a `default_index_root_uri`.
pub fn load_index_config_from_user_config(
    config_format: ConfigFormat,
    config_content: &[u8],
    default_index_root_uri: &Uri,
) -> anyhow::Result<IndexConfig> {
    let versioned_index_config: VersionedIndexConfig = config_format.parse(config_content)?;
    let index_config_for_serialization: IndexConfigForSerialization = versioned_index_config.into();
    index_config_for_serialization.build_and_validate(Some(default_index_root_uri))
}

/// Parses and validates an [`IndexConfig`] update.
///
/// Ensures that the new configuration is valid in itself and compared to the
/// current index config. If the new configuration omits some fields, the
/// default values will be used, not those of the current index config. The only
/// exception is the index_uri because it cannot be updated.
pub fn load_index_config_update(
    config_format: ConfigFormat,
    index_config_bytes: &[u8],
    current_index_config: &IndexConfig,
) -> anyhow::Result<IndexConfig> {
    let current_index_parent_dir = &current_index_config
        .index_uri
        .parent()
        .context("Unexpected `index_uri` format on current configuration")?;
    let new_index_config = load_index_config_from_user_config(
        config_format,
        index_config_bytes,
        current_index_parent_dir,
    )?;
    ensure!(
        current_index_config.index_id == new_index_config.index_id,
        "`index_id` in config file {} does not match updated `index_id` {}",
        current_index_config.index_id,
        new_index_config.index_id
    );
    ensure!(
        current_index_config.index_uri == new_index_config.index_uri,
        "`index_uri` cannot be updated, current value {}, new expected value {}",
        current_index_config.index_uri,
        new_index_config.index_uri
    );
    ensure!(
        current_index_config.doc_mapping == new_index_config.doc_mapping,
        "`doc_mapping` cannot be updated"
    );
    Ok(new_index_config)
}

impl IndexConfigForSerialization {
    fn index_uri_or_fallback_to_default(
        &self,
        default_index_root_uri_opt: Option<&Uri>,
    ) -> anyhow::Result<Uri> {
        if let Some(index_uri) = &self.index_uri {
            return Ok(index_uri.clone());
        }
        let default_index_root_uri = default_index_root_uri_opt.context("missing `index_uri`")?;
        let index_uri: Uri = default_index_root_uri.join(&self.index_id)
            .context("failed to create default index URI. this should never happen! please, report on https://github.com/quickwit-oss/quickwit/issues")?;
        info!(
            index_id=%self.index_id,
            index_uri=%index_uri,
            "index config does not specify `index_uri`, falling back to default value",
        );
        Ok(index_uri)
    }

    pub fn build_and_validate(
        self,
        default_index_root_uri: Option<&Uri>,
    ) -> anyhow::Result<IndexConfig> {
        validate_identifier("index", &self.index_id)?;

        let index_uri = self.index_uri_or_fallback_to_default(default_index_root_uri)?;

        let index_config = IndexConfig {
            index_id: self.index_id,
            index_uri,
            doc_mapping: self.doc_mapping,
            indexing_settings: self.indexing_settings,
            search_settings: self.search_settings,
            retention_policy_opt: self.retention_policy_opt,
            metrics_group: self.metrics_group,
        };
        validate_index_config(
            &index_config.doc_mapping,
            &index_config.indexing_settings,
            &index_config.search_settings,
            &index_config.retention_policy_opt,
        )?;
        Ok(index_config)
    }
}

impl From<IndexConfig> for VersionedIndexConfig {
    fn from(index_config: IndexConfig) -> Self {
        VersionedIndexConfig::V0_8(index_config.into())
    }
}

impl TryFrom<VersionedIndexConfig> for IndexConfig {
    type Error = anyhow::Error;

    fn try_from(versioned_index_config: VersionedIndexConfig) -> anyhow::Result<Self> {
        match versioned_index_config {
            VersionedIndexConfig::V0_8(v0_8) => v0_8.build_and_validate(None),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct IndexConfigV0_8 {
    #[schema(value_type = String)]
    pub index_id: IndexId,
    #[schema(value_type = String)]
    #[serde(default)]
    pub index_uri: Option<Uri>,
    pub doc_mapping: DocMapping,
    #[serde(default)]
    pub indexing_settings: IndexingSettings,
    #[serde(default)]
    pub search_settings: SearchSettings,
    #[serde(rename = "retention")]
    #[serde(default)]
    pub retention_policy_opt: Option<RetentionPolicy>,
    #[serde(default)]
    pub metrics_group: Option<String>,
}

impl From<IndexConfig> for IndexConfigV0_8 {
    fn from(index_config: IndexConfig) -> Self {
        IndexConfigV0_8 {
            index_id: index_config.index_id,
            index_uri: Some(index_config.index_uri),
            doc_mapping: index_config.doc_mapping,
            indexing_settings: index_config.indexing_settings,
            search_settings: index_config.search_settings,
            retention_policy_opt: index_config.retention_policy_opt,
            metrics_group: index_config.metrics_group,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::merge_policy_config::{MergePolicyConfig, StableLogMergePolicyConfig};

    fn minimal_index_config_for_serialization() -> IndexConfigForSerialization {
        serde_yaml::from_str(
            r#"
            index_id: hdfs-logs
            index_uri: s3://quickwit-indexes/hdfs-logs

            doc_mapping:
                field_mappings:
                    - name: body
                      type: text
                      tokenizer: default
                      record: position

            search_settings:
                default_search_fields: [body]
        "#,
        )
        .unwrap()
    }

    #[test]
    fn test_validate_invalid_merge_policy() {
        // Not yet invalid, but we modify it right after this.
        let mut invalid_index_config: IndexConfigForSerialization =
            minimal_index_config_for_serialization();
        // Set a max merge factor to an inconsistent value.
        let mut stable_log_merge_policy_config = StableLogMergePolicyConfig::default();
        stable_log_merge_policy_config.max_merge_factor =
            stable_log_merge_policy_config.merge_factor - 1;
        invalid_index_config.indexing_settings.merge_policy =
            MergePolicyConfig::StableLog(stable_log_merge_policy_config);
        let validation_err = invalid_index_config
            .build_and_validate(None)
            .unwrap_err()
            .to_string();
        assert_eq!(
            validation_err,
            "index config merge policy `max_merge_factor` must be superior or equal to \
             `merge_factor`"
        );
    }

    #[test]
    fn test_validate_retention_policy() {
        // Not yet invalid, but we modify it right after this.
        let mut invalid_index_config: IndexConfigForSerialization =
            minimal_index_config_for_serialization();
        invalid_index_config.retention_policy_opt = Some(RetentionPolicy {
            retention_period: "90 days".to_string(),
            evaluation_schedule: "hourly".to_string(),
        });
        let validation_err = invalid_index_config
            .build_and_validate(None)
            .unwrap_err()
            .to_string();
        assert!(validation_err.contains("retention policy requires a timestamp field"));
    }

    #[test]
    fn test_minimal_index_config_missing_root_uri_no_default_uri() {
        let config_yaml = r#"
            version: 0.8
            index_id: hdfs-logs
            doc_mapping: {}
        "#;
        let config_parse_result: anyhow::Result<IndexConfig> =
            ConfigFormat::Yaml.parse(config_yaml.as_bytes());
        assert!(format!("{:?}", config_parse_result.unwrap_err()).contains("missing `index_uri`"));
    }

    #[test]
    fn test_minimal_index_config_missing_root_uri_with_default_index_root_uri() {
        let config_yaml = r#"
            version: 0.8
            index_id: hdfs-logs
            doc_mapping: {}
        "#;
        {
            let index_config: IndexConfig = load_index_config_from_user_config(
                ConfigFormat::Yaml,
                config_yaml.as_bytes(),
                // same but without the trailing slash.
                &Uri::for_test("s3://mybucket"),
            )
            .unwrap();
            assert_eq!(index_config.index_uri.as_str(), "s3://mybucket/hdfs-logs");
        }
    }

    #[test]
    fn test_update_index_root_uri() {
        let original_config_yaml = r#"
            version: 0.8
            index_id: hdfs-logs
            doc_mapping: {}
        "#;
        let original_config: IndexConfig = load_index_config_from_user_config(
            ConfigFormat::Yaml,
            original_config_yaml.as_bytes(),
            &Uri::for_test("s3://mybucket"),
        )
        .unwrap();
        {
            // use default in update
            let updated_config_yaml = r#"
                version: 0.8
                index_id: hdfs-logs
                doc_mapping: {}
            "#;
            let updated_config = load_index_config_update(
                ConfigFormat::Yaml,
                updated_config_yaml.as_bytes(),
                &original_config,
            )
            .unwrap();
            assert_eq!(updated_config.index_uri.as_str(), "s3://mybucket/hdfs-logs");
        }
        {
            // use the current index_uri explicitely
            let updated_config_yaml = r#"
                version: 0.8
                index_id: hdfs-logs
                index_uri: s3://mybucket/hdfs-logs
                doc_mapping: {}
            "#;
            let updated_config = load_index_config_update(
                ConfigFormat::Yaml,
                updated_config_yaml.as_bytes(),
                &original_config,
            )
            .unwrap();
            assert_eq!(updated_config.index_uri.as_str(), "s3://mybucket/hdfs-logs");
        }
        {
            // try using a different index_uri
            let updated_config_yaml = r#"
                version: 0.8
                index_id: hdfs-logs
                index_uri: s3://mybucket/new-directory/
                doc_mapping: {}
            "#;
            let load_error = load_index_config_update(
                ConfigFormat::Yaml,
                updated_config_yaml.as_bytes(),
                &original_config,
            )
            .unwrap_err();
            assert!(format!("{:?}", load_error).contains("`index_uri` cannot be updated"));
        }
    }

    #[test]
    fn test_update_reset_defaults() {
        let original_config_yaml = r#"
            version: 0.8
            index_id: hdfs-logs
            doc_mapping:
                field_mappings:
                    - name: timestamp
                      type: datetime
                      fast: true
                timestamp_field: timestamp

            search_settings:
                default_search_fields: [body]

            indexing_settings:
                commit_timeout_secs: 10

            retention:
                period: 90 days
                schedule: daily
        "#;
        let original_config: IndexConfig = load_index_config_from_user_config(
            ConfigFormat::Yaml,
            original_config_yaml.as_bytes(),
            &Uri::for_test("s3://mybucket"),
        )
        .unwrap();

        let updated_config_yaml = r#"
            version: 0.8
            index_id: hdfs-logs
            doc_mapping:
                field_mappings:
                    - name: timestamp
                      type: datetime
                      fast: true
                timestamp_field: timestamp
        "#;
        let updated_config = load_index_config_update(
            ConfigFormat::Yaml,
            updated_config_yaml.as_bytes(),
            &original_config,
        )
        .unwrap();
        assert_eq!(
            updated_config.search_settings.default_search_fields,
            Vec::<String>::default(),
        );
        assert_eq!(
            updated_config.indexing_settings.commit_timeout_secs,
            IndexingSettings::default_commit_timeout_secs()
        );
        assert_eq!(updated_config.retention_policy_opt, None);
    }

    #[test]
    fn test_update_doc_mappings() {
        let original_config_yaml = r#"
            version: 0.8
            index_id: hdfs-logs
            doc_mapping: {}
        "#;
        let original_config: IndexConfig = load_index_config_from_user_config(
            ConfigFormat::Yaml,
            original_config_yaml.as_bytes(),
            &Uri::for_test("s3://mybucket"),
        )
        .unwrap();

        let updated_config_yaml = r#"
            version: 0.8
            index_id: hdfs-logs
            doc_mapping:
                field_mappings:
                    - name: body
                      type: text
                      tokenizer: default
                      record: position
        "#;
        let load_error = load_index_config_update(
            ConfigFormat::Yaml,
            updated_config_yaml.as_bytes(),
            &original_config,
        )
        .unwrap_err();
        assert!(format!("{:?}", load_error).contains("`doc_mapping` cannot be updated"));
    }
}
