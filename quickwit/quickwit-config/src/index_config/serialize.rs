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

use anyhow::Context;
use quickwit_common::uri::Uri;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{
    build_doc_mapper, validate_identifier, ConfigFormat, DocMapping, IndexConfig, IndexingSettings,
    RetentionPolicy, SearchSettings,
};

/// Alias for the latest serialization format.
type IndexConfigForSerialization = IndexConfigV0_4;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "version")]
pub(super) enum VersionedIndexConfig {
    #[serde(rename = "0.4")]
    V0_4(IndexConfigV0_4),
}

impl From<VersionedIndexConfig> for IndexConfigForSerialization {
    fn from(versioned_config: VersionedIndexConfig) -> IndexConfigForSerialization {
        match versioned_config {
            VersionedIndexConfig::V0_4(v0_4) => v0_4,
        }
    }
}

/// Parses and validates an [`IndexConfig`] as supplied by a user with a given URI and config
/// content.
pub fn load_index_config_from_user_config(
    config_format: ConfigFormat,
    file_content: &[u8],
    default_index_root_uri: &Uri,
) -> anyhow::Result<IndexConfig> {
    let versioned_index_config: VersionedIndexConfig = config_format.parse(file_content)?;
    let index_config_for_serialization: IndexConfigForSerialization = versioned_index_config.into();
    index_config_for_serialization.validate_and_build(Some(default_index_root_uri))
}

impl IndexConfigForSerialization {
    fn index_uri_or_fallback_to_default(
        &self,
        default_index_root_uri_opt: Option<&Uri>,
    ) -> anyhow::Result<Uri> {
        if let Some(index_uri) = self.index_uri.as_ref() {
            return Ok(index_uri.clone());
        }
        let default_index_root_uri = default_index_root_uri_opt.context("Missing `index_uri`")?;
        let index_uri: Uri = default_index_root_uri.join(&self.index_id)
            .context("Failed to create default index URI. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")?;
        info!(
            index_id = %self.index_id,
            index_uri = %index_uri,
            "Index config does not specify `index_uri`, falling back to default value.",
        );
        Ok(index_uri)
    }

    pub fn validate_and_build(
        self,
        default_index_root_uri: Option<&Uri>,
    ) -> anyhow::Result<IndexConfig> {
        validate_identifier("Index ID", &self.index_id)?;

        let index_uri = self.index_uri_or_fallback_to_default(default_index_root_uri)?;

        if let Some(retention_policy) = &self.retention_policy {
            retention_policy.validate()?;

            if self.doc_mapping.timestamp_field.is_none() {
                anyhow::bail!(
                    "Failed to validate index config. The retention policy requires a timestamp \
                     field, but the indexing settings do not declare one."
                );
            }
        }

        // Note: this needs a deep refactoring to separate the doc mapping configuration,
        // and doc mapper implementations.
        // TODO see if we should store the byproducton the IndexConfig.
        build_doc_mapper(&self.doc_mapping, &self.search_settings)?;

        self.indexing_settings.merge_policy.validate()?;

        Ok(IndexConfig {
            index_id: self.index_id,
            index_uri,
            doc_mapping: self.doc_mapping,
            indexing_settings: self.indexing_settings,
            search_settings: self.search_settings,
            retention_policy: self.retention_policy,
        })
    }
}

impl From<IndexConfig> for VersionedIndexConfig {
    fn from(index_config: IndexConfig) -> Self {
        VersionedIndexConfig::V0_4(index_config.into())
    }
}

impl TryFrom<VersionedIndexConfig> for IndexConfig {
    type Error = anyhow::Error;

    fn try_from(versioned_index_config: VersionedIndexConfig) -> anyhow::Result<Self> {
        match versioned_index_config {
            VersionedIndexConfig::V0_4(v3) => v3.validate_and_build(None),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexConfigV0_4 {
    pub index_id: String,
    #[serde(default)]
    pub index_uri: Option<Uri>,
    pub doc_mapping: DocMapping,
    #[serde(default)]
    pub indexing_settings: IndexingSettings,
    #[serde(default)]
    pub search_settings: SearchSettings,
    #[serde(rename = "retention")]
    #[serde(default)]
    pub retention_policy: Option<RetentionPolicy>,
}

impl From<IndexConfig> for IndexConfigV0_4 {
    fn from(index_config: IndexConfig) -> Self {
        IndexConfigV0_4 {
            index_id: index_config.index_id,
            index_uri: Some(index_config.index_uri),
            doc_mapping: index_config.doc_mapping,
            indexing_settings: index_config.indexing_settings,
            search_settings: index_config.search_settings,
            retention_policy: index_config.retention_policy,
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
            .validate_and_build(None)
            .unwrap_err()
            .to_string();
        assert_eq!(
            validation_err,
            "Index config merge policy `max_merge_factor` must be superior or equal to \
             `merge_factor`."
        );
    }

    #[test]
    fn test_validate_retention_policy() {
        // Not yet invalid, but we modify it right after this.
        let mut invalid_index_config: IndexConfigForSerialization =
            minimal_index_config_for_serialization();
        invalid_index_config.retention_policy = Some(RetentionPolicy {
            retention_period: "90 days".to_string(),
            evaluation_schedule: "hourly".to_string(),
        });
        let validation_err = invalid_index_config
            .validate_and_build(None)
            .unwrap_err()
            .to_string();
        assert!(validation_err.contains("The retention policy requires a timestamp field"));
    }

    #[test]
    fn test_minimal_index_config_missing_root_uri_no_default_uri() {
        let config_yaml = r#"
            version: 0.4
            index_id: hdfs-logs
            doc_mapping: {}
        "#;
        let config_parse_result: anyhow::Result<IndexConfig> =
            ConfigFormat::Yaml.parse(config_yaml.as_bytes());
        assert!(format!("{:?}", config_parse_result.unwrap_err()).contains("Missing `index_uri`"));
    }

    #[test]
    fn test_minimal_index_config_missing_root_uri_with_default_index_root_uri() {
        let config_yaml = r#"
            version: 0.4
            index_id: hdfs-logs
            doc_mapping: {}
        "#;
        {
            let index_config: IndexConfig = load_index_config_from_user_config(
                ConfigFormat::Yaml,
                config_yaml.as_bytes(),
                // same but without the trailing slash.
                &Uri::from_well_formed("s3://mybucket"),
            )
            .unwrap();
            assert_eq!(index_config.index_uri.as_str(), "s3://mybucket/hdfs-logs");
        }
    }
}
