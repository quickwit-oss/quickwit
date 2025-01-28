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

use std::collections::HashSet;

use anyhow::{ensure, Context};
use quickwit_common::uri::Uri;
use quickwit_doc_mapper::DocMapperBuilder;
use quickwit_proto::types::{DocMappingUid, IndexId};
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
    // The two versions use the same format but for v0.8 and below, we need to set the
    // `doc_mapping_uid` to the nil value upon deserialization.
    #[serde(rename = "0.9")]
    V0_9(IndexConfigV0_8),
    // Retro compatibility
    #[serde(rename = "0.8")]
    #[serde(alias = "0.7")]
    V0_8(IndexConfigV0_8),
}

impl From<VersionedIndexConfig> for IndexConfigForSerialization {
    fn from(versioned_config: VersionedIndexConfig) -> IndexConfigForSerialization {
        match versioned_config {
            VersionedIndexConfig::V0_8(v0_8) => v0_8,
            VersionedIndexConfig::V0_9(v0_8) => v0_8,
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
        .expect("index URI should have a parent");
    let mut new_index_config = load_index_config_from_user_config(
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

    // verify the new mapping is coherent
    let doc_mapper_builder = DocMapperBuilder {
        doc_mapping: new_index_config.doc_mapping.clone(),
        default_search_fields: new_index_config
            .search_settings
            .default_search_fields
            .clone(),
        legacy_type_tag: None,
    };
    doc_mapper_builder
        .try_build()
        .context("invalid mapping update")?;

    {
        let new_mapping_uid = new_index_config.doc_mapping.doc_mapping_uid;
        // we verify whether they are equal ignoring the mapping uid as it is generated at random:
        // we don't want to record a mapping change when nothing really happened.
        new_index_config.doc_mapping.doc_mapping_uid =
            current_index_config.doc_mapping.doc_mapping_uid;
        if new_index_config.doc_mapping != current_index_config.doc_mapping {
            new_index_config.doc_mapping.doc_mapping_uid = new_mapping_uid;
            ensure!(
                current_index_config.doc_mapping.doc_mapping_uid
                    != new_index_config.doc_mapping.doc_mapping_uid,
                "`doc_mapping_doc_mapping_uid` must change when the doc mapping is updated",
            );
            ensure!(
                current_index_config.doc_mapping.timestamp_field
                    == new_index_config.doc_mapping.timestamp_field,
                "`doc_mapping.timestamp_field` cannot be updated, current value {}, new expected \
                 value {}",
                current_index_config
                    .doc_mapping
                    .timestamp_field
                    .as_deref()
                    .unwrap_or("<none>"),
                new_index_config
                    .doc_mapping
                    .timestamp_field
                    .as_deref()
                    .unwrap_or("<none>"),
            );
            // TODO: i'm not sure this is necessary, we can relax this requirement once we know
            // for sure
            let current_tokenizers: HashSet<_> =
                current_index_config.doc_mapping.tokenizers.iter().collect();
            let new_tokenizers: HashSet<_> =
                new_index_config.doc_mapping.tokenizers.iter().collect();
            ensure!(
                new_tokenizers.is_superset(&current_tokenizers),
                "`.doc_mapping.tokenizers` must be a superset of previously available tokenizers"
            );
        } else {
            // the docmapping is unchanged, keep the old uid
        }
    }

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
        VersionedIndexConfig::V0_9(index_config.into())
    }
}

impl TryFrom<VersionedIndexConfig> for IndexConfig {
    type Error = anyhow::Error;

    fn try_from(versioned_index_config: VersionedIndexConfig) -> anyhow::Result<Self> {
        match versioned_index_config {
            VersionedIndexConfig::V0_8(mut v0_8) => {
                // Override the randomly generated doc mapping UID with the nil value.
                v0_8.doc_mapping.doc_mapping_uid = DocMappingUid::default();
                v0_8.build_and_validate(None)
            }
            VersionedIndexConfig::V0_9(v0_8) => v0_8.build_and_validate(None),
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
            jitter_secs: None,
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
    fn test_default_dynamic_mapping_matches_docs() {
        let minimal_config_yaml = r#"
            version: 0.8
            index_id: hdfs-logs
            doc_mapping:
              doc_mapping_uid: 00000000000000000000000000
        "#;
        let docs_config_yaml = r#"
            version: 0.8
            index_id: hdfs-logs
            doc_mapping:
                doc_mapping_uid: 00000000000000000000000000
                mode: dynamic
                dynamic_mapping:
                    indexed: true
                    stored: true
                    tokenizer: default
                    record: basic
                    expand_dots: true
                    fast: true
        "#;
        {
            let minimal_index_config: IndexConfig = load_index_config_from_user_config(
                ConfigFormat::Yaml,
                minimal_config_yaml.as_bytes(),
                &Uri::for_test("s3://mybucket"),
            )
            .unwrap();
            let docs_index_config: IndexConfig = load_index_config_from_user_config(
                ConfigFormat::Yaml,
                docs_config_yaml.as_bytes(),
                &Uri::for_test("s3://mybucket"),
            )
            .unwrap();
            assert_eq!(minimal_index_config, docs_index_config);
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
            // use the current index_uri explicitly
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
        let updated_config = load_index_config_update(
            ConfigFormat::Yaml,
            updated_config_yaml.as_bytes(),
            &original_config,
        )
        .unwrap();
        assert_eq!(updated_config.doc_mapping.field_mappings.len(), 1);
    }

    #[test]
    fn test_update_doc_mappings_failing_cases() {
        let original_config_yaml = r#"
            version: 0.8
            index_id: hdfs-logs
            doc_mapping:
                mode: lenient
                doc_mapping_uid: 00000000000000000000000000
                timestamp_field: timestamp
                field_mappings:
                    - name: timestamp
                      type: datetime
                      fast: true
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
                mode: lenient
                doc_mapping_uid: 00000000000000000000000000
                timestamp_field: timestamp
                field_mappings:
                    - name: timestamp
                      type: datetime
                      fast: true
                    - name: body
                      type: text
                      tokenizer: default
                      record: position
        "#;
        load_index_config_update(
            ConfigFormat::Yaml,
            updated_config_yaml.as_bytes(),
            &original_config,
        )
        .expect_err("mapping changed but uid fixed should error");

        let updated_config_yaml = r#"
            version: 0.8
            index_id: hdfs-logs
            doc_mapping:
                mode: lenient
                field_mappings:
                    - name: timestamp
                      type: datetime
                      fast: true
        "#;
        load_index_config_update(
            ConfigFormat::Yaml,
            updated_config_yaml.as_bytes(),
            &original_config,
        )
        .expect_err("timestamp field removed should error");

        let updated_config_yaml = r#"
            version: 0.8
            index_id: hdfs-logs
            doc_mapping:
                mode: lenient
                timestamp_field: timestamp
                field_mappings:
                    - name: body
                      type: text
                      tokenizer: default
                      record: position
        "#;
        load_index_config_update(
            ConfigFormat::Yaml,
            updated_config_yaml.as_bytes(),
            &original_config,
        )
        .expect_err("field required for timestamp is absent");

        let updated_config_yaml = r#"
            version: 0.8
            index_id: hdfs-logs
            doc_mapping:
                mode: lenient
                timestamp_field: timestamp
                field_mappings:
                    - name: timestamp
                      type: datetime
                      fast: true
            search_settings:
              default_search_fields: ["i_dont_exist"]
        "#;
        load_index_config_update(
            ConfigFormat::Yaml,
            updated_config_yaml.as_bytes(),
            &original_config,
        )
        .expect_err("field required for default search is absent");
    }
}
