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

pub(crate) mod serialize;

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use byte_unit::Byte;
use chrono::Utc;
use cron::Schedule;
use humantime::parse_duration;
use quickwit_common::uri::Uri;
use quickwit_doc_mapper::{
    DefaultDocMapper, DefaultDocMapperBuilder, DocMapper, FieldMappingEntry, Mode, ModeType,
    QuickwitJsonOptions, TokenizerEntry,
};
use quickwit_proto::IndexId;
use serde::{Deserialize, Serialize};
pub use serialize::load_index_config_from_user_config;

use crate::index_config::serialize::VersionedIndexConfig;
use crate::merge_policy_config::{MergePolicyConfig, StableLogMergePolicyConfig};
use crate::TestableForRegression;

// Note(fmassot): `DocMapping` is a struct only used for
// serialization/deserialization of `DocMapper` parameters.
// This is partly a duplicate of the `DefaultDocMapper` and
// can be viewed as a temporary hack for 0.2 release before
// refactoring.
#[quickwit_macros::serde_multikey]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DocMapping {
    #[serde(default)]
    #[schema(value_type = Vec<FieldMappingEntryForSerialization>)]
    /// The mapping of the index schema fields.
    ///
    /// This defines the name, type and other information about the field(s).
    ///
    /// Properties are determined by the specified type, for more information
    /// please see: <https://quickwit.io/docs/configuration/index-config#field-types>
    pub field_mappings: Vec<FieldMappingEntry>,
    #[schema(value_type = Vec<String>)]
    #[serde(default)]
    pub tag_fields: BTreeSet<String>,
    #[serde(default)]
    pub store_source: bool,
    #[serde(default)]
    pub index_field_presence: bool,
    #[serde(default)]
    pub timestamp_field: Option<String>,
    #[serde_multikey(
        deserializer = Mode::from_parts,
        serializer = Mode::into_parts,
        fields = (
            #[serde(default)]
            mode: ModeType,
            #[serde(skip_serializing_if = "Option::is_none")]
            dynamic_mapping: Option<QuickwitJsonOptions>
        ),
    )]
    pub mode: Mode,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_key: Option<String>,
    #[schema(value_type = u32)]
    #[serde(default = "DefaultDocMapper::default_max_num_partitions")]
    pub max_num_partitions: NonZeroU32,
    #[serde(default)]
    pub tokenizers: Vec<TokenizerEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct IndexingResources {
    #[schema(value_type = String, default = "2 GB")]
    #[serde(default = "IndexingResources::default_heap_size")]
    pub heap_size: Byte,
    /// Sets the maximum write IO throughput in bytes/sec for the merge and delete pipelines.
    /// The IO limit is applied both to the downloader and to the merge executor.
    /// On hardware where IO is limited, this parameter can help limiting the impact of
    /// merges/deletes on indexing.
    #[schema(value_type = String)]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_merge_write_throughput: Option<Byte>,
}

impl PartialEq for IndexingResources {
    fn eq(&self, other: &Self) -> bool {
        self.heap_size == other.heap_size
    }
}

impl IndexingResources {
    fn default_heap_size() -> Byte {
        Byte::from_bytes(2_000_000_000) // 2GB
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> Self {
        Self {
            heap_size: Byte::from_bytes(20_000_000), // 20MB
            ..Default::default()
        }
    }
}

impl Default for IndexingResources {
    fn default() -> Self {
        Self {
            heap_size: Self::default_heap_size(),
            max_merge_write_throughput: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct IndexingSettings {
    #[schema(default = 60)]
    #[serde(default = "IndexingSettings::default_commit_timeout_secs")]
    pub commit_timeout_secs: usize,
    #[schema(default = 8)]
    #[serde(default = "IndexingSettings::default_docstore_compression_level")]
    pub docstore_compression_level: i32,
    #[schema(default = 1_000_000)]
    #[serde(default = "IndexingSettings::default_docstore_blocksize")]
    pub docstore_blocksize: usize,
    /// The merge policy aims to eventually produce mature splits that have a larger size but
    /// are within close range of `split_num_docs_target`.
    ///
    /// In other words, splits that contain a number of documents greater than or equal to
    /// `split_num_docs_target` are considered mature and never merged.
    #[serde(default = "IndexingSettings::default_split_num_docs_target")]
    pub split_num_docs_target: usize,
    #[serde(default)]
    pub merge_policy: MergePolicyConfig,
    #[serde(default)]
    pub resources: IndexingResources,
}

impl IndexingSettings {
    pub fn commit_timeout(&self) -> Duration {
        Duration::from_secs(self.commit_timeout_secs as u64)
    }

    fn default_commit_timeout_secs() -> usize {
        60
    }

    pub fn default_docstore_blocksize() -> usize {
        1_000_000
    }

    pub fn default_docstore_compression_level() -> i32 {
        8
    }

    pub fn default_split_num_docs_target() -> usize {
        10_000_000
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> Self {
        Self {
            resources: IndexingResources::for_test(),
            ..Default::default()
        }
    }
}

impl Default for IndexingSettings {
    fn default() -> Self {
        Self {
            commit_timeout_secs: Self::default_commit_timeout_secs(),
            docstore_blocksize: Self::default_docstore_blocksize(),
            docstore_compression_level: Self::default_docstore_compression_level(),
            split_num_docs_target: Self::default_split_num_docs_target(),
            merge_policy: MergePolicyConfig::default(),
            resources: IndexingResources::default(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchSettings {
    #[serde(default)]
    pub default_search_fields: Vec<String>,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RetentionPolicy {
    /// Duration of time for which the splits should be retained, expressed in a human-friendly way
    /// (`1 hour`, `3 days`, `a week`, ...).
    #[serde(rename = "period")]
    retention_period: String,

    /// Defines the frequency at which the retention policy is evaluated and applied, expressed in
    /// a human-friendly way (`hourly`, `daily`, ...) or as a cron expression (`0 0 * * * *`,
    /// `0 0 0 * * *`).
    #[serde(default = "RetentionPolicy::default_schedule")]
    #[serde(rename = "schedule")]
    evaluation_schedule: String,
}

impl RetentionPolicy {
    pub fn new(retention_period: String, evaluation_schedule: String) -> Self {
        Self {
            retention_period,
            evaluation_schedule,
        }
    }

    fn default_schedule() -> String {
        "hourly".to_string()
    }

    pub fn retention_period(&self) -> anyhow::Result<Duration> {
        parse_duration(&self.retention_period).with_context(|| {
            format!(
                "failed to parse retention period `{}`",
                self.retention_period
            )
        })
    }

    pub fn evaluation_schedule(&self) -> anyhow::Result<Schedule> {
        let evaluation_schedule = prepend_at_char(&self.evaluation_schedule);

        Schedule::from_str(&evaluation_schedule).with_context(|| {
            format!(
                "failed to parse retention evaluation schedule `{}`",
                self.evaluation_schedule
            )
        })
    }

    pub fn duration_until_next_evaluation(&self) -> anyhow::Result<Duration> {
        let schedule = self.evaluation_schedule()?;
        let future_date = schedule
            .upcoming(Utc)
            .next()
            .expect("Failed to obtain next evaluation date.");
        let duration = (future_date - Utc::now())
            .to_std()
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
        Ok(duration)
    }

    fn validate(&self) -> anyhow::Result<()> {
        self.retention_period()?;
        self.evaluation_schedule()?;
        Ok(())
    }
}

/// Prepends an `@` char at the start of the cron expression if necessary:
/// `hourly` -> `@hourly`
fn prepend_at_char(schedule: &str) -> String {
    let trimmed_schedule = schedule.trim();

    if !trimmed_schedule.is_empty()
        && !trimmed_schedule.starts_with('@')
        && trimmed_schedule.chars().all(|ch| ch.is_ascii_alphabetic())
    {
        return format!("@{trimmed_schedule}");
    }
    trimmed_schedule.to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[serde(into = "VersionedIndexConfig")]
#[serde(try_from = "VersionedIndexConfig")]
pub struct IndexConfig {
    pub index_id: IndexId,
    pub index_uri: Uri,
    pub doc_mapping: DocMapping,
    pub indexing_settings: IndexingSettings,
    pub search_settings: SearchSettings,
    pub retention_policy: Option<RetentionPolicy>,
}

impl IndexConfig {
    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(index_id: &str, index_uri: &str) -> Self {
        let index_uri = Uri::from_str(index_uri).unwrap();
        let doc_mapping_json = r#"{
            "mode": "lenient",
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "datetime",
                    "fast": true
                },
                {
                    "name": "body",
                    "type": "text",
                    "stored": true
                },
                {
                    "name": "response_date",
                    "type": "datetime",
                    "fast": true
                },
                {
                    "name": "response_time",
                    "type": "f64",
                    "fast": true
                },
                {
                    "name": "response_payload",
                    "type": "bytes",
                    "fast": true
                },
                {
                    "name": "owner",
                    "type": "text",
                    "tokenizer": "raw"
                },
                {
                    "name": "attributes",
                    "type": "object",
                    "field_mappings": [
                        {
                            "name": "tags",
                            "type": "array<i64>"
                        },
                        {
                            "name": "server",
                            "type": "text"
                        },
                        {
                            "name": "server.status",
                            "type": "array<text>"
                        },
                        {
                            "name": "server.payload",
                            "type": "array<bytes>"
                        }
                    ]
                }
            ],
            "timestamp_field": "timestamp",
            "tag_fields": ["owner"],
            "store_source": true
        }"#;
        let doc_mapping = serde_json::from_str(doc_mapping_json).unwrap();
        let indexing_settings = IndexingSettings {
            resources: IndexingResources::for_test(),
            ..Default::default()
        };
        let search_settings = SearchSettings {
            default_search_fields: vec![
                "body".to_string(),
                r#"attributes.server"#.to_string(),
                r"attributes.server\.status".to_string(),
            ],
        };
        IndexConfig {
            index_id: index_id.to_string(),
            index_uri,
            doc_mapping,
            indexing_settings,
            search_settings,
            retention_policy: Default::default(),
        }
    }
}

impl TestableForRegression for IndexConfig {
    fn sample_for_regression() -> Self {
        let tenant_id_mapping = serde_json::from_str(
            r#"{
                "name": "tenant_id",
                "type": "u64",
                "fast": true
        }"#,
        )
        .unwrap();
        let timestamp_mapping = serde_json::from_str(
            r#"{
                "name": "timestamp",
                "type": "datetime",
                "fast": true
        }"#,
        )
        .unwrap();
        let log_level_mapping = serde_json::from_str(
            r#"{
                "name": "log_level",
                "type": "text",
                "tokenizer": "raw"
        }"#,
        )
        .unwrap();
        let message_mapping = serde_json::from_str(
            r#"{
                "name": "message",
                "type": "text",
                "record": "position",
                "tokenizer": "default"
        }"#,
        )
        .unwrap();
        let tokenizer = serde_json::from_str(
            r#"{
                "name": "custom_tokenizer",
                "type": "regex",
                "pattern": "[^\\p{L}\\p{N}]+"
            }"#,
        )
        .unwrap();
        let doc_mapping = DocMapping {
            index_field_presence: true,
            field_mappings: vec![
                tenant_id_mapping,
                timestamp_mapping,
                log_level_mapping,
                message_mapping,
            ],
            tag_fields: ["tenant_id", "log_level"]
                .into_iter()
                .map(|tag_field| tag_field.to_string())
                .collect::<BTreeSet<String>>(),
            store_source: true,
            mode: Mode::default(),
            partition_key: Some("tenant_id".to_string()),
            max_num_partitions: NonZeroU32::new(100).unwrap(),
            timestamp_field: Some("timestamp".to_string()),
            tokenizers: vec![tokenizer],
        };
        let retention_policy = Some(RetentionPolicy::new(
            "90 days".to_string(),
            "daily".to_string(),
        ));
        let stable_log_config = StableLogMergePolicyConfig {
            merge_factor: 9,
            max_merge_factor: 11,
            ..Default::default()
        };
        let merge_policy = MergePolicyConfig::StableLog(stable_log_config);
        let indexing_resources = IndexingResources {
            heap_size: Byte::from_bytes(3),
            ..Default::default()
        };
        let indexing_settings = IndexingSettings {
            commit_timeout_secs: 301,
            split_num_docs_target: 10_000_001,
            merge_policy,
            resources: indexing_resources,
            ..Default::default()
        };
        let search_settings = SearchSettings {
            default_search_fields: vec!["message".to_string()],
        };
        IndexConfig {
            index_id: "my-index".to_string(),
            index_uri: Uri::from_well_formed("s3://quickwit-indexes/my-index"),
            doc_mapping,
            indexing_settings,
            retention_policy,
            search_settings,
        }
    }

    fn test_equality(&self, other: &Self) {
        assert_eq!(self.index_id, other.index_id);
        assert_eq!(self.index_uri, other.index_uri);
        assert_eq!(
            self.doc_mapping
                .field_mappings
                .iter()
                .map(|field_mapping| &field_mapping.name)
                .collect::<Vec<_>>(),
            other
                .doc_mapping
                .field_mappings
                .iter()
                .map(|field_mapping| &field_mapping.name)
                .collect::<Vec<_>>(),
        );
        assert_eq!(self.doc_mapping.tag_fields, other.doc_mapping.tag_fields,);
        assert_eq!(
            self.doc_mapping.store_source,
            other.doc_mapping.store_source,
        );
        assert_eq!(self.indexing_settings, other.indexing_settings);
        assert_eq!(self.search_settings, other.search_settings);
    }
}

/// Builds and returns the doc mapper associated with index.
pub fn build_doc_mapper(
    doc_mapping: &DocMapping,
    search_settings: &SearchSettings,
) -> anyhow::Result<Arc<dyn DocMapper>> {
    let builder = DefaultDocMapperBuilder {
        store_source: doc_mapping.store_source,
        index_field_presence: doc_mapping.index_field_presence,
        default_search_fields: search_settings.default_search_fields.clone(),
        timestamp_field: doc_mapping.timestamp_field.clone(),
        field_mappings: doc_mapping.field_mappings.clone(),
        tag_fields: doc_mapping.tag_fields.iter().cloned().collect(),
        mode: doc_mapping.mode.clone(),
        partition_key: doc_mapping.partition_key.clone(),
        max_num_partitions: doc_mapping.max_num_partitions,
        tokenizers: doc_mapping.tokenizers.clone(),
    };
    Ok(Arc::new(builder.try_build()?))
}

#[cfg(test)]
mod tests {

    use cron::TimeUnitSpec;

    use super::*;
    use crate::merge_policy_config::MergePolicyConfig;
    use crate::ConfigFormat;

    fn get_index_config_filepath(index_config_filename: &str) -> String {
        format!(
            "{}/resources/tests/index_config/{}",
            env!("CARGO_MANIFEST_DIR"),
            index_config_filename
        )
    }

    #[track_caller]
    fn test_index_config_parse_aux(config_format: ConfigFormat) {
        let index_config_filepath =
            get_index_config_filepath(&format!("hdfs-logs.{config_format:?}").to_lowercase());
        let file = std::fs::read_to_string(index_config_filepath).unwrap();
        let index_config = load_index_config_from_user_config(
            config_format,
            file.as_bytes(),
            &Uri::from_well_formed("s3://defaultbucket/"),
        )
        .unwrap();
        assert_eq!(index_config.doc_mapping.tokenizers.len(), 1);
        assert_eq!(index_config.doc_mapping.tokenizers[0].name, "service_regex");
        assert_eq!(index_config.doc_mapping.field_mappings.len(), 5);
        assert_eq!(index_config.doc_mapping.field_mappings[0].name, "tenant_id");
        assert_eq!(index_config.doc_mapping.field_mappings[1].name, "timestamp");
        assert_eq!(
            index_config.doc_mapping.field_mappings[2].name,
            "severity_text"
        );
        assert_eq!(index_config.doc_mapping.field_mappings[3].name, "body");
        assert_eq!(index_config.doc_mapping.field_mappings[4].name, "resource");

        assert_eq!(
            index_config
                .doc_mapping
                .tag_fields
                .into_iter()
                .collect::<Vec<String>>(),
            vec!["tenant_id".to_string()]
        );
        let expected_retention_policy = RetentionPolicy {
            retention_period: "90 days".to_string(),
            evaluation_schedule: "daily".to_string(),
        };
        assert_eq!(
            index_config.retention_policy.unwrap(),
            expected_retention_policy
        );
        assert!(index_config.doc_mapping.store_source);

        assert_eq!(
            index_config.doc_mapping.timestamp_field.unwrap(),
            "timestamp"
        );
        assert_eq!(index_config.indexing_settings.commit_timeout_secs, 61);
        assert_eq!(
            index_config.indexing_settings.merge_policy,
            MergePolicyConfig::StableLog(StableLogMergePolicyConfig {
                merge_factor: 9,
                max_merge_factor: 11,
                maturation_period: Duration::from_secs(48 * 3600),
                ..Default::default()
            })
        );
        assert_eq!(
            index_config.indexing_settings.resources,
            IndexingResources {
                heap_size: Byte::from_bytes(3_000_000_000),
                ..Default::default()
            }
        );
        assert_eq!(
            index_config.search_settings,
            SearchSettings {
                default_search_fields: vec!["severity_text".to_string(), "body".to_string()],
            }
        );
    }

    #[test]
    fn test_index_config_from_json() {
        test_index_config_parse_aux(ConfigFormat::Json);
    }

    #[test]
    fn test_index_config_from_toml() {
        test_index_config_parse_aux(ConfigFormat::Toml);
    }

    #[test]
    fn test_index_config_from_yaml() {
        test_index_config_parse_aux(ConfigFormat::Yaml);
    }

    #[test]
    fn test_index_config_default_values() {
        let default_index_root_uri = Uri::from_well_formed("s3://defaultbucket/");
        {
            let index_config_filepath = get_index_config_filepath("minimal-hdfs-logs.yaml");
            let file_content = std::fs::read_to_string(index_config_filepath).unwrap();
            let index_config = load_index_config_from_user_config(
                ConfigFormat::Yaml,
                file_content.as_bytes(),
                &default_index_root_uri,
            )
            .unwrap();

            assert_eq!(index_config.index_id, "hdfs-logs");
            assert_eq!(index_config.index_uri, "s3://quickwit-indexes/hdfs-logs");
            assert_eq!(index_config.doc_mapping.field_mappings.len(), 1);
            assert_eq!(index_config.doc_mapping.field_mappings[0].name, "body");
            assert!(!index_config.doc_mapping.store_source);
            assert_eq!(index_config.indexing_settings, IndexingSettings::default());
            assert_eq!(
                index_config.search_settings,
                SearchSettings {
                    default_search_fields: vec!["body".to_string()],
                }
            );
        }
        {
            let index_config_filepath = get_index_config_filepath("partial-hdfs-logs.yaml");
            let file_content = std::fs::read_to_string(index_config_filepath).unwrap();
            let index_config = load_index_config_from_user_config(
                ConfigFormat::Yaml,
                file_content.as_bytes(),
                &default_index_root_uri,
            )
            .unwrap();

            assert_eq!(index_config.index_id, "hdfs-logs");
            assert_eq!(index_config.index_uri, "s3://quickwit-indexes/hdfs-logs");
            assert_eq!(index_config.doc_mapping.field_mappings.len(), 2);
            assert_eq!(index_config.doc_mapping.field_mappings[0].name, "body");
            assert_eq!(index_config.doc_mapping.field_mappings[1].name, "timestamp");
            assert!(!index_config.doc_mapping.store_source);
            assert_eq!(
                index_config.indexing_settings,
                IndexingSettings {
                    commit_timeout_secs: 42,
                    merge_policy: MergePolicyConfig::default(),
                    resources: IndexingResources {
                        ..Default::default()
                    },
                    ..Default::default()
                }
            );
            assert_eq!(
                index_config.search_settings,
                SearchSettings {
                    default_search_fields: vec!["body".to_string()],
                }
            );
        }
    }

    #[test]
    #[should_panic(expected = "empty URI")]
    fn test_config_validates_uris() {
        let config_yaml = r#"
            version: 0.6
            index_id: hdfs-logs
            index_uri: ''
            doc_mapping: {}
        "#;
        serde_yaml::from_str::<IndexConfig>(config_yaml).unwrap();
    }

    #[test]
    fn test_minimal_index_config_default_dynamic() {
        let config_yaml = r#"
            version: 0.6
            index_id: hdfs-logs
            index_uri: "s3://my-index"
            doc_mapping: {}
        "#;
        let minimal_config: IndexConfig = load_index_config_from_user_config(
            ConfigFormat::Yaml,
            config_yaml.as_bytes(),
            &Uri::from_well_formed("s3://my-index"),
        )
        .unwrap();
        assert_eq!(
            minimal_config.doc_mapping.mode.mode_type(),
            ModeType::Dynamic
        );
    }

    #[test]
    fn test_index_config_with_malformed_maturation_duration() {
        let config_yaml = r#"
            version: 0.6
            index_id: hdfs-logs
            index_uri: "s3://my-index"
            doc_mapping: {}
            indexing_settings:
              merge_policy:
                type: limit_merge
                maturation_period: x
        "#;
        let parsing_config_error = load_index_config_from_user_config(
            ConfigFormat::Yaml,
            config_yaml.as_bytes(),
            &Uri::from_well_formed("s3://my-index"),
        )
        .unwrap_err();
        println!("{parsing_config_error:?}");
        assert!(parsing_config_error
            .root_cause()
            .to_string()
            .contains("failed to parse human-readable duration `x`"));
    }

    #[test]
    fn test_retention_policy_serialization() {
        let retention_policy = RetentionPolicy {
            retention_period: "90 days".to_string(),
            evaluation_schedule: "hourly".to_string(),
        };
        let retention_policy_yaml = serde_yaml::to_string(&retention_policy).unwrap();
        assert_eq!(
            serde_yaml::from_str::<RetentionPolicy>(&retention_policy_yaml).unwrap(),
            retention_policy,
        );
    }

    #[test]
    fn test_retention_policy_deserialization() {
        {
            let retention_policy_yaml = r#"
            period: 90 days
        "#;
            let retention_policy =
                serde_yaml::from_str::<RetentionPolicy>(retention_policy_yaml).unwrap();

            let expected_retention_policy = RetentionPolicy {
                retention_period: "90 days".to_string(),
                evaluation_schedule: "hourly".to_string(),
            };
            assert_eq!(retention_policy, expected_retention_policy);
        }
        {
            let retention_policy_yaml = r#"
            period: 90 days
            schedule: daily
        "#;
            let retention_policy =
                serde_yaml::from_str::<RetentionPolicy>(retention_policy_yaml).unwrap();

            let expected_retention_policy = RetentionPolicy {
                retention_period: "90 days".to_string(),
                evaluation_schedule: "daily".to_string(),
            };
            assert_eq!(retention_policy, expected_retention_policy);
        }
    }

    #[test]
    fn test_parse_retention_policy_period() {
        {
            let retention_policy = RetentionPolicy {
                retention_period: "1 hour".to_string(),
                evaluation_schedule: "hourly".to_string(),
            };
            assert_eq!(
                retention_policy.retention_period().unwrap(),
                Duration::from_secs(3600)
            );
            {
                let retention_policy = RetentionPolicy {
                    retention_period: "foo".to_string(),
                    evaluation_schedule: "hourly".to_string(),
                };
                assert_eq!(
                    retention_policy.retention_period().unwrap_err().to_string(),
                    "failed to parse retention period `foo`"
                );
            }
        }
    }

    #[test]
    fn test_prepend_at_char() {
        assert_eq!(prepend_at_char(""), "");
        assert_eq!(prepend_at_char("* * 0 0 0"), "* * 0 0 0");
        assert_eq!(prepend_at_char("hourly"), "@hourly");
        assert_eq!(prepend_at_char("@hourly"), "@hourly");
    }

    #[test]
    fn test_parse_retention_policy_schedule() {
        let hourly_schedule = Schedule::from_str("@hourly").unwrap();
        {
            let retention_policy = RetentionPolicy {
                retention_period: "1 hour".to_string(),
                evaluation_schedule: "@hourly".to_string(),
            };
            assert_eq!(
                retention_policy.evaluation_schedule().unwrap(),
                hourly_schedule
            );
        }
        {
            let retention_policy = RetentionPolicy {
                retention_period: "1 hour".to_string(),
                evaluation_schedule: "hourly".to_string(),
            };
            assert_eq!(
                retention_policy.evaluation_schedule().unwrap(),
                hourly_schedule
            );
        }
        {
            let retention_policy = RetentionPolicy {
                retention_period: "1 hour".to_string(),
                evaluation_schedule: "0 * * * * *".to_string(),
            };
            let evaluation_schedule = retention_policy.evaluation_schedule().unwrap();
            assert_eq!(evaluation_schedule.seconds().count(), 1);
            assert_eq!(evaluation_schedule.minutes().count(), 60);
        }
    }

    #[test]
    fn test_retention_policy_validate() {
        {
            let retention_policy = RetentionPolicy {
                retention_period: "1 hour".to_string(),
                evaluation_schedule: "hourly".to_string(),
            };
            retention_policy.validate().unwrap();
        }
        {
            let retention_policy = RetentionPolicy {
                retention_period: "foo".to_string(),
                evaluation_schedule: "hourly".to_string(),
            };
            retention_policy.validate().unwrap_err();
        }
        {
            let retention_policy = RetentionPolicy {
                retention_period: "1 hour".to_string(),
                evaluation_schedule: "foo".to_string(),
            };
            retention_policy.validate().unwrap_err();
        }
    }

    #[test]
    fn test_retention_schedule_duration() {
        let schedule_test_helper_fn = |schedule_str: &str| {
            let hourly_schedule = Schedule::from_str(&prepend_at_char(schedule_str)).unwrap();
            let retention_policy = RetentionPolicy {
                retention_period: "1 hour".to_string(),
                evaluation_schedule: schedule_str.to_string(),
            };

            let next_evaluation_duration = chrono::Duration::nanoseconds(
                retention_policy
                    .duration_until_next_evaluation()
                    .unwrap()
                    .as_nanos() as i64,
            );
            let next_evaluation_date = Utc::now() + next_evaluation_duration;
            let expected_date = hourly_schedule.upcoming(Utc).next().unwrap();
            assert_eq!(next_evaluation_date.timestamp(), expected_date.timestamp());
        };

        schedule_test_helper_fn("hourly");
        schedule_test_helper_fn("daily");
        schedule_test_helper_fn("weekly");
        schedule_test_helper_fn("monthly");
        schedule_test_helper_fn("* * * ? * ?");
    }
}
