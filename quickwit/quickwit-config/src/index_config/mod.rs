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

pub(crate) mod serialize;

use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Context};
use bytesize::ByteSize;
use chrono::Utc;
use cron::Schedule;
use humantime::parse_duration;
use quickwit_common::uri::Uri;
use quickwit_doc_mapper::{DocMapper, DocMapperBuilder, DocMapping};
use quickwit_proto::types::IndexId;
use rand::{distributions, thread_rng, Rng};
use serde::{Deserialize, Serialize};
pub use serialize::{load_index_config_from_user_config, load_index_config_update};
use siphasher::sip::SipHasher;
use tracing::warn;

use crate::index_config::serialize::VersionedIndexConfig;
use crate::merge_policy_config::MergePolicyConfig;

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct IndexingResources {
    #[schema(value_type = String, default = "2 GB")]
    #[serde(default = "IndexingResources::default_heap_size")]
    pub heap_size: ByteSize,
    // DEPRECATED: See #4439
    #[schema(value_type = String)]
    #[serde(default)]
    #[serde(skip_serializing)]
    max_merge_write_throughput: Option<ByteSize>,
}

impl PartialEq for IndexingResources {
    fn eq(&self, other: &Self) -> bool {
        self.heap_size == other.heap_size
    }
}

impl Hash for IndexingResources {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.heap_size.hash(state);
    }
}

impl IndexingResources {
    fn default_heap_size() -> ByteSize {
        ByteSize::gb(2)
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> Self {
        Self {
            heap_size: ByteSize::mb(20),
            ..Default::default()
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.max_merge_write_throughput.is_some() {
            warn!(
                "`max_merge_write_throughput` is deprecated and will be removed in a future \
                 version. See #4439. A global limit now exists in indexer configuration."
            );
        }
        Ok(())
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Hash, utoipa::ToSchema)]
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
    /// (`1 hour`, `3 days`, `1 week`, ...).
    #[serde(rename = "period")]
    pub retention_period: String,

    /// Defines the frequency at which the retention policy is evaluated and applied, expressed in
    /// a human-friendly way (`hourly`, `daily`, ...) or as a cron expression (`0 0 * * * *`,
    /// `0 0 0 * * *`).
    #[serde(default = "RetentionPolicy::default_schedule")]
    #[serde(rename = "schedule")]
    pub evaluation_schedule: String,

    /// A jitter to apply to the schedule. The policy will be evaluated [0..jitter_second] seconds
    /// after the scheduled time. When many indexes use the same schedule, this can be used to
    /// spread the load instead of causing a very bursty load.o
    ///
    /// If unset, a default jitter of `min(1 hour, next_next_evaluation - next_evaluation)` is
    /// applied. Said otherwise, an operation may start any time between the next time it's
    /// scheduled, and the time after that, but no later than 1h after the scheduled time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jitter_secs: Option<u64>,
}

impl RetentionPolicy {
    pub fn default_schedule() -> String {
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

    fn evaluation_schedule(&self) -> anyhow::Result<Schedule> {
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
        let mut schedule_iter = schedule.upcoming(Utc);
        let future_date = schedule_iter
            .next()
            .expect("Failed to obtain next evaluation date.");
        let mut duration = (future_date - Utc::now())
            .to_std()
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
        let jitter_secs = self.jitter_secs.unwrap_or_else(|| {
            if let Some(next_next_date) = schedule_iter.next() {
                let time_between_schedules = next_next_date - future_date;
                time_between_schedules.num_seconds().clamp(0, 3600) as u64
            } else {
                // we don't know when the schedule is. That's odd. Let's allow no jitter
                0
            }
        });
        let jitter = thread_rng().sample::<u64, _>(distributions::Standard) % (jitter_secs + 1);
        duration += Duration::from_secs(jitter);
        Ok(duration)
    }

    pub(super) fn validate(&self) -> anyhow::Result<()> {
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
    pub retention_policy_opt: Option<RetentionPolicy>,
}

impl IndexConfig {
    /// Return a fingerprint of parameters relevant for indexers
    ///
    /// This should remain private to this crate to avoid confusion with the
    /// full indexing pipeline fingerprint that also includes the source's
    /// fingerprint.
    pub(crate) fn indexing_params_fingerprint(&self) -> u64 {
        let mut hasher = SipHasher::new();
        self.doc_mapping.doc_mapping_uid.hash(&mut hasher);
        self.indexing_settings.hash(&mut hasher);
        hasher.finish()
    }

    /// Compares IndexConfig level fingerprints
    ///
    /// This method is meant to enable IndexConfig level fingerprint comparison
    /// without taking the risk of mixing them up with pipeline level
    /// fingerprints (computed by
    /// [`crate::indexing_pipeline_params_fingerprint()`]).
    pub fn equals_fingerprint(&self, other: &Self) -> bool {
        self.indexing_params_fingerprint() == other.indexing_params_fingerprint()
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(index_id: &str, index_uri: &str) -> Self {
        let index_uri = Uri::from_str(index_uri).unwrap();
        let doc_mapping_json = r#"{
            "doc_mapping_uid": "00000000000000000000000000",
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
            retention_policy_opt: Default::default(),
        }
    }
}

#[cfg(any(test, feature = "testsuite"))]
impl crate::TestableForRegression for IndexConfig {
    fn sample_for_regression() -> Self {
        use std::collections::BTreeSet;
        use std::num::NonZeroU32;

        use quickwit_doc_mapper::Mode;
        use quickwit_proto::types::DocMappingUid;

        use crate::merge_policy_config::StableLogMergePolicyConfig;

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
            doc_mapping_uid: DocMappingUid::for_test(1),
            mode: Mode::default(),
            field_mappings: vec![
                tenant_id_mapping,
                timestamp_mapping,
                log_level_mapping,
                message_mapping,
            ],
            timestamp_field: Some("timestamp".to_string()),
            tag_fields: BTreeSet::from_iter(["tenant_id".to_string(), "log_level".to_string()]),
            partition_key: Some("tenant_id".to_string()),
            max_num_partitions: NonZeroU32::new(100).unwrap(),
            index_field_presence: true,
            store_document_size: false,
            store_source: true,
            tokenizers: vec![tokenizer],
        };
        let retention_policy = Some(RetentionPolicy {
            retention_period: "90 days".to_string(),
            evaluation_schedule: "daily".to_string(),
            jitter_secs: None,
        });
        let stable_log_config = StableLogMergePolicyConfig {
            merge_factor: 9,
            max_merge_factor: 11,
            ..Default::default()
        };
        let merge_policy = MergePolicyConfig::StableLog(stable_log_config);
        let indexing_resources = IndexingResources {
            heap_size: ByteSize::mb(50),
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
            index_uri: Uri::for_test("s3://quickwit-indexes/my-index"),
            doc_mapping,
            indexing_settings,
            retention_policy_opt: retention_policy,
            search_settings,
        }
    }

    fn assert_equality(&self, other: &Self) {
        assert_eq!(self.index_id, other.index_id);
        assert_eq!(self.index_uri, other.index_uri);
        assert_eq!(self.doc_mapping, other.doc_mapping);
        assert_eq!(self.indexing_settings, other.indexing_settings);
        assert_eq!(self.search_settings, other.search_settings);
    }
}

/// Builds and returns the doc mapper associated with an index.
pub fn build_doc_mapper(
    doc_mapping: &DocMapping,
    search_settings: &SearchSettings,
) -> anyhow::Result<Arc<DocMapper>> {
    let builder = DocMapperBuilder {
        doc_mapping: doc_mapping.clone(),
        default_search_fields: search_settings.default_search_fields.clone(),
        legacy_type_tag: None,
    };
    Ok(Arc::new(builder.try_build()?))
}

/// Validates the objects that make up an index configuration. This is a "free" function as opposed
/// to a method on `IndexConfig` so we can reuse it for validating index templates.
pub(super) fn validate_index_config(
    doc_mapping: &DocMapping,
    indexing_settings: &IndexingSettings,
    search_settings: &SearchSettings,
    retention_policy_opt: &Option<RetentionPolicy>,
) -> anyhow::Result<()> {
    // Note: this needs a deep refactoring to separate the doc mapping configuration,
    // and doc mapper implementations.
    // TODO see if we should store the byproducton the IndexConfig.
    build_doc_mapper(doc_mapping, search_settings)?;

    indexing_settings.merge_policy.validate()?;
    indexing_settings.resources.validate()?;

    if let Some(retention_policy) = retention_policy_opt {
        retention_policy.validate()?;

        ensure!(
            doc_mapping.timestamp_field.is_some(),
            "retention policy requires a timestamp field, but doc mapping does not declare one"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use cron::TimeUnitSpec;
    use quickwit_doc_mapper::ModeType;

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
            &Uri::for_test("s3://defaultbucket/"),
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
            jitter_secs: None,
        };
        assert_eq!(
            index_config.retention_policy_opt.unwrap(),
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
            MergePolicyConfig::StableLog(crate::StableLogMergePolicyConfig {
                merge_factor: 9,
                max_merge_factor: 11,
                maturation_period: Duration::from_secs(48 * 3600),
                ..Default::default()
            })
        );
        assert_eq!(
            index_config.indexing_settings.resources,
            IndexingResources {
                heap_size: ByteSize::gb(3),
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
    fn test_indexer_config_default_values() {
        let default_index_root_uri = Uri::for_test("s3://defaultbucket/");
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
            version: 0.8
            index_id: hdfs-logs
            index_uri: ''
            doc_mapping: {}
        "#;
        serde_yaml::from_str::<IndexConfig>(config_yaml).unwrap();
    }

    #[test]
    fn test_minimal_index_config_default_dynamic() {
        let config_yaml = r#"
            version: 0.8
            index_id: hdfs-logs
            index_uri: "s3://my-index"
            doc_mapping: {}
        "#;
        let minimal_config: IndexConfig = load_index_config_from_user_config(
            ConfigFormat::Yaml,
            config_yaml.as_bytes(),
            &Uri::for_test("s3://my-index"),
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
            version: 0.8
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
            &Uri::for_test("s3://my-index"),
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
            jitter_secs: None,
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
                jitter_secs: None,
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
                jitter_secs: None,
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
                jitter_secs: None,
            };
            assert_eq!(
                retention_policy.retention_period().unwrap(),
                Duration::from_secs(3600)
            );
            {
                let retention_policy = RetentionPolicy {
                    retention_period: "foo".to_string(),
                    evaluation_schedule: "hourly".to_string(),
                    jitter_secs: None,
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
                jitter_secs: None,
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
                jitter_secs: None,
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
                jitter_secs: None,
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
                jitter_secs: None,
            };
            retention_policy.validate().unwrap();
        }
        {
            let retention_policy = RetentionPolicy {
                retention_period: "foo".to_string(),
                evaluation_schedule: "hourly".to_string(),
                jitter_secs: None,
            };
            retention_policy.validate().unwrap_err();
        }
        {
            let retention_policy = RetentionPolicy {
                retention_period: "1 hour".to_string(),
                evaluation_schedule: "foo".to_string(),
                jitter_secs: None,
            };
            retention_policy.validate().unwrap_err();
        }
    }

    #[test]
    fn test_retention_schedule_duration() {
        let schedule_test_helper_fn = |schedule_str: &str| {
            let schedule = Schedule::from_str(&prepend_at_char(schedule_str)).unwrap();
            let retention_policy = RetentionPolicy {
                retention_period: "1 hour".to_string(),
                evaluation_schedule: schedule_str.to_string(),
                jitter_secs: Some(0),
            };

            let next_evaluation_duration = chrono::Duration::nanoseconds(
                retention_policy
                    .duration_until_next_evaluation()
                    .unwrap()
                    .as_nanos() as i64,
            );
            let next_evaluation_date = Utc::now() + next_evaluation_duration;
            let expected_date = schedule.upcoming(Utc).next().unwrap();
            assert_eq!(next_evaluation_date.timestamp(), expected_date.timestamp());
        };

        schedule_test_helper_fn("hourly");
        schedule_test_helper_fn("daily");
        schedule_test_helper_fn("weekly");
        schedule_test_helper_fn("monthly");
        schedule_test_helper_fn("* * * ? * ?");
    }

    #[test]
    fn test_retention_schedule_durationi_with_jitter() {
        let schedule_test_helper_fn = |schedule_str: &str| {
            let schedule = Schedule::from_str(&prepend_at_char(schedule_str)).unwrap();
            let retention_policy = RetentionPolicy {
                retention_period: "1 hour".to_string(),
                evaluation_schedule: schedule_str.to_string(),
                jitter_secs: Some(60 * 30),
            };

            for _ in 0..11 {
                // we run this a few times in case we are unlucky and pick a null jitter.
                // This happens in one in 3601 tries, 11 unlucky tries in a row is as likely as
                // finding the right aes128 key to decrypt some message at random on 1st try.
                let next_evaluation_duration = chrono::Duration::nanoseconds(
                    retention_policy
                        .duration_until_next_evaluation()
                        .unwrap()
                        .as_nanos() as i64,
                );
                let next_evaluation_date = Utc::now() + next_evaluation_duration;
                let expected_date_early = schedule.upcoming(Utc).next().unwrap();
                let expected_date_late =
                    schedule.upcoming(Utc).next().unwrap() + chrono::Duration::seconds(30 * 60);
                assert!(next_evaluation_date.timestamp() >= expected_date_early.timestamp());
                assert!(next_evaluation_date.timestamp() <= expected_date_late.timestamp());
                if next_evaluation_date.timestamp() != expected_date_early.timestamp() {
                    return;
                }
            }
            panic!("got no jitter at all on multiple successive runs")
        };

        schedule_test_helper_fn("hourly");
        schedule_test_helper_fn("daily");
        schedule_test_helper_fn("weekly");
        schedule_test_helper_fn("monthly");
        schedule_test_helper_fn("* * * ? * ?");
    }

    #[test]
    fn test_retention_schedule_durationi_with_default_jitter() {
        let schedule_test_helper_fn = |schedule_str: &str| {
            let schedule = Schedule::from_str(&prepend_at_char(schedule_str)).unwrap();
            let retention_policy = RetentionPolicy {
                retention_period: "1 hour".to_string(),
                evaluation_schedule: schedule_str.to_string(),
                jitter_secs: None,
            };
            let max_1s_delay = schedule_str.starts_with('*');
            let (limit, max_delay) = if max_1s_delay {
                // one of our policies only allow 2 start dates, to make the test reliable, try a
                // few more times
                (128, 1)
            } else {
                (11, 3600)
            };
            for _ in 0..limit {
                // we run this a few times in case we are unlucky and pick a null jitter.
                // This happens in one in 3601 tries, 11 unlucky tries in a row is as likely as
                // finding the right aes128 key to decrypt some message at random on 1st try.
                let next_evaluation_duration = chrono::Duration::nanoseconds(
                    retention_policy
                        .duration_until_next_evaluation()
                        .unwrap()
                        .as_nanos() as i64,
                );
                let next_evaluation_date = Utc::now() + next_evaluation_duration;
                let expected_date_early = schedule.upcoming(Utc).next().unwrap();
                let expected_date_late =
                    schedule.upcoming(Utc).next().unwrap() + chrono::Duration::seconds(max_delay);
                assert!(dbg!(next_evaluation_date.timestamp()) >= expected_date_early.timestamp());
                assert!(next_evaluation_date.timestamp() <= expected_date_late.timestamp());
                if next_evaluation_date.timestamp() != expected_date_early.timestamp() {
                    return;
                }
            }
            panic!("got no jitter at all on multiple successive runs")
        };

        schedule_test_helper_fn("hourly");
        schedule_test_helper_fn("daily");
        schedule_test_helper_fn("weekly");
        schedule_test_helper_fn("monthly");
        schedule_test_helper_fn("* * * ? * ?");
    }
}
