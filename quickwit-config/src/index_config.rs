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

use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroU64;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use byte_unit::Byte;
use chrono::Utc;
use cron::Schedule;
use humantime::parse_duration;
use json_comments::StripComments;
use quickwit_common::uri::{Extension, Uri};
use quickwit_doc_mapper::{
    DefaultDocMapper, DefaultDocMapperBuilder, DocMapper, FieldMappingEntry, ModeType,
    QuickwitJsonOptions, SortBy, SortByConfig, SortOrder,
};
use serde::de::{Error, IgnoredAny};
use serde::{Deserialize, Deserializer, Serialize};

use crate::source_config::SourceConfig;
use crate::validate_identifier;

// Note(fmassot): `DocMapping` is a struct only used for
// serialization/deserialization of `DocMapper` parameters.
// This is partly a duplicate of the `DocMapper` and can
// be viewed as a temporary hack for 0.2 release before
// refactoring.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocMapping {
    #[serde(default)]
    pub field_mappings: Vec<FieldMappingEntry>,
    #[serde(default)]
    pub tag_fields: BTreeSet<String>,
    #[serde(default)]
    pub store_source: bool,
    #[serde(default)]
    pub mode: ModeType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dynamic_mapping: Option<QuickwitJsonOptions>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub partition_key: String,
    #[serde(default = "DefaultDocMapper::default_max_num_partitions")]
    pub max_num_partitions: NonZeroU64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexingResources {
    #[serde(default, rename = "num_threads", skip_serializing)]
    pub __num_threads_deprecated: IgnoredAny, // DEPRECATED
    #[serde(default = "IndexingResources::default_heap_size")]
    pub heap_size: Byte,
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
            __num_threads_deprecated: IgnoredAny,
            heap_size: Byte::from_bytes(20_000_000), // 20MB
        }
    }
}

impl Default for IndexingResources {
    fn default() -> Self {
        Self {
            __num_threads_deprecated: IgnoredAny,
            heap_size: Self::default_heap_size(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergePolicy {
    #[serde(default, rename = "demux_factor", skip_serializing)]
    pub __demux_factor_deprecated: IgnoredAny, // DEPRECATED
    #[serde(default = "MergePolicy::default_merge_factor")]
    pub merge_factor: usize,
    #[serde(default = "MergePolicy::default_max_merge_factor")]
    pub max_merge_factor: usize,
}

impl PartialEq for MergePolicy {
    fn eq(&self, other: &Self) -> bool {
        self.merge_factor == other.merge_factor && self.max_merge_factor == other.max_merge_factor
    }
}

impl Eq for MergePolicy {}

impl MergePolicy {
    fn default_merge_factor() -> usize {
        10
    }

    fn default_max_merge_factor() -> usize {
        12
    }
}

impl Default for MergePolicy {
    fn default() -> Self {
        Self {
            __demux_factor_deprecated: serde::de::IgnoredAny,
            merge_factor: Self::default_merge_factor(),
            max_merge_factor: Self::default_max_merge_factor(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexingSettings {
    #[serde(default, rename = "demux_enabled", skip_serializing)]
    pub __demux_enabled_deprecated: IgnoredAny,
    #[serde(default, rename = "demux_field", skip_serializing)]
    pub __demux_field_deprecated: IgnoredAny,
    pub timestamp_field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_order: Option<SortOrder>,
    #[serde(default = "IndexingSettings::default_commit_timeout_secs")]
    pub commit_timeout_secs: usize,
    #[serde(default = "IndexingSettings::default_docstore_compression_level")]
    pub docstore_compression_level: i32,
    #[serde(default = "IndexingSettings::default_docstore_blocksize")]
    pub docstore_blocksize: usize,
    /// A split containing a number of docs greather than or equal to this value is considered
    /// mature.
    #[serde(default = "IndexingSettings::default_split_num_docs_target")]
    pub split_num_docs_target: usize,
    #[serde(default = "IndexingSettings::default_merge_enabled")]
    pub merge_enabled: bool,
    #[serde(default)]
    pub merge_policy: MergePolicy,
    #[serde(default)]
    pub resources: IndexingResources,
}

impl PartialEq for IndexingSettings {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp_field == other.timestamp_field
            && self.sort_field == other.sort_field
            && self.sort_order == other.sort_order
            && self.commit_timeout_secs == other.commit_timeout_secs
            && self.docstore_compression_level == other.docstore_compression_level
            && self.docstore_blocksize == other.docstore_blocksize
            && self.split_num_docs_target == other.split_num_docs_target
            && self.merge_enabled == other.merge_enabled
            && self.merge_policy == other.merge_policy
            && self.resources == other.resources
    }
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

    fn default_split_num_docs_target() -> usize {
        10_000_000
    }

    fn default_merge_enabled() -> bool {
        true
    }

    pub fn sort_by(&self) -> SortBy {
        if let Some(field_name) = self.sort_field.clone() {
            let order = self.sort_order.unwrap_or_default();
            return SortBy::FastField { field_name, order };
        }
        SortBy::DocId
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
            __demux_enabled_deprecated: IgnoredAny,
            __demux_field_deprecated: IgnoredAny,
            timestamp_field: None,
            sort_field: None,
            sort_order: None,
            commit_timeout_secs: Self::default_commit_timeout_secs(),
            docstore_blocksize: Self::default_docstore_blocksize(),
            docstore_compression_level: Self::default_docstore_compression_level(),
            split_num_docs_target: Self::default_split_num_docs_target(),
            merge_enabled: Self::default_merge_enabled(),
            merge_policy: MergePolicy::default(),
            resources: IndexingResources::default(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchSettings {
    #[serde(default)]
    pub default_search_fields: Vec<String>,
}

/// Defines on which split attribute the retention policy is applied relatively.
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum RetentionPolicyCutoffReference {
    /// The split is deleted when `now() - split.publish_timestamp >= retention_policy.period`
    PublishTimestamp,
    /// The split is deleted when `now() - split.time_range.end >= retention_policy.period`
    SplitTimestampField,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionPolicy {
    /// Duration of time for which the splits should be retained, expressed in a human-friendly way
    /// (`1 hour`, `3 days`, `a week`, ...).
    #[serde(rename = "period")]
    retention_period: String,
    /// Determines on which split attribute the retention policy is applied relatively. See
    /// [`RetentionPolicyCutoffReference`] for more details.
    pub cutoff_reference: RetentionPolicyCutoffReference,

    /// Defines the frequency at which the retention policy is evaluated and applied, expressed in
    /// a human-friendly way (`hourly`, `daily`, ...) or as a cron expression (`0 0 * * * *`,
    /// `0 0 0 * * *`).
    #[serde(default = "RetentionPolicy::default_schedule")]
    #[serde(rename = "schedule")]
    evaluation_schedule: String,
}

impl RetentionPolicy {
    pub fn new(
        retention_period: String,
        cutoff_reference: RetentionPolicyCutoffReference,
        evaluation_schedule: String,
    ) -> Self {
        Self {
            retention_period,
            cutoff_reference,
            evaluation_schedule,
        }
    }

    fn default_schedule() -> String {
        "hourly".to_string()
    }

    pub fn retention_period(&self) -> anyhow::Result<Duration> {
        parse_duration(&self.retention_period).with_context(|| {
            format!(
                "Failed to parse retention period `{}`.",
                self.retention_period
            )
        })
    }

    pub fn evaluation_schedule(&self) -> anyhow::Result<Schedule> {
        let evaluation_schedule = prepend_at_char(&self.evaluation_schedule);

        Schedule::from_str(&evaluation_schedule).with_context(|| {
            format!(
                "Failed to parse retention evaluation schedule `{}`.",
                self.evaluation_schedule
            )
        })
    }

    pub fn duration_till_next_evaluation(&self) -> anyhow::Result<Duration> {
        let schedule = self.evaluation_schedule()?;
        let future_date = schedule
            .upcoming(Utc)
            .next()
            .context("Failed to obtain next evaluation date.")?;
        let duration = (future_date - Utc::now())
            .to_std()
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
        Ok(duration)
    }

    fn requires_timestamp_field(&self) -> bool {
        matches!(
            self.cutoff_reference,
            RetentionPolicyCutoffReference::SplitTimestampField
        )
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexConfig {
    pub version: usize,
    pub index_id: String,
    #[serde(default)]
    #[serde(deserialize_with = "deser_and_validate_uri")]
    pub index_uri: Option<Uri>,
    pub doc_mapping: DocMapping,
    #[serde(default)]
    pub indexing_settings: IndexingSettings,
    #[serde(default)]
    pub search_settings: SearchSettings,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
    #[serde(rename = "retention")]
    #[serde(default)]
    pub retention_policy: Option<RetentionPolicy>,
}

impl IndexConfig {
    /// Parses and validates an [`IndexConfig`] from a given URI and config content.
    pub async fn load(uri: &Uri, file_content: &[u8]) -> anyhow::Result<Self> {
        let config = Self::from_uri(uri, file_content).await?;
        config.validate()?;
        Ok(config)
    }

    async fn from_uri(uri: &Uri, file_content: &[u8]) -> anyhow::Result<Self> {
        let parser_fn = match uri.extension() {
            Some(Extension::Json) => Self::from_json,
            Some(Extension::Toml) => Self::from_toml,
            Some(Extension::Yaml) => Self::from_yaml,
            Some(Extension::Unknown(extension)) => bail!(
                "Failed to read index config file `{}`: file extension `.{}` is not supported. \
                 Supported file formats and extensions are JSON (.json), TOML (.toml), and YAML \
                 (.yaml or .yml).",
                uri,
                extension
            ),
            None => bail!(
                "Failed to read index config file `{}`: file extension is missing. Supported file \
                 formats and extensions are JSON (.json), TOML (.toml), and YAML (.yaml or .yml).",
                uri
            ),
        };
        parser_fn(file_content)
    }

    fn from_json(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_reader(StripComments::new(bytes))
            .context("Failed to parse JSON index config file.")
    }

    fn from_toml(bytes: &[u8]) -> anyhow::Result<Self> {
        toml::from_slice(bytes).context("Failed to parse TOML index config file.")
    }

    fn from_yaml(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_yaml::from_slice(bytes).context("Failed to parse YAML index config file.")
    }

    pub fn sources(&self) -> HashMap<String, SourceConfig> {
        self.sources
            .iter()
            .map(|source| (source.source_id.clone(), source.clone()))
            .collect()
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        validate_identifier("Index ID", &self.index_id)?;

        if let Some(retention_policy) = &self.retention_policy {
            retention_policy.validate()?;

            if retention_policy.requires_timestamp_field()
                && self.indexing_settings.timestamp_field.is_none()
            {
                bail!(
                    "Failed to validate index config. The retention policy cutoff reference \
                     requires a timestamp field, but the indexing settings do not declare one."
                );
            }
        }
        if self.sources.len() > self.sources().len() {
            bail!("Index config contains duplicate sources.")
        }
        for source in &self.sources {
            source.validate()?;
        }
        // Validation is made by building the doc mapper.
        // Note: this needs a deep refactoring to separate the doc mapping configuration,
        // and doc mapper implementations.
        build_doc_mapper(
            &self.doc_mapping,
            &self.search_settings,
            &self.indexing_settings,
        )?;
        if self.indexing_settings.merge_policy.max_merge_factor
            < self.indexing_settings.merge_policy.merge_factor
        {
            bail!(
                "Index config merge policy `max_merge_factor` must be superior or equal to \
                 `merge_factor`."
            )
        }
        Ok(())
    }
}

/// Builds and returns the doc mapper associated with index.
pub fn build_doc_mapper(
    doc_mapping: &DocMapping,
    search_settings: &SearchSettings,
    indexing_settings: &IndexingSettings,
) -> anyhow::Result<Arc<dyn DocMapper>> {
    let sort_by = match indexing_settings.sort_by() {
        SortBy::DocId => None,
        SortBy::FastField { field_name, order } => Some(SortByConfig { field_name, order }),
        SortBy::Score { order } => Some(SortByConfig {
            field_name: "_score".to_string(),
            order,
        }),
    };
    let builder = DefaultDocMapperBuilder {
        store_source: doc_mapping.store_source,
        default_search_fields: search_settings.default_search_fields.clone(),
        timestamp_field: indexing_settings.timestamp_field.clone(),
        sort_by,
        field_mappings: doc_mapping.field_mappings.clone(),
        tag_fields: doc_mapping.tag_fields.iter().cloned().collect(),
        mode: doc_mapping.mode,
        dynamic_mapping: doc_mapping.dynamic_mapping.clone(),
        partition_key: doc_mapping.partition_key.clone(),
        max_num_partitions: doc_mapping.max_num_partitions,
    };
    Ok(Arc::new(builder.try_build()?))
}

/// Deserializes and validates a [`Uri`].
fn deser_and_validate_uri<'de, D>(deserializer: D) -> Result<Option<Uri>, D::Error>
where D: Deserializer<'de> {
    let uri_opt: Option<String> = Deserialize::deserialize(deserializer)?;
    uri_opt
        .map(|uri| Uri::try_new(&uri))
        .transpose()
        .map_err(D::Error::custom)
}

#[cfg(test)]
mod tests {

    use cron::TimeUnitSpec;

    use super::*;
    use crate::SourceParams;

    fn get_index_config_filepath(index_config_filename: &str) -> String {
        format!(
            "{}/resources/tests/index_config/{}",
            env!("CARGO_MANIFEST_DIR"),
            index_config_filename
        )
    }

    macro_rules! test_parser {
        ($test_function_name:ident, $file_extension:expr) => {
            #[tokio::test]
            async fn $test_function_name() -> anyhow::Result<()> {
                let index_config_filepath = get_index_config_filepath(&format!(
                    "hdfs-logs.{}",
                    stringify!($file_extension)
                ));
                let file = std::fs::read_to_string(&index_config_filepath).unwrap();
                let index_config = IndexConfig::load(
                    &Uri::try_new(&index_config_filepath).unwrap(),
                    file.as_bytes(),
                )
                .await?;
                assert_eq!(index_config.version, 0);

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
                    cutoff_reference: RetentionPolicyCutoffReference::SplitTimestampField,
                    evaluation_schedule: "daily".to_string(),
                };
                assert_eq!(
                    index_config.retention_policy.unwrap(),
                    expected_retention_policy
                );
                assert_eq!(index_config.doc_mapping.store_source, true);

                assert_eq!(
                    index_config.indexing_settings.timestamp_field.unwrap(),
                    "timestamp"
                );
                assert_eq!(
                    index_config.indexing_settings.sort_field.unwrap(),
                    "timestamp"
                );
                assert_eq!(
                    index_config.indexing_settings.sort_order.unwrap(),
                    SortOrder::Asc
                );
                assert_eq!(index_config.indexing_settings.commit_timeout_secs, 61);

                assert_eq!(
                    index_config.indexing_settings.split_num_docs_target,
                    10_000_001
                );
                assert_eq!(
                    index_config.indexing_settings.merge_policy,
                    MergePolicy {
                        merge_factor: 9,
                        max_merge_factor: 11,
                        ..Default::default()
                    }
                );
                assert_eq!(
                    index_config.indexing_settings.resources,
                    IndexingResources {
                        __num_threads_deprecated: serde::de::IgnoredAny,
                        heap_size: Byte::from_bytes(3_000_000_000)
                    }
                );
                assert_eq!(
                    index_config.search_settings,
                    SearchSettings {
                        default_search_fields: vec![
                            "severity_text".to_string(),
                            "body".to_string()
                        ],
                    }
                );
                assert_eq!(index_config.sources.len(), 2);
                {
                    let source = &index_config.sources[0];
                    assert_eq!(source.source_id, "hdfs-logs-kafka-source");
                    assert!(matches!(source.source_params, SourceParams::Kafka(_)));
                }
                {
                    let source = &index_config.sources[1];
                    assert_eq!(source.source_id, "hdfs-logs-kinesis-source");
                    assert!(matches!(source.source_params, SourceParams::Kinesis(_)));
                }
                Ok(())
            }
        };
    }

    test_parser!(test_index_config_from_json, json);
    test_parser!(test_index_config_from_toml, toml);
    test_parser!(test_index_config_from_yaml, yaml);

    #[tokio::test]
    async fn test_index_config_default_values() {
        {
            let index_config_filepath = get_index_config_filepath("minimal-hdfs-logs.yaml");
            let file_content = std::fs::read_to_string(&index_config_filepath).unwrap();

            let index_config_uri =
                Uri::try_new(&get_index_config_filepath("minimal-hdfs-logs.yaml")).unwrap();
            let index_config = IndexConfig::from_uri(&index_config_uri, file_content.as_bytes())
                .await
                .unwrap();

            assert_eq!(index_config.index_id, "hdfs-logs");
            assert_eq!(
                index_config.index_uri.unwrap(),
                "s3://quickwit-indexes/hdfs-logs"
            );
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
            assert!(index_config.sources.is_empty());
        }
        {
            let index_config_filepath = get_index_config_filepath("partial-hdfs-logs.yaml");
            let file_content = std::fs::read_to_string(&index_config_filepath).unwrap();

            let index_config_uri =
                Uri::try_new(&get_index_config_filepath("partial-hdfs-logs.yaml")).unwrap();
            let index_config = IndexConfig::from_uri(&index_config_uri, file_content.as_bytes())
                .await
                .unwrap();

            assert_eq!(index_config.version, 0);
            assert_eq!(index_config.index_id, "hdfs-logs");
            assert_eq!(
                index_config.index_uri.unwrap(),
                "s3://quickwit-indexes/hdfs-logs"
            );
            assert_eq!(index_config.doc_mapping.field_mappings.len(), 2);
            assert_eq!(index_config.doc_mapping.field_mappings[0].name, "body");
            assert_eq!(index_config.doc_mapping.field_mappings[1].name, "timestamp");
            assert!(!index_config.doc_mapping.store_source);
            assert_eq!(
                index_config.indexing_settings,
                IndexingSettings {
                    sort_field: Some("timestamp".to_string()),
                    commit_timeout_secs: 42,
                    merge_policy: MergePolicy::default(),
                    resources: IndexingResources {
                        __num_threads_deprecated: serde::de::IgnoredAny,
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
            assert!(index_config.sources.is_empty());
        }
    }

    #[tokio::test]
    async fn test_validate() {
        let index_config_filepath = get_index_config_filepath("minimal-hdfs-logs.yaml");
        let file_content = std::fs::read_to_string(&index_config_filepath).unwrap();
        let index_config_uri = Uri::try_new(&index_config_filepath).unwrap();
        let index_config = IndexConfig::from_uri(&index_config_uri, file_content.as_bytes())
            .await
            .unwrap();
        {
            let mut invalid_index_config = index_config.clone();
            // Set a max merge factor to an inconsistent value.
            invalid_index_config
                .indexing_settings
                .merge_policy
                .max_merge_factor = index_config.indexing_settings.merge_policy.merge_factor - 1;
            assert!(invalid_index_config.validate().is_err());
            assert!(invalid_index_config
                .validate()
                .unwrap_err()
                .to_string()
                .contains(
                    "Index config merge policy `max_merge_factor` must be superior or equal to \
                     `merge_factor`."
                ));
        }
        {
            // Add two sources with same id.
            let mut invalid_index_config = index_config.clone();
            invalid_index_config.sources = vec![
                SourceConfig {
                    source_id: "void_1".to_string(),
                    num_pipelines: 1,
                    source_params: SourceParams::void(),
                },
                SourceConfig {
                    source_id: "void_1".to_string(),
                    num_pipelines: 1,
                    source_params: SourceParams::void(),
                },
            ];
            assert!(invalid_index_config.validate().is_err());
            assert!(invalid_index_config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("Index config contains duplicate sources."));
        }
        {
            // Add source file params with no filepath.
            let mut invalid_index_config = index_config;
            invalid_index_config.sources = vec![SourceConfig {
                source_id: "file_params_1".to_string(),
                num_pipelines: 1,
                source_params: SourceParams::stdin(),
            }];
            assert!(invalid_index_config.validate().is_err());
            assert!(invalid_index_config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("must contain a `filepath`"));
        }
    }

    #[test]
    #[should_panic(expected = "URI is empty.")]
    fn test_config_validates_uris() {
        let config_yaml = r#"
            version: 0
            index_id: hdfs-logs
            index_uri: ''
            doc_mapping: {}
        "#;
        serde_yaml::from_str::<IndexConfig>(config_yaml).unwrap();
    }

    #[test]
    fn test_minimal_index_config() {
        let config_yaml = r#"
            version: 0
            index_id: hdfs-logs
            doc_mapping: {}
        "#;
        let minimal_config = serde_yaml::from_str::<IndexConfig>(config_yaml).unwrap();
        assert_eq!(minimal_config.doc_mapping.mode, ModeType::Lenient);
    }

    #[test]
    fn test_retention_policy_serialization() {
        let retention_policy = RetentionPolicy {
            retention_period: "90 days".to_string(),
            cutoff_reference: RetentionPolicyCutoffReference::PublishTimestamp,
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
            cutoff_reference: publish_timestamp
        "#;
            let retention_policy =
                serde_yaml::from_str::<RetentionPolicy>(retention_policy_yaml).unwrap();

            let expected_retention_policy = RetentionPolicy {
                retention_period: "90 days".to_string(),
                cutoff_reference: RetentionPolicyCutoffReference::PublishTimestamp,
                evaluation_schedule: "hourly".to_string(),
            };
            assert_eq!(retention_policy, expected_retention_policy);
        }
        {
            let retention_policy_yaml = r#"
            period: 90 days
            cutoff_reference: publish_timestamp
            schedule: daily
        "#;
            let retention_policy =
                serde_yaml::from_str::<RetentionPolicy>(retention_policy_yaml).unwrap();

            let expected_retention_policy = RetentionPolicy {
                retention_period: "90 days".to_string(),
                cutoff_reference: RetentionPolicyCutoffReference::PublishTimestamp,
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
                cutoff_reference: RetentionPolicyCutoffReference::PublishTimestamp,
                evaluation_schedule: "hourly".to_string(),
            };
            assert_eq!(
                retention_policy.retention_period().unwrap(),
                Duration::from_secs(3600)
            );
            {
                let retention_policy = RetentionPolicy {
                    retention_period: "foo".to_string(),
                    cutoff_reference: RetentionPolicyCutoffReference::PublishTimestamp,
                    evaluation_schedule: "hourly".to_string(),
                };
                assert_eq!(
                    retention_policy.retention_period().unwrap_err().to_string(),
                    "Failed to parse retention period `foo`."
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
                cutoff_reference: RetentionPolicyCutoffReference::PublishTimestamp,
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
                cutoff_reference: RetentionPolicyCutoffReference::PublishTimestamp,
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
                cutoff_reference: RetentionPolicyCutoffReference::PublishTimestamp,
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
                cutoff_reference: RetentionPolicyCutoffReference::PublishTimestamp,
                evaluation_schedule: "hourly".to_string(),
            };
            retention_policy.validate().unwrap();
        }
        {
            let retention_policy = RetentionPolicy {
                retention_period: "foo".to_string(),
                cutoff_reference: RetentionPolicyCutoffReference::PublishTimestamp,
                evaluation_schedule: "hourly".to_string(),
            };
            retention_policy.validate().unwrap_err();
        }
        {
            let retention_policy = RetentionPolicy {
                retention_period: "1 hour".to_string(),
                cutoff_reference: RetentionPolicyCutoffReference::PublishTimestamp,
                evaluation_schedule: "foo".to_string(),
            };
            retention_policy.validate().unwrap_err();
        }
    }
}
