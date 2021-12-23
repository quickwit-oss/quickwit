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

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::path::Path;
use std::time::Duration;

use anyhow::{bail, Context};
use byte_unit::Byte;
use json_comments::StripComments;
use quickwit_common::uri::Uri;
use quickwit_index_config::{FieldMappingEntry, SortBy, SortOrder};
use quickwit_storage::load_file;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DocMapping {
    pub field_mappings: Vec<FieldMappingEntry>,
    #[serde(default)]
    pub tag_fields: BTreeSet<String>,
    #[serde(default = "DocMapping::default_store_source")]
    pub store_source: bool,
}

impl DocMapping {
    fn default_store_source() -> bool {
        true
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IndexingResources {
    #[serde(default = "IndexingResources::default_num_threads")]
    pub num_threads: usize,
    #[serde(default = "IndexingResources::default_heap_size")]
    pub heap_size: Byte,
}

impl IndexingResources {
    fn default_num_threads() -> usize {
        1
    }

    fn default_heap_size() -> Byte {
        Byte::from_bytes(2_000_000_000) // 2GB
    }

    pub fn for_test() -> Self {
        Self {
            num_threads: 1,
            heap_size: Byte::from_bytes(20_000_000), // 20MB
        }
    }
}

impl Default for IndexingResources {
    fn default() -> Self {
        Self {
            num_threads: Self::default_num_threads(),
            heap_size: Self::default_heap_size(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MergePolicy {
    #[serde(default = "MergePolicy::default_demux_factor")]
    pub demux_factor: usize,
    #[serde(default = "MergePolicy::default_merge_factor")]
    pub merge_factor: usize,
    #[serde(default = "MergePolicy::default_max_merge_factor")]
    pub max_merge_factor: usize,
}

impl MergePolicy {
    fn default_demux_factor() -> usize {
        8
    }

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
            demux_factor: Self::default_demux_factor(),
            merge_factor: Self::default_merge_factor(),
            max_merge_factor: Self::default_max_merge_factor(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IndexingSettings {
    #[serde(default)]
    pub demux_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub demux_field: Option<String>,
    pub timestamp_field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_order: Option<SortOrder>,
    #[serde(default = "IndexingSettings::default_commit_timeout_secs")]
    pub commit_timeout_secs: usize,
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

impl IndexingSettings {
    pub fn commit_timeout(&self) -> Duration {
        Duration::from_secs(self.commit_timeout_secs as u64)
    }

    fn default_commit_timeout_secs() -> usize {
        60
    }

    fn default_split_num_docs_target() -> usize {
        10_000_000
    }

    fn default_merge_enabled() -> bool {
        false
    }

    pub fn sort_by(&self) -> SortBy {
        if let Some(field_name) = self.sort_field.clone() {
            let order = self.sort_order.unwrap_or_default();
            return SortBy::FastField { field_name, order };
        }
        SortBy::DocId
    }

    // TODO(guilload) Hide this method if possible.
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
            demux_enabled: false,
            demux_field: None,
            timestamp_field: None,
            sort_field: None,
            sort_order: None,
            commit_timeout_secs: Self::default_commit_timeout_secs(),
            split_num_docs_target: Self::default_split_num_docs_target(),
            merge_enabled: Self::default_merge_enabled(),
            merge_policy: MergePolicy::default(),
            resources: IndexingResources::default(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct SearchSettings {
    #[serde(default)]
    pub default_search_fields: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceConfig {
    pub source_id: String,
    pub source_type: String,
    pub params: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexConfig {
    pub version: usize,
    pub doc_mapping: DocMapping,
    #[serde(default)]
    pub indexing_settings: IndexingSettings,
    #[serde(default)]
    pub search_settings: SearchSettings,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
}

impl IndexConfig {
    // Loads config from a given path and validates it.
    pub async fn load(uri: &Uri) -> anyhow::Result<Self> {
        let config = IndexConfig::from_uri(uri).await?;
        config.validate()?;
        Ok(config)
    }

    async fn from_uri(uri: &Uri) -> anyhow::Result<Self> {
        let parser_fn = match Path::new(uri.as_ref()).extension().and_then(OsStr::to_str) {
            Some("json") => Self::from_json,
            Some("toml") => Self::from_toml,
            Some("yaml") | Some("yml") => Self::from_yaml,
            Some(extension) => bail!(
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
        let file_content = load_file(uri).await?;
        parser_fn(file_content.as_slice())
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

    fn validate(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_resource_path(resource_filename: &str) -> String {
        format!(
            "{}/resources/tests/index_config/{}",
            env!("CARGO_MANIFEST_DIR"),
            resource_filename
        )
    }

    macro_rules! test_parser {
        ($test_function_name:ident, $file_extension:expr) => {
            #[tokio::test]
            async fn $test_function_name() -> anyhow::Result<()> {
                let index_config_uri = Uri::try_new(&get_resource_path(&format!(
                    "hdfs-logs.{}",
                    stringify!($file_extension)
                )))?;
                let index_config = IndexConfig::from_uri(&index_config_uri).await?;
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
                assert_eq!(index_config.doc_mapping.store_source, true);

                assert_eq!(
                    index_config.indexing_settings.demux_field.unwrap(),
                    "tenant_id"
                );
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
                        demux_factor: 7,
                        merge_factor: 9,
                        max_merge_factor: 11,
                    }
                );
                assert_eq!(
                    index_config.indexing_settings.resources,
                    IndexingResources {
                        num_threads: 3,
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
                    assert_eq!(source.source_type, "kafka");
                }
                {
                    let source = &index_config.sources[1];
                    assert_eq!(source.source_id, "hdfs-logs-kinesis-source");
                    assert_eq!(source.source_type, "kinesis");
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
            let index_config_uri =
                Uri::try_new(&get_resource_path("minimal-hdfs-logs.yaml")).unwrap();
            let index_config = IndexConfig::from_uri(&index_config_uri).await.unwrap();

            assert_eq!(index_config.doc_mapping.field_mappings.len(), 1);
            assert_eq!(index_config.doc_mapping.field_mappings[0].name, "body");
            assert!(index_config.doc_mapping.store_source);
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
            let index_config_uri =
                Uri::try_new(&get_resource_path("partial-hdfs-logs.yaml")).unwrap();
            let index_config = IndexConfig::from_uri(&index_config_uri).await.unwrap();

            assert_eq!(index_config.version, 0);
            assert_eq!(index_config.doc_mapping.field_mappings.len(), 2);
            assert_eq!(index_config.doc_mapping.field_mappings[0].name, "body");
            assert_eq!(index_config.doc_mapping.field_mappings[1].name, "timestamp");
            assert!(index_config.doc_mapping.store_source);
            assert_eq!(
                index_config.indexing_settings,
                IndexingSettings {
                    sort_field: Some("timestamp".to_string()),
                    commit_timeout_secs: 42,
                    merge_policy: MergePolicy {
                        demux_factor: 7,
                        ..Default::default()
                    },
                    resources: IndexingResources {
                        num_threads: 3,
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

    #[test]
    fn test_validate() {
        // TODO
    }
}
