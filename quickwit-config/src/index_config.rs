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

use std::ffi::OsStr;
use std::path::Path;

use anyhow::{bail, Context};
use byte_unit::Byte;
use quickwit_index_config::FieldMappingEntry;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DocMapping {
    pub field_mappings: Vec<FieldMappingEntry>,
    pub tag_fields: Vec<String>,
    pub store_source: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct IndexingResources {
    pub num_threads: usize,
    pub heap_size: Byte,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct MergePolicy {
    pub demux_factor: usize,
    pub merge_factor: usize,
    pub max_merge_factor: usize,
    pub min_level_num_docs: usize,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct IndexingSettings {
    pub demux_field: String,
    pub timestamp_field: String,
    pub commit_timeout_secs: usize,
    /// The maximum number of documents allowed in a split.
    pub split_max_num_docs: usize,
    pub merge_policy: MergePolicy,
    pub resources: IndexingResources,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct SearchSettings {
    pub default_search_fields: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct SourceConfig {
    pub source_id: String,
    pub source_type: String,
    pub params: toml::Value,
}

#[derive(Serialize, Deserialize)]
pub struct IndexConfig {
    pub index_id: String,
    pub index_uri: String,
    pub doc_mapping: DocMapping,
    pub indexing_settings: IndexingSettings,
    pub search_settings: SearchSettings,
    pub sources: Vec<SourceConfig>,
}

impl IndexConfig {
    // TODO: asyncify?
    pub fn from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let parser_fn = match path.as_ref().extension().and_then(OsStr::to_str) {
            Some("json") => Self::from_json,
            Some("toml") => Self::from_toml,
            Some("yaml") | Some("yml") => Self::from_yaml,
            Some(extension) => bail!(
                "Failed to read index config file: file extension `.{}` is not supported. \
                 Supported file formats and extensions are JSON (.json), TOML (.toml), and YAML \
                 (.yaml or .yml).",
                extension
            ),
            None => bail!(
                "Failed to read index config file: file extension is missing. Supported file \
                 formats and extensions are JSON (.json), TOML (.toml), and YAML (.yaml or .yml)."
            ),
        };
        let file_content = std::fs::read_to_string(path)?;
        parser_fn(file_content.as_bytes())
    }

    pub fn from_json(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice::<IndexConfig>(bytes)
            .context("Failed to parse JSON index config file.")
    }

    pub fn from_toml(bytes: &[u8]) -> anyhow::Result<Self> {
        toml::from_slice::<IndexConfig>(bytes).context("Failed to parse TOML index config file.")
    }

    pub fn from_yaml(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_yaml::from_slice::<IndexConfig>(bytes)
            .context("Failed to parse YAML index config file.")
    }

    // TODO
    pub fn validate(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn get_resource_path(relative_resource_path: &str) -> PathBuf {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/tests/index_object/");
        path.push(relative_resource_path);
        path
    }

    macro_rules! test_parser {
        ($test_function_name:ident, $file_extension:expr) => {
            #[test]
            fn $test_function_name() -> anyhow::Result<()> {
                let index_config_filepath =
                    get_resource_path(&format!("hdfs-logs.{}", stringify!($file_extension)));
                let index_config = IndexConfig::from_file(index_config_filepath)?;
                assert_eq!(index_config.index_id, "hdfs-logs");
                assert_eq!(index_config.index_uri, "s3://quickwit-indexes/hdfs-logs");

                assert_eq!(index_config.doc_mapping.field_mappings.len(), 5);
                assert_eq!(
                    index_config.doc_mapping.field_mappings[0].name,
                    "cluster_id"
                );
                assert_eq!(index_config.doc_mapping.field_mappings[1].name, "timestamp");
                assert_eq!(
                    index_config.doc_mapping.field_mappings[2].name,
                    "severity_text"
                );
                assert_eq!(index_config.doc_mapping.field_mappings[3].name, "body");
                assert_eq!(index_config.doc_mapping.field_mappings[4].name, "resource");

                assert_eq!(
                    index_config.doc_mapping.tag_fields,
                    vec!["cluster_id".to_string()]
                );
                assert_eq!(index_config.doc_mapping.store_source, true);

                assert_eq!(index_config.indexing_settings.demux_field, "cluster_id");
                assert_eq!(index_config.indexing_settings.timestamp_field, "timestamp");
                assert_eq!(index_config.indexing_settings.commit_timeout_secs, 60);

                assert_eq!(
                    index_config.indexing_settings.split_max_num_docs,
                    10_000_000
                );
                assert_eq!(
                    index_config.indexing_settings.merge_policy,
                    MergePolicy {
                        demux_factor: 6,
                        merge_factor: 10,
                        max_merge_factor: 12,
                        min_level_num_docs: 100_000,
                    }
                );
                assert_eq!(
                    index_config.indexing_settings.resources,
                    IndexingResources {
                        num_threads: 1,
                        heap_size: Byte::from_bytes(1_000_000_000)
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

    test_parser!(test_from_json, json);
    test_parser!(test_from_toml, toml);
    test_parser!(test_from_yaml, yaml);

    #[test]
    fn test_default_values() {
        // TODO
    }

    #[test]
    fn test_validate() {
        // TODO
    }
}
