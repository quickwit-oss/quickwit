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

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use itertools::Itertools;
use quickwit_config::{
    DocMapping, IndexingResources, IndexingSettings, SearchSettings, SourceConfig,
};
use quickwit_index_config::{
    DefaultIndexConfig as DefaultDocMapper, DefaultIndexConfigBuilder as DocMapperBuilder,
    IndexConfig as DocMapper, SortBy, SortByConfig, SortOrder,
};
use serde::{Deserialize, Deserializer, Serialize};

use crate::checkpoint::{IndexCheckpoint, SourceCheckpoint};
use crate::split_metadata::utc_now_timestamp;
use crate::{MetastoreError, MetastoreResult};

/// An index metadata carries all meta data about an index.
#[derive(Clone, Debug, Serialize)]
#[serde(into = "VersionedIndexMetadata")]
pub struct IndexMetadata {
    /// Index ID, uniquely identifies an index when querying the metastore.
    pub index_id: String,
    /// Index URI, defines the location of the storage that holds the split files.
    pub index_uri: String,
    /// Checkpoint relative to a source or a set of sources. It expresses up to which point
    /// documents have been indexed.
    pub checkpoint: IndexCheckpoint,
    /// Describes how ingested JSON documents are indexed.
    pub doc_mapping: DocMapping,
    /// Configures various indexing settings such as commit timeout, max split size, indexing
    /// resources.
    pub indexing_settings: IndexingSettings,
    /// Configures various search settings such as default search fields.
    pub search_settings: SearchSettings,
    /// Data sources keyed by their `source_id`.
    pub sources: HashMap<String, SourceConfig>,
    /// Time at which the index was created.
    pub create_timestamp: i64,
    /// Time at which the index was last updated.
    pub update_timestamp: i64,
}

impl IndexMetadata {
    /// Returns an [`IndexMetadata`] object with multiple hard coded values for tests.
    #[doc(hidden)]
    pub fn for_test(index_id: &str, index_uri: &str) -> Self {
        let doc_mapping_json = r#"{
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "i64",
                    "fast": true
                },
                {
                    "name": "body",
                    "type": "text",
                    "stored": true
                },
                {
                    "name": "response_date",
                    "type": "date",
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
            "tag_fields": ["owner"],
            "store_source": true
        }"#;
        let doc_mapping = serde_json::from_str(doc_mapping_json).unwrap();
        let indexing_settings = IndexingSettings {
            timestamp_field: Some("timestamp".to_string()),
            sort_field: Some("timestamp".to_string()),
            sort_order: Some(SortOrder::Desc),
            resources: IndexingResources::for_test(),
            ..Default::default()
        };
        let search_settings = SearchSettings {
            default_search_fields: vec![
                "body".to_string(),
                "attributes.server".to_string(),
                "attributes.server.status".to_string(),
            ],
        };
        let now_timestamp = utc_now_timestamp();
        Self {
            index_id: index_id.to_string(),
            index_uri: index_uri.to_string(),
            checkpoint: Default::default(),
            doc_mapping,
            indexing_settings,
            search_settings,
            sources: Default::default(),
            create_timestamp: now_timestamp,
            update_timestamp: now_timestamp,
        }
    }

    pub(crate) fn add_source(&mut self, source: SourceConfig) -> MetastoreResult<()> {
        let entry = self.sources.entry(source.source_id.clone());
        let source_id = source.source_id.clone();
        if let Entry::Occupied(_) = entry {
            return Err(MetastoreError::SourceAlreadyExists {
                source_id: source_id.clone(),
                source_type: source.source_type,
            });
        }
        entry.or_insert(source);
        self.checkpoint.add_source(&source_id);
        Ok(())
    }

    pub(crate) fn delete_source(&mut self, source_id: &str) -> MetastoreResult<()> {
        self.sources
            .remove(source_id)
            .ok_or_else(|| MetastoreError::SourceDoesNotExist {
                source_id: source_id.to_string(),
            })?;
        self.checkpoint.remove_source(source_id);
        Ok(())
    }

    /// Builds and returns the doc mapper associated with index.
    pub fn build_doc_mapper(&self) -> anyhow::Result<Arc<dyn DocMapper>> {
        let mut builder = DocMapperBuilder::new();
        builder.default_search_fields = self.search_settings.default_search_fields.clone();
        builder.demux_field = self.indexing_settings.demux_field.clone();
        builder.sort_by = match self.indexing_settings.sort_by() {
            SortBy::DocId => None,
            SortBy::FastField { field_name, order } => Some(SortByConfig { field_name, order }),
        };
        builder.timestamp_field = self.indexing_settings.timestamp_field.clone();
        builder.field_mappings = self.doc_mapping.field_mappings.clone();
        builder.tag_fields = self.doc_mapping.tag_fields.iter().cloned().collect();
        builder.store_source = self.doc_mapping.store_source;
        Ok(Arc::new(builder.build()?))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct UnversionedIndexMetadata {
    pub index_id: String,
    pub index_uri: String,
    pub index_config: DefaultDocMapper,
    pub checkpoint: SourceCheckpoint,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "version")]
pub(crate) enum VersionedIndexMetadata {
    #[serde(rename = "0")]
    V0(IndexMetadataV0),
    #[serde(rename = "1")]
    V1(IndexMetadataV1),
    #[serde(rename = "unversioned")]
    Unversioned(UnversionedIndexMetadata),
}

impl From<IndexMetadata> for VersionedIndexMetadata {
    fn from(index_metadata: IndexMetadata) -> Self {
        VersionedIndexMetadata::V1(index_metadata.into())
    }
}

impl From<VersionedIndexMetadata> for IndexMetadata {
    fn from(index_metadata: VersionedIndexMetadata) -> Self {
        match index_metadata {
            VersionedIndexMetadata::Unversioned(unversioned) => unversioned.into(),
            VersionedIndexMetadata::V0(v0) => v0.into(),
            VersionedIndexMetadata::V1(v1) => v1.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct IndexMetadataV1 {
    pub index_id: String,
    pub index_uri: String,
    pub checkpoint: IndexCheckpoint,
    pub doc_mapping: DocMapping,
    #[serde(default)]
    pub indexing_settings: IndexingSettings,
    pub search_settings: SearchSettings,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<SourceConfig>,
    #[serde(default = "utc_now_timestamp")]
    pub create_timestamp: i64,
    #[serde(default = "utc_now_timestamp")]
    pub update_timestamp: i64,
}

impl From<IndexMetadata> for IndexMetadataV1 {
    fn from(index_metadata: IndexMetadata) -> Self {
        let sources = index_metadata
            .sources
            .into_values()
            .sorted_by(|left, right| left.source_id.cmp(&right.source_id))
            .collect();
        Self {
            index_id: index_metadata.index_id,
            index_uri: index_metadata.index_uri,
            checkpoint: index_metadata.checkpoint,
            doc_mapping: index_metadata.doc_mapping,
            indexing_settings: index_metadata.indexing_settings,
            search_settings: index_metadata.search_settings,
            sources,
            create_timestamp: index_metadata.create_timestamp,
            update_timestamp: index_metadata.update_timestamp,
        }
    }
}

impl From<IndexMetadataV1> for IndexMetadata {
    fn from(v1: IndexMetadataV1) -> Self {
        let sources = v1
            .sources
            .into_iter()
            .map(|source| (source.source_id.clone(), source))
            .collect();
        Self {
            index_id: v1.index_id,
            index_uri: v1.index_uri,
            checkpoint: v1.checkpoint,
            doc_mapping: v1.doc_mapping,
            indexing_settings: v1.indexing_settings,
            search_settings: v1.search_settings,
            sources,
            create_timestamp: v1.create_timestamp,
            update_timestamp: v1.update_timestamp,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct IndexMetadataV0 {
    pub index_id: String,
    pub index_uri: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "SourceCheckpoint::is_empty")]
    pub checkpoint: SourceCheckpoint,
    pub doc_mapping: DocMapping,
    #[serde(default)]
    pub indexing_settings: IndexingSettings,
    pub search_settings: SearchSettings,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<SourceConfig>,
    #[serde(default = "utc_now_timestamp")]
    pub create_timestamp: i64,
    #[serde(default = "utc_now_timestamp")]
    pub update_timestamp: i64,
}

impl From<IndexMetadataV0> for IndexMetadata {
    fn from(v0: IndexMetadataV0) -> Self {
        assert!(
            v0.sources.len() <= 1,
            "Failed to convert the Index metadata. Multiple sources was not really supported. \
             Contact Quickwit for help."
        );
        let mut source_checkpoints: BTreeMap<String, SourceCheckpoint> = Default::default();
        let mut sources: HashMap<String, SourceConfig> = Default::default();
        if let Some(single_source) = v0.sources.into_iter().next() {
            source_checkpoints.insert(single_source.source_id.clone(), v0.checkpoint);
            sources.insert(single_source.source_id.clone(), single_source);
        } else {
            // That's weird. We have a checkpoint but no sources.
            // Well just record the checkpoint for extra safety with the source_id `default-source`.
            if !v0.checkpoint.is_empty() {
                source_checkpoints.insert("default-source".to_string(), v0.checkpoint);
            }
        }
        let checkpoint = IndexCheckpoint::from(source_checkpoints);
        Self {
            index_id: v0.index_id,
            index_uri: v0.index_uri,
            checkpoint,
            doc_mapping: v0.doc_mapping,
            indexing_settings: v0.indexing_settings,
            search_settings: v0.search_settings,
            sources,
            create_timestamp: v0.create_timestamp,
            update_timestamp: v0.update_timestamp,
        }
    }
}

impl From<UnversionedIndexMetadata> for IndexMetadataV0 {
    fn from(unversioned: UnversionedIndexMetadata) -> Self {
        let doc_mapping = DocMapping {
            field_mappings: unversioned
                .index_config
                .field_mappings
                .field_mappings()
                .unwrap_or_else(Vec::new),
            tag_fields: unversioned.index_config.tag_field_names,
            store_source: unversioned.index_config.store_source,
        };
        let (sort_field, sort_order) = match unversioned.index_config.sort_by {
            SortBy::DocId => (None, None),
            SortBy::FastField { field_name, order } => (Some(field_name), Some(order)),
        };
        let indexing_settings = IndexingSettings {
            demux_field: unversioned.index_config.demux_field_name,
            timestamp_field: unversioned.index_config.timestamp_field_name,
            sort_field,
            sort_order,
            ..Default::default()
        };
        let search_settings = SearchSettings {
            default_search_fields: unversioned.index_config.default_search_field_names,
        };
        let now_timestamp = utc_now_timestamp();
        Self {
            index_id: unversioned.index_id,
            index_uri: unversioned.index_uri,
            checkpoint: unversioned.checkpoint,
            doc_mapping,
            indexing_settings,
            search_settings,
            sources: Default::default(),
            create_timestamp: now_timestamp,
            update_timestamp: now_timestamp,
        }
    }
}

impl From<UnversionedIndexMetadata> for IndexMetadata {
    fn from(unversioned: UnversionedIndexMetadata) -> Self {
        IndexMetadataV0::from(unversioned).into()
    }
}

impl<'de> Deserialize<'de> for IndexMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let value: serde_json::value::Value = serde_json::value::Value::deserialize(deserializer)?;
        let has_version_tag = value
            .as_object()
            .map(|obj| obj.contains_key("version"))
            .unwrap_or(false);
        if has_version_tag {
            return Ok(serde_json::from_value::<VersionedIndexMetadata>(value)
                .map_err(serde::de::Error::custom)?
                .into());
        };
        Ok(serde_json::from_value::<UnversionedIndexMetadata>(value)
            .map_err(serde::de::Error::custom)?
            .into())
    }
}
