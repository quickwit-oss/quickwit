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

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::bail;
use chrono::Utc;
use itertools::Itertools;
use quickwit_config::{
    DocMapping, IndexingResources, IndexingSettings, SearchSettings, SourceConfig,
};
use quickwit_index_config::{
    DefaultIndexConfig as DefaultDocMapper, DefaultIndexConfigBuilder as DocMapperBuilder,
    IndexConfig as DocMapper, SortBy, SortByConfig, SortOrder,
};
use serde::{Deserialize, Deserializer, Serialize};

use crate::checkpoint::Checkpoint;
use crate::split_metadata::utc_now_timestamp;

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
    pub checkpoint: Checkpoint,
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
        let now_timestamp = Utc::now().timestamp();
        Self {
            index_id: index_id.to_string(),
            index_uri: index_uri.to_string(),
            checkpoint: Checkpoint::default(),
            doc_mapping,
            indexing_settings,
            search_settings,
            sources: Default::default(),
            create_timestamp: now_timestamp,
            update_timestamp: now_timestamp,
        }
    }

    /// Returns the data source configured for the index.
    // TODO: Remove when support for multi-sources index is added.
    pub fn source(&self) -> anyhow::Result<SourceConfig> {
        if self.sources.len() > 1 {
            bail!("Multi-sources indexes are not supported (yet).")
        }
        self.sources.values().next().cloned().ok_or_else(|| {
            anyhow::anyhow!("No source is configured for the `{}` index.", self.index_id)
        })
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
    pub checkpoint: Checkpoint,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "version")]
pub(crate) enum VersionedIndexMetadata {
    #[serde(rename = "0")]
    V0(IndexMetadataV0),
    #[serde(rename = "unversioned")]
    Unversioned(UnversionedIndexMetadata),
}

impl From<IndexMetadata> for VersionedIndexMetadata {
    fn from(index_metadata: IndexMetadata) -> Self {
        VersionedIndexMetadata::V0(index_metadata.into())
    }
}

impl From<VersionedIndexMetadata> for IndexMetadata {
    fn from(index_metadata: VersionedIndexMetadata) -> Self {
        match index_metadata {
            VersionedIndexMetadata::Unversioned(unversioned) => unversioned.into(),
            VersionedIndexMetadata::V0(v0) => v0.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct IndexMetadataV0 {
    pub index_id: String,
    pub index_uri: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Checkpoint::is_empty")]
    pub checkpoint: Checkpoint,
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

impl From<IndexMetadata> for IndexMetadataV0 {
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

impl From<IndexMetadataV0> for IndexMetadata {
    fn from(v0: IndexMetadataV0) -> Self {
        let sources = v0
            .sources
            .into_iter()
            .map(|source| (source.source_id.clone(), source))
            .collect();
        Self {
            index_id: v0.index_id,
            index_uri: v0.index_uri,
            checkpoint: v0.checkpoint,
            doc_mapping: v0.doc_mapping,
            indexing_settings: v0.indexing_settings,
            search_settings: v0.search_settings,
            sources,
            create_timestamp: v0.create_timestamp,
            update_timestamp: v0.update_timestamp,
        }
    }
}

impl From<UnversionedIndexMetadata> for IndexMetadata {
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
        let now_timestamp = Utc::now().timestamp();
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
