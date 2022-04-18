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
use std::convert::TryFrom;

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use tantivy::schema::{Cardinality, Field, FieldType, Schema, STORED};
use tracing::info;

use super::FieldMappingEntry;
use crate::default_doc_mapper::default_mapper::Mode;
use crate::default_doc_mapper::mapping_tree::{build_mapping_tree, MappingNode, MappingTree};
use crate::default_doc_mapper::QuickwitJsonOptions;
use crate::sort_by::SortBy;
use crate::{DefaultDocMapper, DocMapper, SortByConfig, DYNAMIC_FIELD_NAME, SOURCE_FIELD_NAME};

/// Name of the raw tokenizer.
const RAW_TOKENIZER_NAME: &str = "raw";

fn validate_tag_fields(tag_fields: &[String], schema: &Schema) -> anyhow::Result<()> {
    for tag_field in tag_fields {
        let field = schema
            .get_field(tag_field)
            .ok_or_else(|| anyhow::anyhow!("Tag field `{}` does not exist.", tag_field))?;
        let field_type = schema.get_field_entry(field).field_type();
        match field_type {
            FieldType::Str(options) => {
                let tokenizer_opt = options
                    .get_indexing_options()
                    .map(|text_options| text_options.tokenizer());

                if tokenizer_opt != Some(RAW_TOKENIZER_NAME) {
                    bail!(
                        "Tags collection is only allowed on text fields with the `raw` tokenizer."
                    );
                }
            }
            FieldType::Bytes(_) => {
                bail!("Tags collection is not allowed on `bytes` fields.")
            }
            _ => (),
        }
    }
    Ok(())
}

/// DefaultDocMapperBuilder is here
/// to create a valid DocMapper.
///
/// It is also used to serialize/deserialize a DocMapper.
/// note that this is not the way is the DocMapping is deserialized
/// from the configuration.
#[derive(Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DefaultDocMapperBuilder {
    /// Stores the original source document when set to true.
    #[serde(default)]
    pub store_source: bool,
    /// Name of the fields that are searched by default, unless overridden.
    #[serde(default)]
    pub default_search_fields: Vec<String>,
    /// Name of the field storing the timestamp of the event for time series data.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_field: Option<String>,
    /// Specifies the name of the sort field and the sort order.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_by: Option<SortByConfig>,
    /// Describes which fields are indexed and how.
    #[serde(default)]
    pub field_mappings: Vec<FieldMappingEntry>,
    /// Name of the fields that are tagged.
    #[serde(default)]
    pub tag_fields: Vec<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Name of the field to demux by.
    pub demux_field: Option<String>,
    /// Defines the indexing mode.
    #[serde(default)]
    pub mode: ModeType,
    /// If mode is set to dynamic, `dynamic_mapping` defines
    /// how the unmapped fields should be handled.
    #[serde(default)]
    pub dynamic_mapping: Option<QuickwitJsonOptions>,
}

/// `Mode` describing how the unmapped field should be handled.
#[derive(Serialize, Deserialize, Copy, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ModeType {
    /// Lenient mode: unmapped fields are just ignored.
    Lenient,
    /// Strict mode: when parsing a document with an unmapped field, an error is yielded.
    Strict,
    /// Dynamic mode: unmapped fields are captured and handled according to the
    /// `dynamic_mapping` configuration.
    Dynamic,
}

impl Default for ModeType {
    fn default() -> Self {
        ModeType::Lenient
    }
}

#[cfg(test)]
impl Default for DefaultDocMapperBuilder {
    fn default() -> Self {
        serde_json::from_str("{}").unwrap()
    }
}

impl DefaultDocMapperBuilder {
    pub(crate) fn mode(&self) -> anyhow::Result<Mode> {
        if self.mode != ModeType::Dynamic && self.dynamic_mapping.is_some() {
            bail!(
                "`dynamic_mapping` is only allowed with mode=dynamic. (Here mode=`{:?}`)",
                self.mode
            );
        }
        Ok(match self.mode {
            ModeType::Lenient => Mode::Lenient,
            ModeType::Strict => Mode::Strict,
            ModeType::Dynamic => Mode::Dynamic(self.dynamic_mapping.clone().unwrap_or_default()),
        })
    }

    /// Build a valid `DefaultDocMapper`.
    /// This will consume your `DefaultDocMapperBuilder`.
    pub fn build(self) -> anyhow::Result<DefaultDocMapper> {
        let mode = self.mode()?;
        let mut schema_builder = Schema::builder();
        let field_mappings = build_mapping_tree(&self.field_mappings, &mut schema_builder)?;
        let source_field = if self.store_source {
            Some(schema_builder.add_text_field(SOURCE_FIELD_NAME, STORED))
        } else {
            None
        };

        let dynamic_field = if let Mode::Dynamic(json_options) = &mode {
            Some(schema_builder.add_json_field(DYNAMIC_FIELD_NAME, json_options.clone()))
        } else {
            None
        };

        let schema = schema_builder.build();

        // validate fast fields
        validate_tag_fields(&self.tag_fields, &schema)?;

        // Resolve default search fields
        let mut default_search_field_names = Vec::new();
        for field_name in self.default_search_fields.iter() {
            if default_search_field_names.contains(field_name) {
                bail!("Duplicated default search field: `{}`", field_name)
            }
            schema
                .get_field(field_name)
                .with_context(|| format!("Unknown default search field: `{}`", field_name))?;
            default_search_field_names.push(field_name.clone());
        }

        resolve_timestamp_field(self.timestamp_field.as_ref(), &schema)?;
        resolve_demux_field(self.demux_field.as_ref(), &schema)?;
        let sort_by = resolve_sort_field(self.sort_by, &schema)?;

        // Resolve tag fields
        let mut tag_field_names: BTreeSet<String> = Default::default();
        for tag_field_name in self.tag_fields.iter() {
            if tag_field_names.contains(tag_field_name) {
                bail!("Duplicated tag field: `{}`", tag_field_name)
            }
            schema
                .get_field(tag_field_name)
                .with_context(|| format!("Unknown tag field: `{}`", tag_field_name))?;
            tag_field_names.insert(tag_field_name.clone());
        }
        if let Some(ref demux_field_name) = self.demux_field {
            if !tag_field_names.contains(demux_field_name) {
                info!(
                    "Demux field name `{}` is not in index config tags, add it automatically.",
                    demux_field_name
                );
                tag_field_names.insert(demux_field_name.clone());
            }
        }

        let required_fields = list_required_fields_for_node(&field_mappings);

        Ok(DefaultDocMapper {
            schema,
            source_field,
            dynamic_field,
            default_search_field_names,
            timestamp_field_name: self.timestamp_field,
            sort_by,
            field_mappings,
            tag_field_names,
            required_fields,
            demux_field_name: self.demux_field,
            mode,
        })
    }
}

fn list_required_fields_for_node(node: &MappingNode) -> Vec<Field> {
    node.children().flat_map(list_required_fields).collect()
}

fn list_required_fields(field_mappings: &MappingTree) -> Vec<Field> {
    match field_mappings {
        MappingTree::Leaf(leaf) => {
            if leaf.get_type().is_fast_field() {
                vec![leaf.field()]
            } else {
                Vec::new()
            }
        }
        MappingTree::Node(node) => list_required_fields_for_node(node),
    }
}

fn resolve_timestamp_field(
    timestamp_field_name_opt: Option<&String>,
    schema: &Schema,
) -> anyhow::Result<()> {
    if let Some(ref timestamp_field_name) = timestamp_field_name_opt {
        let timestamp_field = schema
            .get_field(timestamp_field_name)
            .with_context(|| format!("Unknown timestamp field: `{}`", timestamp_field_name))?;

        let timestamp_field_entry = schema.get_field_entry(timestamp_field);
        if !timestamp_field_entry.is_fast() {
            bail!(
                "Timestamp field must be a fast field, please add the fast property to your field \
                 `{}`.",
                timestamp_field_name
            )
        }
        match timestamp_field_entry.field_type() {
            FieldType::I64(options) => {
                if options.get_fastfield_cardinality() == Some(Cardinality::MultiValues) {
                    bail!(
                        "Timestamp field cannot be an array, please change your field `{}` from \
                         an array to a single value.",
                        timestamp_field_name
                    )
                }
            }
            _ => {
                bail!(
                    "Timestamp field must be of type i64, please change your field type `{}` to \
                     i64.",
                    timestamp_field_name
                )
            }
        }
    }
    Ok(())
}

fn resolve_sort_field(
    sort_by_config_opt: Option<SortByConfig>,
    schema: &Schema,
) -> anyhow::Result<SortBy> {
    if let Some(sort_by_config) = sort_by_config_opt {
        let sort_by_field = schema
            .get_field(&sort_by_config.field_name)
            .with_context(|| format!("Unknown sort by field: `{}`", sort_by_config.field_name))?;

        let sort_by_field_entry = schema.get_field_entry(sort_by_field);
        if !sort_by_field_entry.is_fast() {
            bail!(
                "Sort by field must be a fast field, please add the fast property to your field \
                 `{}`.",
                sort_by_config.field_name
            )
        }
        return Ok(sort_by_config.into());
    }
    Ok(SortBy::DocId)
}

fn resolve_demux_field(
    demux_field_name_opt: Option<&String>,
    schema: &Schema,
) -> anyhow::Result<()> {
    if let Some(demux_field_name) = demux_field_name_opt {
        let demux_field = schema
            .get_field(demux_field_name)
            .with_context(|| format!("Unknown demux field: `{}`", demux_field_name))?;

        let demux_field_entry = schema.get_field_entry(demux_field);
        if !demux_field_entry.is_fast() {
            bail!(
                "Demux field must be a fast field, please add the fast property to your field \
                 `{}`.",
                demux_field_name
            )
        }
        if !demux_field_entry.is_indexed() {
            bail!(
                "Demux field must be indexed, please add the indexed property to your field `{}`.",
                demux_field_name
            )
        }
        match demux_field_entry.field_type() {
            FieldType::U64(options) | FieldType::I64(options) => {
                if options.get_fastfield_cardinality() == Some(Cardinality::MultiValues) {
                    bail!(
                        "Demux field cannot be an array, please change your field `{}` from an \
                         array to a single value.",
                        demux_field_name
                    )
                }
            }
            _ => {
                bail!(
                    "Demux field must be of type u64 or i64, please change your field type `{}` \
                     to u64 or i64.",
                    demux_field_name
                )
            }
        }
    }
    Ok(())
}

impl TryFrom<DefaultDocMapperBuilder> for DefaultDocMapper {
    type Error = anyhow::Error;

    fn try_from(value: DefaultDocMapperBuilder) -> Result<DefaultDocMapper, Self::Error> {
        value.build()
    }
}

impl From<DefaultDocMapper> for DefaultDocMapperBuilder {
    fn from(default_doc_mapper: DefaultDocMapper) -> Self {
        let sort_by_config = match &default_doc_mapper.sort_by {
            SortBy::DocId => None,
            SortBy::FastField { field_name, order } => Some(SortByConfig {
                field_name: field_name.clone(),
                order: *order,
            }),
        };
        let demux_field = default_doc_mapper.demux_field_name();
        let mode = default_doc_mapper.mode.mode_type();
        let dynamic_mapping = match &default_doc_mapper.mode {
            Mode::Dynamic(mapping_options) => Some(mapping_options.clone()),
            _ => None,
        };
        Self {
            store_source: default_doc_mapper.source_field.is_some(),
            timestamp_field: default_doc_mapper.timestamp_field_name(),
            field_mappings: default_doc_mapper.field_mappings.into(),
            demux_field,
            sort_by: sort_by_config,
            tag_fields: default_doc_mapper.tag_field_names.into_iter().collect(),
            default_search_fields: default_doc_mapper.default_search_field_names,
            mode,
            dynamic_mapping,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_mapper_builder_deserialize_from_empty_object() {
        let default_mapper_builder: DefaultDocMapperBuilder =
            serde_json::from_str::<DefaultDocMapperBuilder>("{}").unwrap();
        assert!(default_mapper_builder.default_search_fields.is_empty());
        assert!(default_mapper_builder.field_mappings.is_empty());
        assert!(default_mapper_builder.tag_fields.is_empty());
        assert_eq!(default_mapper_builder.mode, ModeType::Lenient);
        assert!(default_mapper_builder.dynamic_mapping.is_none());
        assert!(default_mapper_builder.demux_field.is_none());
        assert!(default_mapper_builder.sort_by.is_none());
        assert_eq!(default_mapper_builder.store_source, false);
        assert!(default_mapper_builder.timestamp_field.is_none());
    }

    #[test]
    fn test_default_mapper_builder_extra_field() {
        assert!(
            serde_json::from_str::<DefaultDocMapperBuilder>(r#"{"unknownfield": "blop"}"#).is_err()
        );
    }
}
