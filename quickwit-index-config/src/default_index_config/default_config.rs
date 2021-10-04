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

use std::collections::HashSet;
use std::convert::TryFrom;

use anyhow::{bail, Context};
use quickwit_proto::SearchRequest;
use serde::{Deserialize, Serialize};
use serde_json::{self, Value as JsonValue};
use tantivy::query::Query;
use tantivy::schema::{
    Cardinality, FieldEntry, FieldType, FieldValue, Schema, SchemaBuilder, Value, STORED, STRING,
};
use tantivy::Document;

use super::field_mapping_entry::DocParsingError;
use super::{default_as_true, FieldMappingEntry, FieldMappingType};
use crate::config::convert_tag_to_string;
use crate::query_builder::build_query;
use crate::{IndexConfig, QueryParserError, SortBy, SortOrder, SOURCE_FIELD_NAME, TAGS_FIELD_NAME};

/// DefaultIndexConfigBuilder is here
/// to create a valid IndexConfig.
#[derive(Default, Serialize, Deserialize, Clone)]
pub struct DefaultIndexConfigBuilder {
    #[serde(default = "default_as_true")]
    store_source: bool,
    default_search_fields: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp_field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sort_by: Option<SortByConfig>,
    field_mappings: Vec<FieldMappingEntry>,
    #[serde(default)]
    tag_fields: Vec<String>,
}

#[derive(Default, Serialize, Deserialize, Clone)]
struct SortByConfig {
    /// Sort field name in the index schema.
    pub field_name: String,
    /// Sort order of the field.
    pub order: SortOrder,
}

impl From<SortByConfig> for SortBy {
    fn from(sort_by_config: SortByConfig) -> Self {
        SortBy::SortByFastField {
            field_name: sort_by_config.field_name,
            order: sort_by_config.order,
        }
    }
}

fn resolve_sort_field(
    sort_config_opt: Option<SortByConfig>,
    schema: &Schema,
) -> anyhow::Result<Option<SortBy>> {
    if let Some(sort_by_config) = sort_config_opt {
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
        return Ok(Some(SortBy::from(sort_by_config)));
    }
    Ok(None)
}

impl DefaultIndexConfigBuilder {
    /// Create a new `DefaultIndexConfigBuilder` for tests.
    pub fn new() -> Self {
        Self {
            store_source: true,
            default_search_fields: vec![],
            timestamp_field: None,
            sort_by: None,
            field_mappings: vec![],
            tag_fields: vec![],
        }
    }

    /// Build a valid `DefaultIndexConfig`.
    /// This will consume your `DefaultIndexConfigBuilder`.
    pub fn build(self) -> anyhow::Result<DefaultIndexConfig> {
        let schema = self.build_schema()?;
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

        // Resolve timestamp field
        if let Some(ref timestamp_field_name) = self.timestamp_field {
            let timestamp_field = schema
                .get_field(timestamp_field_name)
                .with_context(|| format!("Unknown timestamp field: `{}`", timestamp_field_name))?;

            let timestamp_field_entry = schema.get_field_entry(timestamp_field);
            if !timestamp_field_entry.is_fast() {
                bail!(
                    "Timestamp field must be a fast field, please add the fast property to your \
                     field `{}`.",
                    timestamp_field_name
                )
            }
            match timestamp_field_entry.field_type() {
                FieldType::I64(options) | FieldType::U64(options) => {
                    if options.get_fastfield_cardinality() == Some(Cardinality::MultiValues) {
                        bail!(
                            "Timestamp field cannot be an array, please change your field `{}` \
                             from an array to a single value.",
                            timestamp_field_name
                        )
                    }
                }
                _ => {
                    bail!(
                        "Timestamp field must be either of type i64 or u64, please change your \
                         field type `{}` to i64 or u64.",
                        timestamp_field_name
                    )
                }
            }
        }

        // Resolve sort field
        let sort_by = resolve_sort_field(self.sort_by, &schema)?;

        // Resolve tag fields
        let mut tag_field_names = Vec::new();
        for tag_field_name in self.tag_fields.iter() {
            if tag_field_names.contains(tag_field_name) {
                bail!("Duplicated tag field: `{}`", tag_field_name)
            }
            schema
                .get_field(tag_field_name)
                .with_context(|| format!("Unknown tag field: `{}`", tag_field_name))?;
            tag_field_names.push(tag_field_name.clone());
        }

        // Build the root mapping entry, it has an empty name so that we don't prefix all
        // field name with it.
        let field_mappings = FieldMappingEntry::root(FieldMappingType::Object(self.field_mappings));
        Ok(DefaultIndexConfig {
            schema,
            store_source: self.store_source,
            default_search_field_names,
            timestamp_field_name: self.timestamp_field,
            sort_by,
            field_mappings,
            tag_field_names,
        })
    }

    /// Build the schema from the field mappings and store_source parameter.
    fn build_schema(&self) -> anyhow::Result<Schema> {
        let mut builder = SchemaBuilder::new();
        builder.add_text_field(TAGS_FIELD_NAME, STRING);

        let mut unique_field_names: HashSet<String> = HashSet::new();
        for field_mapping in self.field_mappings.iter() {
            for (field_path, field_type) in field_mapping.field_entries()? {
                let field_name = field_path.field_name();
                if field_name == SOURCE_FIELD_NAME {
                    bail!("`_source` is a reserved name, change your field name.");
                }
                if field_name == TAGS_FIELD_NAME {
                    bail!("`_tags` is a reserved name, change your field name.");
                }
                if unique_field_names.contains(&field_name) {
                    bail!(
                        "Field name must be unique, found duplicates for `{}`",
                        field_name
                    );
                }
                unique_field_names.insert(field_name.clone());
                builder.add_field(FieldEntry::new(field_name, field_type));
            }
        }
        if self.store_source {
            builder.add_text_field(SOURCE_FIELD_NAME, STORED);
        }

        Ok(builder.build())
    }
}

impl TryFrom<DefaultIndexConfigBuilder> for DefaultIndexConfig {
    type Error = anyhow::Error;

    fn try_from(value: DefaultIndexConfigBuilder) -> Result<DefaultIndexConfig, Self::Error> {
        value.build()
    }
}

impl From<DefaultIndexConfig> for DefaultIndexConfigBuilder {
    fn from(value: DefaultIndexConfig) -> Self {
        let sort_by_config = value
            .sort_by
            .as_ref()
            .map(|sort_by| match sort_by {
                SortBy::SortByFastField { field_name, order } => Some(SortByConfig {
                    field_name: field_name.clone(),
                    order: *order,
                }),
                SortBy::DocId => None,
            })
            .flatten();
        Self {
            store_source: value.store_source,
            timestamp_field: value.timestamp_field_name(),
            sort_by: sort_by_config,
            default_search_fields: value.default_search_field_names,
            field_mappings: value
                .field_mappings
                .field_mappings()
                .unwrap_or_else(Vec::new),
            tag_fields: value.tag_field_names,
        }
    }
}

/// Default [`IndexConfig`] implementation
/// which defines a set of rules to map json fields
/// to tantivy index fields.
///
/// The mains rules are defined by the field mappings.
#[derive(Serialize, Deserialize, Clone)]
#[serde(
    try_from = "DefaultIndexConfigBuilder",
    into = "DefaultIndexConfigBuilder"
)]
pub struct DefaultIndexConfig {
    /// Store the json source in a text field _source.
    store_source: bool,
    /// Default list of field names used for search.
    default_search_field_names: Vec<String>,
    /// Timestamp field name.
    timestamp_field_name: Option<String>,
    /// Sort field name and order.
    sort_by: Option<SortBy>,
    /// List of field mappings which defines how a json field is mapped to index fields.
    field_mappings: FieldMappingEntry,
    /// Schema generated by the store source and field mappings parameters.
    #[serde(skip_serializing)]
    schema: Schema,
    /// List of field names used for tagging.
    tag_field_names: Vec<String>,
}

impl std::fmt::Debug for DefaultIndexConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter
            .debug_struct("DefaultIndexConfig")
            .field("store_source", &self.store_source)
            .field(
                "default_search_field_names",
                &self.default_search_field_names,
            )
            .field("timestamp_field_name", &self.timestamp_field_name())
            // TODO: complete it.
            .finish()
    }
}

#[typetag::serde(name = "default")]
impl IndexConfig for DefaultIndexConfig {
    fn doc_from_json(&self, doc_json: String) -> Result<Document, DocParsingError> {
        let mut document = Document::default();
        let json_obj: JsonValue = serde_json::from_str(&doc_json).map_err(|_| {
            // FIXME: the error contains some useful information (line, column, ...) that we could
            // surface to the user.
            let doc_json_sample = format!("{:?}...", &doc_json[0..doc_json.len().min(20)]);
            DocParsingError::NotJson(doc_json_sample)
        })?;
        let field_paths_and_values = self.field_mappings.parse(json_obj)?;
        let tags_field_opt = self.schema.get_field(TAGS_FIELD_NAME);
        for (field_path, field_value) in field_paths_and_values {
            let field_name = field_path.field_name();
            let field = self
                .schema
                .get_field(&field_name)
                .ok_or_else(|| DocParsingError::NoSuchFieldInSchema(field_name.clone()))?;
            if self.tag_field_names.contains(&field_name) {
                let tags_field = tags_field_opt.ok_or_else(|| {
                    DocParsingError::NoSuchFieldInSchema(TAGS_FIELD_NAME.to_string())
                })?;
                document.add(FieldValue::new(tags_field, Value::Str(convert_tag_to_string(&field_name, &field_value))));
            }
            document.add(FieldValue::new(field, field_value))
        }
        if self.store_source {
            let source = self.schema.get_field(SOURCE_FIELD_NAME).ok_or_else(|| {
                DocParsingError::NoSuchFieldInSchema(SOURCE_FIELD_NAME.to_string())
            })?;
            // We avoid `document.add_text` here because it systematically performs a copy of the
            // value.
            document.add(FieldValue::new(source, Value::Str(doc_json)));
        }
        Ok(document)
    }

    fn query(
        &self,
        split_schema: Schema,
        request: &SearchRequest,
    ) -> Result<Box<dyn Query>, QueryParserError> {
        build_query(split_schema, request, &self.default_search_field_names)
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn timestamp_field_name(&self) -> Option<String> {
        self.timestamp_field_name.clone()
    }

    fn sort_by(&self) -> crate::SortBy {
        self.sort_by.clone().unwrap_or(crate::SortBy::DocId)
    }

    fn tag_field_names(&self) -> Vec<String> {
        self.tag_field_names.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use anyhow::bail;
    use serde_json::{self, Value as JsonValue};

    use super::DefaultIndexConfig;
    use crate::{
        DefaultIndexConfigBuilder, DocParsingError, IndexConfig, SortBy, SortOrder,
        SOURCE_FIELD_NAME, TAGS_FIELD_NAME,
    };

    const JSON_DOC_VALUE: &str = r#"
        {
            "timestamp": 1586960586000,
            "body": "20200415T072306-0700 INFO This is a great log",
            "response_date": "2021-12-19T16:39:57Z",
            "response_time": 2.3,
            "response_payload": "YWJj",
            "owner": "foo",
            "attributes": {
                "server": "ABC",
                "tags": [22, 23],
                "server.status": ["200", "201"],
                "server.payload": ["YQ==", "Yg=="]
            }
        }"#;

    const EXPECTED_JSON_PATHS_AND_VALUES: &str = r#"{
            "timestamp": [1586960586000],
            "body": ["20200415T072306-0700 INFO This is a great log"],
            "response_date": ["2021-12-19T16:39:57+00:00"],
            "response_time": [2.3],
            "response_payload": [[97,98,99]],
            "owner": ["foo"],
            "body_other_tokenizer": ["20200415T072306-0700 INFO This is a great log"],
            "attributes.server": ["ABC"],
            "attributes.server.payload": [[97], [98]],
            "attributes.tags": [22, 23],
            "attributes.server.status": ["200", "201"]
        }"#;

    #[test]
    fn test_json_deserialize() -> anyhow::Result<()> {
        let config = crate::default_config_for_tests();
        assert!(config.store_source);
        let mut default_search_field_names: Vec<String> = config.default_search_field_names;
        default_search_field_names.sort();
        assert_eq!(
            default_search_field_names,
            ["attributes.server", "attributes.server.status", "body"]
        );
        let field_mappings = config.field_mappings.field_mappings().unwrap_or_default();
        assert_eq!(field_mappings.len(), 7);
        Ok(())
    }

    #[test]
    fn test_json_serialize() -> anyhow::Result<()> {
        let mut config = crate::default_config_for_tests();
        let json_config = serde_json::to_string_pretty(&config)?;
        let mut config_after_serialization =
            serde_json::from_str::<DefaultIndexConfig>(&json_config)?;
        assert_eq!(config.store_source, config_after_serialization.store_source);

        config.default_search_field_names.sort();
        config_after_serialization.default_search_field_names.sort();
        assert_eq!(
            config.default_search_field_names,
            config_after_serialization.default_search_field_names
        );
        assert_eq!(config.schema, config_after_serialization.schema);
        assert_eq!(
            config.timestamp_field_name,
            config_after_serialization.timestamp_field_name
        );
        assert_eq!(
            config.sort_by.unwrap(),
            config_after_serialization.sort_by.unwrap()
        );
        Ok(())
    }

    #[test]
    fn test_parsing_document() -> anyhow::Result<()> {
        let index_config = crate::default_config_for_tests();
        let document = index_config.doc_from_json(JSON_DOC_VALUE.to_string())?;
        let schema = index_config.schema();
        // 7 property entry + 1 field "_source" + two fields values for "tags" field
        // + 2 values inf "server.status" field + 2 values in "server.payload" field
        // + 1 value for special `_tags`
        assert_eq!(document.len(), 15);
        let expected_json_paths_and_values: HashMap<String, JsonValue> =
            serde_json::from_str(EXPECTED_JSON_PATHS_AND_VALUES).unwrap();
        document.field_values().iter().for_each(|field_value| {
            let field_name = schema.get_field_name(field_value.field());
            if field_name == SOURCE_FIELD_NAME {
                assert_eq!(field_value.value().text().unwrap(), JSON_DOC_VALUE);
            } else if field_name == TAGS_FIELD_NAME {
                assert_eq!(field_value.value().text().unwrap(), "owner:foo");
            } else {
                let value = serde_json::to_string(field_value.value()).unwrap();
                let is_value_in_expected_values = expected_json_paths_and_values
                    .get(field_name)
                    .unwrap()
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|expected_value| format!("{}", expected_value))
                    .any(|expected_value| expected_value == value);
                assert!(is_value_in_expected_values);
            }
        });
        Ok(())
    }

    #[test]
    fn test_accept_parsing_document_with_unknown_fields_and_missing_fields() -> anyhow::Result<()> {
        let index_config = crate::default_config_for_tests();
        index_config.doc_from_json(
            r#"{
                "timestamp": 1586960586000,
                "unknown_field": "20200415T072306-0700 INFO This is a great log"
            }"#
            .to_string(),
        )?;
        Ok(())
    }

    #[test]
    fn test_fail_to_parse_document_with_wrong_cardinality() -> anyhow::Result<()> {
        let index_config = crate::default_config_for_tests();
        let result = index_config.doc_from_json(
            r#"{
                "timestamp": 1586960586000,
                "body": ["text 1", "text 2"]
            }"#
            .to_string(),
        );
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(
            error,
            DocParsingError::MultiValuesNotSupported("body".to_owned())
        );
        Ok(())
    }

    #[test]
    fn test_fail_to_parse_document_with_wrong_value() -> anyhow::Result<()> {
        let index_config = crate::default_config_for_tests();
        let result = index_config.doc_from_json(
            r#"{
                "timestamp": 1586960586000,
                "body": 1
            }"#
            .to_string(),
        );
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(
            error,
            DocParsingError::ValueError(
                "body".to_owned(),
                "Expected JSON string, got '1'.".to_owned()
            )
        );
        Ok(())
    }

    #[test]
    fn test_fail_to_build_index_config_with_non_fast_timestamp_field() -> anyhow::Result<()> {
        let index_config = r#"{
            "type": "default",
            "default_search_fields": [],
            "timestamp_field": "timestamp",
            "tag_fields": [],
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "text"
                }
            ]
        }"#;
        let builder = serde_json::from_str::<DefaultIndexConfigBuilder>(index_config)?;
        let expected_msg = "Timestamp field must be a fast field, please add the fast property to \
                            your field `timestamp`."
            .to_string();
        assert_eq!(builder.build().unwrap_err().to_string(), expected_msg);
        Ok(())
    }

    #[test]
    fn test_fail_to_build_index_config_with_multivalued_timestamp_field() -> anyhow::Result<()> {
        let index_config = r#"{
            "type": "default",
            "default_search_fields": [],
            "timestamp_field": "timestamp",
            "tag_fields": [],
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "array<i64>",
                    "fast": true
                }
            ]
        }"#;

        let builder = serde_json::from_str::<DefaultIndexConfigBuilder>(index_config)?;
        let expected_msg = "Timestamp field cannot be an array, please change your field \
                            `timestamp` from an array to a single value."
            .to_string();
        assert_eq!(builder.build().unwrap_err().to_string(), expected_msg);
        Ok(())
    }

    #[test]
    fn test_fail_with_field_name_equal_to_source() -> anyhow::Result<()> {
        let index_config = r#"{
            "type": "default",
            "default_search_fields": [],
            "tag_fields": [],
            "field_mappings": [
                {
                    "name": "_source",
                    "type": "i64"
                }
            ]
        }"#;

        let builder = serde_json::from_str::<DefaultIndexConfigBuilder>(index_config)?;
        let expected_msg = "`_source` is a reserved name, change your field name.".to_string();
        assert_eq!(builder.build().unwrap_err().to_string(), expected_msg);
        Ok(())
    }

    #[test]
    fn test_fail_to_parse_document_with_wrong_base64_value() -> anyhow::Result<()> {
        let index_config = r#"{
            "type": "default",
            "default_search_fields": [],
            "timestamp_field": null,
            "tag_fields": ["image"],
            "field_mappings": [
                {
                    "name": "image",
                    "type": "bytes",
                    "stored": true
                }
            ]
        }"#;

        let builder = serde_json::from_str::<DefaultIndexConfigBuilder>(index_config)?;
        let index_config = builder.build()?;
        let result = index_config.doc_from_json(
            r#"{
            "city": "paris",
            "image": "invalid base64 data"
        }"#
            .to_string(),
        );
        let expected_msg = "The field 'image' could not be parsed: Expected Base64 string, got \
                            'invalid base64 data'.";
        assert_eq!(result.unwrap_err().to_string(), expected_msg);
        Ok(())
    }

    #[test]
    fn test_parse_document_with_tag_fields() -> anyhow::Result<()> {
        let index_config = r#"{
            "type": "default",
            "default_search_fields": [],
            "timestamp_field": null,
            "tag_fields": ["city", "image"],
            "field_mappings": [
                {
                    "name": "city",
                    "type": "text",
                    "stored": true
                },
                {
                    "name": "image",
                    "type": "bytes",
                    "stored": true
                }
            ]
        }"#;

        let builder = serde_json::from_str::<DefaultIndexConfigBuilder>(index_config)?;
        let index_config = builder.build()?;
        let schema = index_config.schema();
        const JSON_DOC_VALUE: &str = r#"{
            "city": "tokio",
            "image": "YWJj"
        }"#;
        let document = index_config.doc_from_json(JSON_DOC_VALUE.to_string())?;

        // 2 properties, + 1 value for "_source" + 2 values for "_tags"
        assert_eq!(document.len(), 5);
        let expected_json_paths_and_values: HashMap<String, JsonValue> = serde_json::from_str(
            r#"{
                "city": ["tokio"],
                "image": [[97,98,99]]
            }"#,
        )
        .unwrap();
        document.field_values().iter().for_each(|field_value| {
            let field_name = schema.get_field_name(field_value.field());
            if field_name == SOURCE_FIELD_NAME {
                assert_eq!(field_value.value().text().unwrap(), JSON_DOC_VALUE);
            } else if field_name == TAGS_FIELD_NAME {
                assert!(
                    vec!["city:tokio", "image:YWJj"].contains(&field_value.value().text().unwrap())
                );
            } else {
                let value = serde_json::to_string(field_value.value()).unwrap();
                let is_value_in_expected_values = expected_json_paths_and_values
                    .get(field_name)
                    .unwrap()
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|expected_value| format!("{}", expected_value))
                    .any(|expected_value| expected_value == value);
                assert!(is_value_in_expected_values);
            }
        });
        Ok(())
    }

    #[test]
    fn test_fail_to_build_index_config_with_non_fast_sort_by_field() -> anyhow::Result<()> {
        let index_config = r#"{
            "type": "default",
            "default_search_fields": [],
            "sort_by": {
                "field_name": "timestamp",
                "order": "asc"
            },
            "tag_fields": [],
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "text"
                }
            ]
        }"#;
        let builder = serde_json::from_str::<DefaultIndexConfigBuilder>(index_config)?;
        let expected_msg = "Sort by field must be a fast field, please add the fast property to \
                            your field `timestamp`."
            .to_string();
        assert_eq!(builder.build().unwrap_err().to_string(), expected_msg);
        Ok(())
    }

    #[test]
    fn test_build_index_config_with_sort_by_field_asc() -> anyhow::Result<()> {
        let index_config = r#"{
            "type": "default",
            "default_search_fields": [],
            "sort_by": {
                "field_name": "timestamp",
                "order": "asc"
            },
            "tag_fields": [],
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "u64",
                    "fast": true
                }
            ]
        }"#;
        let builder = serde_json::from_str::<DefaultIndexConfigBuilder>(index_config)?.build()?;
        match builder.sort_by() {
            SortBy::SortByFastField { field_name, order } => {
                assert_eq!(field_name, "timestamp");
                assert_eq!(order, SortOrder::Asc);
            }
            _ => bail!("Sort by must be a SortByFastField."),
        };
        Ok(())
    }

    #[test]
    fn test_build_index_config_with_sort_by_doc_id_when_no_sort_field_is_specified(
    ) -> anyhow::Result<()> {
        let index_config = r#"{
            "type": "default",
            "default_search_fields": [],
            "tag_fields": [],
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "u64",
                    "fast": true
                }
            ]
        }"#;
        let builder = serde_json::from_str::<DefaultIndexConfigBuilder>(index_config)?.build()?;
        match builder.sort_by() {
            SortBy::DocId => (),
            _ => bail!("Sort by must be DocId."),
        };
        Ok(())
    }

    #[test]
    fn test_index_config_with_a_u64_timestamp_field_is_valid() -> anyhow::Result<()> {
        let index_config = r#"{
            "type": "default",
            "default_search_fields": [],
            "tag_fields": [],
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "u64",
                    "fast": true
                }
            ]
        }"#;
        let _ = serde_json::from_str::<DefaultIndexConfigBuilder>(index_config)?.build()?;
        Ok(())
    }
}
