/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use super::default_as_true;
use super::{
    field_mapping_entry::DocParsingError, resolve_field_name, FieldMappingEntry, FieldMappingType,
};
use crate::{DocMapper, QueryParserError};
use anyhow::{bail, Context};
use quickwit_proto::SearchRequest;
use serde::{Deserialize, Serialize};
use serde_json::{self, Value as JsonValue};
use std::{collections::HashSet, convert::TryFrom};
use tantivy::schema::Cardinality;
use tantivy::{
    query::Query,
    schema::{FieldEntry, FieldType, FieldValue, Schema, SchemaBuilder, STORED},
    Document,
};

static SOURCE_FIELD_NAME: &str = "_source";

/// DefaultDocMapperBuilder is here
/// to create a valid DefaultDocMapper.
#[derive(Default, Serialize, Deserialize, Clone)]
pub struct DefaultDocMapperBuilder {
    #[serde(default = "default_as_true")]
    store_source: bool,
    default_search_fields: Vec<String>,
    timestamp_field: Option<String>,
    field_mappings: Vec<FieldMappingEntry>,
}

impl DefaultDocMapperBuilder {
    /// Create a new `DefaultDocMapperBuilder`.
    // TODO: either remove it or complete implementation
    // with methods to make possible to add / remove
    // default search fields and field mappings.
    pub fn new() -> Self {
        Self {
            store_source: true,
            default_search_fields: vec![],
            timestamp_field: None,
            field_mappings: vec![],
        }
    }

    /// Build a valid `DefaultDocMapper`.
    /// This will consume your `DefaultDocMapperBuilder`.
    pub fn build(self) -> anyhow::Result<DefaultDocMapper> {
        let schema = self.build_schema()?;
        // Resolve default search fields
        let mut default_search_field_names = Vec::new();
        for field_name in self.default_search_fields.iter() {
            if default_search_field_names.contains(field_name) {
                bail!("Duplicated default search field: `{}`", field_name)
            }
            resolve_field_name(&schema, &field_name)
                .with_context(|| format!("Unknown default search field: `{}`", field_name))?;
            default_search_field_names.push(field_name.clone());
        }
        // Resolve timestamp field
        if let Some(ref timestamp_field_name) = self.timestamp_field {
            let timestamp_field = resolve_field_name(&schema, timestamp_field_name)
                .with_context(|| format!("Unknown timestamp field: `{}`", timestamp_field_name))?;

            let timestamp_field_entry = schema.get_field_entry(timestamp_field);
            if !timestamp_field_entry.is_fast() {
                bail!("Timestamp field must be a fast field, please add fast property to your field `{}`.", timestamp_field_name)
            }
            if let FieldType::I64(options) = timestamp_field_entry.field_type() {
                if options.get_fastfield_cardinality() == Some(Cardinality::MultiValues) {
                    bail!("Timestamp field cannot be an array, please change your field `{}` from an array to a single value.", timestamp_field_name)
                }
            } else {
                bail!("Timestamp field must be of type i64, please change your field type `{}` to i64.", timestamp_field_name)
            }
        }

        // Build the root mapping entry, it has an empty name so that we don't prefix all
        // field name with it.
        let field_mappings = FieldMappingEntry::root(FieldMappingType::Object(self.field_mappings));
        Ok(DefaultDocMapper {
            schema,
            store_source: self.store_source,
            default_search_field_names,
            timestamp_field_name: self.timestamp_field,
            field_mappings,
        })
    }

    /// Build the schema from the field mappings and store_source parameter.
    /// Warning: tantivy does not support `.` character but quickwit does, so we must
    /// convert a field name to a tantivy compatible field name
    /// when building a `FieldEntry`.
    // TODO: remove call to tantivy_field_name when field name rules will be relaxed in tantivy.
    fn build_schema(&self) -> anyhow::Result<Schema> {
        let mut builder = SchemaBuilder::new();
        let mut unique_field_names: HashSet<String> = HashSet::new();
        for field_mapping in self.field_mappings.iter() {
            for (field_path, field_type) in field_mapping.field_entries()? {
                let tantivy_field_name = field_path.tantivy_field_name();
                if unique_field_names.contains(&tantivy_field_name) {
                    bail!(
                        "Field name must be unique, found duplicates for `{}`",
                        field_path.field_name()
                    );
                }
                unique_field_names.insert(tantivy_field_name.clone());
                builder.add_field(FieldEntry::new(tantivy_field_name, field_type));
            }
        }
        if self.store_source {
            builder.add_text_field(SOURCE_FIELD_NAME, STORED);
        }
        Ok(builder.build())
    }
}

impl TryFrom<DefaultDocMapperBuilder> for DefaultDocMapper {
    type Error = anyhow::Error;

    fn try_from(value: DefaultDocMapperBuilder) -> Result<DefaultDocMapper, Self::Error> {
        value.build()
    }
}

impl From<DefaultDocMapper> for DefaultDocMapperBuilder {
    fn from(value: DefaultDocMapper) -> Self {
        Self {
            store_source: value.store_source,
            timestamp_field: value.timestamp_field_name(),
            default_search_fields: value.default_search_field_names,
            field_mappings: value
                .field_mappings
                .field_mappings()
                .unwrap_or_else(Vec::new),
        }
    }
}

/// Default [`DocMapper`] implementation
/// which defines a set of rules to map json fields
/// to tantivy index fields.
///
/// The mains rules are defined by the field mappings.
#[derive(Serialize, Deserialize, Clone)]
#[serde(try_from = "DefaultDocMapperBuilder", into = "DefaultDocMapperBuilder")]
pub struct DefaultDocMapper {
    /// Store the json source in a text field _source.
    store_source: bool,
    /// Default list of field names used for search.
    default_search_field_names: Vec<String>,
    /// Timestamp field name.
    timestamp_field_name: Option<String>,
    /// List of field mappings which defines how a json field is mapped to index fields.
    field_mappings: FieldMappingEntry,
    /// Schema generated by the store source and field mappings parameters.
    #[serde(skip_serializing)]
    schema: Schema,
}

impl std::fmt::Debug for DefaultDocMapper {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter
            .debug_struct("DefaultDocMapper")
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
impl DocMapper for DefaultDocMapper {
    fn doc_from_json(&self, doc_json: &str) -> Result<Document, DocParsingError> {
        let mut document = Document::default();
        if self.store_source {
            let source = self.schema.get_field(SOURCE_FIELD_NAME).ok_or_else(|| {
                DocParsingError::NoSuchFieldInSchema(SOURCE_FIELD_NAME.to_string())
            })?;
            document.add_text(source, doc_json);
        }
        let json_obj: JsonValue = serde_json::from_str(doc_json).map_err(|_| {
            let doc_json_sample: String = if doc_json.len() < 20 {
                String::from(doc_json)
            } else {
                format!("{:?}...", &doc_json[0..20])
            };
            DocParsingError::NotJson(doc_json_sample)
        })?;
        let parsing_result = self.field_mappings.parse(&json_obj)?;
        for (field_path, field_value) in parsing_result {
            let field = self
                .schema
                .get_field(&field_path.tantivy_field_name())
                .ok_or_else(|| {
                    DocParsingError::NoSuchFieldInSchema(field_path.tantivy_field_name())
                })?;
            document.add(FieldValue::new(field, field_value))
        }
        Ok(document)
    }

    fn query(&self, request: &SearchRequest) -> Result<Box<dyn Query>, QueryParserError> {
        //TODO: This is just a placeholder implementation
        // allowing us to test few things up front.
        let schema = self.schema();
        let default_fields = self
            .default_search_field_names
            .iter()
            .map(|field_name| schema.get_field(field_name))
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();
        let query_parser = tantivy::query::QueryParser::new(
            schema,
            default_fields,
            tantivy::tokenizer::TokenizerManager::default(),
        );
        let query = query_parser.parse_query(&request.query)?;
        Ok(query)
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn timestamp_field_name(&self) -> Option<String> {
        self.timestamp_field_name.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        default_doc_mapper::default_mapper::SOURCE_FIELD_NAME, DefaultDocMapperBuilder, DocMapper,
        DocParsingError,
    };

    use super::DefaultDocMapper;
    use serde_json::{self, Value as JsonValue};
    use std::collections::HashMap;

    const JSON_DOC_VALUE: &str = r#"
        {
            "timestamp": 1586960586000,
            "body": "20200415T072306-0700 INFO This is a great log",
            "response_date": "2021-12-19T16:39:57Z",
            "response_time": 2.3,
            "response_payload": "YWJj",
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
            "body_other_tokenizer": ["20200415T072306-0700 INFO This is a great log"],
            "attributes__dot__server": ["ABC"],
            "attributes__dot__server__dot__payload": [[97], [98]],
            "attributes__dot__tags": [22, 23],
            "attributes__dot__server__dot__status": ["200", "201"]
        }"#;

    const JSON_MAPPING_VALUE: &str = r#"
        {
            "store_source": true,
            "default_search_fields": [
                "body", "attributes.server", "attributes.server.status"
            ],
            "timestamp_field": "timestamp",
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
            ]
        }"#;

    #[test]
    fn test_json_deserialize() -> anyhow::Result<()> {
        let mapper = serde_json::from_str::<DefaultDocMapper>(JSON_MAPPING_VALUE)?;
        assert!(mapper.store_source);
        let mut default_search_field_names: Vec<String> = mapper.default_search_field_names;
        default_search_field_names.sort();
        assert_eq!(
            default_search_field_names,
            ["attributes.server", "attributes.server.status", "body"]
        );
        let field_mappings = mapper.field_mappings.field_mappings().unwrap_or_default();
        assert_eq!(field_mappings.len(), 6);
        Ok(())
    }

    #[test]
    fn test_json_serialize() -> anyhow::Result<()> {
        let mut mapper = serde_json::from_str::<DefaultDocMapper>(JSON_MAPPING_VALUE)?;
        let json_mapper = serde_json::to_string_pretty(&mapper)?;
        let mut mapper_after_serialization =
            serde_json::from_str::<DefaultDocMapper>(&json_mapper)?;
        assert_eq!(mapper.store_source, mapper_after_serialization.store_source);

        mapper.default_search_field_names.sort();
        mapper_after_serialization.default_search_field_names.sort();
        assert_eq!(
            mapper.default_search_field_names,
            mapper_after_serialization.default_search_field_names
        );
        assert_eq!(mapper.schema, mapper_after_serialization.schema);
        Ok(())
    }

    #[test]
    fn test_parsing_document() -> anyhow::Result<()> {
        let doc_mapper = serde_json::from_str::<DefaultDocMapper>(JSON_MAPPING_VALUE)?;
        let document = doc_mapper.doc_from_json(JSON_DOC_VALUE)?;
        let schema = doc_mapper.schema();
        // 6 property entry + 1 field "_source" + two fields values for "tags" field
        // + 2 values inf "server.status" field + 2 values in "server.payload" field
        assert_eq!(document.len(), 13);
        let expected_json_paths_and_values: HashMap<String, JsonValue> =
            serde_json::from_str(EXPECTED_JSON_PATHS_AND_VALUES).unwrap();
        document.field_values().iter().for_each(|field_value| {
            let field_name = schema.get_field_name(field_value.field());
            if field_name == SOURCE_FIELD_NAME {
                assert_eq!(field_value.value().text().unwrap(), JSON_DOC_VALUE, "");
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
        let doc_mapper = serde_json::from_str::<DefaultDocMapper>(JSON_MAPPING_VALUE)?;
        doc_mapper.doc_from_json(
            r#"{
                "timestamp": 1586960586000,
                "unknown_field": "20200415T072306-0700 INFO This is a great log"
            }"#,
        )?;
        Ok(())
    }

    #[test]
    fn test_fail_to_parse_document_with_wrong_cardinality() -> anyhow::Result<()> {
        let doc_mapper = serde_json::from_str::<DefaultDocMapper>(JSON_MAPPING_VALUE)?;
        let result = doc_mapper.doc_from_json(
            r#"{
                "timestamp": 1586960586000,
                "body": ["text 1", "text 2"]
            }"#,
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
        let doc_mapper = serde_json::from_str::<DefaultDocMapper>(JSON_MAPPING_VALUE)?;
        let result = doc_mapper.doc_from_json(
            r#"{
                "timestamp": 1586960586000,
                "body": 1
            }"#,
        );
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(
            error,
            DocParsingError::ValueError(
                "body".to_owned(),
                "expected json string, got '1'.".to_owned()
            )
        );
        Ok(())
    }

    #[test]
    fn test_fail_to_build_docmapper_with_non_fast_timestamp_field() -> anyhow::Result<()> {
        let mapper_config = r#"{
            "type": "default",
            "default_search_fields": [],
            "timestamp_field": "timestamp",
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "text"
                }
            ]
        }"#;

        let builder = serde_json::from_str::<DefaultDocMapperBuilder>(mapper_config)?;
        let expected_msg = "Timestamp field must be a fast field, please add fast property to your field `timestamp`.".to_string();
        assert_eq!(builder.build().unwrap_err().to_string(), expected_msg);
        Ok(())
    }

    #[test]
    fn test_fail_to_build_docmapper_with_multivalued_timestamp_field() -> anyhow::Result<()> {
        let mapper_config = r#"{
            "type": "default",
            "default_search_fields": [],
            "timestamp_field": "timestamp",
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "array<i64>",
                    "fast": true
                }
            ]
        }"#;

        let builder = serde_json::from_str::<DefaultDocMapperBuilder>(mapper_config)?;
        let expected_msg = "Timestamp field cannot be an array, please change your field `timestamp` from an array to a single value.".to_string();
        assert_eq!(builder.build().unwrap_err().to_string(), expected_msg);
        Ok(())
    }
}
