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

use crate::{mapper::SearchRequest, DocMapper};
use serde::{Deserialize, Serialize};
use serde_json::{self, Value as JsonValue};
use std::collections::HashMap;
use tantivy::{
    query::Query,
    schema::{DocParsingError, FieldEntry, FieldValue, Schema, SchemaBuilder, STORED},
    Document,
};

#[derive(Serialize, Deserialize)]
pub struct DefaultDocMapper {
    schema: Schema, // transient
    config: DocMapperConfig,
}

impl DefaultDocMapper {
    pub fn new(config: DocMapperConfig) -> anyhow::Result<DefaultDocMapper> {
        Ok(DefaultDocMapper {
            schema: config.schema(),
            config,
        })
    }

    /// Walk through the json object and for each json path :
    /// - find the corresponding schema field
    /// - create a tantivy::FieldValue with the found field and the json value
    /// - add the FieldValue to the document
    fn fill_document(
        &self,
        json_obj: &JsonValue,
        document: &mut Document,
    ) -> Result<(), DocParsingError> {
        let mut values_by_json_path = HashMap::new();
        let mut json_path: Vec<String> = vec![];
        get_json_paths_and_values(&json_obj, &mut json_path, &mut values_by_json_path);
        for (path, values) in values_by_json_path.iter() {
            let field = self
                .schema
                .get_field(path)
                .ok_or_else(|| DocParsingError::NoSuchFieldInSchema(path.clone()))?;
            let field_entry = self.schema.get_field_entry(field);
            let field_type = field_entry.field_type();
            for value in values.iter() {
                let value = field_type
                    .value_from_json(value)
                    .map_err(|e| DocParsingError::ValueError(path.clone(), e))?;
                document.add(FieldValue::new(field, value));
            }
        }
        Ok(())
    }
}

/// Walk through a json object and fill the map `values_by_json_path` for each node with:
/// - key: json path as a string
/// - value: list of json values matching the key
fn get_json_paths_and_values(
    object: &JsonValue,
    current_path: &mut Vec<String>,
    values_by_json_path: &mut HashMap<String, Vec<JsonValue>>,
) {
    match *object {
        JsonValue::Array(ref object_array) => {
            for value in object_array.iter() {
                get_json_paths_and_values(value, current_path, values_by_json_path);
            }
        }
        JsonValue::Object(ref object_map) => {
            for (key, value) in object_map.iter() {
                current_path.push(key.clone());
                get_json_paths_and_values(value, current_path, values_by_json_path);
                current_path.pop();
            }
        }
        JsonValue::Null => {
            // TODO: define how to handle null values
        }
        _ => {
            let path = current_path.join(".");
            let entry = values_by_json_path.entry(path).or_insert_with(Vec::new);
            entry.push(object.clone());
        }
    }
}

#[typetag::serde]
impl DocMapper for DefaultDocMapper {
    fn doc_from_json(&self, doc_json: &str) -> Result<Document, DocParsingError> {
        let mut document = Document::default();
        if self.config.store_source {
            let source = self
                .schema
                .get_field("_source")
                .ok_or_else(|| DocParsingError::NoSuchFieldInSchema("_source".to_string()))?;
            document.add_text(source, doc_json);
        }
        let json_obj = serde_json::from_str(doc_json).map_err(|_| {
            let doc_json_sample: String = if doc_json.len() < 20 {
                String::from(doc_json)
            } else {
                format!("{:?}...", &doc_json[0..20])
            };
            DocParsingError::NotJSON(doc_json_sample)
        })?;
        self.fill_document(&json_obj, &mut document)?;
        Ok(document)
    }

    fn query(&self, _request: SearchRequest) -> Box<dyn Query> {
        todo!()
    }

    fn schema(&self) -> Schema {
        self.config.schema()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct DocMapperConfig {
    pub store_source: bool,
    pub ignore_unknown_fields: bool,
    pub properties: Vec<FieldEntry>,
}

impl DocMapperConfig {
    /// Build the schema from the config
    pub fn schema(&self) -> Schema {
        let mut builder = SchemaBuilder::new();
        self.properties.iter().for_each(|entry| {
            builder.add_field(entry.clone());
        });
        if self.store_source {
            builder.add_text_field("_source", STORED);
        }
        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::{get_json_paths_and_values, DefaultDocMapper, DocMapper, DocMapperConfig};
    use serde_json::{self, Value as JsonValue};
    use std::collections::HashMap;

    const JSON_DOC_VALUE: &str = r#"
        {
            "timestamp": 1586960586000,
            "severity_text": "INFO",
            "resources.label": "20200415T072306-0700 INFO This is a great log",
            "attributes": {
                "server": "ABC",
                "tags": ["prod", "region-1"]
            },
            "meta": [
                {
                    "organization": "quickwit"
                }
            ]
        }"#;

    const EXPECTED_JSON_PATHS_AND_VALUES: &str = r#"{
            "timestamp": [1586960586000],
            "severity_text": ["INFO"],
            "resources.label": ["20200415T072306-0700 INFO This is a great log"],
            "attributes.server": ["ABC"],
            "attributes.tags": ["prod", "region-1"],
            "meta.organization": ["quickwit"]
        }"#;

    const JSON_MAPPING_VALUE: &str = r#"
        {
            "store_source": true,
            "ignore_unknown_fields": true,
            "properties": [{
                "name": "timestamp",
                "type": "u64",
                "options": {
                    "indexed": true,
                    "fast": "single",
                    "stored": false
                }
            }, 
            {
                "name": "severity_text",
                "type": "text",
                "options": {
                    "indexing": {
                      "record": "basic",
                      "tokenizer": "raw"
                    },
                    "stored": false
                }
            }, 
            {
                "name": "resources.label",
                "type": "text",
                "options": {
                    "indexing": {
                      "record": "position",
                      "tokenizer": "default"
                    },
                    "stored": false
                }
            },
            {
                "name": "attributes.server",
                "type": "text",
                "options": {
                    "indexing": {
                      "record": "basic",
                      "tokenizer": "raw"
                    },
                    "stored": false
                }
            },
            {
                "name": "attributes.tags",
                "type": "text",
                "options": {
                    "indexing": {
                      "record": "basic",
                      "tokenizer": "raw"
                    },
                    "stored": false
                }
            },
            {
                "name": "meta.organization",
                "type": "text",
                "options": {
                    "indexing": {
                      "record": "basic",
                      "tokenizer": "raw"
                    },
                    "stored": false
                }
            }]
        }"#;

    #[test]
    fn test_deserialize_mapping() -> anyhow::Result<()> {
        serde_json::from_str::<DocMapperConfig>(JSON_MAPPING_VALUE)?;
        Ok(())
    }

    #[test]
    fn test_config_schema() -> anyhow::Result<()> {
        let config = serde_json::from_str::<DocMapperConfig>(JSON_MAPPING_VALUE)?;
        assert_eq!(config.schema().fields().count(), 7);
        Ok(())
    }

    #[test]
    fn test_get_json_paths_and_values() {
        let json_object = serde_json::from_str(JSON_DOC_VALUE).unwrap();
        let expected_json_paths_and_values: HashMap<String, JsonValue> =
            serde_json::from_str(EXPECTED_JSON_PATHS_AND_VALUES).unwrap();
        let mut values_by_json_path = HashMap::new();
        let mut json_path: Vec<String> = vec![];
        get_json_paths_and_values(
            &JsonValue::Object(json_object),
            &mut json_path,
            &mut values_by_json_path,
        );
        for (json_path, values) in values_by_json_path.iter() {
            assert_eq!(expected_json_paths_and_values.contains_key(json_path), true);
            let expected_values = expected_json_paths_and_values
                .get(json_path)
                .unwrap()
                .as_array()
                .unwrap();
            assert_eq!(values.len(), expected_values.len());
            for idx in 0..expected_values.len() {
                assert_eq!(values[idx], expected_values[idx]);
            }
        }
    }

    #[test]
    fn test_parsing_document() -> anyhow::Result<()> {
        let config = serde_json::from_str::<DocMapperConfig>(JSON_MAPPING_VALUE)?;
        let schema = config.schema();
        let doc_mapper = DefaultDocMapper::new(config)?;
        let document = doc_mapper.doc_from_json(JSON_DOC_VALUE)?;
        // 6 fields with 1 value + one field with two values
        assert_eq!(document.len(), 8);
        let expected_json_paths_and_values: HashMap<String, JsonValue> =
            serde_json::from_str(EXPECTED_JSON_PATHS_AND_VALUES).unwrap();
        document.field_values().iter().for_each(|field_value| {
            let field_name = schema.get_field_name(field_value.field());
            if field_name == "_source" {
                assert_eq!(field_value.value().text().unwrap(), JSON_DOC_VALUE, "");
            } else {
                let value = serde_json::to_string_pretty(field_value.value()).unwrap();
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
}
