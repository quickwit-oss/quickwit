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

use std::collections::HashMap;

use quickwit_doc_mapper::{FieldMappingEntry, FieldMappingType};
use quickwit_metastore::IndexMetadata;
use quickwit_proto::search::{ListFieldType, ListFieldsResponse};
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};

/// Top-level response for `GET /{index}/_mapping(s)`.
///
/// Serializes as `{ "<index_id>": { "mappings": { "properties": { ... } } } }`.
pub(crate) struct ElasticsearchMappingsResponse {
    indices: HashMap<String, IndexMappings>,
}

impl Serialize for ElasticsearchMappingsResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.indices.len()))?;
        for (index_id, mappings) in &self.indices {
            map.serialize_entry(index_id, mappings)?;
        }
        map.end()
    }
}

#[derive(Debug, Serialize)]
struct IndexMappings {
    mappings: MappingProperties,
}

#[derive(Debug, Serialize)]
struct MappingProperties {
    properties: HashMap<String, FieldMapping>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum FieldMapping {
    Leaf {
        #[serde(rename = "type")]
        typ: &'static str,
    },
    Object {
        #[serde(rename = "type")]
        typ: &'static str,
        properties: HashMap<String, FieldMapping>,
    },
}

impl ElasticsearchMappingsResponse {
    pub fn from_doc_mapping(
        indexes_metadata: Vec<IndexMetadata>,
        list_fields_response: Option<&ListFieldsResponse>,
    ) -> Self {
        let indices = indexes_metadata
            .into_iter()
            .map(|index_metadata| {
                let field_mappings = &index_metadata.index_config.doc_mapping.field_mappings;
                let mut properties = build_properties(field_mappings);
                if let Some(list_fields) = list_fields_response {
                    merge_dynamic_fields(&mut properties, list_fields);
                }
                let index_id = index_metadata.index_id().to_string();
                (
                    index_id,
                    IndexMappings {
                        mappings: MappingProperties { properties },
                    },
                )
            })
            .collect();
        Self { indices }
    }
}

fn build_properties(field_mappings: &[FieldMappingEntry]) -> HashMap<String, FieldMapping> {
    let mut properties = HashMap::with_capacity(field_mappings.len());
    for entry in field_mappings {
        if let Some(field_mapping) = field_mapping_from_entry(entry) {
            properties.insert(entry.name.clone(), field_mapping);
        }
    }
    properties
}

fn field_mapping_from_entry(entry: &FieldMappingEntry) -> Option<FieldMapping> {
    match &entry.mapping_type {
        FieldMappingType::Text(..) => Some(FieldMapping::Leaf { typ: "text" }),
        FieldMappingType::I64(..) => Some(FieldMapping::Leaf { typ: "long" }),
        FieldMappingType::U64(..) => Some(FieldMapping::Leaf { typ: "long" }),
        FieldMappingType::F64(..) => Some(FieldMapping::Leaf { typ: "double" }),
        FieldMappingType::Bool(..) => Some(FieldMapping::Leaf { typ: "boolean" }),
        FieldMappingType::DateTime(..) => Some(FieldMapping::Leaf { typ: "date" }),
        FieldMappingType::IpAddr(..) => Some(FieldMapping::Leaf { typ: "ip" }),
        FieldMappingType::Bytes(..) => Some(FieldMapping::Leaf { typ: "binary" }),
        FieldMappingType::Json(..) => Some(FieldMapping::Leaf { typ: "object" }),
        FieldMappingType::Object(options) => {
            let properties = build_properties(&options.field_mappings);
            Some(FieldMapping::Object {
                typ: "object",
                properties,
            })
        }
        FieldMappingType::Concatenate(_) => None,
    }
}

/// Merges dynamic fields from a `ListFieldsResponse` into the properties map.
///
/// Fields already present in the map (from explicit doc mappings) are skipped,
/// as are internal fields (prefixed with `_`).
fn merge_dynamic_fields(
    properties: &mut HashMap<String, FieldMapping>,
    list_fields_response: &ListFieldsResponse,
) {
    for field_entry in &list_fields_response.fields {
        let field_name = &field_entry.field_name;
        if field_name.starts_with('_') {
            continue;
        }
        if properties.contains_key(field_name) {
            continue;
        }
        let Ok(field_type) = ListFieldType::try_from(field_entry.field_type) else {
            continue;
        };
        if let Some(es_type) = es_type_from_list_field_type(field_type) {
            properties.insert(field_name.clone(), FieldMapping::Leaf { typ: es_type });
        }
    }
}

fn es_type_from_list_field_type(field_type: ListFieldType) -> Option<&'static str> {
    match field_type {
        ListFieldType::Str => Some("keyword"),
        ListFieldType::U64 | ListFieldType::I64 => Some("long"),
        ListFieldType::F64 => Some("double"),
        ListFieldType::Bool => Some("boolean"),
        ListFieldType::Date => Some("date"),
        ListFieldType::Bytes => Some("binary"),
        ListFieldType::IpAddr => Some("ip"),
        ListFieldType::Facet | ListFieldType::Json => None,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_field_mapping_from_entry_bool() {
        let entry_json = json!({ "name": "active", "type": "bool" });
        let entry: FieldMappingEntry = serde_json::from_value(entry_json).unwrap();
        let mapping = field_mapping_from_entry(&entry).unwrap();
        let serialized = serde_json::to_value(&mapping).unwrap();
        assert_eq!(serialized, json!({ "type": "boolean" }));
    }

    #[test]
    fn test_field_mapping_from_entry_text() {
        let entry_json = json!({ "name": "message", "type": "text" });
        let entry: FieldMappingEntry = serde_json::from_value(entry_json).unwrap();
        let mapping = field_mapping_from_entry(&entry).unwrap();
        let serialized = serde_json::to_value(&mapping).unwrap();
        assert_eq!(serialized, json!({ "type": "text" }));
    }

    #[test]
    fn test_field_mapping_from_entry_i64() {
        let entry_json = json!({ "name": "count", "type": "i64" });
        let entry: FieldMappingEntry = serde_json::from_value(entry_json).unwrap();
        let mapping = field_mapping_from_entry(&entry).unwrap();
        let serialized = serde_json::to_value(&mapping).unwrap();
        assert_eq!(serialized, json!({ "type": "long" }));
    }

    #[test]
    fn test_field_mapping_from_entry_object() {
        let entry_json = json!({
            "name": "nested",
            "type": "object",
            "field_mappings": [
                { "name": "id", "type": "u64" },
                { "name": "label", "type": "text" }
            ]
        });
        let entry: FieldMappingEntry = serde_json::from_value(entry_json).unwrap();
        let mapping = field_mapping_from_entry(&entry).unwrap();
        let serialized = serde_json::to_value(&mapping).unwrap();
        assert_eq!(
            serialized,
            json!({
                "type": "object",
                "properties": {
                    "id": { "type": "long" },
                    "label": { "type": "text" }
                }
            })
        );
    }

    #[test]
    fn test_field_mapping_from_entry_concatenate_skipped() {
        let entry_json = json!({
            "name": "concat_field",
            "type": "concatenate",
            "concatenate_fields": ["field_a", "field_b"]
        });
        let entry: FieldMappingEntry = serde_json::from_value(entry_json).unwrap();
        assert!(field_mapping_from_entry(&entry).is_none());
    }

    #[test]
    fn test_build_properties_all_leaf_types() {
        let entries: Vec<FieldMappingEntry> = serde_json::from_value(json!([
            { "name": "title", "type": "text" },
            { "name": "count", "type": "i64" },
            { "name": "unsigned", "type": "u64" },
            { "name": "score", "type": "f64" },
            { "name": "active", "type": "bool" },
            { "name": "created_at", "type": "datetime" },
            { "name": "ip_field", "type": "ip" },
            { "name": "data", "type": "bytes" },
            { "name": "payload", "type": "json" },
            {
                "name": "metadata",
                "type": "object",
                "field_mappings": [
                    { "name": "source", "type": "text" }
                ]
            }
        ]))
        .unwrap();

        let props = build_properties(&entries);
        let to_json = |fm: &FieldMapping| serde_json::to_value(fm).unwrap();

        assert_eq!(to_json(&props["title"]), json!({ "type": "text" }));
        assert_eq!(to_json(&props["count"]), json!({ "type": "long" }));
        assert_eq!(to_json(&props["unsigned"]), json!({ "type": "long" }));
        assert_eq!(to_json(&props["score"]), json!({ "type": "double" }));
        assert_eq!(to_json(&props["active"]), json!({ "type": "boolean" }));
        assert_eq!(to_json(&props["created_at"]), json!({ "type": "date" }));
        assert_eq!(to_json(&props["ip_field"]), json!({ "type": "ip" }));
        assert_eq!(to_json(&props["data"]), json!({ "type": "binary" }));
        assert_eq!(to_json(&props["payload"]), json!({ "type": "object" }));

        let meta = to_json(&props["metadata"]);
        assert_eq!(meta["type"], "object");
        assert_eq!(meta["properties"]["source"]["type"], "text");
    }

    #[test]
    fn test_merge_dynamic_fields_skips_existing_and_internal() {
        use quickwit_proto::search::ListFieldsEntryResponse;

        let mut properties = HashMap::new();
        properties.insert("title".to_string(), FieldMapping::Leaf { typ: "text" });

        let list_fields = ListFieldsResponse {
            fields: vec![
                ListFieldsEntryResponse {
                    field_name: "title".to_string(),
                    field_type: ListFieldType::Str as i32,
                    ..Default::default()
                },
                ListFieldsEntryResponse {
                    field_name: "_timestamp".to_string(),
                    field_type: ListFieldType::Date as i32,
                    ..Default::default()
                },
                ListFieldsEntryResponse {
                    field_name: "dynamic_field".to_string(),
                    field_type: ListFieldType::Str as i32,
                    ..Default::default()
                },
            ],
        };

        merge_dynamic_fields(&mut properties, &list_fields);

        assert_eq!(properties.len(), 2);
        assert!(properties.contains_key("title"));
        assert!(properties.contains_key("dynamic_field"));
        assert!(!properties.contains_key("_timestamp"));
    }
}
