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

use std::collections::{BTreeMap, HashMap};

use quickwit_config::DocMapping;
use quickwit_doc_mapper::{FieldMappingEntry, FieldMappingType};
use quickwit_metastore::IndexMetadata;
use quickwit_proto::types::IndexId;
use serde::Serialize;

type Mappings = HashMap<String, Properties>;

#[derive(Debug, Serialize)]
pub(crate) struct ElasticsearchMappingsResponse {
    mappings: Mappings,
}

#[derive(Debug, Serialize)]
pub(crate) struct Mapping {
    properties: Properties,
}

#[derive(Debug, Serialize, PartialEq)]
pub(crate) enum Properties {
    Bool,
    Object(Mappings),
}

impl ElasticsearchMappingsResponse {
    pub fn from_doc_mapping(indexes_metadata: Vec<IndexMetadata>) -> Self {
        // let mappings = indexes_metadata.into_iter().map(|index_metadata| {
        //     let doc_mapping = index_metadata.index_config.doc_mapping;
        //     let properties = doc_mapping.field_mappings.into_iter().map(|field_mapping| {
        //         (field_mapping.name, Properties::from_field_mapping(field_mapping))
        //     }).collect();
        //     (index_metadata.index_uid.index_id, Mapping { properties })
        // }).collect();
        Self { mappings: HashMap::new() }
    }
}

impl Properties {
    pub fn from_field_mapping(field_mapping: FieldMappingEntry) -> Self {
        match field_mapping.mapping_type {
            FieldMappingType::Bool(_, _) => Properties::Bool,
            FieldMappingType::Object(options) => {
                let mappings = options.field_mappings.into_iter().map(|field_mapping| {
                    (field_mapping.name.clone(), Properties::from_field_mapping(field_mapping))
                }).collect();
                Properties::Object(mappings)
            }
            _ => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_properties_from_bool_field_mapping() {
        let bool_field_mapping_json = json!({
            "name": "test",
            "type": "bool",
        });
        let bool_field_mapping = serde_json::from_value::<FieldMappingEntry>(bool_field_mapping_json).unwrap();
        let properties = Properties::from_field_mapping(bool_field_mapping);
        assert_eq!(properties, Properties::Bool);
    }

    // #[test]
    // fn test_elasticsearch_mapping_response_serialize() {
    //     let properties = HashMap::from_iter([
    //         (
    //             "level".to_string(),
    //             Property {
    //                 typ: "keyword".to_string(),
    //             },
    //         ),
    //         (
    //             "message".to_string(),
    //             Property {
    //                 typ: "text".to_string(),
    //             },
    //         ),
    //         (
    //             "timestamp".to_string(),
    //             Property {
    //                 typ: "date".to_string(),
    //             },
    //         ),
    //         (
    //             "tags".to_string(),
    //             Property {
    //                 typ: "object".to_string(),
    //             },
    //         ),
    //         (
    //             "status".to_string(),
    //             Property {
    //                 typ: "integer".to_string(),
    //             },
    //         ),
    //     ]);
    //     let mappings = Mappings { properties };
    //     let response = ElasticsearchMappingResponse {
    //         index_id: "test-index".to_string(),
    //         mappings,
    //     };
    //     let serialized = serde_json::to_string(&response).unwrap();
    //     let expected = json!({
    //         "test-index": {
    //             "mappings": {
    //                 "properties": {
    //                     "level": { "type": "keyword"},
    //                     "message": { "type": "text"},
    //                     "timestamp": { "type": "date"},
    //                     "tags": { "type": "object"},
    //                     "status": { "type": "integer"},
    //                 }
    //             }
    //         }
    //     });
    //     assert_eq!(serialized, expected);
    // }
}
