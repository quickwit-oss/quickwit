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

use std::collections::BTreeMap;

use serde_json::{Value as JsonValue, json};
use tantivy::Document;
use tantivy::schema::{FieldType, IndexRecordOption, OwnedValue};

use crate::DocMapper;

fn mapping_config() -> JsonValue {
    json!({"field_mappings": [
        {"name": "origin", "type": "initial_split_id", "description": "origin"},
        {"name": "metadata", "type": "object", "field_mappings": [
            {"name": "id", "type": "initial_split_id", "stored": false}
        ]},
        {"name": "literal.id", "type": "initial_split_id"},
        {"name": "literal", "type": "object", "field_mappings": [
            {"name": "id", "type": "text", "tokenizer": "raw"}
        ]}
    ]})
}

#[test]
fn test_initial_split_id_schema_and_roundtrip() {
    let mapper: DocMapper = serde_json::from_value(mapping_config()).unwrap();
    let schema = mapper.schema();
    let names: Vec<_> = mapper
        .initial_split_id_fields()
        .iter()
        .map(|field| schema.get_field_name(*field))
        .collect();
    assert_eq!(names, ["origin", "metadata.id", r"literal\.id"]);
    for field in mapper.initial_split_id_fields() {
        let entry = schema.get_field_entry(*field);
        let FieldType::Str(options) = entry.field_type() else {
            panic!("initial split IDs must be ordinary string fields");
        };
        assert!(options.is_fast());
        assert_eq!(options.get_fast_field_tokenizer_name(), Some("raw"));
        assert_eq!(options.is_stored(), entry.name() != "metadata.id");
        let indexing = options.get_indexing_options().unwrap();
        assert_eq!(indexing.tokenizer(), "raw");
        assert_eq!(indexing.index_option(), IndexRecordOption::Basic);
        assert!(!indexing.fieldnorms());
    }
    let serialized = serde_json::to_value(&mapper).unwrap();
    assert_eq!(
        serialized["field_mappings"][0],
        json!({
            "name": "origin", "type": "initial_split_id", "stored": true, "description": "origin"
        })
    );
    let roundtrip: DocMapper = serde_json::from_value(serialized.clone()).unwrap();
    assert_eq!(serde_json::to_value(&roundtrip).unwrap(), serialized);
    assert_eq!(roundtrip.schema(), schema);
    assert_eq!(
        roundtrip.initial_split_id_fields(),
        mapper.initial_split_id_fields()
    );
    let named = ["origin", r"literal\.id"].map(|name| {
        (
            name.to_string(),
            vec![OwnedValue::Str("first-split".to_string())],
        )
    });
    assert_eq!(
        JsonValue::Object(mapper.doc_to_json(BTreeMap::from(named)).unwrap()),
        json!({
            "origin": "first-split", "literal.id": "first-split"
        })
    );
}

#[test]
fn test_initial_split_id_configuration_validation() {
    for (key, value) in [
        ("type", json!("array<initial_split_id>")),
        ("fast", json!(false)),
        ("fast", json!(true)),
        ("tokenizer", json!("raw")),
        ("indexed", json!(false)),
        ("record", json!("position")),
        ("fieldnorms", json!(true)),
        ("normalizer", json!("lowercase")),
        ("generated", json!("initial_split_id")),
        ("stored", json!("yes")),
    ] {
        let mut config = mapping_config();
        config["field_mappings"][0][key] = value;
        assert!(
            serde_json::from_value::<DocMapper>(config.clone()).is_err(),
            "{config}"
        );
    }
    for partition_key in [
        "origin",
        "origin.part",
        "metadata.id",
        r"literal\.id",
        "hash_mod((literal.id,hash_mod(origin, 3)), 8)",
    ] {
        let mut config = mapping_config();
        config["partition_key"] = json!(partition_key);
        let error = serde_json::from_value::<DocMapper>(config).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cannot be used in the partition key"),
            "{error}"
        );
    }
    for field in ["origin", "metadata.id", r"literal\.id"] {
        let mut config = mapping_config();
        config["timestamp_field"] = json!(field);
        assert!(serde_json::from_value::<DocMapper>(config).is_err());
        let mut config = mapping_config();
        config["field_mappings"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "name": "all", "type": "concatenate", "concatenate_fields": [field]
            }));
        assert!(
            serde_json::from_value::<DocMapper>(config).is_err(),
            "{field}"
        );
    }
    // Routing may use the ordinary nested field, not the generated field with a literal dot.
    let mut config = mapping_config();
    config["partition_key"] = json!("literal.id");
    let mapper: DocMapper = serde_json::from_value(config).unwrap();
    mapper
        .doc_from_json_str(r#"{"literal":{"id":"tenant"}}"#)
        .unwrap();
}

#[test]
fn test_initial_split_id_input_validation_and_opt_in() {
    for mode in ["strict", "lenient", "dynamic"] {
        let mut config = mapping_config();
        config["mode"] = json!(mode);
        let mapper: DocMapper = serde_json::from_value(config).unwrap();
        for value in [
            json!(null),
            json!("spoofed"),
            json!(0),
            json!(false),
            json!([]),
            json!(["spoofed"]),
            json!({}),
        ] {
            for input in [
                json!({"origin": value}),
                json!({"metadata": {"id": value}}),
                json!({"literal.id": value}),
            ] {
                let json_str = input.to_string();
                let error = mapper.doc_from_json_str(&json_str).unwrap_err();
                assert!(
                    error.to_string().contains("must not be supplied"),
                    "{mode}: {input}: {error}"
                );
                let borrowed: serde_json_borrow::Value = serde_json::from_str(&json_str).unwrap();
                assert_eq!(
                    mapper
                        .validate_json_obj(borrowed.as_object().unwrap())
                        .unwrap_err()
                        .to_string(),
                    error.to_string()
                );
            }
        }
        // Missing generated fields are valid and stay absent until split selection.
        let (_, document) = mapper.doc_from_json_str("{}").unwrap();
        assert!(
            mapper
                .initial_split_id_fields()
                .iter()
                .all(|field| document.get_first(*field).is_none())
        );
        let empty: serde_json_borrow::Value = serde_json::from_str("{}").unwrap();
        mapper
            .validate_json_obj(empty.as_object().unwrap())
            .unwrap();
    }
    let ordinary: DocMapper = serde_json::from_value(json!({"field_mappings": [
        {"name": "origin", "type": "text"}, {"name": "initial_split_id", "type": "text"}
    ]}))
    .unwrap();
    assert!(ordinary.initial_split_id_fields().is_empty());
    let input = json!({"origin": "user-value", "initial_split_id": "also-user-value"});
    let (_, document) = ordinary.doc_from_json_str(&input.to_string()).unwrap();
    assert_eq!(
        JsonValue::Object(
            ordinary
                .doc_to_json(document.to_named_doc(&ordinary.schema()).0)
                .unwrap()
        ),
        input
    );
}
