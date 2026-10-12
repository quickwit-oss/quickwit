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

use std::borrow::Cow;

use quickwit_doc_mapper::borrowed_json::{normalize, parse_json_object, parse_json_object_str};
use quickwit_doc_mapper::{DocMapper, RoutingExpr};
use serde_json_borrow::Value;
use tantivy::Document;
use tantivy::schema::OwnedValue;

#[test]
fn strings_and_keys_keep_their_storage_through_normalization() {
    let raw = r#"{"z":"plain","esca\u0070ed":"line\ntext","a":1}"#;
    let mut tree: Value = serde_json::from_str(raw).unwrap();
    let object = tree.as_object().unwrap();
    let plain = object.get("z").unwrap();
    assert!(matches!(plain, Value::Str(Cow::Borrowed(_))));
    let plain_ptr = plain.as_str().unwrap().as_ptr();
    assert_eq!(plain_ptr, raw[raw.find("plain").unwrap()..].as_ptr());
    let key = object
        .as_vec()
        .iter()
        .find(|(key, _)| &**key == "z")
        .unwrap();
    assert!(matches!(key.0.0, Cow::Borrowed(_)));
    assert_eq!(key.0.as_ptr(), raw[raw.find('z').unwrap()..].as_ptr());
    let escaped = object
        .as_vec()
        .iter()
        .find(|(key, _)| &**key == "escaped")
        .unwrap();
    assert!(matches!(escaped.0.0, Cow::Owned(_)));
    assert!(matches!(escaped.1, Value::Str(Cow::Owned(_))));
    let escaped_key_ptr = escaped.0.as_ptr();
    let escaped_value_ptr = escaped.1.as_str().unwrap().as_ptr();
    normalize(&mut tree);
    let object = tree.as_object().unwrap();
    assert_eq!(
        object.get("z").unwrap().as_str().unwrap().as_ptr(),
        plain_ptr
    );
    let escaped = object
        .as_vec()
        .iter()
        .find(|(key, _)| &**key == "escaped")
        .unwrap();
    assert_eq!(escaped.0.as_ptr(), escaped_key_ptr);
    assert_eq!(escaped.1.as_str().unwrap().as_ptr(), escaped_value_ptr);
}

#[test]
fn duplicates_keep_first_key_position_and_last_value_recursively() {
    let raw = r#"{"z":{"discard":1},"\u0061":{"old":1},"b":[{"x":1,"c":3,"a":4,"\u0078":2}],"a":{"z":1,"m":0,"a":3,"z":2},"m":false,"z":"last\nvalue"}"#;
    let mut tree: Value = serde_json::from_str(raw).unwrap();
    let object = tree.as_object().unwrap();
    let last_value_ptr = object.as_vec().last().unwrap().1.as_str().unwrap().as_ptr();
    let first_a_ptr = object.as_vec()[1].0.as_ptr();
    normalize(&mut tree);
    let object = tree.as_object().unwrap();
    assert_eq!(object.len(), 4);
    assert_eq!(
        object.get("z").unwrap().as_str().unwrap().as_ptr(),
        last_value_ptr
    );
    assert_eq!(
        object
            .as_vec()
            .iter()
            .find(|(key, _)| &**key == "a")
            .unwrap()
            .0
            .as_ptr(),
        first_a_ptr
    );
    let expected: serde_json::Value = serde_json::from_str(raw).unwrap();
    // Includes exact recursive object order and last-wins semantics, not just value equality.
    assert_eq!(
        serde_json::to_string(&tree).unwrap(),
        serde_json::to_string(&expected).unwrap()
    );
    let keys: Vec<_> = object.keys().collect();
    let expected_keys: Vec<_> = expected
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(keys, expected_keys);
}

#[test]
fn routing_matches_owned_for_scalars_objects_arrays_and_duplicates() {
    for raw in [
        r#"{"route":{"z":1,"a":2,"z":3}}"#,
        r#"{"route":[{"z":1,"a":2},-0.0,18446744073709551615]}"#,
        r#"{"z":0,"route":{"z":1,"m":[{"z":1,"b":2,"a":3,"\u0062":4},[{"y":5,"c":6,"b":7}]],"a":2,"\u007a":3},"a":false}"#,
        r#"{"route":[{"z":1,"d":2,"a":3,"\u007a":4},[{"c":true,"a":null,"b":-0.0}]]}"#,
        r#"{"route":1,"\u0072oute":"last\nvalue"}"#,
        r#"{"route":-1}"#,
        r#"{"route":0}"#,
        r#"{"route":1.0}"#,
        r#"{"route":-0}"#,
        r#"{"route":null}"#,
        r#"{"route":true}"#,
        r#"{}"#,
    ] {
        let owned: serde_json::Value = serde_json::from_str(raw).unwrap();
        assert_routing_and_entry_points(raw, owned);
    }
}

fn assert_routing_and_entry_points(raw: &str, owned: serde_json::Value) {
    let byte_tree = parse_json_object(raw.as_bytes()).unwrap();
    let str_tree = parse_json_object_str(raw).unwrap();
    let expected_json = serde_json::to_string(&owned).unwrap();
    for tree in [&byte_tree, &str_tree] {
        assert_eq!(serde_json::to_string(tree).unwrap(), expected_json);
    }
    for expression in ["route", "route.z", "route.number", "hash_mod(route, 100)"] {
        let routing = RoutingExpr::new(expression).unwrap();
        let expected_hash = routing.eval_hash(owned.as_object().unwrap());
        eprintln!("owned hash {expected_hash}: {expression}: {raw}");
        for tree in [&byte_tree, &str_tree] {
            assert_eq!(
                expected_hash,
                routing.eval_hash(tree.value().as_object().unwrap()),
                "{expression}: {raw}"
            );
        }
        let mapper: DocMapper = serde_json::from_value(serde_json::json!({
            "mode":"dynamic", "store_source":true, "partition_key":expression
        }))
        .unwrap();
        let expected_value = OwnedValue::from(owned.clone());
        for result in [
            mapper.doc_from_json_str(raw),
            mapper.doc_from_json_bytes(raw.as_bytes()),
            mapper.doc_from_json_obj(owned.as_object().unwrap().clone(), raw.len() as u64),
        ] {
            let (partition, doc) = result.unwrap();
            assert_eq!(partition, expected_hash, "{expression}: {raw}");
            let named = doc.to_named_doc(&mapper.schema()).0;
            for field in ["_source", "_dynamic"] {
                if owned.as_object().unwrap().is_empty() && field == "_dynamic" {
                    assert!(!named.contains_key(field));
                } else {
                    assert_eq!(
                        named[field],
                        std::slice::from_ref(&expected_value),
                        "{field}: {raw}"
                    );
                }
            }
        }
    }
}

#[test]
fn routing_owned_numeric_constructors_match_raw_entry_points() {
    for number in [
        serde_json::Value::from(42u64),
        serde_json::Value::from(42i64),
        serde_json::Value::from(-42i64),
        serde_json::Value::from(1.5f64),
        serde_json::Value::from(-0.0f64),
    ] {
        let mut owned: serde_json::Value = serde_json::from_str(
            r#"{"route":{"z":true,"number":null,"a":[{"z":1,"b":2,"a":3}]},"a":false}"#,
        )
        .unwrap();
        owned["route"]["number"] = number;
        let raw = serde_json::to_string(&owned).unwrap();
        assert_routing_and_entry_points(&raw, owned);
    }
}

#[test]
fn source_dynamic_root_order_is_not_sorted_separately() {
    let mapper: DocMapper =
        serde_json::from_str(r#"{"mode":"dynamic","store_source":true}"#).unwrap();
    let raw = r#"{"z":1,"a":{"z":2,"a":3},"m":[{"z":4,"b":5,"a":6}]}"#;
    let expected: serde_json::Value = serde_json::from_str(raw).unwrap();
    let expected_keys: Vec<_> = expected
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    let (_, doc) = mapper.doc_from_json_str(raw).unwrap();
    let named = doc.to_named_doc(&mapper.schema()).0;
    for field in ["_source", "_dynamic"] {
        let OwnedValue::Object(entries) = &named[field][0] else {
            panic!("object")
        };
        let keys: Vec<_> = entries.iter().map(|(key, _)| key.as_str()).collect();
        assert_eq!(keys, expected_keys);
        assert_eq!(named[field], [OwnedValue::from(expected.clone())]);
    }
}

#[test]
fn validation_sees_only_surviving_duplicate_values() {
    let mapper: DocMapper = serde_json::from_str(
        r#"{"mode":"strict","field_mappings":[{"name":"count","type":"u64"}]}"#,
    )
    .unwrap();
    for (raw, valid) in [
        (r#"{"count":"bad","count":1}"#, true),
        (r#"{"count":1,"count":"bad"}"#, false),
    ] {
        let tree = parse_json_object(raw.as_bytes()).unwrap();
        assert_eq!(
            mapper
                .validate_json_obj(tree.value().as_object().unwrap())
                .is_ok(),
            valid
        );
        assert_eq!(mapper.doc_from_json_str(raw).is_ok(), valid);
    }
}

#[test]
fn string_and_byte_entry_points_preserve_values_and_errors() {
    let mapper: DocMapper = serde_json::from_str(
        r#"{"mode":"dynamic","store_source":true,"field_mappings":[{"name":"count","type":"u64"}]}"#,
    ).unwrap();
    for raw in [
        r#"{"z":"line\nquote\"unicode\u00e9","count":1,"count":2,"a":[{"z":1,"a":2}]}"#,
        r#"{"count":1.0}"#,
        r#"{"count":[]}"#,
        "null",
        "[]",
        "{} trailing",
        "{",
        r#"{"count":1e400}"#,
    ] {
        match (
            mapper.doc_from_json_str(raw),
            mapper.doc_from_json_bytes(raw.as_bytes()),
        ) {
            (Ok((str_partition, str_doc)), Ok((byte_partition, byte_doc))) => {
                assert_eq!(str_partition, byte_partition);
                assert_eq!(
                    str_doc.to_named_doc(&mapper.schema()).0,
                    byte_doc.to_named_doc(&mapper.schema()).0
                );
            }
            (Err(str_error), Err(byte_error)) => assert_eq!(str_error, byte_error),
            _ => panic!("entry points disagree for {raw}"),
        }
    }
}
