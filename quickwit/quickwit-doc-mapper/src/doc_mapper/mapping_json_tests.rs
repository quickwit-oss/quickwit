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

use serde_json::Value as Owned;
use serde_json_borrow::Value as Borrowed;
use tantivy::TantivyDocument;
use tantivy::schema::OwnedValue;

use super::{JsonRead, MappingJson, Scalar};
use crate::doc_mapper::mapping_tree::build_mapping_tree;

#[test]
fn owned_containers_and_scalar_storage_are_native() {
    let raw = r#"{"key":"owned string","number":42,"values":[-1,0,1,1.0,-0,-0.0,9223372036854775808,18446744073709551615]}"#;
    let owned: Owned = serde_json::from_str(raw).unwrap();
    let string_ptr = owned["key"].as_str().unwrap().as_ptr();
    let key_ptr = owned
        .as_object()
        .unwrap()
        .get_key_value("key")
        .unwrap()
        .0
        .as_ptr();
    let value_ptr = &owned["key"] as *const Owned;
    let array_ptr = owned["values"].as_array().unwrap().as_ptr();
    let number_ptr = owned["number"].as_number().unwrap() as *const serde_json::Number;
    let Some(Scalar::Number(Cow::Borrowed(number))) = owned["number"].scalar() else {
        panic!("owned numbers must stay borrowed");
    };
    assert_eq!(number as *const _, number_ptr);
    assert_eq!(number.as_u64(), Some(42));
    assert_eq!(number.as_i64(), Some(42));
    // Characterize the old upstream conversion trap; the native mapper never uses it.
    let upstream = Borrowed::from(&owned["number"]);
    assert_eq!(upstream.as_i64(), Some(42));
    assert_eq!(upstream.as_u64(), None);
    let object = owned.into_object().unwrap();
    assert_eq!(object.get_key_value("key").unwrap().0.as_ptr(), key_ptr);
    assert_eq!(&object["key"] as *const Owned, value_ptr);
    assert_eq!(object["key"].as_str().unwrap().as_ptr(), string_ptr);
    assert_eq!(object["values"].as_array().unwrap().as_ptr(), array_ptr);
    assert_eq!(
        serde_json::to_string(&object).unwrap(),
        serde_json::to_string(&crate::borrowed_json::parse_json_object(raw.as_bytes()).unwrap())
            .unwrap()
    );
    for (key, value) in Owned::into_entries(object) {
        if key == "values" {
            let array = value.into_array().unwrap();
            assert_eq!(array.as_ptr(), array_ptr);
            for index in [4, 5] {
                assert_eq!(
                    array[index].as_f64().unwrap().to_bits(),
                    (-0.0f64).to_bits()
                );
            }
        } else if key == "key" {
            assert_eq!(key.as_ptr(), key_ptr);
            assert_eq!(value.as_str().unwrap().as_ptr(), string_ptr);
        }
    }
}

#[test]
fn borrowed_containers_are_native() {
    let raw = r#"{"z":["line\ntext",1],"a":false}"#;
    let value = crate::borrowed_json::parse_json_object_str(raw)
        .unwrap()
        .into_value();
    let object = value.as_object().unwrap();
    let entries_ptr = object.as_vec().as_ptr().cast::<()>();
    let array_ptr = object.get("z").unwrap().as_array().unwrap().as_ptr();
    let text_ptr = object.get("z").unwrap().as_array().unwrap()[0]
        .as_str()
        .unwrap()
        .as_ptr();
    let object = value.into_object().unwrap();
    assert_eq!(object.as_vec().as_ptr().cast::<()>(), entries_ptr);
    for (key, value) in Borrowed::into_entries(object) {
        if &*key == "z" {
            let values = value.into_array().unwrap();
            assert_eq!(values.as_ptr(), array_ptr);
            assert_eq!(values[0].as_str().unwrap().as_ptr(), text_ptr);
        }
    }
}

fn find<'a, V: MappingJson>(object: &'a V::ObjectMap, key: &str) -> (&'a str, &'a V) {
    V::entries(object).find(|(name, _)| *name == key).unwrap()
}

fn dynamic_storage<V: MappingJson>(input: V) {
    let entries: Vec<crate::FieldMappingEntry> = serde_json::from_str(
        r#"[
        {"name":"z","type":"object","field_mappings":[
            {"name":"known","type":"array<text>"},
            {"name":"deep","type":"object","field_mappings":[{"name":"known","type":"text"}]}
        ]}
    ]"#,
    )
    .unwrap();
    let mut schema = tantivy::schema::Schema::builder();
    let mapping = build_mapping_tree(&entries, &mut schema).unwrap();
    let object = input.object().unwrap();
    let (z_key, z_value) = find::<V>(object, "z");
    let z_key_ptr = z_key.as_ptr();
    let (deep_key, deep_value) = find::<V>(z_value.object().unwrap(), "deep");
    let deep_key_ptr = deep_key.as_ptr();
    let (escaped_key, escaped_value) = find::<V>(deep_value.object().unwrap(), "escaped");
    let escaped_key_ptr = escaped_key.as_ptr();
    let array_ptr = escaped_value.array().unwrap().as_ptr();
    let text_ptr = escaped_value.array().unwrap()[0].as_str().unwrap().as_ptr();
    let Ok(input) = input.into_object() else {
        panic!("object")
    };
    let mut path = Vec::new();
    let mut document = TantivyDocument::default();
    let dynamic = mapping
        .field_mappings
        .doc_from_json::<V>(input, crate::ModeType::Dynamic, &mut document, &mut path)
        .unwrap()
        .unwrap();
    assert!(path.is_empty());
    let (z_key, z_value) = find::<V>(&dynamic, "z");
    assert_eq!(z_key.as_ptr(), z_key_ptr);
    let (deep_key, deep_value) = find::<V>(z_value.object().unwrap(), "deep");
    assert_eq!(deep_key.as_ptr(), deep_key_ptr);
    let (escaped_key, escaped_value) = find::<V>(deep_value.object().unwrap(), "escaped");
    assert_eq!(escaped_key.as_ptr(), escaped_key_ptr);
    assert_eq!(escaped_value.array().unwrap().as_ptr(), array_ptr);
    assert_eq!(
        escaped_value.array().unwrap()[0].as_str().unwrap().as_ptr(),
        text_ptr
    );
    assert_eq!(V::entries(z_value.object().unwrap()).count(), 1);
    assert_eq!(V::entries(deep_value.object().unwrap()).count(), 1);
    let schema = schema.build();
    let field = schema.get_field("z.known").unwrap();
    assert_eq!(
        document
            .get_all(field)
            .map(OwnedValue::from)
            .collect::<Vec<_>>(),
        [OwnedValue::Str("mapped\ntext".into())]
    );
}

#[test]
fn local_dynamic_subtrees_move_native_storage_in_both_representations() {
    let raw = r#"{"a":null,"\u007a":{"known":[null,"mapped\ntext"],"d\u0065ep":{"known":"ok","esca\u0070ed":["owned\ntext",null,{"a":1}]}}}"#;
    dynamic_storage(serde_json::from_str::<Owned>(raw).unwrap());
    dynamic_storage(
        crate::borrowed_json::parse_json_object_str(raw)
            .unwrap()
            .into_value(),
    );
}

fn fail_fast_array<V: MappingJson>(input: V) {
    let entries: Vec<crate::FieldMappingEntry> = serde_json::from_str(
        r#"[{"name":"items","type":"array<text>"},{"name":"later","type":"text"}]"#,
    )
    .unwrap();
    let mut schema = tantivy::schema::Schema::builder();
    let mapping = build_mapping_tree(&entries, &mut schema).unwrap();
    let schema = schema.build();
    let Ok(input) = input.into_object() else {
        panic!("object")
    };
    let mut document = TantivyDocument::default();
    let error = mapping
        .field_mappings
        .doc_from_json::<V>(
            input,
            crate::ModeType::Strict,
            &mut document,
            &mut Vec::new(),
        )
        .err()
        .unwrap();
    assert_eq!(
        error,
        crate::DocParsingError::ValueError("items".into(), "expected string, got `[1]`".into())
    );
    let items = schema.get_field("items").unwrap();
    assert_eq!(
        document
            .get_all(items)
            .map(OwnedValue::from)
            .collect::<Vec<_>>(),
        [OwnedValue::Str("before".into())]
    );
    assert!(
        document
            .get_first(schema.get_field("later").unwrap())
            .is_none()
    );
}

#[test]
fn native_array_mapping_stops_at_first_error_without_later_insertions() {
    let raw = r#"{"items":["before",[1],"after"],"later":"unvisited"}"#;
    fail_fast_array(serde_json::from_str::<Owned>(raw).unwrap());
    fail_fast_array(
        crate::borrowed_json::parse_json_object_str(raw)
            .unwrap()
            .into_value(),
    );
}
