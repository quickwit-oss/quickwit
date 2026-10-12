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

use quickwit_doc_mapper::{DocMapper, DocParsingError};
use serde_json::{Value, json};
use tantivy::Document;
use tantivy::schema::OwnedValue;

fn assert_error(mapper: &DocMapper, raw: &str, expected: DocParsingError) {
    for result in [
        mapper.doc_from_json_str(raw),
        mapper.doc_from_json_bytes(raw.as_bytes()),
        mapper.doc_from_json_obj(serde_json::from_str(raw).unwrap(), raw.len() as u64),
    ] {
        let error = result.unwrap_err();
        assert_eq!(error.to_string(), expected.to_string(), "{raw}");
        assert_eq!(error, expected, "{raw}");
    }
}

#[test]
fn mapping_diagnostics_match_owned_json_spelling() {
    let nested = r#"{"z":["line\nquote\"unicode\u00e9",{"esca\u0070ed":-0.0,"a":1.0}],"a":0,"z":[{"z":1,"a":-0,"esca\u0070ed":"line\nquote\"unicode\u00e9"},[null,true,18446744073709551615,1e-100,"last\nvalue"]]}"#;
    let array = format!("[{nested},[1.0,-0.0,\"escaped\\tvalue\"]]");
    for (typ, prefix, extra) in [
        ("text", "expected string", json!({})),
        ("bool", "expected boolean", json!({})),
        ("ip", "expected string", json!({})),
        (
            "bytes",
            "expected base64 string",
            json!({"input_format":"base64"}),
        ),
        (
            "datetime",
            "failed to parse datetime: expected a float, integer, or string",
            json!({}),
        ),
        ("u64", "expected JSON number", json!({"coerce":false})),
        (
            "i64",
            "expected JSON number or string",
            json!({"coerce":true}),
        ),
        (
            "f64",
            "expected JSON number or string",
            json!({"coerce":true}),
        ),
    ] {
        for (field_type, input_value, displayed_value) in [
            (typ.to_string(), nested.to_string(), nested.to_string()),
            (
                format!("array<{typ}>"),
                format!("[null,{array}]"),
                array.clone(),
            ),
        ] {
            let mut field = json!({"name":"value", "type":field_type});
            field
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            let mapper: DocMapper =
                serde_json::from_value(json!({"mode":"strict", "field_mappings":[
                    {"name":"z", "type":"object", "field_mappings":[field]}
                ]}))
                .unwrap();
            let normalized: Value = serde_json::from_str(&displayed_value).unwrap();
            let raw = format!(r#"{{"z":{{"value":{input_value}}}}}"#);
            assert_error(
                &mapper,
                &raw,
                DocParsingError::ValueError(
                    "z.value".into(),
                    format!("{prefix}, got `{normalized}`"),
                ),
            );
        }
    }
    for (typ, prefix, suffix, extra) in [
        ("text", "expected string, got `", "`", json!({})),
        ("json", "expected object, got `", "`", json!({})),
        (
            "object",
            "expected an JSON object, got ",
            "",
            json!({"field_mappings":[{"name":"known","type":"text"}]}),
        ),
    ] {
        let mut field = json!({"name":"value", "type":typ});
        field
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let mapper: DocMapper = serde_json::from_value(json!({"field_mappings":[field]})).unwrap();
        for raw_value in [
            "0",
            "-1",
            "1.0",
            "-0",
            "-0.0",
            "1e100",
            "1e-100",
            "18446744073709551615",
            "true",
        ] {
            let normalized: Value = serde_json::from_str(raw_value).unwrap();
            assert_error(
                &mapper,
                &format!(r#"{{"value":{raw_value}}}"#),
                DocParsingError::ValueError(
                    "value".into(),
                    format!("{prefix}{normalized}{suffix}"),
                ),
            );
        }
    }
    for (typ, prefix, suffix) in [
        ("object", "expected an JSON object, got ", ""),
        ("array<json>", "expected object, got `", "`"),
    ] {
        let field = if typ == "object" {
            json!({"name":"value", "type":typ, "field_mappings":[{"name":"known","type":"text"}]})
        } else {
            json!({"name":"value", "type":typ})
        };
        let mapper: DocMapper = serde_json::from_value(json!({"field_mappings":[field]})).unwrap();
        let input = if typ == "object" {
            array.clone()
        } else {
            format!("[{array}]")
        };
        let normalized: Value = serde_json::from_str(&array).unwrap();
        assert_error(
            &mapper,
            &format!(r#"{{"value":{input}}}"#),
            DocParsingError::ValueError("value".into(), format!("{prefix}{normalized}{suffix}")),
        );
    }
}

#[test]
fn legacy_unnormalized_validation_diagnostics_still_normalize() {
    let raw = r#"{"value":{"z":1,"a":"line\nvalue","z":2}}"#;
    for (typ, prefix) in [
        ("text", "expected string"),
        ("bool", "expected boolean"),
        ("ip", "expected string"),
        ("bytes", "expected base64 string"),
        ("u64", "expected JSON number or string"),
        (
            "datetime",
            "failed to parse datetime: expected a float, integer, or string",
        ),
    ] {
        let mapper: DocMapper =
            serde_json::from_value(json!({"field_mappings":[{"name":"value","type":typ}]}))
                .unwrap();
        // Deliberately NOT normalized: validator Display has pre-existing last-wins/order behavior.
        let borrowed: serde_json_borrow::Value = serde_json::from_str(raw).unwrap();
        let owned: Value = serde_json::from_str(raw).unwrap();
        assert_eq!(
            mapper
                .validate_json_obj(borrowed.as_object().unwrap())
                .unwrap_err(),
            DocParsingError::ValueError(
                "value".into(),
                format!("{prefix}, got `{}`", owned["value"])
            )
        );
    }
}

const MIXED_CONFIG: &str = r#"{"mode":"dynamic","field_mappings":[
    {"name":"z","type":"object","field_mappings":[{"name":"known","type":"text"}]}
]}"#;

#[test]
fn mixed_mapping_dynamic_subtree_structure_and_order() {
    let mapper: DocMapper = serde_json::from_str(MIXED_CONFIG).unwrap();
    for count in [32, 256, 1024] {
        let root = (0..count)
            .map(|index| format!("\"a{index:04}\":{index},"))
            .collect::<String>();
        let children = (0..count)
            .map(|index| format!(",\"u{index:04}\":{index}"))
            .collect::<String>();
        let raw = format!(r#"{{{root}"z":{{"known":"ok"{children}}}}}"#);
        let dynamic_raw = format!(r#"{{{root}"z":{{{}}}}}"#, children.trim_start_matches(','));
        let expected: Value = serde_json::from_str(&dynamic_raw).unwrap();
        let expected = OwnedValue::from(expected);
        for result in [
            mapper.doc_from_json_str(&raw),
            mapper.doc_from_json_bytes(raw.as_bytes()),
        ] {
            let named = result.unwrap().1.to_named_doc(&mapper.schema()).0;
            assert_eq!(named["_dynamic"], std::slice::from_ref(&expected));
            assert_eq!(named["z.known"], [OwnedValue::Str("ok".into())]);
        }
    }
}

#[test]
fn local_dynamic_subtrees_preserve_modes_arrays_nulls_and_concatenate() {
    let config = json!({"mode":"dynamic", "field_mappings":[
        {"name":"z", "type":"object", "field_mappings":[
            {"name":"known", "type":"array<text>"},
            {"name":"deep", "type":"object", "field_mappings":[{"name":"known","type":"text"}]}
        ]},
        {"name":"all", "type":"concatenate", "concatenate_fields":["z.known"], "include_dynamic_fields":true}
    ]});
    let raw = r#"{"a":null,"z":{"known":[null,"line\nvalue","last"],"u":[null,{"z":"last\ntext","a":false},[1]],"deep":{"known":"mapped","esca\u0070ed":{"z":null,"a":2}}}}"#;
    let expected: Value = serde_json::from_str(r#"{"a":null,"z":{"u":[null,{"z":"last\ntext","a":false},[1]],"deep":{"esca\u0070ed":{"z":null,"a":2}}}}"#).unwrap();
    fn primitives(value: &Value, values: &mut Vec<OwnedValue>) {
        match value {
            Value::Null => (),
            Value::Array(array) => array.iter().for_each(|child| primitives(child, values)),
            Value::Object(object) => object.values().for_each(|child| primitives(child, values)),
            _ => values.push(OwnedValue::from(value.clone())),
        }
    }
    for mode in ["dynamic", "lenient", "strict"] {
        let mut config = config.clone();
        config["mode"] = json!(mode);
        config["field_mappings"][1]["include_dynamic_fields"] = json!(mode == "dynamic");
        let mapper: DocMapper = serde_json::from_value(config).unwrap();
        if mode == "strict" {
            assert_error(
                &mapper,
                raw,
                DocParsingError::NoSuchFieldInSchema("a".into()),
            );
        } else {
            let named = mapper
                .doc_from_json_str(raw)
                .unwrap()
                .1
                .to_named_doc(&mapper.schema())
                .0;
            let mut concat = vec![
                OwnedValue::Str("line\nvalue".into()),
                OwnedValue::Str("last".into()),
            ];
            assert_eq!(named["z.known"], concat);
            assert_eq!(named["z.deep.known"], [OwnedValue::Str("mapped".into())]);
            if mode == "dynamic" {
                assert_eq!(named["_dynamic"], [OwnedValue::from(expected.clone())]);
                primitives(&expected, &mut concat);
            } else {
                assert!(!named.contains_key("_dynamic"));
            }
            assert_eq!(named["all"], concat);
        }
        for raw in [
            "{}",
            r#"{"z":{}}"#,
            r#"{"z":{"deep":{},"known":[null,"ok"]}}"#,
            r#"{"z":{"deep":{"known":null}}}"#,
        ] {
            let named = mapper
                .doc_from_json_str(raw)
                .unwrap()
                .1
                .to_named_doc(&mapper.schema())
                .0;
            assert!(!named.contains_key("_dynamic"), "{mode}: {raw}");
        }
        if mode != "strict" {
            let named = mapper
                .doc_from_json_str(r#"{"a":1,"z":{"known":[null,"ok"],"deep":{}}}"#)
                .unwrap()
                .1
                .to_named_doc(&mapper.schema())
                .0;
            if mode == "dynamic" {
                assert_eq!(named["_dynamic"], [OwnedValue::from(json!({"a":1}))]);
            } else {
                assert!(!named.contains_key("_dynamic"));
            }
        }
        assert_error(
            &mapper,
            r#"{"z":{"deep":null}}"#,
            DocParsingError::ValueError(
                "z.deep".into(),
                "expected an JSON object, got null".into(),
            ),
        );
        assert_error(
            &mapper,
            r#"{"z":{"known":["ok",[1]]}}"#,
            DocParsingError::ValueError("z.known".into(), "expected string, got `[1]`".into()),
        );
    }
}

#[test]
fn mixed_mapping_error_order_follows_input_policy() {
    let mapper: DocMapper = serde_json::from_str(r#"{"mode":"strict","field_mappings":[{"name":"z","type":"object","field_mappings":[{"name":"known","type":"text"}]}]}"#).unwrap();
    let raw = r#"{"z":{"u":null,"known":{}},"a":1}"#;
    let reference: Value = serde_json::from_str(raw).unwrap();
    let (first_key, first_value) = reference.as_object().unwrap().iter().next().unwrap();
    let expected = if first_key == "z" {
        format!(
            "z.{}",
            first_value.as_object().unwrap().keys().next().unwrap()
        )
    } else {
        first_key.clone()
    };
    assert_error(&mapper, raw, DocParsingError::NoSuchFieldInSchema(expected));
}
