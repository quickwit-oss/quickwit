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
use tantivy::Document;
use tantivy::schema::OwnedValue;

#[test]
fn json_mapping_compatibility_fixture() {
    let mapper: DocMapper = serde_json::from_str(r#"{
        "mode":"dynamic", "store_source":true, "index_field_presence":true,
        "partition_key":"tenant",
        "field_mappings":[
            {"name":"tenant","type":"text","tokenizer":"raw"},
            {"name":"text","type":"text"},
            {"name":"count","type":"u64"},
            {"name":"json","type":"json"},
            {"name":"nested","type":"object","field_mappings":[{"name":"known","type":"text"}]},
            {"name":"all","type":"concatenate","concatenate_fields":["text","count","json"],"include_dynamic_fields":true}
        ]
    }"#).unwrap();
    let raw = r#"{"tenant":"old","tenant":"new","text":"2024-01-02T03:04:05Z","count":1,"json":{"z":null,"a":[1,18446744073709551615,1.0,"2024-01-02T03:04:05Z"]},"nested":{"unknown":{"z":2,"a":1},"known":"ok"}}"#;
    let (partition, doc) = mapper.doc_from_json_str(raw).unwrap();
    let (owned_partition, owned_doc) = mapper
        .doc_from_json_obj(serde_json::from_str(raw).unwrap(), raw.len() as u64)
        .unwrap();
    assert_eq!(partition, owned_partition);
    assert_eq!(
        doc.to_named_doc(&mapper.schema()).0,
        owned_doc.to_named_doc(&mapper.schema()).0
    );
    let named = doc.to_named_doc(&mapper.schema()).0;
    assert_eq!(named["tenant"], [OwnedValue::Str("new".into())]);
    assert_eq!(
        named["text"],
        [OwnedValue::Str("2024-01-02T03:04:05Z".into())]
    );
    assert_eq!(named["count"], [OwnedValue::U64(1)]);
    // Reference traversal comes from real serde_json parsing, independent of normalization.
    let reference: serde_json::Map<String, serde_json::Value> = serde_json::from_str(raw).unwrap();
    let mut expected_concat = Vec::new();
    for key in reference.keys() {
        match key.as_str() {
            "text" => expected_concat.push(OwnedValue::Str("2024-01-02T03:04:05Z".into())),
            "count" => expected_concat.push(OwnedValue::U64(1)),
            "json" => expected_concat.extend([
                OwnedValue::I64(1),
                OwnedValue::U64(u64::MAX),
                OwnedValue::F64(1.0),
                OwnedValue::Str("2024-01-02T03:04:05Z".into()),
            ]),
            _ => (),
        }
    }
    // Dynamic concatenation follows mapped fields; retain nested map iteration order.
    expected_concat.extend(
        reference["nested"]["unknown"]
            .as_object()
            .unwrap()
            .values()
            .cloned()
            .map(OwnedValue::from),
    );
    assert_eq!(named["all"], expected_concat);
    let expected_source: OwnedValue = serde_json::from_str::<serde_json::Value>(raw)
        .unwrap()
        .into();
    assert_eq!(named["_source"], [expected_source]);
    let expected_dynamic: OwnedValue =
        serde_json::from_str::<serde_json::Value>(r#"{"nested":{"unknown":{"z":2,"a":1}}}"#)
            .unwrap()
            .into();
    assert_eq!(named["_dynamic"], [expected_dynamic]);
    assert!(!named["_field_presence"].is_empty());
}

#[test]
fn json_mapping_errors_and_cardinality() {
    let mapper: DocMapper = serde_json::from_str(r#"{"mode":"strict","field_mappings":[{"name":"count","type":"u64"},{"name":"tags","type":"array<text>"}]}"#).unwrap();
    for raw in [
        r#"{"count":-1}"#,
        r#"{"count":[]}"#,
        r#"{"tags":[null,1]}"#,
        r#"{"z":1,"a":2}"#,
        r#"{"count":1.0}"#,
    ] {
        let from_raw = mapper.doc_from_json_str(raw).unwrap_err();
        let from_owned = mapper
            .doc_from_json_obj(serde_json::from_str(raw).unwrap(), raw.len() as u64)
            .unwrap_err();
        assert_eq!(from_raw.to_string(), from_owned.to_string());
    }
    assert_eq!(
        mapper
            .doc_from_json_str(r#"{"count":null,"tags":[null,"ok"]}"#)
            .unwrap()
            .1
            .to_named_doc(&mapper.schema())
            .0["tags"],
        [OwnedValue::Str("ok".into())]
    );
    for raw in ["null", "[]", "1", "{} trailing", "{", "{\"count\":1e400}"] {
        assert!(matches!(
            mapper.doc_from_json_str(raw),
            Err(quickwit_doc_mapper::DocParsingError::NotJsonObject(_))
        ));
    }
}

#[test]
fn direct_json_insertion_matches_owned_value_conversion() {
    let mapper: DocMapper = serde_json::from_str(
        r#"{
        "mode":"dynamic", "store_source":true,
        "field_mappings":[{"name":"json","type":"json"}]
    }"#,
    )
    .unwrap();
    let raw = r#"{
        "duplicate":"first", "duplicate":"last",
        "escaped\u0022":"line\nquote\"unicode\u00e9",
        "json":{
            "numbers":[-9223372036854775808,-1,0,-0,1,1.0,-0.0,9223372036854775807,9223372036854775808,18446744073709551615,18446744073709551616,1e-100,1e100],
            "dates":["2024-01-02T03:04:05Z","2024-01-02t03:04:05z","2024-01-02T03:04:05.123456789+02:30","2024-01-02","2024-01-02T25:00:00Z"," 2024-01-02T03:04:05Z","2024-01-02T03:04:05Z "],
            "nested":[null,{},[],[true,false,{"z":1,"a":"text"}]],
            "negative_zeros":[-0,-0.0]
        },
        "dynamic":{"date":"2024-01-02T03:04:05Z","nested":[null,true,1,18446744073709551615]}
    }"#;
    let mut expected: serde_json::Value = serde_json::from_str(raw).unwrap();
    let (_, doc) = mapper.doc_from_json_bytes(raw.as_bytes()).unwrap();
    let named = doc.to_named_doc(&mapper.schema()).0;
    // Desired policy: all object levels follow serde_json map order, including the root.
    // Do not reproduce the former extra _source/_dynamic root BTreeMap sort.
    assert_eq!(named["_source"], [OwnedValue::from(expected.clone())]);
    let json = expected.as_object_mut().unwrap().remove("json").unwrap();
    assert_eq!(named["json"], [OwnedValue::from(json)]);
    assert_eq!(named["_dynamic"], [OwnedValue::from(expected)]);

    // OwnedValue equality considers +0.0 and -0.0 equal, so check their bits explicitly.
    let OwnedValue::Object(json_fields) = &named["json"][0] else {
        panic!("expected a JSON object");
    };
    let (_, OwnedValue::Array(zeros)) = json_fields
        .iter()
        .find(|(key, _)| key == "negative_zeros")
        .unwrap()
    else {
        panic!("expected the negative-zero array");
    };
    for zero in zeros {
        let OwnedValue::F64(value) = zero else {
            panic!("expected a floating-point negative zero");
        };
        assert_eq!(value.to_bits(), (-0.0f64).to_bits());
    }
}

#[test]
fn direct_insertion_preserves_fixed_errors() {
    let mapper: DocMapper = serde_json::from_str(
        r#"{"mode":"strict","field_mappings":[
        {"name":"text","type":"text"},
        {"name":"count","type":"u64"},
        {"name":"json","type":"json"},
        {"name":"all","type":"concatenate","concatenate_fields":["text","count","json"]}
    ]}"#,
    )
    .unwrap();
    for (raw, expected) in [
        (
            r#"{"text":1}"#,
            DocParsingError::ValueError("text".into(), "expected string, got `1`".into()),
        ),
        (
            r#"{"count":-1}"#,
            DocParsingError::ValueError(
                "count".into(),
                "expected u64, got inconvertible JSON number `-1`".into(),
            ),
        ),
        (
            r#"{"count":1.0}"#,
            DocParsingError::ValueError(
                "count".into(),
                "expected u64, got inconvertible JSON number `1.0`".into(),
            ),
        ),
        (
            r#"{"json":"no"}"#,
            DocParsingError::ValueError("json".into(), "expected object, got `\"no\"`".into()),
        ),
        (
            r#"{"text":[]}"#,
            DocParsingError::MultiValuesNotSupported("text".into()),
        ),
        (
            r#"{"z":1,"a":2}"#,
            DocParsingError::NoSuchFieldInSchema(
                serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(
                    r#"{"z":1,"a":2}"#,
                )
                .unwrap()
                .keys()
                .next()
                .unwrap()
                .clone(),
            ),
        ),
    ] {
        assert_eq!(mapper.doc_from_json_str(raw).unwrap_err(), expected);
    }
    assert_eq!(
        mapper
            .doc_from_json_bytes(b"{\"text\":\"\xff\"}")
            .unwrap_err(),
        DocParsingError::NotJsonObject("document contains some invalid UTF-8 characters".into())
    );
}
