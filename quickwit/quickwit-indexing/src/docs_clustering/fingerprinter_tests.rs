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

use quickwit_config::DocsClusteringConfig;
use quickwit_doc_mapper::{BorrowedJsonDoc, RandomJsonDocs};
use serde_json::Value as JsonValue;

use super::fingerprinter::Fingerprinter;

fn parse(s: &str) -> JsonValue {
    serde_json::from_str(s).unwrap()
}

fn test_docs_clustering_config() -> DocsClusteringConfig {
    docs_clustering_config(serde_json::json!([
        {
            "fingerprint": [{
                "kind": "structure",
                "exclude": ["tag", "custom"]
            }]
        },
        {
            "fingerprint": [{
                "path": "message",
                "kind": "tokenized"
            },
            {
                "path": "service",
                "kind": "raw"
            }]
        }
    ]))
}

fn test_fingerprinter() -> Fingerprinter {
    let docs_clustering_config = test_docs_clustering_config();
    Fingerprinter::new(&docs_clustering_config)
}

fn docs_clustering_config(json_value: JsonValue) -> DocsClusteringConfig {
    serde_json::from_value(json_value).unwrap()
}

#[test]
fn configured_fingerprinter_returns_config() {
    let docs_clustering_config = test_docs_clustering_config();
    let fingerprinter = test_fingerprinter();
    assert_eq!(fingerprinter.config(), &docs_clustering_config);
}

#[test]
fn identical_logs_have_equal_fingerprint() {
    let fingerprinter = test_fingerprinter();
    let doc = parse(r#"{"message":"server started at 8080","service":"api"}"#);
    assert_eq!(
        fingerprinter.fingerprint(&doc),
        fingerprinter.fingerprint(&doc)
    );
}

#[test]
fn dotted_key_and_nested_path_have_different_schema_fingerprints() {
    let fingerprinter = test_fingerprinter();
    let dotted_key_doc = parse(r#"{"a.b":1}"#);
    let nested_path_doc = parse(r#"{"a":{"b":1}}"#);
    let dotted_key_fingerprint = fingerprinter.fingerprint(&dotted_key_doc);
    let nested_path_fingerprint = fingerprinter.fingerprint(&nested_path_doc);

    assert_ne!(dotted_key_fingerprint[0], nested_path_fingerprint[0]);
    assert_eq!(dotted_key_fingerprint[1], nested_path_fingerprint[1]);
}

#[test]
fn same_message_template_has_equal_fingerprint() {
    let fingerprinter = test_fingerprinter();
    let doc1 = parse(r#"{"message":"server started at 8080","service":"api"}"#);
    let doc2 = parse(r#"{"message":"server started at 9090","service":"api"}"#);
    assert_eq!(
        fingerprinter.fingerprint(&doc1),
        fingerprinter.fingerprint(&doc2)
    );
}

#[test]
fn different_message_template_changes_grouping_fingerprint_only() {
    let fingerprinter = test_fingerprinter();
    let doc1 = parse(r#"{"message":"server started at 8080","service":"api"}"#);
    let doc2 = parse(r#"{"message":"connection from 1.2.3.4","service":"api"}"#);
    let doc1_fingerprint = fingerprinter.fingerprint(&doc1);
    let doc2_fingerprint = fingerprinter.fingerprint(&doc2);
    assert_eq!(doc1_fingerprint[0], doc2_fingerprint[0]);
    assert_ne!(doc1_fingerprint[1], doc2_fingerprint[1]);
}

#[test]
fn tokenized_field_respects_hardcoded_grouping_token_limit() {
    let docs_clustering_config = docs_clustering_config(serde_json::json!([
        {
            "fingerprint": [{
                "kind": "structure"
            }]
        },
        {
            "fingerprint": [{
                "path": "message",
                "kind": "tokenized"
            }]
        }
    ]));
    let fingerprinter = Fingerprinter::new(&docs_clustering_config);
    let prefix = "alpha ".repeat(25);
    let doc1 = parse(&format!(r#"{{"message":"{prefix}123"}}"#));
    let doc2 = parse(&format!(r#"{{"message":"{prefix}beta"}}"#));
    assert_eq!(
        fingerprinter.fingerprint(&doc1)[1],
        fingerprinter.fingerprint(&doc2)[1]
    );
}

#[test]
fn different_service_changes_grouping_fingerprint_only() {
    let fingerprinter = test_fingerprinter();
    let doc1 = parse(r#"{"message":"server started at 8080","service":"api"}"#);
    let doc2 = parse(r#"{"message":"server started at 8080","service":"worker"}"#);
    let doc1_fingerprint = fingerprinter.fingerprint(&doc1);
    let doc2_fingerprint = fingerprinter.fingerprint(&doc2);
    assert_eq!(doc1_fingerprint[0], doc2_fingerprint[0]);
    assert_ne!(doc1_fingerprint[1], doc2_fingerprint[1]);
}

#[test]
fn ignored_custom_shape_does_not_change_fingerprint() {
    let fingerprinter = test_fingerprinter();
    let doc1 = parse(r#"{"message":"server started at 8080","service":"api","custom":{"a":1}}"#);
    let doc2 = parse(r#"{"message":"server started at 8080","service":"api","custom":{"b":2}}"#);
    assert_eq!(
        fingerprinter.fingerprint(&doc1),
        fingerprinter.fingerprint(&doc2)
    );
}

#[test]
fn extra_non_ignored_shape_changes_schema_fingerprint_only() {
    let fingerprinter = test_fingerprinter();
    let doc1 = parse(r#"{"message":"server started at 8080","service":"api"}"#);
    let doc2 = parse(r#"{"message":"server started at 8080","service":"api","host":"web-1"}"#);
    let doc1_fingerprint = fingerprinter.fingerprint(&doc1);
    let doc2_fingerprint = fingerprinter.fingerprint(&doc2);
    assert_ne!(doc1_fingerprint[0], doc2_fingerprint[0]);
    assert_eq!(doc1_fingerprint[1], doc2_fingerprint[1]);
}

#[test]
fn configured_raw_field_changes_grouping_fingerprint_only() {
    let docs_clustering_config = docs_clustering_config(serde_json::json!([
        {
            "fingerprint": [{
                "kind": "structure"
            }]
        },
        {
            "fingerprint": [{
                "path": "host",
                "kind": "raw"
            }]
        }
    ]));
    let fingerprinter = Fingerprinter::new(&docs_clustering_config);
    let doc1 = parse(r#"{"message":"same","host":"web-1"}"#);
    let doc2 = parse(r#"{"message":"same","host":"web-2"}"#);
    let doc1_fingerprint = fingerprinter.fingerprint(&doc1);
    let doc2_fingerprint = fingerprinter.fingerprint(&doc2);
    assert_eq!(doc1_fingerprint[0], doc2_fingerprint[0]);
    assert_ne!(doc1_fingerprint[1], doc2_fingerprint[1]);
}

#[test]
fn raw_field_hashes_non_string_values() {
    let docs_clustering_config = docs_clustering_config(serde_json::json!([
        {
            "fingerprint": [{
                "kind": "structure"
            }]
        },
        {
            "fingerprint": [{
                "path": "status",
                "kind": "raw"
            }]
        }
    ]));
    let fingerprinter = Fingerprinter::new(&docs_clustering_config);
    let doc1 = parse(r#"{"status":200}"#);
    let doc2 = parse(r#"{"status":500}"#);
    let doc1_fingerprint = fingerprinter.fingerprint(&doc1);
    let doc2_fingerprint = fingerprinter.fingerprint(&doc2);
    assert_eq!(doc1_fingerprint[0], doc2_fingerprint[0]);
    assert_ne!(doc1_fingerprint[1], doc2_fingerprint[1]);
}

#[test]
fn raw_field_preserves_json_type_and_structure_boundaries() {
    let docs_clustering_config = docs_clustering_config(serde_json::json!([
        {
            "fingerprint": [{
                "path": "value",
                "kind": "raw"
            }]
        }
    ]));
    let fingerprinter = Fingerprinter::new(&docs_clustering_config);
    let docs = [
        parse(r#"{"value":"null"}"#),
        parse(r#"{"value":null}"#),
        parse(r#"{"value":""}"#),
        parse(r#"{"value":[]}"#),
        parse(r#"{"value":{}}"#),
        parse(r#"{"value":false}"#),
        parse(r#"{"value":-1}"#),
        parse(r#"{"value":18446744073709551615}"#),
    ];
    let fingerprints: Vec<_> = docs
        .iter()
        .map(|doc| fingerprinter.fingerprint(doc))
        .collect();

    for (left_idx, left_fingerprint) in fingerprints.iter().enumerate() {
        for right_fingerprint in &fingerprints[left_idx + 1..] {
            assert_ne!(left_fingerprint, right_fingerprint);
        }
    }
}

#[test]
fn absent_grouping_values_preserve_field_position() {
    let docs_clustering_config = docs_clustering_config(serde_json::json!([
        {
            "fingerprint": [{
                "kind": "structure"
            }]
        },
        {
            "fingerprint": [{
                "path": "a",
                "kind": "raw"
            },
            {
                "path": "b",
                "kind": "raw"
            }]
        }
    ]));
    let fingerprinter = Fingerprinter::new(&docs_clustering_config);
    let doc1 = parse(r#"{"a":"x","b":null}"#);
    let doc2 = parse(r#"{"a":null,"b":"x"}"#);
    let doc1_fingerprint = fingerprinter.fingerprint(&doc1);
    let doc2_fingerprint = fingerprinter.fingerprint(&doc2);

    assert_eq!(doc1_fingerprint[0], doc2_fingerprint[0]);
    assert_ne!(doc1_fingerprint[1], doc2_fingerprint[1]);
}

/// Exercises every clustering method, nested paths and exclusions.
fn differential_docs_clustering_config() -> DocsClusteringConfig {
    docs_clustering_config(serde_json::json!([
        {"fingerprint": [{"kind": "structure"}]},
        {"fingerprint": [{"kind": "structure", "exclude": ["attributes", "inner.body"]}]},
        {"fingerprint": [
            {"kind": "raw", "path": "service"},
            {"kind": "raw", "path": "count"},
            {"kind": "tokenized", "path": "body"}
        ]},
        {"fingerprint": [
            {"kind": "raw", "path": "attributes"},
            {"kind": "raw", "path": "inner.zone"},
            {"kind": "tokenized", "path": "inner.body", "max_tokens": 3},
            {"kind": "raw", "path": "tags"},
            {"kind": "raw", "path": "values"}
        ]}
    ]))
}

fn assert_same_fingerprint(fingerprinter: &Fingerprinter, json_doc: &str) {
    let json_value = JsonValue::Object(serde_json::from_str(json_doc).unwrap());
    let borrowed_json_doc = BorrowedJsonDoc::parse(json_doc.as_bytes()).unwrap();
    assert_eq!(
        fingerprinter.fingerprint_borrowed(&borrowed_json_doc),
        fingerprinter.fingerprint(&json_value),
        "doc: {json_doc}"
    );
}

#[test]
fn borrowed_fingerprint_same_as_owned_fingerprint_hand_written() {
    let fingerprinter = Fingerprinter::new(&differential_docs_clustering_config());
    let json_docs = [
        r#"{}"#,
        r#"{"service": "api", "body": "server started at 8080", "count": 3}"#,
        r#"{"body": "first", "service": "api", "body": "connection from 1.2.3.4"}"#,
        r#"{"service": "api", "count": 3, "count": -3.5e10, "count": 18446744073709551615}"#,
        r#"{"service": 12, "body": 12, "count": "12"}"#,
        r#"{"service": null, "body": null, "count": [1, 2.0, -0.0, 1e3]}"#,
        r#"{"attributes": {"z": 1, "a": {"y": [true, null], "b": "x"}, "z": 2}}"#,
        r#"{"inner": {"zone": {"b": 1, "a": 2}, "body": "job 123 finished in 42ms"}}"#,
        r#"{"inner": {"body": "a b c d e f", "body": "x"}, "inner": {"zone": "eu"}}"#,
        r#"{"body": "esc\"aped\nline \u00e9t\u00e9 \ud83d\ude00", "service": "s\u0000"}"#,
        r#"{"tags": ["a", {"k": "v", "j": [1]}, []], "values": {}}"#,
        r#"{"k.with.dots": {"x": 1}, "k": {"with": {"dots": 2}}, "": {"": null}}"#,
        r#"{"b": 1, "a": 2, "é": 3, "c": {"y": 1, "x": 2}}"#,
    ];
    for json_doc in json_docs {
        assert_same_fingerprint(&fingerprinter, json_doc);
    }
}

#[test]
fn borrowed_fingerprint_same_as_owned_fingerprint_random() {
    let fingerprinter = Fingerprinter::new(&differential_docs_clustering_config());
    let mut random_json_docs = RandomJsonDocs::new(0x9e37_79b9_7f4a_7c15);
    for _ in 0..20_000 {
        let json_doc = random_json_docs.next_doc();
        assert_same_fingerprint(&fingerprinter, &json_doc);
    }
}
