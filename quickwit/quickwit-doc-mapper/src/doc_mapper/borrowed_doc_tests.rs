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

//! Differential tests: `DocMapper::doc_from_borrowed_json` must produce the same partitions,
//! documents and errors as `DocMapper::doc_from_json_obj`.

use tantivy::TantivyDocument as Document;
use tantivy::schema::OwnedValue;

use crate::doc_mapper::{BorrowedJsonDoc, DocMapper, JsonObject};

/// A field mapping exercising every leaf type, arrays, objects, concatenate fields, coercion and
/// the partition key. `{mode}` is replaced by each mode.
const DOC_MAPPING_TEMPLATE: &str = r#"{
    "mode": "{mode}",
    "store_source": {store_source},
    "store_document_size": true,
    "index_field_presence": true,
    "timestamp_field": "timestamp",
    "partition_key": "service,hash_mod(resource.host, 7)",
    "field_mappings": [
        {"name": "timestamp", "type": "datetime", "fast": true,
         "input_formats": ["rfc3339", "unix_timestamp", "%Y-%m-%d %H:%M:%S"]},
        {"name": "service", "type": "text", "tokenizer": "raw", "fast": true},
        {"name": "body", "type": "text"},
        {"name": "count", "type": "u64"},
        {"name": "delta", "type": "i64", "coerce": false},
        {"name": "ratio", "type": "f64"},
        {"name": "flag", "type": "bool"},
        {"name": "ip", "type": "ip"},
        {"name": "payload", "type": "bytes"},
        {"name": "hex_payload", "type": "bytes", "input_format": "hex"},
        {"name": "tags", "type": "array<text>", "tokenizer": "raw"},
        {"name": "values", "type": "array<i64>"},
        {"name": "attributes", "type": "json"},
        {"name": "events", "type": "array<json>"},
        {"name": "resource", "type": "object", "field_mappings": [
            {"name": "host", "type": "text", "tokenizer": "raw"},
            {"name": "pid", "type": "u64"},
            {"name": "inner", "type": "object", "field_mappings": [
                {"name": "zone", "type": "text"}
            ]}
        ]},
        {"name": "all_text", "type": "concatenate",
         "concatenate_fields": ["body", "service", "attributes", "count", "flag"],
         "include_dynamic_fields": {include_dynamic}}
    ]
}"#;

fn build_doc_mappers() -> Vec<(String, DocMapper)> {
    let mut doc_mappers = Vec::new();
    for mode in ["dynamic", "lenient", "strict"] {
        for store_source in ["true", "false"] {
            let include_dynamic = if mode == "dynamic" { "true" } else { "false" };
            let doc_mapping_json = DOC_MAPPING_TEMPLATE
                .replace("{mode}", mode)
                .replace("{store_source}", store_source)
                .replace("{include_dynamic}", include_dynamic);
            let doc_mapper: DocMapper = serde_json::from_str(&doc_mapping_json).unwrap();
            doc_mappers.push((format!("{mode}/store_source={store_source}"), doc_mapper));
        }
    }
    // The default dynamic mapping, and the dynamic mapping used by OTel logs.
    let default_doc_mapper: DocMapper = serde_json::from_str("{}").unwrap();
    doc_mappers.push(("default".to_string(), default_doc_mapper));
    let otel_like_doc_mapper: DocMapper = serde_json::from_str(
        r#"{
            "mode": "dynamic",
            "dynamic_mapping": {"indexed": false, "stored": true, "expand_dots": true},
            "field_mappings": [
                {"name": "timestamp_nanos", "type": "datetime", "input_formats": ["unix_timestamp"],
                 "fast": true},
                {"name": "service_name", "type": "text", "tokenizer": "raw", "fast": true},
                {"name": "severity_text", "type": "text", "tokenizer": "raw"},
                {"name": "body", "type": "json"},
                {"name": "attributes", "type": "json", "tokenizer": "raw"},
                {"name": "resource", "type": "object", "field_mappings": [
                    {"name": "host", "type": "text"}
                ]}
            ]
        }"#,
    )
    .unwrap();
    doc_mappers.push(("otel_like".to_string(), otel_like_doc_mapper));
    doc_mappers
}

/// Returns the field values in insertion order. Tantivy's `PartialEq` on documents ignores the
/// order of the values, which is not strict enough here.
fn ordered_field_values(document: &Document) -> Vec<(u32, OwnedValue)> {
    document
        .field_values()
        .map(|(field, value)| (field.field_id(), OwnedValue::from(value)))
        .collect()
}

fn assert_same_conversion(doc_mapper_name: &str, doc_mapper: &DocMapper, json_doc: &str) {
    let document_len = json_doc.len() as u64;
    let owned_result = serde_json::from_str::<JsonObject>(json_doc)
        .map_err(|error| error.to_string())
        .map(|json_obj| doc_mapper.doc_from_json_obj(json_obj, document_len));
    let borrowed_doc_result = BorrowedJsonDoc::parse(json_doc.as_bytes());
    let borrowed_result = borrowed_doc_result
        .as_ref()
        .map_err(|error| error.to_string())
        .map(|borrowed_doc| doc_mapper.doc_from_borrowed_json(borrowed_doc, document_len));
    let context = || format!("doc mapper: {doc_mapper_name}, doc: {json_doc}");
    match (owned_result, borrowed_result) {
        (Ok(Ok((owned_partition, owned_document))), Ok(Ok((partition, document)))) => {
            assert_eq!(owned_partition, partition, "{}", context());
            assert_eq!(
                ordered_field_values(&owned_document),
                ordered_field_values(&document),
                "{}",
                context()
            );
        }
        (Ok(Err(owned_error)), Ok(Err(error))) => {
            assert_eq!(owned_error, error, "{}", context());
        }
        (Err(owned_parse_error), Err(parse_error)) => {
            assert_eq!(owned_parse_error, parse_error, "{}", context());
        }
        (owned_result, borrowed_result) => {
            panic!(
                "{}: owned: {:?}, borrowed: {:?}",
                context(),
                owned_result.map(|result| result.map(|(_, doc)| ordered_field_values(&doc))),
                borrowed_result.map(|result| result.map(|(_, doc)| ordered_field_values(&doc)))
            );
        }
    }
}

const HAND_WRITTEN_DOCS: &[&str] = &[
    r#"{}"#,
    r#"{"timestamp": "2024-01-02T03:04:05Z", "service": "api", "body": "hello world"}"#,
    r#"{"timestamp": 1704164645, "count": 3, "delta": -4, "ratio": 0.5, "flag": true}"#,
    r#"{"timestamp": 1704164645.123, "ratio": 3, "count": 18446744073709551615}"#,
    r#"{"timestamp": "2024-01-02 03:04:05", "ip": "192.168.0.1", "payload": "aGVsbG8="}"#,
    r#"{"ip": "::1", "hex_payload": "deadbeef", "tags": ["a", null, "b"], "values": [1, -2]}"#,
    r#"{"attributes": {"k": "v", "n": 1, "d": "2024-01-02T03:04:05+01:00", "z": [null, {}]}}"#,
    r#"{"events": [{"name": "e1"}, null, {"name": "e2", "at": "2024-01-02T03:04:05Z"}]}"#,
    r#"{"resource": {"host": "h1", "pid": 12, "inner": {"zone": "z1"}}}"#,
    r#"{"resource": {"host": "h1", "unmapped": 1, "inner": {"zone": "z", "other": [1, "x"]}}}"#,
    r#"{"resource": {"inner": {"other": null}}, "unmapped_root": {"a": {"b": "2024-01-02T03:04:05Z"}}}"#,
    r#"{"service": "a", "service": "b", "count": 1, "count": 2, "x": 1, "x": {"y": 2}}"#,
    r#"{"body": "esc\"aped \u00e9 \ud83d\ude00\n", "service": "\t"}"#,
    r#"{"unmapped": "1 not a date", "unmapped2": "2024-13-45T00:00:00Z", "u3": ""}"#,
    r#"{"unmapped": [1, -1, 1.5, 18446744073709551615, true, null, "s", [], {}]}"#,
    // Errors.
    r#"{"count": -1}"#,
    r#"{"count": "12"}"#,
    r#"{"delta": "12"}"#,
    r#"{"count": 1.5}"#,
    r#"{"ratio": "not a float"}"#,
    r#"{"flag": "true"}"#,
    r#"{"ip": "not an ip"}"#,
    r#"{"ip": 1}"#,
    r#"{"payload": "not base64!"}"#,
    r#"{"payload": 12}"#,
    r#"{"hex_payload": "xyz"}"#,
    r#"{"timestamp": "yesterday"}"#,
    r#"{"timestamp": true}"#,
    r#"{"timestamp": [1, 2]}"#,
    r#"{"body": 12}"#,
    r#"{"body": ["a", "b"]}"#,
    r#"{"tags": [["nested"]]}"#,
    r#"{"tags": "single"}"#,
    r#"{"attributes": "not an object"}"#,
    r#"{"events": [1]}"#,
    r#"{"resource": "not an object"}"#,
    r#"{"resource": null}"#,
    r#"{"resource": {"inner": 3}}"#,
    r#"{"resource": {"pid": "x", "host": 1}}"#,
    r#"{"zzz": 1, "count": "bad"}"#,
    r#"{"aaa": 1, "count": "bad"}"#,
    // Parse errors.
    r#"[]"#,
    r#"{"a": }"#,
    r#"{"a": 1} {"b": 2}"#,
];

#[test]
fn test_borrowed_doc_same_as_owned_doc_hand_written() {
    for (doc_mapper_name, doc_mapper) in build_doc_mappers() {
        for json_doc in HAND_WRITTEN_DOCS {
            assert_same_conversion(&doc_mapper_name, &doc_mapper, json_doc);
        }
    }
}

/// Deterministic xorshift generator, to keep failures reproducible without a new dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn pick<'a>(&mut self, items: &[&'a str]) -> &'a str {
        items[self.below(items.len() as u64) as usize]
    }
}

const KEYS: &[&str] = &[
    "timestamp",
    "service",
    "body",
    "count",
    "delta",
    "ratio",
    "flag",
    "ip",
    "payload",
    "hex_payload",
    "tags",
    "values",
    "attributes",
    "events",
    "resource",
    "host",
    "pid",
    "inner",
    "zone",
    "all_text",
    "unmapped",
    "a",
    "b",
    "é",
    "",
    "timestamp_nanos",
    "service_name",
    "severity_text",
    "k.with.dots",
];

const STRINGS: &[&str] = &[
    "",
    "text",
    "2024-01-02T03:04:05Z",
    "2024-01-02T03:04:05.123+02:00",
    "2024-01-02 03:04:05",
    "1704164645",
    "-12",
    "1.5",
    "192.168.1.1",
    "::ffff:10.0.0.1",
    "aGVsbG8=",
    "deadbeef",
    "true",
    "9 lives",
    "esc\\\"aped\\n",
    "\\u00e9t\\u00e9",
    "\\ud83d\\ude00",
];

const NUMBERS: &[&str] = &[
    "0",
    "1",
    "-1",
    "42",
    "1704164645",
    "1704164645123",
    "-9223372036854775808",
    "9223372036854775808",
    "18446744073709551615",
    "18446744073709551616",
    "0.5",
    "-0.0",
    "1e3",
    "1.7976931348623157e308",
    "3.0",
];

fn write_random_value(rng: &mut Rng, depth: usize, output: &mut String) {
    let kind = if depth >= 3 {
        rng.below(5)
    } else {
        rng.below(8)
    };
    match kind {
        0 => output.push_str("null"),
        1 => output.push_str(if rng.below(2) == 0 { "true" } else { "false" }),
        2 | 3 => output.push_str(rng.pick(NUMBERS)),
        4 => {
            output.push('"');
            output.push_str(rng.pick(STRINGS));
            output.push('"');
        }
        5 => {
            output.push('[');
            let num_elements = rng.below(4);
            for i in 0..num_elements {
                if i > 0 {
                    output.push(',');
                }
                write_random_value(rng, depth + 1, output);
            }
            output.push(']');
        }
        _ => write_random_object(rng, depth + 1, output),
    }
}

/// Keys are drawn from a small vocabulary, so objects regularly contain duplicate keys.
fn write_random_object(rng: &mut Rng, depth: usize, output: &mut String) {
    output.push('{');
    let num_entries = rng.below(6);
    for i in 0..num_entries {
        if i > 0 {
            output.push(',');
        }
        output.push('"');
        output.push_str(rng.pick(KEYS));
        output.push_str("\":");
        write_random_value(rng, depth, output);
    }
    output.push('}');
}

#[test]
fn test_borrowed_doc_same_as_owned_doc_random() {
    let doc_mappers = build_doc_mappers();
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    let mut num_successes = 0;
    for _ in 0..20_000 {
        let mut json_doc = String::new();
        write_random_object(&mut rng, 0, &mut json_doc);
        for (doc_mapper_name, doc_mapper) in &doc_mappers {
            assert_same_conversion(doc_mapper_name, doc_mapper, &json_doc);
        }
        let json_obj: JsonObject = serde_json::from_str(&json_doc).unwrap();
        if doc_mappers[0]
            .1
            .doc_from_json_obj(json_obj, json_doc.len() as u64)
            .is_ok()
        {
            num_successes += 1;
        }
    }
    // Make sure the generator does not only produce invalid documents.
    assert!(
        num_successes > 1_000,
        "only {num_successes} valid documents"
    );
}
