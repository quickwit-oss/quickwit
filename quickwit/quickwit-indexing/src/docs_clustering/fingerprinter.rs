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

//! Fingerprint computation.
//!
//! A fingerprint contains one hash for each configured fingerprint policy, in configuration order.
//! Each policy can combine:
//! 1. structure fields, represented by sorted sets of leaf JSON paths;
//! 2. raw fields, represented by their exact JSON values;
//! 3. tokenized fields, represented by their token types.
//!
//! ```text
//! JSON document
//!     |
//!     +--> policy 0 fields --> hash 0
//!     +--> policy 1 fields --> hash 1
//!     +--> ...
//!     +--> policy N fields --> hash N
//!     |
//!     v
//! Fingerprint [hash 0, hash 1, ..., hash N]
//! ```
//!
//! Tokenized fields use the sequence of token types as a lightweight message template. This keeps
//! the stable structure of a value while ignoring volatile literals such as IDs, ports, UUIDs, or
//! IP addresses. By default, the first 50 tokens contribute to the fingerprint; `max_tokens` can
//! override this limit for each tokenized method.
//!
//! Examples:
//! ```text
//! "server started at 8080"
//!     -> Word Gap Word Gap Word Gap Number
//!
//! "server started at 9090"
//!     -> Word Gap Word Gap Word Gap Number
//! ```
//!
//! These two values produce the same tokenized signature and policy hash, so they can be ordered
//! together. A different shape produces a different signature:
//!
//! ```text
//! "connection from 1.2.3.4"
//!     -> Word Gap Word Gap IPv4
//! ```
//!
//! Raw fields preserve JSON types and nested structure, which is useful for dimensions such as
//! `service` or numeric status codes. Missing fields are encoded as absent so configured field
//! positions remain distinct within a policy hash.
use std::hash::Hasher;
use std::ops::Deref;
use std::sync::Arc;

/// Fingerprints only live in the indexing pipeline (they order documents inside a split and are
/// never persisted), so the hash function can change freely. FxHash hashes a word at a time; FNV
/// (previously) does one multiply per byte, which made fingerprinting long attribute paths cost
/// as much as indexing them.
type FingerprintHasher = rustc_hash::FxHasher;
use quickwit_config::{
    ClusteringMethod, ClusteringPolicy, DocsClusteringConfig, FingerprintPolicy, JsonPath,
};
use serde_json::Value as JsonValue;
use smallvec::SmallVec;

use super::tokenize;

const PATH_COMPONENT_SEPARATOR: u8 = 0xF9;
const FIELD_ABSENT: u8 = 0xFA;
const FIELD_PRESENT: u8 = 0xFB;
const PATH_SEPARATOR: u8 = 0xFC;
const FIELD_BOUNDARY: u8 = 0xFD;
const TOKENIZED_TOKEN_SEPARATOR: u8 = 0xFE;
const DEFAULT_MAX_GROUPING_TOKENS: usize = 50;

// Inline 4 hashes to avoid heap allocations.
// This is usually enough for most use cases.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Fingerprint(SmallVec<[u64; 4]>);

impl Deref for Fingerprint {
    type Target = [u64];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
impl Fingerprint {
    pub(crate) fn new<const N: usize>(hashes: [u64; N]) -> Self {
        let mut fingerprint = SmallVec::new();
        fingerprint.extend_from_slice(&hashes);
        Self(fingerprint)
    }
}

#[derive(Clone)]
pub struct Fingerprinter {
    config: Arc<DocsClusteringConfig>,
    policies: Arc<[FingerprintPolicy]>,
}

impl Fingerprinter {
    pub fn new(config: &DocsClusteringConfig) -> Self {
        let mut policies = Vec::new();
        for policy in &config.policies {
            match policy {
                ClusteringPolicy::Fingerprint { fingerprint } => {
                    policies.push(fingerprint.clone());
                }
            }
        }

        Self {
            config: Arc::new(config.clone()),
            policies: policies.into_boxed_slice().into(),
        }
    }

    pub fn config(&self) -> &DocsClusteringConfig {
        &self.config
    }

    /// First path component of every field whose value (not just presence) is hashed.
    pub fn value_root_fields(&self) -> impl Iterator<Item = &str> {
        self.policies
            .iter()
            .flat_map(|policy| policy.fingerprint.iter())
            .filter_map(|method| match method {
                ClusteringMethod::Structure { .. } => None,
                ClusteringMethod::Raw { path } | ClusteringMethod::Tokenized { path, .. } => {
                    path.first().map(String::as_str)
                }
            })
    }

    pub fn fingerprint(&self, json_value: &JsonValue) -> Fingerprint {
        let mut fingerprint = SmallVec::new();
        for policy in self.policies.iter() {
            let mut hasher = FingerprintHasher::default();

            for method in policy.fingerprint.iter() {
                match method {
                    ClusteringMethod::Structure { exclude } => {
                        self.hash_structure(json_value, exclude, &mut hasher);
                    }
                    ClusteringMethod::Raw { path } => {
                        self.hash_raw_value(json_value, path, &mut hasher);
                    }
                    ClusteringMethod::Tokenized { path, max_tokens } => {
                        self.hash_string_tokenized(json_value, path, *max_tokens, &mut hasher);
                    }
                }
            }

            fingerprint.push(hasher.finish());
        }

        Fingerprint(fingerprint)
    }

    fn hash_structure(
        &self,
        value: &JsonValue,
        exclude: &[JsonPath],
        hasher: &mut FingerprintHasher,
    ) {
        fn walk<'a>(
            json_value: &'a JsonValue,
            exclude: &[JsonPath],
            current: &mut Vec<&'a str>,
            paths: &mut Vec<Vec<&'a str>>,
        ) {
            fn is_excluded(exclude: &[JsonPath], path: &[&str]) -> bool {
                exclude.iter().any(|excluded_path| {
                    excluded_path.len() == path.len()
                        && excluded_path.iter().zip(path.iter()).all(
                            |(excluded_component, component)| {
                                excluded_component.as_str() == *component
                            },
                        )
                })
            }

            match json_value {
                JsonValue::Object(obj) => {
                    for (key, child_value) in obj.iter() {
                        current.push(key.as_str());
                        if !is_excluded(exclude, current) {
                            walk(child_value, exclude, current, paths);
                        }
                        current.pop();
                    }
                }
                _ => paths.push(current.clone()),
            }
        }

        let mut current = Vec::with_capacity(16);
        let mut paths = Vec::with_capacity(32);
        walk(value, exclude, &mut current, &mut paths);
        paths.sort_unstable();

        for path in paths {
            for component in path {
                hasher.write(component.as_bytes());
                hasher.write_u8(PATH_COMPONENT_SEPARATOR);
            }
            hasher.write_u8(PATH_SEPARATOR);
        }
    }

    fn hash_raw_value(
        &self,
        json_value: &JsonValue,
        path: &JsonPath,
        hasher: &mut FingerprintHasher,
    ) {
        fn hash_raw_value_inner(json_value: &JsonValue, hasher: &mut FingerprintHasher) {
            const RAW_NULL: u8 = 0;
            const RAW_BOOL: u8 = 1;
            const RAW_NUMBER: u8 = 2;
            const RAW_STRING: u8 = 3;
            const RAW_ARRAY: u8 = 4;
            const RAW_OBJECT: u8 = 5;

            match json_value {
                JsonValue::Null => hasher.write_u8(RAW_NULL),
                JsonValue::Bool(value) => {
                    hasher.write_u8(RAW_BOOL);
                    hasher.write_u8(*value as u8);
                }
                JsonValue::Number(value) => {
                    hasher.write_u8(RAW_NUMBER);
                    let number = value.to_string();
                    hasher.write(number.as_bytes());
                }
                JsonValue::String(value) => {
                    hasher.write_u8(RAW_STRING);
                    hasher.write_usize(value.len());
                    hasher.write(value.as_bytes());
                }
                JsonValue::Array(values) => {
                    hasher.write_u8(RAW_ARRAY);
                    hasher.write_usize(values.len());
                    for value in values {
                        hash_raw_value_inner(value, hasher);
                    }
                }
                JsonValue::Object(map) => {
                    hasher.write_u8(RAW_OBJECT);
                    hasher.write_usize(map.len());
                    for (key, value) in map.iter() {
                        hasher.write_usize(key.len());
                        hasher.write(key.as_bytes());
                        hash_raw_value_inner(value, hasher);
                    }
                }
            }
        }

        let Some(json_value) = get_leaf_json_value(json_value, path) else {
            hasher.write_u8(FIELD_ABSENT);
            hasher.write_u8(FIELD_BOUNDARY);
            return;
        };
        hasher.write_u8(FIELD_PRESENT);
        hash_raw_value_inner(json_value, hasher);
        hasher.write_u8(FIELD_BOUNDARY);
    }

    fn hash_string_tokenized(
        &self,
        json_value: &JsonValue,
        path: &JsonPath,
        max_tokens: Option<usize>,
        hasher: &mut FingerprintHasher,
    ) {
        let Some(value) = get_leaf_string(json_value, path) else {
            hasher.write_u8(FIELD_ABSENT);
            hasher.write_u8(FIELD_BOUNDARY);
            return;
        };
        hasher.write_u8(FIELD_PRESENT);

        let max_tokens = max_tokens.unwrap_or(DEFAULT_MAX_GROUPING_TOKENS);
        for span in tokenize(value).take(max_tokens) {
            hasher.write_u8(span.token_type as u8);
            hasher.write_u8(TOKENIZED_TOKEN_SEPARATOR);
        }

        hasher.write_u8(FIELD_BOUNDARY);
    }
}

/// Fingerprints of Arrow rows (Parquet bulk load), without building a `serde_json::Value`.
///
/// Produces the same hashes as [`Fingerprinter::fingerprint`] on the row's JSON encoding, as long
/// as no raw or tokenized policy reads a timestamp column (their JSON text is not reproduced;
/// see [`Fingerprinter::value_root_fields`]).
#[cfg(feature = "parquet")]
impl Fingerprinter {
    pub fn fingerprint_row(&self, row: quickwit_doc_mapper::RowValue<'_>) -> Fingerprint {
        let mut fingerprint = SmallVec::new();
        for policy in self.policies.iter() {
            let mut hasher = FingerprintHasher::default();
            for method in policy.fingerprint.iter() {
                match method {
                    ClusteringMethod::Structure { exclude } => {
                        row_hash_structure(row, exclude, &mut hasher);
                    }
                    ClusteringMethod::Raw { path } => {
                        match row_get(row, path) {
                            Some(value) => {
                                hasher.write_u8(FIELD_PRESENT);
                                row_hash_raw(value, &mut hasher);
                            }
                            None => hasher.write_u8(FIELD_ABSENT),
                        }
                        hasher.write_u8(FIELD_BOUNDARY);
                    }
                    ClusteringMethod::Tokenized { path, max_tokens } => {
                        let text_opt = row_get(row, path).and_then(|value| match value.leaf {
                            quickwit_doc_mapper::RowLeaf::Str(text) => Some(text),
                            _ => None,
                        });
                        let Some(text) = text_opt else {
                            hasher.write_u8(FIELD_ABSENT);
                            hasher.write_u8(FIELD_BOUNDARY);
                            continue;
                        };
                        hasher.write_u8(FIELD_PRESENT);
                        let max_tokens = max_tokens.unwrap_or(DEFAULT_MAX_GROUPING_TOKENS);
                        for span in tokenize(text).take(max_tokens) {
                            hasher.write_u8(span.token_type as u8);
                            hasher.write_u8(TOKENIZED_TOKEN_SEPARATOR);
                        }
                        hasher.write_u8(FIELD_BOUNDARY);
                    }
                }
            }
            fingerprint.push(hasher.finish());
        }
        Fingerprint(fingerprint)
    }
}

#[cfg(feature = "parquet")]
fn row_get<'a>(
    mut value: quickwit_doc_mapper::RowValue<'a>,
    path: &[String],
) -> Option<quickwit_doc_mapper::RowValue<'a>> {
    for component in path {
        value = value.get(component)?;
    }
    Some(value)
}

#[cfg(feature = "parquet")]
fn row_hash_structure(
    row: quickwit_doc_mapper::RowValue<'_>,
    exclude: &[JsonPath],
    hasher: &mut FingerprintHasher,
) {
    use quickwit_doc_mapper::{RowLeaf, RowValue};
    // `ArrowDocBuilder::json_row` lists entries sorted by key (root columns and map keys), and a
    // key is either a leaf or an object: a depth-first walk visits the leaf paths in the sorted
    // order `hash_structure` uses, so they can be hashed on the fly, without collecting them.
    fn walk<'a>(
        value: RowValue<'a>,
        exclude: &[JsonPath],
        current: &mut Vec<&'a str>,
        hasher: &mut FingerprintHasher,
    ) {
        if let RowLeaf::Object(..) = value.leaf {
            for (key, child) in value.entries() {
                current.push(key);
                let is_excluded = exclude.iter().any(|excluded_path| {
                    excluded_path.len() == current.len()
                        && excluded_path
                            .iter()
                            .zip(current.iter())
                            .all(|(excluded, component)| excluded.as_str() == *component)
                });
                if !is_excluded {
                    walk(child, exclude, current, hasher);
                }
                current.pop();
            }
        } else {
            for component in current.iter() {
                hasher.write(component.as_bytes());
                hasher.write_u8(PATH_COMPONENT_SEPARATOR);
            }
            hasher.write_u8(PATH_SEPARATOR);
        }
    }
    let mut current = Vec::with_capacity(16);
    walk(row, exclude, &mut current, hasher);
}

#[cfg(feature = "parquet")]
fn row_hash_raw(value: quickwit_doc_mapper::RowValue<'_>, hasher: &mut FingerprintHasher) {
    use quickwit_doc_mapper::RowLeaf;
    // Same encoding as `hash_raw_value_inner`.
    const RAW_BOOL: u8 = 1;
    const RAW_NUMBER: u8 = 2;
    const RAW_STRING: u8 = 3;
    const RAW_OBJECT: u8 = 5;
    let mut write_number = |number: serde_json::Number| {
        hasher.write_u8(RAW_NUMBER);
        hasher.write(number.to_string().as_bytes());
    };
    match value.leaf {
        RowLeaf::Bool(value) => {
            hasher.write_u8(RAW_BOOL);
            hasher.write_u8(value as u8);
        }
        RowLeaf::I64(number) => write_number(number.into()),
        RowLeaf::U64(number) => write_number(number.into()),
        RowLeaf::F64(number) => {
            write_number(serde_json::Number::from_f64(number).expect("finite float"))
        }
        RowLeaf::Str(text) => {
            hasher.write_u8(RAW_STRING);
            hasher.write_usize(text.len());
            hasher.write(text.as_bytes());
        }
        RowLeaf::Object(start, end) => {
            hasher.write_u8(RAW_OBJECT);
            hasher.write_usize((end - start) as usize);
            for (key, child) in value.entries() {
                hasher.write_usize(key.len());
                hasher.write(key.as_bytes());
                row_hash_raw(child, hasher);
            }
        }
        // Excluded by the caller (`value_root_fields`), and never produced for documents.
        RowLeaf::Timestamp(_) => {
            hasher.write_u8(RAW_STRING);
            hasher.write_usize(0);
        }
    }
}

fn get_leaf_json_value<'a>(json_value: &'a JsonValue, path: &[String]) -> Option<&'a JsonValue> {
    if path.is_empty() {
        return Some(json_value);
    }
    let JsonValue::Object(obj) = json_value else {
        return None;
    };
    get_leaf_json_value(obj.get(path.first()?)?, &path[1..])
}

fn get_leaf_string<'a>(json_value: &'a JsonValue, path: &[String]) -> Option<&'a str> {
    get_leaf_json_value(json_value, path)?.as_str()
}

#[cfg(test)]
mod tests {
    use quickwit_config::DocsClusteringConfig;
    use serde_json::Value as JsonValue;

    use super::Fingerprinter;

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

    /// `fingerprint_row` on an Arrow row equals `fingerprint` on its `arrow_json` encoding.
    #[cfg(feature = "parquet")]
    #[test]
    fn arrow_row_fingerprint_matches_json_fingerprint() {
        use std::sync::Arc;

        use arrow_array::builder::{MapBuilder, StringBuilder};
        use arrow_array::{
            ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
            UInt64Array,
        };
        use quickwit_doc_mapper::{ArrowDocBuilder, DocMapper};

        let doc_mapper: DocMapper = serde_json::from_value(serde_json::json!({
            "mode": "dynamic",
            "dynamic_mapping": {"indexed": false, "stored": true},
            "field_mappings": [
                {"name": "Timestamp", "type": "datetime", "fast": true},
                {"name": "Body", "type": "text"},
                {"name": "ServiceName", "type": "text", "tokenizer": "raw"}
            ],
            "timestamp_field": "Timestamp"
        }))
        .unwrap();
        let mut attributes = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        let rows: [&[(&str, Option<&str>)]; 5] = [
            &[("k8s.pod.name", Some("cart-1")), ("app", Some("cart"))],
            &[],
            &[("b", Some("2")), ("a", Some("1")), ("a", Some("3"))],
            &[("x", None), ("y", Some("2026-01-01T00:00:00Z"))],
            // Keys that are prefixes of one another, and a key sorting between column names.
            &[
                ("ab", Some("1")),
                ("a.b", Some("2")),
                ("a", Some("3")),
                ("", Some("4")),
                ("Body", Some("5")),
            ],
        ];
        for (row_idx, row) in rows.iter().enumerate() {
            for (key, value) in row.iter() {
                attributes.keys().append_value(key);
                match value {
                    Some(value) => attributes.values().append_value(value),
                    None => attributes.values().append_null(),
                }
            }
            attributes.append(row_idx != 1).unwrap();
        }
        let batch = RecordBatch::try_from_iter(vec![
            (
                "Timestamp",
                Arc::new(
                    TimestampNanosecondArray::from(vec![1i64, 2, 3, 4, 5]).with_timezone("UTC"),
                ) as ArrayRef,
            ),
            (
                "Body",
                Arc::new(StringArray::from(vec![
                    Some("server started at 8080"),
                    Some("job 123 finished in 42ms"),
                    None,
                    Some("connection from 1.2.3.4"),
                    Some("server started at 9090"),
                ])),
            ),
            (
                "ServiceName",
                Arc::new(StringArray::from(vec![
                    Some("api"),
                    Some("worker"),
                    Some("api"),
                    None,
                    Some("api"),
                ])),
            ),
            (
                "Count",
                Arc::new(Int64Array::from(vec![
                    Some(-1),
                    None,
                    Some(7),
                    Some(0),
                    Some(1),
                ])),
            ),
            (
                "Big",
                Arc::new(UInt64Array::from(vec![
                    Some(u64::MAX),
                    Some(1),
                    None,
                    Some(2),
                    Some(3),
                ])),
            ),
            (
                "Score",
                Arc::new(Float64Array::from(vec![
                    Some(0.1),
                    Some(1e300),
                    Some(-0.0),
                    None,
                    Some(2.5),
                ])),
            ),
            ("ResourceAttributes", Arc::new(attributes.finish())),
        ])
        .unwrap();
        let builder = ArrowDocBuilder::try_new(&doc_mapper, &batch.schema()).unwrap();

        // The searchbench policy, plus raw values on a map, an integer and a float column.
        let fingerprinter = Fingerprinter::new(&docs_clustering_config(serde_json::json!([
            {"fingerprint": [{"kind": "structure"}]},
            {"fingerprint": [{"kind": "raw", "path": "ServiceName"}]},
            {"fingerprint": [{"kind": "tokenized", "path": "Body"}]},
            {"fingerprint": [{"kind": "raw", "path": "ResourceAttributes"}]},
            {"fingerprint": [{"kind": "raw", "path": "ResourceAttributes.app"}]},
            {"fingerprint": [{"kind": "raw", "path": "Count"}, {"kind": "raw", "path": "Big"}]},
            {"fingerprint": [{"kind": "raw", "path": "Score"}]},
            {"fingerprint": [{"kind": "structure", "exclude": ["ResourceAttributes"]}]},
            {"fingerprint": [{"kind": "raw", "path": "Missing"}, {"kind": "tokenized", "path": "Count"}]}
        ])));
        assert!(
            fingerprinter
                .value_root_fields()
                .all(|field| field != "Timestamp")
        );

        let mut writer = arrow_json::WriterBuilder::new()
            .with_explicit_nulls(false)
            .build::<_, arrow_json::writer::LineDelimited>(Vec::new());
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        let ndjson = writer.into_inner();
        let mut num_checked = 0;
        for (row, line) in ndjson
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .enumerate()
        {
            let json_doc: JsonValue = serde_json::from_slice(line).unwrap();
            let Some(json_row) = builder.json_row(&batch, row) else {
                continue;
            };
            assert_eq!(
                fingerprinter.fingerprint_row(json_row.root()),
                fingerprinter.fingerprint(&json_doc),
                "row {row}: {json_doc}"
            );
            num_checked += 1;
        }
        assert_eq!(num_checked, 5);
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
        let doc1 =
            parse(r#"{"message":"server started at 8080","service":"api","custom":{"a":1}}"#);
        let doc2 =
            parse(r#"{"message":"server started at 8080","service":"api","custom":{"b":2}}"#);
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
}
