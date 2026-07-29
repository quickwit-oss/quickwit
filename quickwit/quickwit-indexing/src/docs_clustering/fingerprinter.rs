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
//! A fingerprint contains two independent components:
//! 1. the schema hash, represented by the sorted set of leaf JSON paths;
//! 2. the grouping hash, represented by raw string values or token types from the configured string
//!    fields.
//!
//! ```text
//! JSON document
//!     |
//!     +--> schema paths, excluding paths configured by `structure.exclude`
//!     |
//!     +--> configured grouping fields
//!              |
//!              +--> Tokenized: hash token types, not literal values
//!              +--> Raw:       hash the exact string value
//!     |
//!     v
//! Fingerprint
//! ```
//!
//! Tokenized fields use the sequence of token types as a lightweight message template. This keeps
//! the stable structure of a value while ignoring volatile literals such as IDs, ports, UUIDs, or
//! IP addresses. Only the first 50 tokens contribute to the fingerprint.
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
//! These two values produce the same tokenized signature and can be ordered together. A different
//! shape produces a different signature:
//!
//! ```text
//! "connection from 1.2.3.4"
//!     -> Word Gap Word Gap IPv4
//! ```
//!
//! Raw string fields keep exact-value differences, which is useful for dimensions such as
//! `service`. Missing fields and non-string values are encoded as absent so configured field
//! positions remain distinct.
use std::hash::Hasher;
use std::sync::Arc;

use fnv::FnvHasher;
use quickwit_config::{GroupingConfig, GroupingField};
use serde_json::Value as JsonValue;

use super::tokenize;

const PATH_COMPONENT_SEPARATOR: u8 = 0xF9;
const FIELD_ABSENT: u8 = 0xFA;
const FIELD_PRESENT: u8 = 0xFB;
const PATH_SEPARATOR: u8 = 0xFC;
const FIELD_BOUNDARY: u8 = 0xFD;
const TOKENIZED_TOKEN_SEPARATOR: u8 = 0xFE;
const MAX_GROUPING_TOKENS: usize = 50;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Fingerprint {
    pub(super) schema: u64,
    pub(super) grouping: u64,
}

#[cfg(test)]
impl Fingerprint {
    pub(crate) fn for_test(schema: u64, grouping: u64) -> Self {
        Self { schema, grouping }
    }
}

type JsonPath = Box<[String]>;

#[derive(Clone, Copy, Debug, PartialEq)]
enum FingerprintFieldKind {
    Tokenized,
    Raw,
}

#[derive(Clone)]
pub struct Fingerprinter {
    config: Arc<GroupingConfig>,
    grouping_fields: Arc<[(JsonPath, FingerprintFieldKind)]>,
    ignored_paths: Arc<[JsonPath]>,
}

impl Fingerprinter {
    pub fn new(config: &GroupingConfig) -> Self {
        let mut grouping_fields = Vec::new();
        let mut ignored_paths = Vec::new();

        // Collect the paths from the nested grouping config.
        fn collect_paths(
            config: &GroupingConfig,
            grouping_fields: &mut Vec<(JsonPath, FingerprintFieldKind)>,
            ignored_paths: &mut Vec<JsonPath>,
        ) {
            fn parse_path(path: &str) -> JsonPath {
                path.split('.').map(ToString::to_string).collect()
            }

            for field in &config.fingerprint {
                match field {
                    GroupingField::Structure { exclude, .. } => {
                        ignored_paths.extend(exclude.iter().map(|path| parse_path(path)));
                    }
                    GroupingField::Raw { path } => {
                        grouping_fields.push((parse_path(path), FingerprintFieldKind::Raw));
                    }
                    GroupingField::Tokenized { path } => {
                        grouping_fields.push((parse_path(path), FingerprintFieldKind::Tokenized));
                    }
                }
            }
            if let Some(child_grouping) = config.grouping.as_deref() {
                collect_paths(child_grouping, grouping_fields, ignored_paths);
            }
        }

        collect_paths(config, &mut grouping_fields, &mut ignored_paths);
        // Sort the paths to ensure a stable ordering of the fingerprint.
        grouping_fields.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));

        Self {
            config: Arc::new(config.clone()),
            grouping_fields: Arc::from(grouping_fields.into_boxed_slice()),
            ignored_paths: Arc::from(ignored_paths.into_boxed_slice()),
        }
    }

    pub fn config(&self) -> &GroupingConfig {
        &self.config
    }

    pub fn fingerprint(&self, json_value: &JsonValue) -> Fingerprint {
        let mut schema_hasher = FnvHasher::default();
        self.hash_schema(json_value, &mut schema_hasher);
        let mut grouping_hasher = FnvHasher::default();
        self.hash_grouping_fields(json_value, &mut grouping_hasher);
        Fingerprint {
            schema: schema_hasher.finish(),
            grouping: grouping_hasher.finish(),
        }
    }

    fn hash_schema(&self, value: &JsonValue, hasher: &mut FnvHasher) {
        fn walk<'a>(
            fingerprinter: &Fingerprinter,
            json_value: &'a JsonValue,
            current: &mut Vec<&'a str>,
            paths: &mut Vec<Vec<&'a str>>,
        ) {
            match json_value {
                JsonValue::Object(obj) => {
                    for (key, child_value) in obj.iter() {
                        current.push(key.as_str());
                        if !fingerprinter.is_ignored(current) {
                            walk(fingerprinter, child_value, current, paths);
                        }
                        current.pop();
                    }
                }
                _ => paths.push(current.clone()),
            }
        }

        let mut current = Vec::with_capacity(16);
        let mut paths = Vec::with_capacity(32);
        walk(self, value, &mut current, &mut paths);
        paths.sort_unstable();

        for path in paths {
            for component in path {
                hasher.write(component.as_bytes());
                hasher.write_u8(PATH_COMPONENT_SEPARATOR);
            }
            hasher.write_u8(PATH_SEPARATOR);
        }
    }

    fn hash_grouping_fields(&self, json_value: &JsonValue, hasher: &mut FnvHasher) {
        for (path, kind) in self.grouping_fields.iter() {
            let Some(value) = get_leaf_string(json_value, path) else {
                hasher.write_u8(FIELD_ABSENT);
                hasher.write_u8(FIELD_BOUNDARY);
                continue;
            };
            hasher.write_u8(FIELD_PRESENT);
            match kind {
                FingerprintFieldKind::Tokenized => {
                    for span in tokenize(value).take(MAX_GROUPING_TOKENS) {
                        hasher.write_u8(span.token_type as u8);
                        hasher.write_u8(TOKENIZED_TOKEN_SEPARATOR);
                    }
                }
                FingerprintFieldKind::Raw => {
                    hasher.write(value.as_bytes());
                }
            }
            hasher.write_u8(FIELD_BOUNDARY);
        }
    }

    fn is_ignored(&self, path: &[&str]) -> bool {
        self.ignored_paths.iter().any(|ignored_path| {
            ignored_path.len() == path.len()
                && ignored_path
                    .iter()
                    .zip(path.iter())
                    .all(|(ignored_component, component)| ignored_component == component)
        })
    }
}

fn get_leaf_string<'a>(json_value: &'a JsonValue, path: &[String]) -> Option<&'a str> {
    if path.is_empty() {
        return json_value.as_str();
    }
    let JsonValue::Object(obj) = json_value else {
        return None;
    };
    get_leaf_string(obj.get(path.first()?)?, &path[1..])
}

#[cfg(test)]
mod tests {
    use quickwit_config::{DocsClusteringConfig, GroupingConfig};
    use serde_json::Value as JsonValue;

    use super::Fingerprinter;

    fn parse(s: &str) -> JsonValue {
        serde_json::from_str(s).unwrap()
    }

    fn test_docs_clustering_config() -> DocsClusteringConfig {
        docs_clustering_config(serde_json::json!({
            "fingerprint": [{
                "path": "$",
                "kind": "structure",
                "exclude": ["tag", "custom"]
            }],
            "grouping": {
                "fingerprint": [
                    {
                        "path": "message",
                        "kind": "tokenized"
                    },
                    {
                        "path": "service",
                        "kind": "raw"
                    }
                ]
            }
        }))
    }

    fn test_fingerprinter() -> Fingerprinter {
        let docs_clustering_config = test_docs_clustering_config();
        Fingerprinter::new(&docs_clustering_config.grouping)
    }

    fn docs_clustering_config(json_value: JsonValue) -> DocsClusteringConfig {
        DocsClusteringConfig {
            grouping: serde_json::from_value::<GroupingConfig>(json_value).unwrap(),
        }
    }

    #[test]
    fn configured_fingerprinter_returns_config() {
        let docs_clustering_config = test_docs_clustering_config();
        let fingerprinter = test_fingerprinter();
        assert_eq!(fingerprinter.config(), &docs_clustering_config.grouping);
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

        assert_ne!(
            dotted_key_fingerprint.schema,
            nested_path_fingerprint.schema
        );
        assert_eq!(
            dotted_key_fingerprint.grouping,
            nested_path_fingerprint.grouping
        );
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
        assert_eq!(doc1_fingerprint.schema, doc2_fingerprint.schema);
        assert_ne!(doc1_fingerprint.grouping, doc2_fingerprint.grouping);
    }

    #[test]
    fn tokenized_field_respects_hardcoded_grouping_token_limit() {
        let docs_clustering_config = docs_clustering_config(serde_json::json!({
            "fingerprint": [{
                "path": "$",
                "kind": "structure"
            }],
            "grouping": {
                "fingerprint": [{
                    "path": "message",
                    "kind": "tokenized"
                }]
            }
        }));
        let fingerprinter = Fingerprinter::new(&docs_clustering_config.grouping);
        let prefix = "alpha ".repeat(25);
        let doc1 = parse(&format!(r#"{{"message":"{prefix}123"}}"#));
        let doc2 = parse(&format!(r#"{{"message":"{prefix}beta"}}"#));
        assert_eq!(
            fingerprinter.fingerprint(&doc1).grouping,
            fingerprinter.fingerprint(&doc2).grouping
        );
    }

    #[test]
    fn different_service_changes_grouping_fingerprint_only() {
        let fingerprinter = test_fingerprinter();
        let doc1 = parse(r#"{"message":"server started at 8080","service":"api"}"#);
        let doc2 = parse(r#"{"message":"server started at 8080","service":"worker"}"#);
        let doc1_fingerprint = fingerprinter.fingerprint(&doc1);
        let doc2_fingerprint = fingerprinter.fingerprint(&doc2);
        assert_eq!(doc1_fingerprint.schema, doc2_fingerprint.schema);
        assert_ne!(doc1_fingerprint.grouping, doc2_fingerprint.grouping);
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
        assert_ne!(doc1_fingerprint.schema, doc2_fingerprint.schema);
        assert_eq!(doc1_fingerprint.grouping, doc2_fingerprint.grouping);
    }

    #[test]
    fn configured_raw_field_changes_grouping_fingerprint_only() {
        let docs_clustering_config = docs_clustering_config(serde_json::json!({
            "fingerprint": [{
                "path": "$",
                "kind": "structure"
            }],
            "grouping": {
                "fingerprint": [{
                    "path": "host",
                    "kind": "raw"
                }]
            }
        }));
        let fingerprinter = Fingerprinter::new(&docs_clustering_config.grouping);
        let doc1 = parse(r#"{"message":"same","host":"web-1"}"#);
        let doc2 = parse(r#"{"message":"same","host":"web-2"}"#);
        let doc1_fingerprint = fingerprinter.fingerprint(&doc1);
        let doc2_fingerprint = fingerprinter.fingerprint(&doc2);
        assert_eq!(doc1_fingerprint.schema, doc2_fingerprint.schema);
        assert_ne!(doc1_fingerprint.grouping, doc2_fingerprint.grouping);
    }

    #[test]
    fn non_string_grouping_values_are_ignored() {
        let docs_clustering_config = docs_clustering_config(serde_json::json!({
            "fingerprint": [{
                "path": "$",
                "kind": "structure"
            }],
            "grouping": {
                "fingerprint": [{
                    "path": "status",
                    "kind": "raw"
                }]
            }
        }));
        let fingerprinter = Fingerprinter::new(&docs_clustering_config.grouping);
        let doc1 = parse(r#"{"status":200}"#);
        let doc2 = parse(r#"{"status":500}"#);
        assert_eq!(
            fingerprinter.fingerprint(&doc1),
            fingerprinter.fingerprint(&doc2)
        );
    }

    #[test]
    fn absent_grouping_values_preserve_field_position() {
        let docs_clustering_config = docs_clustering_config(serde_json::json!({
            "fingerprint": [{
                "path": "$",
                "kind": "structure"
            }],
            "grouping": {
                "fingerprint": [
                    {
                        "path": "a",
                        "kind": "raw"
                    },
                    {
                        "path": "b",
                        "kind": "raw"
                    }
                ]
            }
        }));
        let fingerprinter = Fingerprinter::new(&docs_clustering_config.grouping);
        let doc1 = parse(r#"{"a":"x","b":null}"#);
        let doc2 = parse(r#"{"a":null,"b":"x"}"#);
        let doc1_fingerprint = fingerprinter.fingerprint(&doc1);
        let doc2_fingerprint = fingerprinter.fingerprint(&doc2);

        assert_eq!(doc1_fingerprint.schema, doc2_fingerprint.schema);
        assert_ne!(doc1_fingerprint.grouping, doc2_fingerprint.grouping);
    }
}
