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
use quickwit_doc_mapper::BorrowedJsonDoc;
use serde_json::Value as JsonValue;
use smallvec::SmallVec;

use super::json_view::{BorrowedJsonNode, JsonView, JsonViewKind};
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

    /// Computes the fingerprint of a document parsed as an owned JSON value.
    pub fn fingerprint(&self, json_value: &JsonValue) -> Fingerprint {
        self.fingerprint_view(json_value)
    }

    /// Computes the fingerprint of a document parsed as a [`BorrowedJsonDoc`]. Returns the same
    /// fingerprint as [`Self::fingerprint`] for the same JSON input.
    pub fn fingerprint_borrowed(&self, json_doc: &BorrowedJsonDoc) -> Fingerprint {
        self.fingerprint_view(BorrowedJsonNode::Root(json_doc))
    }

    fn fingerprint_view<'a>(&self, json_view: impl JsonView<'a>) -> Fingerprint {
        let mut fingerprint = SmallVec::new();
        for policy in self.policies.iter() {
            let mut hasher = FingerprintHasher::default();

            for method in policy.fingerprint.iter() {
                match method {
                    ClusteringMethod::Structure { exclude } => {
                        hash_structure(json_view, exclude, &mut hasher);
                    }
                    ClusteringMethod::Raw { path } => {
                        hash_raw_value(json_view, path, &mut hasher);
                    }
                    ClusteringMethod::Tokenized { path, max_tokens } => {
                        hash_string_tokenized(json_view, path, *max_tokens, &mut hasher);
                    }
                }
            }

            fingerprint.push(hasher.finish());
        }

        Fingerprint(fingerprint)
    }
}

fn is_excluded(exclude: &[JsonPath], path: &[&str]) -> bool {
    exclude.iter().any(|excluded_path| {
        excluded_path.len() == path.len()
            && excluded_path
                .iter()
                .zip(path.iter())
                .all(|(excluded_component, component)| excluded_component.as_str() == *component)
    })
}

fn collect_leaf_paths<'a, V: JsonView<'a>>(
    json_view: V,
    exclude: &[JsonPath],
    current: &mut Vec<&'a str>,
    paths: &mut Vec<Vec<&'a str>>,
) {
    if !matches!(json_view.kind(), JsonViewKind::Object { .. }) {
        paths.push(current.clone());
        return;
    }
    json_view.for_each_entry(|key, child_view| {
        current.push(key);
        if !is_excluded(exclude, current) {
            collect_leaf_paths(child_view, exclude, current, paths);
        }
        current.pop();
    });
}

fn hash_structure<'a>(
    json_view: impl JsonView<'a>,
    exclude: &[JsonPath],
    hasher: &mut FingerprintHasher,
) {
    let mut current = Vec::with_capacity(16);
    let mut paths = Vec::with_capacity(32);
    collect_leaf_paths(json_view, exclude, &mut current, &mut paths);
    paths.sort_unstable();

    for path in paths {
        for component in path {
            hasher.write(component.as_bytes());
            hasher.write_u8(PATH_COMPONENT_SEPARATOR);
        }
        hasher.write_u8(PATH_SEPARATOR);
    }
}

fn hash_raw_value_inner<'a, V: JsonView<'a>>(json_view: V, hasher: &mut FingerprintHasher) {
    const RAW_NULL: u8 = 0;
    const RAW_BOOL: u8 = 1;
    const RAW_NUMBER: u8 = 2;
    const RAW_STRING: u8 = 3;
    const RAW_ARRAY: u8 = 4;
    const RAW_OBJECT: u8 = 5;

    match json_view.kind() {
        JsonViewKind::Null => hasher.write_u8(RAW_NULL),
        JsonViewKind::Bool(value) => {
            hasher.write_u8(RAW_BOOL);
            hasher.write_u8(value as u8);
        }
        JsonViewKind::Number(value) => {
            hasher.write_u8(RAW_NUMBER);
            let number = value.to_string();
            hasher.write(number.as_bytes());
        }
        JsonViewKind::Str(value) => {
            hasher.write_u8(RAW_STRING);
            hasher.write_usize(value.len());
            hasher.write(value.as_bytes());
        }
        JsonViewKind::Array { len } => {
            hasher.write_u8(RAW_ARRAY);
            hasher.write_usize(len);
            json_view.for_each_element(|element_view| hash_raw_value_inner(element_view, hasher));
        }
        JsonViewKind::Object { len } => {
            hasher.write_u8(RAW_OBJECT);
            hasher.write_usize(len);
            json_view.for_each_entry(|key, child_view| {
                hasher.write_usize(key.len());
                hasher.write(key.as_bytes());
                hash_raw_value_inner(child_view, hasher);
            });
        }
    }
}

fn hash_raw_value<'a>(
    json_view: impl JsonView<'a>,
    path: &JsonPath,
    hasher: &mut FingerprintHasher,
) {
    let Some(leaf_view) = get_leaf_json_view(json_view, path) else {
        hasher.write_u8(FIELD_ABSENT);
        hasher.write_u8(FIELD_BOUNDARY);
        return;
    };
    hasher.write_u8(FIELD_PRESENT);
    hash_raw_value_inner(leaf_view, hasher);
    hasher.write_u8(FIELD_BOUNDARY);
}

fn hash_string_tokenized<'a>(
    json_view: impl JsonView<'a>,
    path: &JsonPath,
    max_tokens: Option<usize>,
    hasher: &mut FingerprintHasher,
) {
    let Some(value) = get_leaf_string(json_view, path) else {
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

fn get_leaf_json_view<'a, V: JsonView<'a>>(json_view: V, path: &[String]) -> Option<V> {
    let Some((first_component, remaining_components)) = path.split_first() else {
        return Some(json_view);
    };
    get_leaf_json_view(json_view.get(first_component)?, remaining_components)
}

fn get_leaf_string<'a>(json_view: impl JsonView<'a>, path: &[String]) -> Option<&'a str> {
    let JsonViewKind::Str(value) = get_leaf_json_view(json_view, path)?.kind() else {
        return None;
    };
    Some(value)
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
