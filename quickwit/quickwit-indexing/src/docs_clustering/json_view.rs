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

//! Read-only view over a JSON tree, so the [`Fingerprinter`](super::Fingerprinter) can hash both
//! owned `serde_json` values and the borrowed tree built by [`BorrowedJsonDoc`].
//!
//! Both implementations must expose the same object entries in the same order for the same JSON
//! input: fingerprints are only comparable if they are computed identically. `BorrowedJsonDoc`
//! guarantees this by mirroring the key ordering and duplicate key resolution of
//! `serde_json::Map`.

use quickwit_doc_mapper::{BorrowedJsonDoc, BorrowedObject, BorrowedValue};
use serde_json::{Number, Value as JsonValue};

/// Kind of a JSON value, with access to its scalar content.
pub(crate) enum JsonViewKind<'a> {
    Null,
    Bool(bool),
    Number(&'a Number),
    Str(&'a str),
    Array { len: usize },
    Object { len: usize },
}

/// Read-only access to a JSON value. Iteration uses callbacks to avoid allocations.
pub(crate) trait JsonView<'a>: Copy {
    fn kind(self) -> JsonViewKind<'a>;

    /// Calls `callback` on each element if `self` is an array.
    fn for_each_element(self, callback: impl FnMut(Self));

    /// Calls `callback` on each entry, in the iteration order of `serde_json::Map`, if `self` is
    /// an object.
    fn for_each_entry(self, callback: impl FnMut(&'a str, Self));

    /// Returns the value associated with `key` if `self` is an object.
    fn get(self, key: &str) -> Option<Self>;
}

impl<'a> JsonView<'a> for &'a JsonValue {
    fn kind(self) -> JsonViewKind<'a> {
        match self {
            JsonValue::Null => JsonViewKind::Null,
            JsonValue::Bool(value) => JsonViewKind::Bool(*value),
            JsonValue::Number(number) => JsonViewKind::Number(number),
            JsonValue::String(value) => JsonViewKind::Str(value),
            JsonValue::Array(values) => JsonViewKind::Array { len: values.len() },
            JsonValue::Object(map) => JsonViewKind::Object { len: map.len() },
        }
    }

    fn for_each_element(self, callback: impl FnMut(Self)) {
        if let JsonValue::Array(values) = self {
            values.iter().for_each(callback);
        }
    }

    fn for_each_entry(self, mut callback: impl FnMut(&'a str, Self)) {
        if let JsonValue::Object(map) = self {
            for (key, value) in map {
                callback(key, value);
            }
        }
    }

    fn get(self, key: &str) -> Option<Self> {
        self.as_object()?.get(key)
    }
}

/// A node of a [`BorrowedJsonDoc`]: the root object is not stored as a [`BorrowedValue`].
#[derive(Clone, Copy)]
pub(crate) enum BorrowedJsonNode<'b, 'a> {
    Root(&'b BorrowedJsonDoc<'a>),
    Value(&'b BorrowedValue<'a>),
}

impl<'b, 'a> BorrowedJsonNode<'b, 'a> {
    fn object_entries(self) -> Option<&'b BorrowedObject<'a>> {
        match self {
            BorrowedJsonNode::Root(json_doc) => Some(json_doc.root()),
            BorrowedJsonNode::Value(BorrowedValue::Object(entries)) => Some(entries),
            BorrowedJsonNode::Value(_) => None,
        }
    }
}

impl<'b, 'a: 'b> JsonView<'b> for BorrowedJsonNode<'b, 'a> {
    fn kind(self) -> JsonViewKind<'b> {
        if let Some(entries) = self.object_entries() {
            return JsonViewKind::Object { len: entries.len() };
        }
        let BorrowedJsonNode::Value(json_value) = self else {
            unreachable!("the root is an object")
        };
        match json_value {
            BorrowedValue::Null => JsonViewKind::Null,
            BorrowedValue::Bool(value) => JsonViewKind::Bool(*value),
            BorrowedValue::Number(number) => JsonViewKind::Number(number),
            BorrowedValue::Str(value) => JsonViewKind::Str(value),
            BorrowedValue::Array(values) => JsonViewKind::Array { len: values.len() },
            BorrowedValue::Object(_) => unreachable!("objects are handled above"),
        }
    }

    fn for_each_element(self, callback: impl FnMut(Self)) {
        if let BorrowedJsonNode::Value(BorrowedValue::Array(values)) = self {
            values
                .iter()
                .map(BorrowedJsonNode::Value)
                .for_each(callback);
        }
    }

    fn for_each_entry(self, mut callback: impl FnMut(&'b str, Self)) {
        let Some(entries) = self.object_entries() else {
            return;
        };
        for (key, value) in entries {
            callback(key, BorrowedJsonNode::Value(value));
        }
    }

    fn get(self, key: &str) -> Option<Self> {
        let child_value = match self {
            BorrowedJsonNode::Root(json_doc) => json_doc.get(key)?,
            BorrowedJsonNode::Value(json_value) => json_value.get(key)?,
        };
        Some(BorrowedJsonNode::Value(child_value))
    }
}
