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

//! Exposes borrowed JSON values to tantivy through its [`Value`] trait, so that objects are
//! written into the tantivy document without building intermediate `OwnedValue`s.

use std::borrow::Cow;
use std::slice;

use serde_json::Number;
use tantivy::schema::document::{ReferenceValue, ReferenceValueLeaf};
use tantivy::schema::{Field, Value};
use tantivy::time::format_description::well_known::Rfc3339;
use tantivy::time::{OffsetDateTime, UtcOffset};
use tantivy::{DateTime, TantivyDocument as Document};

use super::borrowed_json::{BorrowedObject, BorrowedValue, serde_json_preserves_order};
use super::mapping_tree::NumVal;

/// Unmapped fields collected while walking the mapping tree in dynamic mode, mirroring the
/// `dynamic_json_obj` map of the owned path. Entries are pushed while iterating over sorted
/// objects, so they are sorted by key too.
pub(crate) type DynamicObject<'b, 'a> = Vec<(&'b str, DynamicEntry<'b, 'a>)>;

#[derive(Debug)]
pub(crate) enum DynamicEntry<'b, 'a> {
    /// An unmapped field, with its whole JSON value.
    Value(&'b BorrowedValue<'a>),
    /// The unmapped fields of an object mapped with an `object` field mapping. Never empty.
    Object(DynamicObject<'b, 'a>),
}

/// Adds a JSON object to a document, like `document.add_object(field, object.into())` does in
/// the owned path (string values that look like RFC 3339 dates become dates, and the top-level
/// keys are sorted because `add_object` takes a `BTreeMap`).
pub(crate) fn add_borrowed_object(
    document: &mut Document,
    field: Field,
    json_obj: &BorrowedObject,
) {
    let value_view = TantivyValueView::JsonObject {
        entries: json_obj,
        sort_keys: true,
    };
    document.add_field_value(field, value_view);
}

/// Adds a JSON value to a document. See [`add_borrowed_object`].
pub(crate) fn add_borrowed_value(document: &mut Document, field: Field, json_val: &BorrowedValue) {
    document.add_field_value(field, TantivyValueView::Json(json_val));
}

/// Adds the unmapped fields to the dynamic field. See [`add_borrowed_object`].
pub(crate) fn add_dynamic_object(
    document: &mut Document,
    field: Field,
    dynamic_obj: &DynamicObject,
) {
    let value_view = TantivyValueView::DynamicObject {
        entries: dynamic_obj,
        sort_keys: true,
    };
    document.add_field_value(field, value_view);
}

/// Adds every primitive value of the unmapped fields to the concatenate fields, mirroring
/// `JsonValueIterator` followed by `map_primitive_json_to_concatenate_value` in the owned path.
pub(crate) fn add_dynamic_concatenate_values(
    document: &mut Document,
    concatenate_fields: &[Field],
    dynamic_obj: &DynamicObject,
) {
    for (_key, dynamic_entry) in dynamic_obj {
        match dynamic_entry {
            DynamicEntry::Value(json_val) => {
                add_concatenate_leaves(document, concatenate_fields, json_val);
            }
            DynamicEntry::Object(child_dynamic_obj) => {
                add_dynamic_concatenate_values(document, concatenate_fields, child_dynamic_obj);
            }
        }
    }
}

/// Adds every primitive value of `json_val`, in depth-first order, to the concatenate fields.
/// Nulls are skipped and strings are never interpreted as dates.
pub(crate) fn add_concatenate_leaves(
    document: &mut Document,
    concatenate_fields: &[Field],
    json_val: &BorrowedValue,
) {
    let leaf: ReferenceValueLeaf = match json_val {
        BorrowedValue::Null => return,
        BorrowedValue::Array(elements) => {
            for element in elements {
                add_concatenate_leaves(document, concatenate_fields, element);
            }
            return;
        }
        BorrowedValue::Object(entries) => {
            for (_key, child_json_val) in entries {
                add_concatenate_leaves(document, concatenate_fields, child_json_val);
            }
            return;
        }
        BorrowedValue::Str(text) => ReferenceValueLeaf::Str(text),
        BorrowedValue::Bool(bool_val) => (*bool_val).into(),
        BorrowedValue::Number(number) => {
            if let Some(i64_val) = i64::from_json_number(number) {
                i64_val.into()
            } else if let Some(u64_val) = u64::from_json_number(number) {
                u64_val.into()
            } else if let Some(f64_val) = f64::from_json_number(number) {
                f64_val.into()
            } else {
                return;
            }
        }
    };
    for field in concatenate_fields {
        document.add_leaf_field_value(*field, leaf.clone());
    }
}

/// Mirrors `From<serde_json::Value> for OwnedValue` for numbers.
fn number_to_leaf(number: &Number) -> ReferenceValueLeaf<'static> {
    if let Some(i64_val) = number.as_i64() {
        ReferenceValueLeaf::I64(i64_val)
    } else if let Some(u64_val) = number.as_u64() {
        ReferenceValueLeaf::U64(u64_val)
    } else if let Some(f64_val) = number.as_f64() {
        ReferenceValueLeaf::F64(f64_val)
    } else {
        // Without the `arbitrary_precision` feature, a number is always an i64, a u64 or a f64.
        panic!("unsupported serde_json number `{number}`")
    }
}

/// Mirrors `From<serde_json::Value> for OwnedValue` for strings: strings that start with a digit
/// and parse as RFC 3339 are converted into dates.
fn str_to_leaf(text: &str) -> ReferenceValueLeaf<'_> {
    // Same pre-check as tantivy's `can_be_rfc3339_date_time`.
    let Some(first_byte) = text.as_bytes().first() else {
        return ReferenceValueLeaf::Str(text);
    };
    if !first_byte.is_ascii_digit() {
        return ReferenceValueLeaf::Str(text);
    }
    match OffsetDateTime::parse(text, &Rfc3339) {
        Ok(date_time) => {
            ReferenceValueLeaf::Date(DateTime::from_utc(date_time.to_offset(UtcOffset::UTC)))
        }
        Err(_) => ReferenceValueLeaf::Str(text),
    }
}

#[derive(Debug, Clone, Copy)]
enum TantivyValueView<'b, 'a> {
    Json(&'b BorrowedValue<'a>),
    /// `sort_keys` mirrors the `BTreeMap` used by `add_object` for top-level objects. Objects are
    /// already sorted unless the `preserve_order` feature of `serde_json` is enabled.
    JsonObject {
        entries: &'b BorrowedObject<'a>,
        sort_keys: bool,
    },
    DynamicObject {
        entries: &'b DynamicObject<'b, 'a>,
        sort_keys: bool,
    },
}

impl<'b, 'a: 'b> TantivyValueView<'b, 'a> {
    fn object_iter(&self) -> Option<ObjectIter<'b, 'a>> {
        let (object_iter, sort_keys) = match *self {
            TantivyValueView::Json(BorrowedValue::Object(entries)) => {
                (ObjectIter::Json(entries.iter()), false)
            }
            TantivyValueView::Json(_) => return None,
            TantivyValueView::JsonObject { entries, sort_keys } => {
                (ObjectIter::Json(entries.iter()), sort_keys)
            }
            TantivyValueView::DynamicObject { entries, sort_keys } => {
                (ObjectIter::Dynamic(entries.iter()), sort_keys)
            }
        };
        if !sort_keys || !serde_json_preserves_order() {
            return Some(object_iter);
        }
        let mut sorted_entries: Vec<(&'b str, TantivyValueView<'b, 'a>)> = object_iter.collect();
        sorted_entries.sort_by(|left, right| left.0.cmp(right.0));
        Some(ObjectIter::Sorted(sorted_entries.into_iter()))
    }
}

impl<'b, 'a: 'b> Value<'b> for TantivyValueView<'b, 'a> {
    type ArrayIter = ArrayIter<'b, 'a>;
    type ObjectIter = ObjectIter<'b, 'a>;

    fn as_value(&self) -> ReferenceValue<'b, Self> {
        if let Some(object_iter) = self.object_iter() {
            return ReferenceValue::Object(object_iter);
        }
        let TantivyValueView::Json(json_val) = *self else {
            unreachable!("objects are handled above")
        };
        match json_val {
            BorrowedValue::Null => ReferenceValue::Leaf(ReferenceValueLeaf::Null),
            BorrowedValue::Bool(bool_val) => ReferenceValue::Leaf((*bool_val).into()),
            BorrowedValue::Number(number) => ReferenceValue::Leaf(number_to_leaf(number)),
            BorrowedValue::Str(text) => ReferenceValue::Leaf(str_to_leaf(text)),
            BorrowedValue::Array(elements) => ReferenceValue::Array(ArrayIter(elements.iter())),
            BorrowedValue::Object(_) => unreachable!("objects are handled above"),
        }
    }
}

struct ArrayIter<'b, 'a>(slice::Iter<'b, BorrowedValue<'a>>);

impl<'b, 'a: 'b> Iterator for ArrayIter<'b, 'a> {
    type Item = TantivyValueView<'b, 'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(TantivyValueView::Json)
    }
}

enum ObjectIter<'b, 'a> {
    Json(slice::Iter<'b, (Cow<'a, str>, BorrowedValue<'a>)>),
    Dynamic(slice::Iter<'b, (&'b str, DynamicEntry<'b, 'a>)>),
    Sorted(std::vec::IntoIter<(&'b str, TantivyValueView<'b, 'a>)>),
}

impl<'b, 'a: 'b> Iterator for ObjectIter<'b, 'a> {
    type Item = (&'b str, TantivyValueView<'b, 'a>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ObjectIter::Json(entries) => {
                let (key, json_val) = entries.next()?;
                Some((key.as_ref(), TantivyValueView::Json(json_val)))
            }
            ObjectIter::Dynamic(entries) => {
                let (key, dynamic_entry) = entries.next()?;
                let value_view = match dynamic_entry {
                    DynamicEntry::Value(json_val) => TantivyValueView::Json(json_val),
                    DynamicEntry::Object(child_obj) => TantivyValueView::DynamicObject {
                        entries: child_obj,
                        sort_keys: false,
                    },
                };
                Some((key, value_view))
            }
            ObjectIter::Sorted(entries) => entries.next(),
        }
    }
}
