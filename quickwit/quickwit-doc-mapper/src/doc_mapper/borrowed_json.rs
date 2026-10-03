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

//! A JSON tree borrowing its strings from the input buffer.
//!
//! It is the input of the allocation-light document conversion path
//! ([`crate::DocMapper::doc_from_borrowed_json`]). That path must produce exactly the same tantivy
//! documents, partitions and errors as [`crate::DocMapper::doc_from_json_obj`] applied to a
//! [`serde_json::Map`]. For this reason the tree reproduces the semantics of `serde_json::Value`
//! as built by `serde_json` (without the `arbitrary_precision` feature):
//! - object entries are ordered like in `serde_json::Map`: sorted by key (byte-wise, like
//!   `BTreeMap<String, _>`) by default, or in insertion order when the `preserve_order` feature of
//!   `serde_json` is enabled (it is enabled by some test dependencies);
//! - duplicate keys are resolved by keeping the last value (at the position of the first occurrence
//!   with `preserve_order`);
//! - numbers are stored as `serde_json::Number`, built from the same deserializer events.

use std::borrow::Cow;
use std::fmt;
use std::sync::LazyLock;

use serde::de::{self, Deserialize, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Number, Value as JsonValue};

/// Key `serde_json` reserves to deserialize `RawValue`s when its `raw_value` feature is enabled.
/// See `BorrowedValue` deserialization.
const SERDE_JSON_RAW_VALUE_TOKEN: &str = "$serde_json::private::RawValue";

/// Whether `serde_json::Map` preserves insertion order, i.e. whether the `preserve_order`
/// feature of `serde_json` is enabled. Cargo feature unification can enable it from any crate of
/// the build, so we check the actual behavior.
static SERDE_JSON_PRESERVES_ORDER: LazyLock<bool> = LazyLock::new(|| {
    let mut json_obj = serde_json::Map::new();
    json_obj.insert("b".to_string(), JsonValue::Null);
    json_obj.insert("a".to_string(), JsonValue::Null);
    json_obj.keys().next().map(String::as_str) == Some("b")
});

/// Returns true if `serde_json::Map` (and therefore [`BorrowedObject`]) iterates in insertion order
/// rather than in sorted key order.
pub fn serde_json_preserves_order() -> bool {
    *SERDE_JSON_PRESERVES_ORDER
}

/// A JSON object whose entries have unique keys and are ordered like in `serde_json::Map`.
pub type BorrowedObject<'a> = Vec<(Cow<'a, str>, BorrowedValue<'a>)>;

/// A JSON value borrowing strings from the input when they do not contain escape sequences.
#[derive(Debug, Clone, PartialEq)]
pub enum BorrowedValue<'a> {
    /// JSON `null`.
    Null,
    /// JSON boolean.
    Bool(bool),
    /// JSON number, as built by `serde_json`.
    Number(Number),
    /// JSON string, borrowed from the input unless it contains escape sequences.
    Str(Cow<'a, str>),
    /// JSON array.
    Array(Vec<BorrowedValue<'a>>),
    /// Entries have unique keys and are ordered like in `serde_json::Map`.
    Object(BorrowedObject<'a>),
}

impl<'a> BorrowedValue<'a> {
    /// Returns the value associated with `key` if `self` is an object.
    pub fn get(&self, key: &str) -> Option<&BorrowedValue<'a>> {
        let BorrowedValue::Object(entries) = self else {
            return None;
        };
        get_in_object(entries, key)
    }

    /// Returns true if the value is JSON `null`.
    pub fn is_null(&self) -> bool {
        matches!(self, BorrowedValue::Null)
    }

    /// Converts the value into an owned `serde_json::Value`.
    ///
    /// This allocates and is only meant for error messages and tests.
    pub fn to_serde_json(&self) -> JsonValue {
        match self {
            BorrowedValue::Null => JsonValue::Null,
            BorrowedValue::Bool(bool_val) => JsonValue::Bool(*bool_val),
            BorrowedValue::Number(number) => JsonValue::Number(number.clone()),
            BorrowedValue::Str(text) => JsonValue::String(text.to_string()),
            BorrowedValue::Array(elements) => {
                JsonValue::Array(elements.iter().map(BorrowedValue::to_serde_json).collect())
            }
            BorrowedValue::Object(entries) => JsonValue::Object(
                entries
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_serde_json()))
                    .collect(),
            ),
        }
    }
}

/// Formats the value exactly like the equivalent `serde_json::Value`, so that error messages are
/// the same as the ones of the owned conversion path.
impl fmt::Display for BorrowedValue<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        self.to_serde_json().fmt(formatter)
    }
}

/// Looks up a key in an object. Relies on the keys being unique.
pub(crate) fn get_in_object<'b, 'a>(
    object: &'b BorrowedObject<'a>,
    key: &str,
) -> Option<&'b BorrowedValue<'a>> {
    if *SERDE_JSON_PRESERVES_ORDER {
        return object
            .iter()
            .find(|(entry_key, _)| entry_key.as_ref() == key)
            .map(|(_, value)| value);
    }
    let position = object
        .binary_search_by(|(entry_key, _)| entry_key.as_ref().cmp(key))
        .ok()?;
    Some(&object[position].1)
}

/// A parsed JSON document whose root is an object.
#[derive(Debug, Clone, PartialEq)]
pub struct BorrowedJsonDoc<'a> {
    root: BorrowedObject<'a>,
}

impl<'a> BorrowedJsonDoc<'a> {
    /// Parses a JSON object.
    ///
    /// Accepts and rejects exactly the same inputs as
    /// `serde_json::from_slice::<serde_json::Map<String, Value>>`, with one documented
    /// exception: nested objects whose first key is `serde_json`'s private raw value token are
    /// rejected (`serde_json` would parse the associated string as embedded JSON).
    /// Error messages may differ for invalid UTF-8.
    pub fn parse(json_bytes: &'a [u8]) -> Result<Self, serde_json::Error> {
        // Validating UTF-8 once for the whole document is cheaper than validating each string.
        let json_str = std::str::from_utf8(json_bytes).map_err(de::Error::custom)?;
        let mut deserializer = serde_json::Deserializer::from_str(json_str);
        let root = deserializer.deserialize_map(ObjectVisitor {
            reject_raw_value_token: false,
        })?;
        deserializer.end()?;
        Ok(BorrowedJsonDoc { root })
    }

    /// Returns the entries of the root object. They have unique keys and are ordered like in
    /// `serde_json::Map`.
    pub fn root(&self) -> &BorrowedObject<'a> {
        &self.root
    }

    /// Returns the value associated with `key` in the root object.
    pub fn get(&self, key: &str) -> Option<&BorrowedValue<'a>> {
        get_in_object(&self.root, key)
    }
}

impl<'de> Deserialize<'de> for BorrowedValue<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        deserializer.deserialize_any(ValueVisitor)
    }
}

struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = BorrowedValue<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("any valid JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(BorrowedValue::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(BorrowedValue::Number(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(BorrowedValue::Number(value.into()))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
        // Same as `serde_json::Value`: non-finite floats become `null`.
        match Number::from_f64(value) {
            Some(number) => Ok(BorrowedValue::Number(number)),
            None => Ok(BorrowedValue::Null),
        }
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E> {
        Ok(BorrowedValue::Str(Cow::Borrowed(value)))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(BorrowedValue::Str(Cow::Owned(value.to_string())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(BorrowedValue::Str(Cow::Owned(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(BorrowedValue::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where D: Deserializer<'de> {
        Deserialize::deserialize(deserializer)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(BorrowedValue::Null)
    }

    fn visit_seq<V>(self, mut seq: V) -> Result<Self::Value, V::Error>
    where V: SeqAccess<'de> {
        let mut elements = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(element) = seq.next_element()? {
            elements.push(element);
        }
        Ok(BorrowedValue::Array(elements))
    }

    fn visit_map<V>(self, map: V) -> Result<Self::Value, V::Error>
    where V: MapAccess<'de> {
        let object_visitor = ObjectVisitor {
            reject_raw_value_token: true,
        };
        object_visitor.visit_map(map).map(BorrowedValue::Object)
    }
}

struct ObjectVisitor {
    /// `serde_json::Value` (but not `serde_json::Map`) gives a special meaning to objects whose
    /// first key is the raw value token. We do not support it and reject such documents.
    reject_raw_value_token: bool,
}

impl<'de> Visitor<'de> for ObjectVisitor {
    type Value = BorrowedObject<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        // Same as the visitor of `serde_json::Map`.
        formatter.write_str("a map")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        // Same as the visitor of `serde_json::Map`.
        Ok(Vec::new())
    }

    fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
    where V: MapAccess<'de> {
        let mut entries: BorrowedObject<'de> = Vec::with_capacity(map.size_hint().unwrap_or(0));
        while let Some(key) = map.next_key_seed(KeySeed)? {
            if self.reject_raw_value_token
                && entries.is_empty()
                && key.as_ref() == SERDE_JSON_RAW_VALUE_TOKEN
            {
                return Err(de::Error::custom(format!(
                    "unsupported object key `{SERDE_JSON_RAW_VALUE_TOKEN}`"
                )));
            }
            let value: BorrowedValue<'de> = map.next_value()?;
            entries.push((key, value));
        }
        if *SERDE_JSON_PRESERVES_ORDER {
            dedup_in_place_keeping_last(&mut entries);
        } else {
            sort_and_dedup_keeping_last(&mut entries);
        }
        Ok(entries)
    }
}

/// Removes duplicate keys without reordering, keeping the value of the last occurrence at the
/// position of the first one, which is the behavior of `IndexMap::insert`.
fn dedup_in_place_keeping_last(entries: &mut BorrowedObject) {
    let mut positions: Vec<usize> = (0..entries.len()).collect();
    // Stable sort: positions of duplicate keys stay in increasing order.
    positions.sort_by(|left, right| entries[*left].0.cmp(&entries[*right].0));
    let mut is_removed = vec![false; entries.len()];
    // (first position, last position) of each duplicated key.
    let mut swaps: Vec<(usize, usize)> = Vec::new();
    for group in positions.chunk_by(|left, right| entries[*left].0 == entries[*right].0) {
        let [first_position, .., last_position] = group else {
            continue;
        };
        swaps.push((*first_position, *last_position));
        for duplicate_position in &group[1..] {
            is_removed[*duplicate_position] = true;
        }
    }
    if swaps.is_empty() {
        return;
    }
    // The first occurrence of each key gets the last value. The last occurrence gets the first
    // value but is removed.
    for (first_position, last_position) in swaps {
        entries.swap(first_position, last_position);
    }
    let mut position = 0;
    entries.retain(|_| {
        let keep = !is_removed[position];
        position += 1;
        keep
    });
}

/// Sorts the entries by key and removes duplicate keys, keeping the value of the last occurrence,
/// which is the behavior of `BTreeMap::insert`.
fn sort_and_dedup_keeping_last(entries: &mut BorrowedObject) {
    if entries.is_sorted_by(|left, right| left.0 < right.0) {
        // Strictly sorted: no duplicates.
        return;
    }
    // The sort must be stable for the last occurrence of a key to stay last among its duplicates.
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    // `dedup_by` calls the closure with `(later, retained)` and removes `later` when it returns
    // true. Swapping first moves the later value into the retained entry.
    entries.dedup_by(|later, retained| {
        if later.0 != retained.0 {
            return false;
        }
        std::mem::swap(&mut later.1, &mut retained.1);
        true
    });
}

struct KeySeed;

impl<'de> DeserializeSeed<'de> for KeySeed {
    type Value = Cow<'de, str>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where D: Deserializer<'de> {
        deserializer.deserialize_str(KeyVisitor)
    }
}

struct KeyVisitor;

impl<'de> Visitor<'de> for KeyVisitor {
    type Value = Cow<'de, str>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string key")
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E> {
        Ok(Cow::Borrowed(value))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(Cow::Owned(value.to_string()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(Cow::Owned(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_same_as_serde_json(json: &str) {
        let expected: Result<serde_json::Map<String, JsonValue>, _> = serde_json::from_str(json);
        let parsed = BorrowedJsonDoc::parse(json.as_bytes());
        match (expected, parsed) {
            (Ok(expected_obj), Ok(parsed_doc)) => {
                let parsed_value = BorrowedValue::Object(parsed_doc.root).to_serde_json();
                assert_eq!(
                    parsed_value,
                    JsonValue::Object(expected_obj),
                    "input: {json}"
                );
            }
            (Err(expected_error), Err(parsed_error)) => {
                assert_eq!(
                    expected_error.to_string(),
                    parsed_error.to_string(),
                    "input: {json}"
                );
            }
            (expected, parsed) => {
                panic!("input: {json}, serde_json: {expected:?}, borrowed: {parsed:?}")
            }
        }
    }

    #[test]
    fn test_borrowed_json_same_as_serde_json() {
        let inputs = [
            r#"{}"#,
            r#"{"b": 1, "a": 2, "c": {"z": null, "y": [1, -2, 3.5, true, "x"]}}"#,
            r#"{"a": 1, "a": 2, "b": 3, "a": 4}"#,
            r#"{"nested": {"k": 1, "k": {"x": 1}, "j": 0}}"#,
            r#"{"escaped\"key": "line\nbreak \u00e9 \ud83d\ude00", "plain": "abc"}"#,
            r#"{"big": 18446744073709551615, "bigger": 18446744073709551616, "neg": -9223372036854775808}"#,
            r#"{"float": 1e300, "neg_zero": -0.0, "exp": 2E-5, "int_float": 3.0}"#,
            r#"{"é": 1, "e": 2, "Z": 3, "": 4}"#,
            r#"{"a": [[], [{}], [[null]]]}"#,
            "  {\"a\" : 1 }  \n",
            r#"{"a": 1"#,
            r#"{"a": 1} trailing"#,
            r#"[1, 2]"#,
            r#""string""#,
            r#"null"#,
            r#"42"#,
            r#"{"a": 1e400}"#,
            r#"{"a": "\ud800"}"#,
            r#"{"a": nul}"#,
            r#"{1: 2}"#,
            r#"{"$serde_json::private::RawValue": 1}"#,
        ];
        for input in inputs {
            assert_same_as_serde_json(input);
        }
    }

    #[test]
    fn test_borrowed_json_borrows_unescaped_strings() {
        let json = r#"{"key": "value", "escaped": "va\"lue"}"#;
        let doc = BorrowedJsonDoc::parse(json.as_bytes()).unwrap();
        let root = doc.root();
        assert!(matches!(
            get_in_object(root, "key"),
            Some(BorrowedValue::Str(Cow::Borrowed("value")))
        ));
        assert!(matches!(
            get_in_object(root, "escaped"),
            Some(BorrowedValue::Str(Cow::Owned(_)))
        ));
        assert!(get_in_object(root, "missing").is_none());
    }

    #[test]
    fn test_borrowed_json_invalid_utf8() {
        let invalid_utf8: &[u8] = b"{\"a\": \"\xff\"}";
        assert!(
            serde_json::from_slice::<serde_json::Map<String, JsonValue>>(invalid_utf8).is_err()
        );
        assert!(BorrowedJsonDoc::parse(invalid_utf8).is_err());
    }

    #[test]
    fn test_borrowed_json_rejects_nested_raw_value_token() {
        let json = r#"{"a": {"$serde_json::private::RawValue": "1"}}"#;
        let error = BorrowedJsonDoc::parse(json.as_bytes()).unwrap_err();
        assert!(error.to_string().contains("unsupported object key"));
    }

    #[test]
    fn test_borrowed_json_display_same_as_serde_json() {
        let json = r#"{"a": [1, "two", {"b": null}], "c": 1.5}"#;
        let doc = BorrowedJsonDoc::parse(json.as_bytes()).unwrap();
        let expected: JsonValue = serde_json::from_str(json).unwrap();
        let value = BorrowedValue::Object(doc.root().clone());
        assert_eq!(value.to_string(), expected.to_string());
    }
}
