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

//! One-tree normalization to the active serde_json Map semantics. No borrowed actor messages.

use std::fmt;
use std::sync::LazyLock;

use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json_borrow::Value;

/// An object normalized to the active serde_json Map backend. The private field prevents
/// accidentally using unnormalized borrowed values in the consuming mapper.
#[derive(Serialize)]
#[serde(transparent)]
pub struct NormalizedObject<'a>(Value<'a>);

impl<'a> NormalizedObject<'a> {
    /// Read the normalized native tree without adapting its containers.
    pub fn value(&self) -> &Value<'a> {
        &self.0
    }

    pub(crate) fn into_value(self) -> Value<'a> {
        self.0
    }
}

// deserialize_map at the root matches serde_json::Map's object-only parse behavior.
struct Object<'a>(Value<'a>);

impl<'de> Deserialize<'de> for Object<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor;
        impl<'de> Visitor<'de> for ObjectVisitor {
            type Value = Object<'de>;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a map")
            }
            fn visit_map<M: MapAccess<'de>>(self, map: M) -> Result<Self::Value, M::Error> {
                Value::deserialize(serde::de::value::MapAccessDeserializer::new(map)).map(Object)
            }
        }
        deserializer.deserialize_map(ObjectVisitor)
    }
}

/// Parse an object and normalize it to this build's serde_json map semantics.
pub fn parse_json_object(input: &[u8]) -> Result<NormalizedObject<'_>, serde_json::Error> {
    let mut value = serde_json::from_slice::<Object>(input)?.0;
    normalize(&mut value);
    Ok(NormalizedObject(value))
}

/// Preserve serde_json's string reader fast path instead of revalidating UTF-8 via bytes.
pub fn parse_json_object_str(input: &str) -> Result<NormalizedObject<'_>, serde_json::Error> {
    let mut value = serde_json::from_str::<Object>(input)?.0;
    normalize(&mut value);
    Ok(NormalizedObject(value))
}

// serde_json documents two Map backends: BTreeMap (sorted keys), or IndexMap
// (insertion order) with preserve_order. Cargo can enable that dependency feature
// through another crate, so a local cfg cannot tell us which semantics are active.
// Observe Map iteration once per process, not from any input document.
static SERDE_JSON_PRESERVES_ORDER: LazyLock<bool> = LazyLock::new(|| {
    let mut map = serde_json::Map::new();
    map.insert("z".to_string(), serde_json::Value::Null);
    map.insert("a".to_string(), serde_json::Value::Null);
    map.keys().next().map(String::as_str) == Some("z")
});

/// Restore this build's serde_json semantics IN PLACE, without a second DOM.
/// Superseded values are dropped before descending. Sorting and swapping move Cow storage.
pub fn normalize(value: &mut Value<'_>) {
    let preserve_order = *SERDE_JSON_PRESERVES_ORDER;
    normalize_with_order(value, preserve_order);
}

fn normalize_with_order(value: &mut Value<'_>, preserve_order: bool) {
    match value {
        Value::Array(values) => {
            for child in values {
                normalize_with_order(child, preserve_order);
            }
        }
        Value::Object(object) => {
            let entries = object.as_vec_mut();
            if !preserve_order {
                // Stable sort preserves duplicate input order. Keep the first key and last value.
                entries.sort_by(|left, right| left.0.cmp(&right.0));
                entries.dedup_by(|later, earlier| {
                    if later.0 == earlier.0 {
                        std::mem::swap(&mut earlier.1, &mut later.1);
                        true
                    } else {
                        false
                    }
                });
            } else if entries.len() > 1 {
                // Linear-time duplicate discovery, preserving the first key position as IndexMap
                // does. Only index metadata is allocated; no cloned keys or second value tree.
                // Document keys are untrusted: use randomized hashing, unlike schema lookups.
                let mut first_positions = std::collections::HashMap::new();
                let mut duplicates = Vec::new();
                for (index, (key, _)) in entries.iter().enumerate() {
                    let first = *first_positions.entry(&**key).or_insert(index);
                    if first != index {
                        duplicates.push((first, index));
                    }
                }
                drop(first_positions);
                for &(first, later) in &duplicates {
                    entries[first].1 = std::mem::take(&mut entries[later].1);
                }
                let mut duplicates = duplicates.into_iter().peekable();
                let mut index = 0;
                entries.retain(|_| {
                    let remove = matches!(duplicates.peek(), Some(&(_, later)) if later == index);
                    if remove {
                        duplicates.next();
                    }
                    index += 1;
                    !remove
                });
            }
            for (_, child) in entries {
                normalize_with_order(child, preserve_order);
            }
        }
        _ => (),
    }
}
