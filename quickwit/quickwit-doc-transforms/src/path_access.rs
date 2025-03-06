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

use std::sync::OnceLock;

use fnv::FnvHashSet;
use serde_json::Value;

/// Attributes that represent special fields in Datadog.
#[allow(dead_code)]
pub static CORE_ATTRIBUTES: &[&str] = &[
    "custom",
    "discovery_timestamp",
    "host",
    "host_id",
    "id",
    "ingest_size_in_bytes",
    "message",
    "random_draw",
    "service",
    "source",
    "source_type",
    "source_fragment_id",
    "span_id",
    "status",
    "tag",
    "tags",
    "timestamp",
    "trace_id",
    "trace_id_low",
];

/// Any attribute that's not a field is tag.
#[allow(dead_code)]
pub fn is_core_attr(attribute: &str) -> bool {
    /// Use OnceLock from std to create a FnvHashSet of CORE_ATTRIBUTES
    static CORE_ATTRIBUTES_SET: OnceLock<FnvHashSet<&'static str>> = OnceLock::new();
    let core_attributes = CORE_ATTRIBUTES_SET.get_or_init(|| {
        let mut set = FnvHashSet::default();
        for attr in CORE_ATTRIBUTES {
            set.insert(*attr);
        }
        set
    });
    core_attributes.contains(attribute)
}

/// A path stored as pre-split segments for efficient nested access.
#[derive(Debug, Clone)]
pub struct ParsedPath {
    pub segments: Vec<String>,
}

/// Splits a dotted path like `"attributes.role"` into segments `["attributes", "role"]`.
pub fn parse_path(path_str: &str) -> ParsedPath {
    let segments = path_str
        .split('.')
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    ParsedPath { segments }
}

/// Recursively get a **mutable reference** to a nested path, if it exists.
pub fn get_nested_mut<'a>(root: &'a mut Value, segments: &[String]) -> Option<&'a mut Value> {
    let mut current = root;
    for seg in segments {
        match current {
            Value::Object(map) => {
                current = map.get_mut(seg)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

/// Like `get_nested_mut`, but we create intermediate objects if missing.
pub fn set_or_create_nested_mut(root: &mut serde_json::Value, segments: &[String], value: Value) {
    let mut current = root;
    for (i, segment) in segments.iter().enumerate() {
        let is_last = (i + 1) == segments.len();
        if !current.is_object() {
            *current = Value::Object(serde_json::Map::new());
        }
        let map = current.as_object_mut().unwrap();
        if is_last {
            // Final segment: if not present, insert Null
            if !map.contains_key(segment) {
                map.insert(segment.clone(), Value::Null);
            }
            *map.get_mut(segment).unwrap() = value;
            return;
        } else {
            // Intermediate
            if !map.contains_key(segment) {
                map.insert(segment.clone(), Value::Object(serde_json::Map::new()));
            }
            current = map.get_mut(segment).unwrap();
        }
    }
    *current = value;
}

/// Remove a nested field at `segments`, returning the removed `Value` if any.
pub fn remove_nested(root: &mut Value, segments: &[String]) -> Option<Value> {
    if segments.is_empty() {
        return None;
    }
    let mut current = root;
    for segment in &segments[..segments.len() - 1] {
        match current {
            Value::Object(map) => {
                current = map.get_mut(segment)?;
            }
            _ => return None,
        }
    }
    if let Value::Object(map) = current {
        map.remove(segments.last().unwrap())
    } else {
        None
    }
}
