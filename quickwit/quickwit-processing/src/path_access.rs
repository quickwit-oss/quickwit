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
use serde_json::{Map, Value};

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
    pub original: String,
}
impl ParsedPath {
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.segments.iter().map(|s| s.as_str())
    }
}
impl AsRef<[String]> for ParsedPath {
    fn as_ref(&self) -> &[String] {
        &self.segments
    }
}

impl From<&str> for ParsedPath {
    fn from(path: &str) -> Self {
        parse_path(path)
    }
}

/// Splits a dotted path like `"attributes.role"` into segments `["attributes", "role"]`.
pub fn parse_path(path_str: &str) -> ParsedPath {
    let segments = path_str
        .split('.')
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    ParsedPath {
        segments,
        original: path_str.to_string(),
    }
}

/// Recursively get a reference to a nested path, if it exists.
/// We accept serde_json::Map here as this is the type of `custom`.
///
/// If an array is encountered, on our way to the value, this function
/// simply return None.
pub fn get_nested<'a>(
    mut root: &'a serde_json::Map<String, Value>,
    path: &str,
) -> Option<&'a Value> {
    // path[left..right] is always the current key to search
    let mut right = path.find('.').unwrap_or(path.len());
    let mut current_value: Option<&Value> = root.get(&path[..right]);
    let mut left = 0;
    // loop until we have processed all segments
    while right < path.len() {
        if let Some(current_value) = &current_value.and_then(|value| value.as_object()) {
            // if current is found, set the key to the next segment,
            // and set root to current to search deeper.
            left = right + 1;
            right = path[left..]
                .find('.')
                .map(|position| position + left)
                .unwrap_or(path.len());
            root = current_value;
        } else {
            // if current is none, try adding next segment to the key and search again
            right = path[right + 1..]
                .find('.')
                .map(|position| position + right + 1)
                .unwrap_or(path.len());
        }
        current_value = root.get(&path[left..right]);
    }
    current_value
}

fn dotted_key_matches_segments<S>(key: &str, segments: &[S]) -> bool
where
    S: AsRef<str>,
{
    let mut parts = key.split('.');
    for segment in segments {
        if parts.next() != Some(segment.as_ref()) {
            return false;
        }
    }
    parts.next().is_none()
}

fn get_from_map_by_segments<'a, S>(
    root: &'a serde_json::Map<String, Value>,
    segments: &[S],
) -> Option<&'a Value>
where
    S: AsRef<str>,
{
    match segments {
        [] => None,
        [segment] => root.get(segment.as_ref()),
        _ => root
            .iter()
            .find_map(|(key, value)| dotted_key_matches_segments(key, segments).then_some(value)),
    }
}

pub fn get_nested_segments<'a, S>(
    mut root: &'a serde_json::Map<String, Value>,
    segments: &[S],
) -> Option<&'a Value>
where
    S: AsRef<str>,
{
    if segments.is_empty() {
        return None;
    }

    let mut start = 0;
    while start < segments.len() {
        let mut advanced = false;
        for end in start + 1..=segments.len() {
            let Some(value) = get_from_map_by_segments(root, &segments[start..end]) else {
                continue;
            };

            if end == segments.len() {
                return Some(value);
            }

            if let Some(object) = value.as_object() {
                root = object;
                start = end;
                advanced = true;
                break;
            }
        }

        if !advanced {
            return None;
        }
    }

    None
}

/// Remove a nested field from a serde_json Map using the `remove_nested` function.
///
/// This function wraps the map in a `Value::Object`, calls `remove_nested`,
/// and then writes the modified object back into the map.
pub fn remove_nested_from_map(map: &mut Map<String, Value>, segments: &[String]) -> Option<Value> {
    // Temporarily take the map out and wrap it in a Value.
    let mut root = Value::Object(std::mem::take(map));
    let removed = remove_nested_from_json_value(&mut root, segments);
    // Write the modified object back into the map.
    *map = match root {
        Value::Object(new_map) => new_map,
        _ => unreachable!("The root must remain an object."),
    };
    removed
}

/// Remove a nested field at `segments`, returning the removed `Value` if any.
///
/// This function will return None if it encounters an array on its way to the value.
fn remove_nested_from_json_value(root: &mut Value, segments: &[String]) -> Option<Value> {
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
    current.as_object_mut()?.remove(segments.last().unwrap())
}

/// Recursively traverses a JSON object.
/// For each nested value that matches the given path, the callback is called with a reference to
/// the value.
///
/// _Multiple matches_ are possible if the path contains arrays.
pub fn traverse_in_json_obj<'a>(
    root: &'a Map<String, Value>,
    segments: &[String],
    callback: &mut impl FnMut(&'a Value),
) {
    let Some((head, tail)) = segments.split_first() else {
        return;
    };
    // Since the root is a map, pull the first segment to look up in the root.
    if let Some(next_node) = root.get(head) {
        traverse_in_json_value(next_node, tail, callback);
    }
}

/// Recursively traverses a Value using a slice of segments.
/// Arrays are handled by iterating over every element with the same remaining segments.
fn traverse_in_json_value<'a>(
    value: &'a Value,
    segments: &[String],
    callback: &mut impl FnMut(&'a Value),
) {
    // If the current value is an array, apply the same segments to each element.
    if let Value::Array(arr) = value {
        for element in arr {
            traverse_in_json_value(element, segments, callback);
        }
        return;
    }

    if segments.is_empty() {
        // Note that it is possible for a json object to be emitted here.
        callback(value);
        return;
    }

    // If there is another segment to process, then we expect the value to be an object.
    if let Value::Object(map) = value {
        traverse_in_json_obj(map, segments, callback);
    }
}

/// Sets a value at a given dot-separated path, e.g. `a.b.c = value`.
/// If the path does not exist, it is created. If an array is encountered along the path,
/// the same remaining segments are applied to every element.
/// The `segments` parameter is a slice of references to String.
pub fn set_value_at_path_on_map(
    root: &mut Map<String, Value>,
    segments: &[String],
    new_value: Value,
) {
    if segments.is_empty() {
        return;
    }
    // Ensure the first segment exists; if not, create it as an object.
    let first = &segments[0];
    let child = root
        .entry(first.clone())
        .or_insert(Value::Object(Map::new()));
    set_value_at_path(child, &segments[1..], new_value);
}

/// Recursively traverses a mutable JSON value using a slice of path segments.
/// When an array is encountered, the same remaining segments are applied to every element.
/// When no more segments remain, the current value is replaced with `new_value`.
fn set_value_at_path(value: &mut Value, segments: &[String], new_value: Value) {
    if segments.is_empty() {
        // No more segments: replace the current value.
        *value = new_value;
    } else {
        let next_segment = &segments[0];
        match value {
            Value::Object(map) => {
                // Ensure the key exists, creating an object if needed.
                let child = map
                    .entry(next_segment.clone())
                    .or_insert(Value::Object(Map::new()));
                set_value_at_path(child, &segments[1..], new_value);
            }
            Value::Array(arr) => {
                // Apply to the first element in the array.
                if let Some(element) = arr.get_mut(0) {
                    set_value_at_path(element, segments, new_value.clone());
                }
            }
            _ => {
                // For any other type, replace it with an object and continue.
                *value = Value::Object(Map::new());
                if let Value::Object(map) = value {
                    let child = map
                        .entry(next_segment.clone())
                        .or_insert(Value::Object(Map::new()));
                    set_value_at_path(child, &segments[1..], new_value);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_single_segment() {
        let mut root = Map::new();
        let segments = vec!["key".to_string()];
        set_value_at_path_on_map(&mut root, &segments, json!("value"));
        // For a single-segment path, the value is directly set.
        assert_eq!(root.get("key"), Some(&json!("value")));
    }

    #[test]
    fn test_nested_path_creation() {
        let mut root = Map::new();
        let segments = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        set_value_at_path_on_map(&mut root, &segments, json!(42));
        // Expected nested structure: { "a": { "b": { "c": 42 } } }
        let a = root.get("a").unwrap();
        let b = a.get("b").unwrap();
        let c = b.get("c").unwrap();
        assert_eq!(c, &json!(42));
    }

    #[test]
    fn test_overwrite_existing_value() {
        // Start with an existing nested value.
        let mut root: Map<String, Value> = serde_json::from_value(json!({
            "a": {
                "b": {
                    "c": 1
                }
            }
        }))
        .unwrap();
        let segments = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        set_value_at_path_on_map(&mut root, &segments, json!(99));
        // The value at "a.b.c" should now be updated to 99.
        let c = root
            .get("a")
            .and_then(|v| v.get("b"))
            .and_then(|v| v.get("c"))
            .unwrap();
        assert_eq!(c, &json!(99));
    }

    #[test]
    fn test_array_handling() {
        // "a" is an array; update the first element's nested "b" -> "c" value.
        let mut root: Value = json!({
            "a": [
                { "b": { "c": 1 } },
                { "b": { "c": 2 } }
            ]
        });
        let segments = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        set_value_at_path_on_map(root.as_object_mut().unwrap(), &segments, json!(100));
        let arr = root.get("a").and_then(|v| v.as_array()).unwrap();
        let c = arr[0].get("b").and_then(|v| v.get("c")).unwrap();
        assert_eq!(c, &json!(100));
        let c = arr[1].get("b").and_then(|v| v.get("c")).unwrap();
        assert_eq!(c, &json!(2));
    }

    #[test]
    fn test_non_object_intermediate() {
        // "a" starts as a non-object value (a number).
        let mut root: Value = json!({
            "a": 10
        });
        let segments = vec!["a".to_string(), "b".to_string()];
        set_value_at_path_on_map(root.as_object_mut().unwrap(), &segments, json!("new"));
        // "a" should be replaced with an object containing key "b" with value "new".
        let b = root.get("a").and_then(|v| v.get("b")).unwrap();
        assert_eq!(b, &json!("new"));
    }

    #[test]
    fn test_traverse_with_callback_found() {
        let data = json!({
            "users": [
                { "profile": { "name": "Alice", "city": "Wonderland" } },
                { "profile": { "name": "Bob", "city": "Builderland" } }
            ]
        });
        let root = data.as_object().unwrap();
        let mut results = Vec::new();
        let path = ["users", "profile", "city"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        traverse_in_json_obj(root, &path, &mut |v| {
            results.push(v.clone());
        });

        assert_eq!(results.len(), 2);
        assert_eq!(results[0], json!("Wonderland"));
        assert_eq!(results[1], json!("Builderland"));
    }

    #[test]
    fn test_traverse_with_callback_not_found() {
        let data = json!({
            "users": [
                { "profile": { "name": "Alice", "city": "Wonderland" } },
                { "profile": { "name": "Bob", "city": "Builderland" } }
            ]
        });
        let root = data.as_object().unwrap();
        let mut results = Vec::new();
        let path = ["users", "profile", "country"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        traverse_in_json_obj(root, &path, &mut |v| {
            results.push(v.clone());
        });

        // There is no "country" field, so we expect no results.
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_traverse_with_callback_empty_path() {
        let data = json!({
            "key": "value"
        });
        let root = data.as_object().unwrap();
        let mut results = Vec::new();
        let path = vec![];

        traverse_in_json_obj(root, &path, &mut |v| {
            results.push(v.clone());
        });

        // If no segments are provided, no results should be produced.
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_traverse_with_callback_nested_array() {
        let data = json!({
            "a": {
                "b": [
                    { "c": 1 },
                    { "c": 2 },
                    { "d": 3 }  // This one should not match
                ]
            }
        });
        let root = data.as_object().unwrap();
        let mut results = Vec::new();
        let path = ["a", "b", "c"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        traverse_in_json_obj(root, &path, &mut |v| {
            results.push(v.clone());
        });

        assert_eq!(results.len(), 2, "{results:?}");
        assert_eq!(results[0], json!(1));
        assert_eq!(results[1], json!(2));
    }

    #[test]
    fn test_parse_path() {
        let parsed = parse_path("attributes.role");
        assert_eq!(parsed.segments, vec!["attributes", "role"]);

        let parsed = parse_path("single");
        assert_eq!(parsed.segments, vec!["single"]);

        // Edge case: empty string leads to a single empty segment
        let parsed = parse_path("");
        assert_eq!(parsed.segments, vec![""]);
    }

    #[test]
    fn test_get_nested() {
        let data = json!({
            "a": {
                "b": {
                    "c": "value"
                },
                "d": "another value"
            },
            "e": "value at root"
        });
        let map = data.as_object().expect("expected object");

        // Test retrieving nested value: "a" -> "b" -> "c"
        let value = get_nested(map, "a.b.c");
        assert_eq!(value, Some(&Value::String("value".to_string())));

        // Test retrieving a top-level key: "e"
        let value = get_nested(map, "e");
        assert_eq!(value, Some(&Value::String("value at root".to_string())));

        // Test a non-existent nested key: "a" -> "x"
        let value = get_nested(map, "a.x");
        assert!(value.is_none());

        // Test when a non-object is encountered before the path ends:
        // "a" -> "d" -> "something" should return None
        let value = get_nested(map, "a.d.something");
        assert!(value.is_none());

        // Test with an empty iterator, should return None
        let value = get_nested(map, "");
        assert!(value.is_none());
    }

    #[test]
    fn test_get_nested_segments() {
        let json = serde_json::json!({
            "a": {
                "b": { "c": 42 },
                "x.y": 7
            },
            "e": "test"
        });
        let map = json.as_object().unwrap();

        let value = get_nested_segments(map, &["a", "b", "c"]);
        assert_eq!(value, Some(&serde_json::json!(42)));

        let value = get_nested_segments(map, &["a", "x", "y"]);
        assert_eq!(value, Some(&serde_json::json!(7)));

        let value = get_nested_segments(map, &["e"]);
        assert_eq!(value, Some(&serde_json::json!("test")));

        let value = get_nested_segments(map, &["missing"]);
        assert!(value.is_none());

        let empty: [&str; 0] = [];
        let value = get_nested_segments(map, &empty);
        assert!(value.is_none());
    }

    #[test]
    fn test_get_nested_different_depths() {
        let data = json!({
            "a": {
                "b": {
                    "c": "value"
                },
                "d": "another value"
            },
            "e": "value at root"
        });
        let map = data.as_object().expect("expected object");

        // Test retrieving nested value: "a" -> "b" -> "c"
        let value = get_nested(map, "a.b.c");
        assert_eq!(value, Some(&Value::String("value".to_string())));

        let data = json!({
            "a.b": {
                "c": "value",
                "d": "another value"
            },
            "e": "value at root"
        });
        let map = data.as_object().expect("expected object");

        // Test retrieving nested value: "a.b" -> "c"
        let value = get_nested(map, "a.b.c");
        assert_eq!(value, Some(&Value::String("value".to_string())));

        let data = json!({
            "a.b.c": "value",
            "e": "value at root"
        });
        let map = data.as_object().expect("expected object");

        // Test retrieving nested value: "a.b.c"
        let value = get_nested(map, "a.b.c");
        assert_eq!(value, Some(&Value::String("value".to_string())));

        let data = json!({
            "a": {
                "b.c": "value",
                "d": "another value"
            },
            "e": "value at root"
        });
        let map = data.as_object().expect("expected object");

        // Test retrieving nested value: "a -> b.c"
        let value = get_nested(map, "a.b.c");
        assert_eq!(value, Some(&Value::String("value".to_string())));
    }

    #[test]
    fn test_set_or_create_nested_mut() {
        let mut value = json!({});
        let parsed = parse_path("attributes.role");

        // Should create intermediates and set to "admin"
        set_value_at_path(&mut value, &parsed.segments, json!("admin"));
        assert_eq!(value, json!({"attributes": {"role": "admin"}}));

        // Overwrite existing
        set_value_at_path(&mut value, &parsed.segments, json!("user"));
        assert_eq!(value, json!({"attributes": {"role": "user"}}));

        // If an intermediate is not an object, it should be overwritten with an object
        let mut value2 = json!({ "attributes": "not-an-object" });
        set_value_at_path(&mut value2, &parsed.segments, json!("test"));
        assert_eq!(value2, json!({ "attributes": { "role": "test" } }));
    }

    #[test]
    fn test_remove_nested() {
        // Basic removal
        let mut value = json!({
            "attributes": {
                "role": "admin",
                "other": "stuff"
            }
        });
        let parsed = parse_path("attributes.role");
        let removed = remove_nested_from_json_value(&mut value, &parsed.segments);
        assert_eq!(removed, Some(json!("admin")));
        assert_eq!(value, json!({"attributes": {"other": "stuff"}}));

        // Removing a non-existent field
        let parsed_missing = parse_path("attributes.missing");
        let removed_none = remove_nested_from_json_value(&mut value, &parsed_missing.segments);
        assert_eq!(removed_none, None);

        // Removal from top-level
        let mut value2 = json!({
            "top": "something",
            "another": "field"
        });
        let removed_top = remove_nested_from_json_value(&mut value2, &["top".to_string()]);
        assert_eq!(removed_top, Some(json!("something")));
        assert_eq!(value2, json!({"another": "field"}));
    }
}
