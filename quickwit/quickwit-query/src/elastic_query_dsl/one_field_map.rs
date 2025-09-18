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

use std::fmt;
use std::marker::PhantomData;

use serde::de::Visitor;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize};

/// Helper to serialize/deserialize `{"my_field": {..}}` object
/// often present in Elasticsearch DSL.
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct OneFieldMap<V> {
    pub field: String,
    pub value: V,
}

impl<V: Serialize> Serialize for OneFieldMap<V> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(&self.field, &self.value)?;
        map.end()
    }
}

struct OneFieldMapVisitor<V> {
    _data: PhantomData<V>,
}

impl<'de, V: Deserialize<'de>> Visitor<'de> for OneFieldMapVisitor<V> {
    type Value = OneFieldMap<V>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Expected a map with a single field.")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where A: serde::de::MapAccess<'de> {
        if let Some(num_keys) = map.size_hint()
            && num_keys != 1
        {
            return Err(serde::de::Error::custom(format!(
                "expected a single field. got {num_keys}"
            )));
        }
        let Some((key, val)) = map.next_entry()? else {
            return Err(serde::de::Error::custom(
                "expected a single field. got none",
            ));
        };
        if let Some(second_key) = map.next_key::<String>()? {
            return Err(serde::de::Error::custom(format!(
                "expected a single field. got several ({key}, {second_key}, ...)"
            )));
        }
        Ok(OneFieldMap {
            field: key,
            value: val,
        })
    }
}

impl<'de, V: Deserialize<'de>> Deserialize<'de> for OneFieldMap<V> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        deserializer.deserialize_map(OneFieldMapVisitor {
            _data: Default::default(),
        })
    }
}

#[cfg(test)]
mod tests {

    use serde::{Deserialize, Serialize};

    use crate::OneFieldMap;
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
    struct Property {
        count: usize,
    }

    #[test]
    fn test_one_field_hash_map_simple() {
        let one_field_map = OneFieldMap {
            field: "my-field".to_string(),
            value: Property { count: 2 },
        };
        let json = serde_json::to_value(one_field_map).unwrap();
        assert_eq!(&json, &serde_json::json!({"my-field": {"count": 2}}));
        let deser_ser = serde_json::from_value::<OneFieldMap<Property>>(json).unwrap();
        assert_eq!(deser_ser.field.as_str(), "my-field");
        assert_eq!(deser_ser.value.count, 2);
    }

    #[test]
    fn test_one_field_hash_map_deserialize_error_too_many_fields() {
        let deser: serde_json::Result<OneFieldMap<Property>> =
            serde_json::from_value(serde_json::json!({
                "my-field": {"count": 2},
                "my-field2": {"count": 2}
            }));
        let deser_err = deser.unwrap_err();
        assert_eq!(deser_err.to_string(), "expected a single field. got 2");
    }

    #[test]
    fn test_one_field_hash_map_deserialize_error_no_fields() {
        let deser: serde_json::Result<OneFieldMap<Property>> =
            serde_json::from_value(serde_json::json!({}));
        let deser_err = deser.unwrap_err();
        assert_eq!(deser_err.to_string(), "expected a single field. got 0");
    }
}
