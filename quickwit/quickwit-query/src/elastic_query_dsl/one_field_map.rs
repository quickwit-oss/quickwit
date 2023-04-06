// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

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
        if let Some(num_keys) = map.size_hint() {
            if num_keys != 1 {
                return Err(serde::de::Error::custom(format!(
                    "Expected a single field. Got {num_keys}."
                )));
            }
        }
        let Some((key, val)) = map.next_entry()? else {
            return Err(serde::de::Error::custom("Expected a single field. Got none."));
        };
        if let Some(second_key) = map.next_key::<String>()? {
            return Err(serde::de::Error::custom(format!(
                "Expected a single field. Got several ({key}, {second_key}, ...)."
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
        assert_eq!(deser_err.to_string(), "Expected a single field. Got 2.");
    }

    #[test]
    fn test_one_field_hash_map_deserialize_error_no_fields() {
        let deser: serde_json::Result<OneFieldMap<Property>> =
            serde_json::from_value(serde_json::json!({}));
        let deser_err = deser.unwrap_err();
        assert_eq!(deser_err.to_string(), "Expected a single field. Got 0.");
    }
}
