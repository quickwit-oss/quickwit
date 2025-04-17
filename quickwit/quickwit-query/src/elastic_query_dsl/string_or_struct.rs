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

use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, de};

/// The point of `StringOrStructForSerialization` is to support
/// the two following formats for various queries.
///
/// `{"field": {"query": "my query", "default_operator": "OR"}}`
///
/// and the shorter.
/// `{"field": "my query"}`
///
/// If a integer is passed, we cast it to string. Floats are not supported.
///
/// We don't use untagged enum to support this, in order to keep good errors.
///
/// The code below is adapted from solution described here: <https://serde.rs/string-or-struct.html>
#[derive(Deserialize)]
#[serde(transparent)]
pub(crate) struct StringOrStructForSerialization<T>
where
    T: From<String>,
    for<'de2> T: Deserialize<'de2>,
{
    #[serde(deserialize_with = "string_or_struct")]
    pub inner: T,
}

struct StringOrStructVisitor<T> {
    phantom_data: PhantomData<T>,
}

fn string_or_struct<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: From<String> + Deserialize<'de>,
{
    deserializer.deserialize_any(StringOrStructVisitor {
        phantom_data: Default::default(),
    })
}

impl<'de, T> Visitor<'de> for StringOrStructVisitor<T>
where
    T: From<String>,
    T: Deserialize<'de>,
{
    type Value = T;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let type_str = std::any::type_name::<T>();
        formatter.write_str(&format!("string or map to deserialize {type_str}."))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where E: de::Error {
        self.visit_str(&v.to_string())
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where E: de::Error {
        self.visit_str(&v.to_string())
    }

    fn visit_str<E>(self, query: &str) -> Result<Self::Value, E>
    where E: serde::de::Error {
        Ok(T::from(query.to_string()))
    }

    fn visit_map<M>(self, map: M) -> Result<T, M::Error>
    where M: MapAccess<'de> {
        Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))
    }
}
