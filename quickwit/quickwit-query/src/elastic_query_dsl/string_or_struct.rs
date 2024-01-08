// Copyright (C) 2024 Quickwit, Inc.
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

use serde::de::{MapAccess, Visitor};
use serde::{de, Deserialize, Deserializer};

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
/// The code below is adapted from solution described here: https://serde.rs/string-or-struct.html
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
