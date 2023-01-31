// Copyright (C) 2022 Quickwit, Inc.
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

use std::convert::Infallible;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serializer};

/// A helper struct to serialize/deserialize a comma separated list.
#[derive(Debug, Deserialize)]
pub struct SimpleList(pub Vec<String>);

impl FromStr for SimpleList {
    type Err = Infallible;

    fn from_str(str_sequence: &str) -> Result<Self, Self::Err> {
        let items = str_sequence
            .trim_matches(',')
            .split(',')
            .map(|item| item.to_owned())
            .collect::<Vec<_>>();
        Ok(Self(items))
    }
}

/// Serializes an `Option<&[Serialize]>` with
/// `Some(value)` to a comma separated string of values.
/// Used to serialize values within the query string
pub fn to_simple_list<S, T>(
    value: &Option<Vec<T>>,
    serializer: S,
) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
where
    S: Serializer,
    T: ToString,
{
    let vec = &value
        .as_ref()
        .expect("attempt to serialize Option::None value");

    let serialized_str = vec
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>() // do not collect here
        .join(",");

    serializer.serialize_str(&serialized_str)
}

/// Deserializes a comma separated string of values
/// into a [`Vec<T>`].
/// Used to deserialize list of values from the query string.
pub fn from_simple_list<'de, D, T>(deserializer: D) -> Result<Option<Vec<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    <T as FromStr>::Err: ToString,
{
    let str_sequence = String::deserialize(deserializer)?;
    let list = str_sequence
        .trim_matches(',')
        .split(',')
        .map(|item| T::from_str(item))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| serde::de::Error::custom(err.to_string()))?;
    Ok(Some(list))
}
