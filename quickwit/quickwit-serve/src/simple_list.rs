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

use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serializer};

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
