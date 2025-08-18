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

use std::fmt::{self, Display};

use serde::de::{self, Deserializer, IntoDeserializer, Visitor};
use serde::Deserialize;
use serde_json::Value;

/// Deserialize a number.
///
/// If the value is a string, it can be optionally coerced to a number.
fn deserialize_num_with_coerce<'de, T, D>(deserializer: D, coerce: bool) -> Result<T, String>
where
    T: std::str::FromStr + Deserialize<'de>,
    T::Err: fmt::Display,
    D: Deserializer<'de>,
{
    struct CoerceVisitor<T> {
        coerce: bool,
        marker: std::marker::PhantomData<T>,
    }

    impl<'de, T> Visitor<'de> for CoerceVisitor<T>
    where
        T: std::str::FromStr + Deserialize<'de>,
        T::Err: fmt::Display,
    {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            if self.coerce {
                formatter
                    .write_str("any number of i64, u64, or f64 or a string that can be coerced")
            } else {
                formatter.write_str("any number of i64, u64, or f64")
            }
        }

        fn visit_str<E>(self, v: &str) -> Result<T, E>
        where E: de::Error {
            if self.coerce {
                v.parse::<T>().map_err(|_e| {
                    de::Error::custom(format!(
                        "failed to coerce JSON string `\"{}\"` to {}",
                        v,
                        std::any::type_name::<T>(),
                    ))
                })
            } else {
                Err(de::Error::custom(format!(
                    "expected JSON number, got string `\"{}\"`. enable coercion to {} with the \
                     `coerce` parameter in the field mapping",
                    v,
                    std::any::type_name::<T>()
                )))
            }
        }

        fn visit_i64<E>(self, v: i64) -> Result<T, E>
        where E: de::Error {
            T::deserialize(v.into_deserializer()).map_err(|_: E| {
                de::Error::custom(format!(
                    "expected {}, got inconvertible JSON number `{}`",
                    std::any::type_name::<T>(),
                    v
                ))
            })
        }

        fn visit_u64<E>(self, v: u64) -> Result<T, E>
        where E: de::Error {
            T::deserialize(v.into_deserializer()).map_err(|_: E| {
                de::Error::custom(format!(
                    "expected {}, got inconvertible JSON number `{}`",
                    std::any::type_name::<T>(),
                    v
                ))
            })
        }

        fn visit_f64<E>(self, v: f64) -> Result<T, E>
        where E: de::Error {
            T::deserialize(v.into_deserializer()).map_err(|_: E| {
                de::Error::custom(format!(
                    "expected {}, got inconvertible JSON number `{}`",
                    std::any::type_name::<T>(),
                    v
                ))
            })
        }

        fn visit_map<M>(self, mut map: M) -> Result<T, M::Error>
        where M: de::MapAccess<'de> {
            let json_value: Value =
                Deserialize::deserialize(de::value::MapAccessDeserializer::new(&mut map))?;
            Err(de::Error::custom(error_message(json_value, self.coerce)))
        }

        fn visit_seq<S>(self, mut seq: S) -> Result<T, S::Error>
        where S: de::SeqAccess<'de> {
            let json_value: Value =
                Deserialize::deserialize(de::value::SeqAccessDeserializer::new(&mut seq))?;
            Err(de::Error::custom(error_message(json_value, self.coerce)))
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where E: de::Error {
            Err(de::Error::custom(error_message("null", self.coerce)))
        }

        fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
        where E: de::Error {
            Err(de::Error::custom(error_message(v, self.coerce)))
        }
    }

    deserializer
        .deserialize_any(CoerceVisitor {
            coerce,
            marker: std::marker::PhantomData,
        })
        .map_err(|err| err.to_string())
}

fn error_message<T: Display>(got: T, coerce: bool) -> String {
    if coerce {
        format!("expected JSON number or string, got `{}`", got)
    } else {
        format!("expected JSON, got `{}`", got)
    }
}

pub fn deserialize_i64<'de, D>(deserializer: D, coerce: bool) -> Result<i64, String>
where D: Deserializer<'de> {
    deserialize_num_with_coerce(deserializer, coerce)
}

pub fn deserialize_u64<'de, D>(deserializer: D, coerce: bool) -> Result<u64, String>
where D: Deserializer<'de> {
    deserialize_num_with_coerce(deserializer, coerce)
}

pub fn deserialize_f64<'de, D>(deserializer: D, coerce: bool) -> Result<f64, String>
where D: Deserializer<'de> {
    deserialize_num_with_coerce(deserializer, coerce)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_deserialize_i64_with_coercion() {
        let json_data = json!("-123");
        let result: i64 = deserialize_i64(json_data.into_deserializer(), true).unwrap();
        assert_eq!(result, -123);

        let json_data = json!("456");
        let result: i64 = deserialize_i64(json_data.into_deserializer(), true).unwrap();
        assert_eq!(result, 456);
    }

    #[test]
    fn test_deserialize_u64_with_coercion() {
        let json_data = json!("789");
        let result: u64 = deserialize_u64(json_data.into_deserializer(), true).unwrap();
        assert_eq!(result, 789);

        let json_data = json!(123);
        let result: u64 = deserialize_u64(json_data.into_deserializer(), false).unwrap();
        assert_eq!(result, 123);
    }

    #[test]
    fn test_deserialize_f64_with_coercion() {
        let json_data = json!("78.9");
        let result: f64 = deserialize_f64(json_data.into_deserializer(), true).unwrap();
        assert_eq!(result, 78.9);

        let json_data = json!(45.6);
        let result: f64 = deserialize_f64(json_data.into_deserializer(), false).unwrap();
        assert_eq!(result, 45.6);
    }

    #[test]
    fn test_deserialize_invalid_string_coercion() {
        let json_data = json!("abc");
        let result: Result<i64, _> = deserialize_i64(json_data.into_deserializer(), true);
        assert!(result.is_err());

        let err_msg = result.unwrap_err().to_string();
        assert_eq!(err_msg, "failed to coerce JSON string `\"abc\"` to i64");
    }

    #[test]
    fn test_deserialize_json_object() {
        let json_data = json!({ "key": "value" });
        let result: Result<i64, _> = deserialize_i64(json_data.into_deserializer(), true);
        assert!(result.is_err());

        let err_msg = result.unwrap_err().to_string();
        assert_eq!(
            err_msg,
            "expected JSON number or string, got `{\"key\":\"value\"}`"
        );
    }
}
