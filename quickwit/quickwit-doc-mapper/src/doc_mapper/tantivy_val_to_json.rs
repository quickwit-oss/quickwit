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

use serde_json::Value as JsonValue;
use tantivy::schema::OwnedValue as TantivyValue;

use super::BinaryFormat;
use super::field_mapping_entry::{NumericOutputFormat, QuickwitNumericOptions};
use super::mapping_tree::LeafType;

pub(crate) trait NumToJson {
    fn to_json(&self, output_format: NumericOutputFormat) -> Option<JsonValue>;
}

impl NumToJson for u64 {
    fn to_json(&self, output_format: NumericOutputFormat) -> Option<JsonValue> {
        let json_value = match output_format {
            NumericOutputFormat::String => JsonValue::String(self.to_string()),
            NumericOutputFormat::Number => JsonValue::Number(serde_json::Number::from(*self)),
        };
        Some(json_value)
    }
}

impl NumToJson for i64 {
    fn to_json(&self, output_format: NumericOutputFormat) -> Option<JsonValue> {
        let json_value = match output_format {
            NumericOutputFormat::String => JsonValue::String(self.to_string()),
            NumericOutputFormat::Number => JsonValue::Number(serde_json::Number::from(*self)),
        };
        Some(json_value)
    }
}
impl NumToJson for f64 {
    fn to_json(&self, output_format: NumericOutputFormat) -> Option<JsonValue> {
        match output_format {
            NumericOutputFormat::String => Some(JsonValue::String(self.to_string())),
            NumericOutputFormat::Number => {
                serde_json::Number::from_f64(*self).map(JsonValue::Number)
            }
        }
    }
}

fn value_to_string(value: TantivyValue) -> Result<JsonValue, TantivyValue> {
    match value {
        TantivyValue::Str(s) => return Ok(JsonValue::String(s)),
        TantivyValue::U64(number) => Some(number.to_string()),
        TantivyValue::I64(number) => Some(number.to_string()),
        TantivyValue::F64(number) => Some(number.to_string()),
        TantivyValue::Bool(b) => Some(b.to_string()),
        TantivyValue::Date(date) => {
            return quickwit_datetime::DateTimeOutputFormat::default()
                .format_to_json(date)
                .map_err(|_| value);
        }
        TantivyValue::IpAddr(ip) => Some(ip.to_string()),
        _ => None,
    }
    .map(JsonValue::String)
    .ok_or(value)
}

fn value_to_bool(value: TantivyValue) -> Result<JsonValue, TantivyValue> {
    match &value {
        TantivyValue::Str(s) => s.parse().ok(),
        TantivyValue::U64(number) => match number {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        },
        TantivyValue::I64(number) => match number {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        },
        TantivyValue::F64(number) => match number {
            0.0 => Some(false),
            1.0 => Some(true),
            _ => None,
        },
        TantivyValue::Bool(b) => Some(*b),
        _ => None,
    }
    .map(JsonValue::Bool)
    .ok_or(value)
}

fn value_to_ip(value: TantivyValue) -> Result<JsonValue, TantivyValue> {
    match &value {
        TantivyValue::Str(s) => s
            .parse::<std::net::Ipv6Addr>()
            .or_else(|_| {
                s.parse::<std::net::Ipv4Addr>()
                    .map(|ip| ip.to_ipv6_mapped())
            })
            .ok(),
        TantivyValue::IpAddr(ip) => Some(*ip),
        _ => None,
    }
    .map(|ip| {
        serde_json::to_value(TantivyValue::IpAddr(ip))
            .expect("Json serialization should never fail.")
    })
    .ok_or(value)
}

fn value_to_float(
    value: TantivyValue,
    numeric_options: &QuickwitNumericOptions,
) -> Result<JsonValue, TantivyValue> {
    match &value {
        TantivyValue::Str(s) => s.parse().ok(),
        TantivyValue::U64(number) => Some(*number as f64),
        TantivyValue::I64(number) => Some(*number as f64),
        TantivyValue::F64(number) => Some(*number),
        TantivyValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
    .and_then(|f64_val| f64_val.to_json(numeric_options.output_format))
    .ok_or(value)
}

fn value_to_u64(
    value: TantivyValue,
    numeric_options: &QuickwitNumericOptions,
) -> Result<JsonValue, TantivyValue> {
    match &value {
        TantivyValue::Str(s) => s.parse().ok(),
        TantivyValue::U64(number) => Some(*number),
        TantivyValue::I64(number) => (*number).try_into().ok(),
        TantivyValue::F64(number) => {
            if (0.0..=(u64::MAX as f64)).contains(number) {
                Some(*number as u64)
            } else {
                None
            }
        }
        TantivyValue::Bool(b) => Some(*b as u64),
        _ => None,
    }
    .and_then(|u64_val| u64_val.to_json(numeric_options.output_format))
    .ok_or(value)
}

fn value_to_i64(
    value: TantivyValue,
    numeric_options: &QuickwitNumericOptions,
) -> Result<JsonValue, TantivyValue> {
    match &value {
        TantivyValue::Str(s) => s.parse().ok(),
        TantivyValue::U64(number) => (*number).try_into().ok(),
        TantivyValue::I64(number) => Some(*number),
        TantivyValue::F64(number) => {
            if ((i64::MIN as f64)..=(i64::MAX as f64)).contains(number) {
                Some(*number as i64)
            } else {
                None
            }
        }
        TantivyValue::Bool(b) => Some(*b as i64),
        _ => None,
    }
    .and_then(|u64_val| u64_val.to_json(numeric_options.output_format))
    .ok_or(value)
}

/// Transforms a tantivy object into a serde_json one, without cloning strings.
/// It still allocates maps.
// TODO we should probably move this to tantivy, it has the opposite conversion already
pub fn tantivy_object_to_json_value(object: Vec<(String, TantivyValue)>) -> JsonValue {
    JsonValue::Object(
        object
            .into_iter()
            .map(|(key, value)| (key, tantivy_value_to_json(value)))
            .collect(),
    )
}

/// Converts Tantivy::Value into Json Value.
///
/// Formatting by defaults, e.g. Rfc3339 for dates.
pub fn tantivy_value_to_json(value: TantivyValue) -> JsonValue {
    match value {
        TantivyValue::Null => JsonValue::Null,
        TantivyValue::Str(s) => JsonValue::String(s),
        TantivyValue::U64(number) => JsonValue::Number(number.into()),
        TantivyValue::I64(number) => JsonValue::Number(number.into()),
        TantivyValue::F64(f) => {
            JsonValue::Number(serde_json::Number::from_f64(f).expect("expected finite f64"))
        }
        TantivyValue::Bool(b) => JsonValue::Bool(b),
        TantivyValue::Array(array) => {
            JsonValue::Array(array.into_iter().map(tantivy_value_to_json).collect())
        }
        TantivyValue::Object(object) => tantivy_object_to_json_value(object),
        // we shouldn't have these types inside a json field in quickwit
        TantivyValue::PreTokStr(pretok) => JsonValue::String(pretok.text),
        TantivyValue::Date(date) => quickwit_datetime::DateTimeOutputFormat::Rfc3339
            .format_to_json(date)
            .expect("Invalid datetime is not allowed."),
        TantivyValue::Facet(facet) => JsonValue::String(facet.to_string()),
        TantivyValue::Bytes(bytes) => BinaryFormat::Base64.format_to_json(&bytes),
        TantivyValue::IpAddr(ip_v6) => {
            let ip_str = if let Some(ip_v4) = ip_v6.to_ipv4_mapped() {
                ip_v4.to_string()
            } else {
                ip_v6.to_string()
            };
            JsonValue::String(ip_str)
        }
    }
}

/// Converts TantivyValue into Json Value and formats according to the LeafType.
///
/// Makes sure the type and value are consistent before converting.
/// For certain LeafType, we use the type options to format the output.
pub fn formatted_tantivy_value_to_json(
    value: TantivyValue,
    leaf_type: &LeafType,
) -> Option<JsonValue> {
    let res = match leaf_type {
        LeafType::Text(_) => value_to_string(value),
        LeafType::Bool(_) => value_to_bool(value),
        LeafType::IpAddr(_) => value_to_ip(value),
        LeafType::F64(numeric_options) => value_to_float(value, numeric_options),
        LeafType::U64(numeric_options) => value_to_u64(value, numeric_options),
        LeafType::I64(numeric_options) => value_to_i64(value, numeric_options),
        LeafType::Json(_) => {
            if let TantivyValue::Object(obj) = value {
                // TODO do we want to allow almost everything here?
                return Some(tantivy_object_to_json_value(obj));
            } else {
                Err(value)
            }
        }
        LeafType::Bytes(bytes_options) => {
            if let TantivyValue::Bytes(ref bytes) = value {
                // TODO we could cast str to bytes
                let json_value = bytes_options.output_format.format_to_json(bytes);
                Ok(json_value)
            } else {
                Err(value)
            }
        }
        LeafType::DateTime(date_time_options) => date_time_options
            .reparse_tantivy_value(&value)
            .map(|date_time| {
                date_time_options
                    .output_format
                    .format_to_json(date_time)
                    .expect("Invalid datetime is not allowed.")
            })
            .ok_or(value),
    };
    match res {
        Ok(res) => Some(res),
        Err(value) => {
            quickwit_common::rate_limited_warn!(
                limit_per_min = 2,
                "the value type `{:?}` doesn't match the requested type `{:?}`",
                value,
                leaf_type
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {

    use tantivy::schema::OwnedValue as TantivyValue;

    use super::*;
    use crate::doc_mapper::field_mapping_entry::{
        BinaryFormat, NumericOutputFormat, QuickwitBytesOptions, QuickwitNumericOptions,
    };
    use crate::doc_mapper::mapping_tree::LeafType;

    #[test]
    fn test_tantivy_value_to_json_value_bytes() {
        let bytes_options_base64 = QuickwitBytesOptions::default();
        assert_eq!(
            formatted_tantivy_value_to_json(
                TantivyValue::Bytes(vec![1, 2, 3]),
                &LeafType::Bytes(bytes_options_base64)
            )
            .unwrap(),
            serde_json::json!("AQID")
        );

        let bytes_options_hex = QuickwitBytesOptions {
            output_format: BinaryFormat::Hex,
            ..Default::default()
        };
        assert_eq!(
            formatted_tantivy_value_to_json(
                TantivyValue::Bytes(vec![1, 2, 3]),
                &LeafType::Bytes(bytes_options_hex)
            )
            .unwrap(),
            serde_json::json!("010203")
        );
    }

    #[test]
    fn test_tantivy_value_to_json_value_f64() {
        let numeric_options_number = QuickwitNumericOptions::default();
        assert_eq!(
            formatted_tantivy_value_to_json(
                TantivyValue::F64(0.1),
                &LeafType::F64(numeric_options_number.clone())
            )
            .unwrap(),
            serde_json::json!(0.1)
        );
        assert_eq!(
            formatted_tantivy_value_to_json(
                TantivyValue::U64(1),
                &LeafType::F64(numeric_options_number.clone())
            )
            .unwrap(),
            serde_json::json!(1.0)
        );
        assert_eq!(
            formatted_tantivy_value_to_json(
                TantivyValue::Str("0.1".to_string()),
                &LeafType::F64(numeric_options_number.clone())
            )
            .unwrap(),
            serde_json::json!(0.1)
        );

        let numeric_options_str = QuickwitNumericOptions {
            output_format: NumericOutputFormat::String,
            ..Default::default()
        };
        assert_eq!(
            formatted_tantivy_value_to_json(
                TantivyValue::F64(0.1),
                &LeafType::F64(numeric_options_str)
            )
            .unwrap(),
            serde_json::json!("0.1")
        );
    }

    #[test]
    fn test_tantivy_value_to_json_value_i64() {
        let numeric_options_number = QuickwitNumericOptions::default();
        assert_eq!(
            formatted_tantivy_value_to_json(
                TantivyValue::I64(-1),
                &LeafType::I64(numeric_options_number.clone())
            )
            .unwrap(),
            serde_json::json!(-1)
        );
        assert_eq!(
            formatted_tantivy_value_to_json(
                TantivyValue::I64(1),
                &LeafType::I64(numeric_options_number)
            )
            .unwrap(),
            serde_json::json!(1)
        );

        let numeric_options_str = QuickwitNumericOptions {
            output_format: NumericOutputFormat::String,
            ..Default::default()
        };
        assert_eq!(
            formatted_tantivy_value_to_json(
                TantivyValue::I64(-1),
                &LeafType::I64(numeric_options_str)
            )
            .unwrap(),
            serde_json::json!("-1")
        );
    }

    #[test]
    fn test_tantivy_value_to_json_value_u64() {
        let numeric_options_number = QuickwitNumericOptions::default();
        assert_eq!(
            formatted_tantivy_value_to_json(
                TantivyValue::U64(1),
                &LeafType::U64(numeric_options_number.clone())
            )
            .unwrap(),
            serde_json::json!(1u64)
        );
        assert_eq!(
            formatted_tantivy_value_to_json(
                TantivyValue::I64(1),
                &LeafType::U64(numeric_options_number)
            )
            .unwrap(),
            serde_json::json!(1u64)
        );

        let numeric_options_str = QuickwitNumericOptions {
            output_format: NumericOutputFormat::String,
            ..Default::default()
        };
        assert_eq!(
            formatted_tantivy_value_to_json(
                TantivyValue::U64(1),
                &LeafType::U64(numeric_options_str)
            )
            .unwrap(),
            serde_json::json!("1")
        );
    }
}
