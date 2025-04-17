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

use std::net::{IpAddr, Ipv6Addr};
use std::str::FromStr;

use base64::Engine;
use once_cell::sync::OnceCell;
use quickwit_datetime::{DateTimeInputFormat, parse_date_time_str, parse_timestamp};
use serde::{Deserialize, Serialize};
use tantivy::schema::IntoIpv6Addr;

fn get_default_date_time_format() -> &'static [DateTimeInputFormat] {
    static DEFAULT_DATE_TIME_FORMATS: OnceCell<Vec<DateTimeInputFormat>> = OnceCell::new();
    DEFAULT_DATE_TIME_FORMATS
        .get_or_init(|| {
            vec![
                DateTimeInputFormat::Rfc3339,
                DateTimeInputFormat::Rfc2822,
                DateTimeInputFormat::Timestamp,
                DateTimeInputFormat::from_str("%Y-%m-%dT%H:%M:%S").unwrap(),
                DateTimeInputFormat::from_str("%Y-%m-%d %H:%M:%S.%f").unwrap(),
                DateTimeInputFormat::from_str("%Y-%m-%d %H:%M:%S").unwrap(),
                DateTimeInputFormat::from_str("%Y-%m-%d").unwrap(),
                DateTimeInputFormat::from_str("%Y/%m/%d").unwrap(),
            ]
        })
        .as_slice()
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
#[serde(untagged)]
pub enum JsonLiteral {
    Number(serde_json::Number),
    // String is a bit special.
    //
    // It can either mean it was passed as a string by the user (via the es query dsl for
    // instance), or it can mean its type is unknown as it was parsed out of tantivy's query
    // language.
    //
    // We have decided to not make a difference at the moment.
    String(String),
    Bool(bool),
}

pub trait InterpretUserInput<'a>: Sized {
    fn interpret_json(user_input: &'a JsonLiteral) -> Option<Self> {
        match user_input {
            JsonLiteral::Number(number) => Self::interpret_number(number),
            JsonLiteral::String(str_val) => Self::interpret_str(str_val),
            JsonLiteral::Bool(bool_val) => Self::interpret_bool(*bool_val),
        }
    }

    fn interpret_number(_number: &serde_json::Number) -> Option<Self> {
        None
    }

    fn interpret_bool(_bool: bool) -> Option<Self> {
        None
    }
    fn interpret_str(_text: &'a str) -> Option<Self> {
        None
    }

    fn name() -> &'static str {
        std::any::type_name::<Self>()
    }
}

impl<'a> InterpretUserInput<'a> for &'a str {
    fn interpret_str(text: &'a str) -> Option<Self> {
        Some(text)
    }
}

impl<'a> InterpretUserInput<'a> for u64 {
    fn interpret_number(number: &serde_json::Number) -> Option<Self> {
        number.as_u64()
    }

    fn interpret_str(text: &'a str) -> Option<Self> {
        text.parse().ok()
    }
}

impl<'a> InterpretUserInput<'a> for i64 {
    fn interpret_number(number: &serde_json::Number) -> Option<Self> {
        number.as_i64()
    }

    fn interpret_str(text: &'a str) -> Option<Self> {
        text.parse().ok()
    }
}

// We refuse NaN and infinity.
impl<'a> InterpretUserInput<'a> for f64 {
    fn interpret_number(number: &serde_json::Number) -> Option<Self> {
        let val = number.as_f64()?;
        if val.is_nan() || val.is_infinite() {
            return None;
        }
        Some(val)
    }

    fn interpret_str(text: &'a str) -> Option<f64> {
        let val: f64 = text.parse().ok()?;
        if val.is_nan() || val.is_infinite() {
            return None;
        }
        Some(val)
    }
}

impl InterpretUserInput<'_> for bool {
    fn interpret_bool(b: bool) -> Option<Self> {
        Some(b)
    }

    fn interpret_str(text: &str) -> Option<Self> {
        text.parse().ok()
    }
}

impl InterpretUserInput<'_> for Ipv6Addr {
    fn interpret_str(text: &str) -> Option<Self> {
        let ip_addr: IpAddr = text.parse().ok()?;
        Some(ip_addr.into_ipv6_addr())
    }
}

impl InterpretUserInput<'_> for tantivy::DateTime {
    fn interpret_str(text: &str) -> Option<Self> {
        let date_time_formats = get_default_date_time_format();
        if let Ok(datetime) = parse_date_time_str(text, date_time_formats) {
            return Some(datetime);
        }
        // Parsing the normal string formats failed.
        // Maybe it is actually a timestamp as a string?
        let possible_timestamp = text.parse::<i64>().ok()?;
        parse_timestamp(possible_timestamp).ok()
    }

    fn interpret_number(number: &serde_json::Number) -> Option<Self> {
        let possible_timestamp = number.as_i64()?;
        parse_timestamp(possible_timestamp).ok()
    }
}

/// Lenient base64 engine that allows users to use padding or not.
const LENIENT_BASE64_ENGINE: base64::engine::GeneralPurpose = base64::engine::GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    base64::engine::GeneralPurposeConfig::new()
        .with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent),
);

impl InterpretUserInput<'_> for Vec<u8> {
    fn interpret_str(mut text: &str) -> Option<Vec<u8>> {
        let Some(first_byte) = text.as_bytes().first().copied() else {
            return Some(Vec::new());
        };
        let mut buffer = Vec::with_capacity(text.len() * 3 / 4);
        if first_byte == b'!' {
            // We use ! as a marker to force base64 decoding.
            text = &text[1..];
        } else {
            buffer.resize(text.len() / 2, 0u8);
            if hex::decode_to_slice(text, &mut buffer[..]).is_ok() {
                return Some(buffer);
            }
            buffer.clear();
        }
        LENIENT_BASE64_ENGINE.decode_vec(text, &mut buffer).ok()?;
        Some(buffer)
    }
}

impl From<bool> for JsonLiteral {
    fn from(b: bool) -> JsonLiteral {
        JsonLiteral::Bool(b)
    }
}

impl From<String> for JsonLiteral {
    fn from(s: String) -> JsonLiteral {
        JsonLiteral::String(s)
    }
}

impl From<u64> for JsonLiteral {
    fn from(number: u64) -> JsonLiteral {
        JsonLiteral::Number(number.into())
    }
}

impl From<i64> for JsonLiteral {
    fn from(number: i64) -> JsonLiteral {
        JsonLiteral::Number(number.into())
    }
}

#[cfg(test)]
mod tests {
    use tantivy::DateTime;
    use time::macros::datetime;

    use crate::JsonLiteral;
    use crate::json_literal::InterpretUserInput;

    #[test]
    fn test_interpret_str_u64() {
        let val_opt = u64::interpret_str("123");
        assert_eq!(val_opt, Some(123u64));
    }

    #[test]
    fn test_interpret_datetime_simple_date() {
        let dt_opt = DateTime::interpret_json(&JsonLiteral::String("2023-05-25".to_string()));
        let expected_datetime = datetime!(2023-05-25 00:00 UTC);
        assert_eq!(dt_opt, Some(DateTime::from_utc(expected_datetime)));
    }

    #[test]
    fn test_interpret_datetime_rfc3339_with_no_timezone() {
        let dt_opt =
            DateTime::interpret_json(&JsonLiteral::String("2023-05-25T18:00:00".to_string()));
        let expected_datetime = datetime!(2023-05-25 18:00 UTC);
        assert_eq!(dt_opt, Some(DateTime::from_utc(expected_datetime)));
    }

    #[test]
    fn test_interpret_datetime_fractional_millis() {
        let dt_opt =
            DateTime::interpret_json(&JsonLiteral::String("2023-05-25 10:20:11.322".to_string()));
        let expected_datetime = datetime!(2023-05-25 10:20:11.322 UTC);
        assert_eq!(dt_opt, Some(DateTime::from_utc(expected_datetime)));
    }

    #[test]
    fn test_interpret_datetime_unix_timestamp_as_string() {
        let dt_opt = DateTime::interpret_json(&JsonLiteral::String("1685086013".to_string()));
        let expected_datetime = datetime!(2023-05-26 07:26:53 UTC);
        assert_eq!(dt_opt, Some(DateTime::from_utc(expected_datetime)));
    }

    #[test]
    fn test_interpret_datetime_unix_timestamp_as_number() {
        let dt_opt = DateTime::interpret_json(&JsonLiteral::Number(1685086013.into()));
        let expected_datetime = datetime!(2023-05-26 07:26:53 UTC);
        assert_eq!(dt_opt, Some(DateTime::from_utc(expected_datetime)));
    }

    #[test]
    fn test_interpret_bytes_base16_lowercase() {
        let bytes_opt = Vec::<u8>::interpret_str("deadbeef");
        assert_eq!(bytes_opt, Some(vec![0xde, 0xad, 0xbe, 0xef]));
    }

    #[test]
    fn test_interpret_bytes_base16_uppercase() {
        let bytes_opt = Vec::<u8>::interpret_str("DEADBEEF");
        assert_eq!(bytes_opt, Some(vec![0xde, 0xad, 0xbe, 0xef]));
    }

    #[test]
    fn test_interpret_bytes_base16_mixed_casing() {
        let bytes_opt = Vec::<u8>::interpret_str("dEadbeef");
        assert_eq!(bytes_opt, Some(vec![0xde, 0xad, 0xbe, 0xef]));
    }

    #[test]
    fn test_interpret_bytes_base64() {
        let decoded = Vec::<u8>::interpret_str("aGVsbG8=").unwrap();
        assert_eq!(decoded, b"hello");
    }

    #[test]
    fn test_interpret_force_ambiguous_base64() {
        let decoded = Vec::<u8>::interpret_str("!beef").unwrap();
        assert_eq!(decoded, &[109, 231, 159]);
    }

    #[test]
    fn test_interpret_with_and_without_padding() {
        let decoded_without_padding = Vec::<u8>::interpret_str("cQ").unwrap();
        let decoded_with_padding = Vec::<u8>::interpret_str("cQ").unwrap();
        assert_eq!(&decoded_with_padding, &decoded_without_padding);
        assert_eq!(&decoded_with_padding, b"q");
    }

    #[test]
    fn test_interpret_bytes_invalid() {
        assert!(Vec::<u8>::interpret_str("deadbeef@").is_none());
    }
}
