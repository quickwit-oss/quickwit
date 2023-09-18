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

use std::fmt::Display;
use std::str::FromStr;

use ouroboros::self_referencing;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as JsonValue;
use time::error::Format;
use time::format_description::well_known::{Iso8601, Rfc2822, Rfc3339};
use time::format_description::FormatItem;
use time::parsing::Parsed;
use time::{OffsetDateTime, PrimitiveDateTime};
use time_fmt::parse::time_format_item::parse_to_format_item;

use crate::TantivyDateTime;

/// A date time parser that holds the format specification `Vec<FormatItem>`.
#[self_referencing]
pub struct StrptimeParser {
    strptime_format: String,
    with_timezone: bool,
    #[borrows(strptime_format)]
    #[covariant]
    items: Vec<FormatItem<'this>>,
}

impl FromStr for StrptimeParser {
    type Err = String;

    fn from_str(strptime_format_str: &str) -> Result<Self, Self::Err> {
        StrptimeParser::try_new(
            strptime_format_str.to_string(),
            strptime_format_str.to_lowercase().contains("%z"),
            |strptime_format: &String| {
                parse_to_format_item(strptime_format).map_err(|err| {
                    format!("invalid format specification `{strptime_format}`. error: {err}.")
                })
            },
        )
    }
}

impl StrptimeParser {
    /// Parse a given date according to the datetime format specified during the StrptimeParser
    /// creation. If the date format does not provide a specific a time, the time will be set to
    /// 00:00:00.
    fn parse_primitive_date_time(&self, date_time_str: &str) -> anyhow::Result<PrimitiveDateTime> {
        let mut parsed = Parsed::new();
        if !parsed
            .parse_items(date_time_str.as_bytes(), self.borrow_items())?
            .is_empty()
        {
            anyhow::bail!(
                "The date time string `{}` does not match the format `{}`.",
                date_time_str,
                self.borrow_strptime_format()
            );
        }
        // The parsed datetime contains a date but seems to be missing "time".
        // We complete it artificially with 00:00:00.
        if parsed.hour_24().is_none()
            && !(parsed.hour_12().is_some() && parsed.hour_12_is_pm().is_some())
        {
            parsed.set_hour_24(0u8);
            parsed.set_minute(0u8);
            parsed.set_second(0u8);
        }
        let date_time = parsed.try_into()?;
        Ok(date_time)
    }

    pub fn parse_date_time(&self, date_time_str: &str) -> Result<OffsetDateTime, String> {
        if *self.borrow_with_timezone() {
            OffsetDateTime::parse(date_time_str, self.borrow_items()).map_err(|err| err.to_string())
        } else {
            self.parse_primitive_date_time(date_time_str)
                .map(|date_time| date_time.assume_utc())
                .map_err(|err| err.to_string())
        }
    }

    pub fn format_date_time(&self, date_time: &OffsetDateTime) -> Result<String, Format> {
        date_time.format(self.borrow_items())
    }
}

impl Clone for StrptimeParser {
    fn clone(&self) -> Self {
        // `self.format` is already known to be a valid format.
        Self::from_str(self.borrow_strptime_format().as_str()).unwrap()
    }
}

impl PartialEq for StrptimeParser {
    fn eq(&self, other: &Self) -> bool {
        self.borrow_strptime_format() == other.borrow_strptime_format()
    }
}

impl Eq for StrptimeParser {}

impl std::fmt::Debug for StrptimeParser {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StrptimeParser")
            .field("format", &self.borrow_strptime_format())
            .finish()
    }
}

impl std::hash::Hash for StrptimeParser {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.borrow_strptime_format().hash(state);
    }
}

// `Strftime` format special characters.
// These characters are taken from the parsing crate we use for compatibility.
const STRFTIME_FORMAT_MARKERS: [&str; 36] = [
    "%a", "%A", "%b", "%B", "%c", "%C", "%d", "%D", "%e", "%f", "%F", "%h", "%H", "%I", "%j", "%k",
    "%l", "%m", "%M", "%n", "%p", "%P", "%r", "%R", "%S", "%t", "%T", "%U", "%w", "%W", "%x", "%X",
    "%y", "%Y", "%z", "%Z",
];

// Checks if a format contains `strftime` special characters.
fn is_strftime_formatting(format_str: &str) -> bool {
    STRFTIME_FORMAT_MARKERS
        .iter()
        .any(|marker| format_str.contains(marker))
}

/// Specifies the datetime and unix timestamp formats to use when parsing date strings.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Default)]
pub enum DateTimeInputFormat {
    Iso8601,
    Rfc2822,
    #[default]
    Rfc3339,
    Strptime(StrptimeParser),
    Timestamp,
}

impl DateTimeInputFormat {
    pub fn as_str(&self) -> &str {
        match self {
            DateTimeInputFormat::Iso8601 => "iso8601",
            DateTimeInputFormat::Rfc2822 => "rfc2822",
            DateTimeInputFormat::Rfc3339 => "rfc3339",
            DateTimeInputFormat::Strptime(parser) => parser.borrow_strptime_format(),
            DateTimeInputFormat::Timestamp => "unix_timestamp",
        }
    }
}

impl Display for DateTimeInputFormat {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for DateTimeInputFormat {
    type Err = String;

    fn from_str(date_time_format_str: &str) -> Result<Self, Self::Err> {
        let date_time_format = match date_time_format_str.to_lowercase().as_str() {
            "iso8601" => DateTimeInputFormat::Iso8601,
            "rfc2822" => DateTimeInputFormat::Rfc2822,
            "rfc3339" => DateTimeInputFormat::Rfc3339,
            "unix_timestamp" => DateTimeInputFormat::Timestamp,
            _ => {
                if !is_strftime_formatting(date_time_format_str) {
                    return Err(format!(
                        "unknown input format: `{date_time_format_str}`. a custom date time \
                         format must contain at least one `strftime` special characters"
                    ));
                }
                DateTimeInputFormat::Strptime(StrptimeParser::from_str(date_time_format_str)?)
            }
        };
        Ok(date_time_format)
    }
}

impl Serialize for DateTimeInputFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for DateTimeInputFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let date_time_format_str: String = Deserialize::deserialize(deserializer)?;
        let date_time_format = date_time_format_str.parse().map_err(D::Error::custom)?;
        Ok(date_time_format)
    }
}

/// Specifies the datetime format to use when displaying datetime values.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Default)]
pub enum DateTimeOutputFormat {
    Iso8601,
    Rfc2822,
    #[default]
    Rfc3339,
    Strptime(StrptimeParser),
    TimestampSecs,
    TimestampMillis,
    TimestampMicros,
    TimestampNanos,
}

impl DateTimeOutputFormat {
    pub fn as_str(&self) -> &str {
        match self {
            DateTimeOutputFormat::Iso8601 => "iso8601",
            DateTimeOutputFormat::Rfc2822 => "rfc2822",
            DateTimeOutputFormat::Rfc3339 => "rfc3339",
            DateTimeOutputFormat::Strptime(parser) => parser.borrow_strptime_format(),
            DateTimeOutputFormat::TimestampSecs => "unix_timestamp_secs",
            DateTimeOutputFormat::TimestampMillis => "unix_timestamp_millis",
            DateTimeOutputFormat::TimestampMicros => "unix_timestamp_micros",
            DateTimeOutputFormat::TimestampNanos => "unix_timestamp_nanos",
        }
    }

    pub fn format_to_json(&self, date_time: TantivyDateTime) -> Result<JsonValue, String> {
        let date = date_time.into_utc();
        let format_result = match &self {
            DateTimeOutputFormat::Rfc3339 => date.format(&Rfc3339).map(JsonValue::String),
            DateTimeOutputFormat::Iso8601 => date.format(&Iso8601::DEFAULT).map(JsonValue::String),
            DateTimeOutputFormat::Rfc2822 => date.format(&Rfc2822).map(JsonValue::String),
            DateTimeOutputFormat::Strptime(strftime_parser) => strftime_parser
                .format_date_time(&date)
                .map(JsonValue::String),
            DateTimeOutputFormat::TimestampSecs => {
                Ok(JsonValue::Number(date_time.into_timestamp_secs().into()))
            }
            DateTimeOutputFormat::TimestampMillis => {
                Ok(JsonValue::Number(date_time.into_timestamp_millis().into()))
            }
            DateTimeOutputFormat::TimestampMicros => {
                Ok(JsonValue::Number(date_time.into_timestamp_micros().into()))
            }
            DateTimeOutputFormat::TimestampNanos => {
                Ok(JsonValue::Number(date_time.into_timestamp_nanos().into()))
            }
        };
        format_result.map_err(|error| error.to_string())
    }
}

impl Display for DateTimeOutputFormat {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for DateTimeOutputFormat {
    type Err = String;

    fn from_str(date_time_format_str: &str) -> Result<Self, Self::Err> {
        let date_time_format = match date_time_format_str.to_lowercase().as_str() {
            "iso8601" => DateTimeOutputFormat::Iso8601,
            "rfc2822" => DateTimeOutputFormat::Rfc2822,
            "rfc3339" => DateTimeOutputFormat::Rfc3339,
            "unix_timestamp_secs" => DateTimeOutputFormat::TimestampSecs,
            "unix_timestamp_millis" => DateTimeOutputFormat::TimestampMillis,
            "unix_timestamp_micros" => DateTimeOutputFormat::TimestampMicros,
            "unix_timestamp_nanos" => DateTimeOutputFormat::TimestampNanos,
            _ => {
                if !is_strftime_formatting(date_time_format_str) {
                    return Err(format!(
                        "unknown output format: `{date_time_format_str}`. a custom date time \
                         format must contain at least one `strftime` special characters"
                    ));
                }
                DateTimeOutputFormat::Strptime(StrptimeParser::from_str(date_time_format_str)?)
            }
        };
        Ok(date_time_format)
    }
}

impl Serialize for DateTimeOutputFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for DateTimeOutputFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let date_time_format_str: String = Deserialize::deserialize(deserializer)?;
        let date_time_format = date_time_format_str.parse().map_err(D::Error::custom)?;
        Ok(date_time_format)
    }
}

#[cfg(test)]
mod tests {
    use time::macros::datetime;

    use super::*;

    #[test]
    fn test_date_time_input_format_ser() {
        let date_time_formats_json = serde_json::to_value(&[
            DateTimeInputFormat::Iso8601,
            DateTimeInputFormat::Rfc2822,
            DateTimeInputFormat::Rfc3339,
            DateTimeInputFormat::Timestamp,
        ])
        .unwrap();

        let expected_date_time_formats =
            serde_json::json!(["iso8601", "rfc2822", "rfc3339", "unix_timestamp",]);
        assert_eq!(date_time_formats_json, expected_date_time_formats);
    }

    #[test]
    fn test_date_time_input_format_deser() {
        let date_time_formats_json = r#"
            [
                "iso8601",
                "rfc2822",
                "rfc3339",
                "unix_timestamp"
            ]
            "#;
        let date_time_formats: Vec<DateTimeInputFormat> =
            serde_json::from_str(date_time_formats_json).unwrap();
        let expected_date_time_formats = [
            DateTimeInputFormat::Iso8601,
            DateTimeInputFormat::Rfc2822,
            DateTimeInputFormat::Rfc3339,
            DateTimeInputFormat::Timestamp,
        ];
        assert_eq!(date_time_formats, &expected_date_time_formats);
    }

    #[test]
    fn test_date_time_output_format_ser() {
        let date_time_formats_json = serde_json::to_value(&[
            DateTimeOutputFormat::Iso8601,
            DateTimeOutputFormat::Rfc2822,
            DateTimeOutputFormat::Rfc3339,
            DateTimeOutputFormat::TimestampSecs,
            DateTimeOutputFormat::TimestampMillis,
            DateTimeOutputFormat::TimestampMicros,
            DateTimeOutputFormat::TimestampNanos,
        ])
        .unwrap();

        let expected_date_time_formats = serde_json::json!([
            "iso8601",
            "rfc2822",
            "rfc3339",
            "unix_timestamp_secs",
            "unix_timestamp_millis",
            "unix_timestamp_micros",
            "unix_timestamp_nanos",
        ]);
        assert_eq!(date_time_formats_json, expected_date_time_formats);
    }

    #[test]
    fn test_date_time_output_format_deser() {
        let date_time_formats_json = r#"
            [
                "iso8601",
                "rfc2822",
                "rfc3339",
                "unix_timestamp_secs",
                "unix_timestamp_millis",
                "unix_timestamp_micros",
                "unix_timestamp_nanos"
            ]
            "#;
        let date_time_formats: Vec<DateTimeOutputFormat> =
            serde_json::from_str(date_time_formats_json).unwrap();
        let expected_date_time_formats = [
            DateTimeOutputFormat::Iso8601,
            DateTimeOutputFormat::Rfc2822,
            DateTimeOutputFormat::Rfc3339,
            DateTimeOutputFormat::TimestampSecs,
            DateTimeOutputFormat::TimestampMillis,
            DateTimeOutputFormat::TimestampMicros,
            DateTimeOutputFormat::TimestampNanos,
        ];
        assert_eq!(date_time_formats, &expected_date_time_formats);
    }

    #[test]
    fn test_fail_date_time_input_format_from_str_with_unknown_format() {
        let formats = vec![
            "test%",
            "test-%v",
            "test-%q",
            "unix_timestamp_secs",
            "unix_timestamp_seconds",
        ];
        for format in formats {
            let error_str = DateTimeInputFormat::from_str(format)
                .unwrap_err()
                .to_string();
            assert!(error_str.contains(&format!("unknown input format: `{format}`")));
        }
    }

    #[test]
    fn test_fail_date_time_output_format_from_str_with_unknown_format() {
        let formats = vec!["test%", "test-%v", "test-%q", "unix_timestamp_seconds"];
        for format in formats {
            let error_str = DateTimeOutputFormat::from_str(format)
                .unwrap_err()
                .to_string();
            assert!(error_str.contains(&format!("unknown output format: `{format}`")));
        }
    }

    #[test]
    fn test_strictly_parse_datetime_format() {
        let parser = StrptimeParser::from_str("%Y-%m-%d").unwrap();
        assert_eq!(
            parser.parse_date_time("2021-01-01").unwrap(),
            datetime!(2021-01-01 00:00:00 UTC)
        );
        let error_str = parser.parse_date_time("2021-01-01TABC").unwrap_err();
        assert_eq!(
            error_str,
            "The date time string `2021-01-01TABC` does not match the format `%Y-%m-%d`."
        );
    }
}
