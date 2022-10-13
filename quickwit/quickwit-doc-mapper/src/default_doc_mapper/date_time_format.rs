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

use std::fmt::Display;
use std::str::FromStr;

use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};
use time_fmt::format::time_format_item::parse_to_format_item;

/// Specifies the datetime and unix timestamp formats to use when parsing date strings.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum DateTimeFormat {
    ISO8601,
    RFC2822,
    RCF3339,
    Strftime {
        strftime_format: String,
        with_timezone: bool,
    },
    TimestampSecs,
    TimestampMillis,
    TimestampMicros,
    TimestampNanos,
}

impl DateTimeFormat {
    pub fn as_str(&self) -> &str {
        match self {
            DateTimeFormat::ISO8601 => "iso8601",
            DateTimeFormat::RFC2822 => "rfc2822",
            DateTimeFormat::RCF3339 => "rfc3339",
            DateTimeFormat::Strftime {
                strftime_format, ..
            } => strftime_format,
            DateTimeFormat::TimestampSecs => "unix_ts_secs",
            DateTimeFormat::TimestampMillis => "unix_ts_millis",
            DateTimeFormat::TimestampMicros => "unix_ts_micros",
            DateTimeFormat::TimestampNanos => "unix_ts_nanos",
        }
    }

    pub fn is_timestamp(&self) -> bool {
        matches!(
            self,
            DateTimeFormat::TimestampSecs
                | DateTimeFormat::TimestampMillis
                | DateTimeFormat::TimestampMicros
                | DateTimeFormat::TimestampNanos
        )
    }
}

impl Display for DateTimeFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for DateTimeFormat {
    type Err = String;

    fn from_str(date_time_format_str: &str) -> Result<Self, Self::Err> {
        let date_time_format = match date_time_format_str.to_lowercase().as_str() {
            "iso8601" => DateTimeFormat::ISO8601,
            "rfc2822" => DateTimeFormat::RFC2822,
            "rfc3339" => DateTimeFormat::RCF3339,
            "unix_ts_secs" => DateTimeFormat::TimestampSecs,
            "unix_ts_millis" => DateTimeFormat::TimestampMillis,
            "unix_ts_micros" => DateTimeFormat::TimestampMicros,
            "unix_ts_nanos" => DateTimeFormat::TimestampNanos,
            _ => {
                // Validate the format.
                let _ = parse_to_format_item(date_time_format_str).map_err(|err| {
                    format!("Invalid format specification `{date_time_format_str}`. Error: {err}.")
                })?;
                DateTimeFormat::Strftime {
                    strftime_format: date_time_format_str.to_string(),
                    with_timezone: date_time_format_str.contains("%z"),
                }
            }
        };
        Ok(date_time_format)
    }
}

impl Serialize for DateTimeFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for DateTimeFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let date_time_format_str = String::deserialize(deserializer)?;
        let date_time_format =
            DateTimeFormat::from_str(&date_time_format_str).map_err(D::Error::custom)?;
        Ok(date_time_format)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date_time_format_ser() {
        let date_time_formats_json = serde_json::to_value(&[
            DateTimeFormat::ISO8601,
            DateTimeFormat::RFC2822,
            DateTimeFormat::RCF3339,
            DateTimeFormat::TimestampSecs,
            DateTimeFormat::TimestampMillis,
            DateTimeFormat::TimestampMicros,
            DateTimeFormat::TimestampNanos,
        ])
        .unwrap();

        let expected_date_time_formats = serde_json::json!([
            "iso8601",
            "rfc2822",
            "rfc3339",
            "unix_ts_secs",
            "unix_ts_millis",
            "unix_ts_micros",
            "unix_ts_nanos",
        ]);
        assert_eq!(date_time_formats_json, expected_date_time_formats);
    }

    #[test]
    fn test_date_time_format_deser() {
        let date_time_formats_json = r#"
            [
                "iso8601",
                "rfc2822",
                "rfc3339",
                "unix_ts_secs",
                "unix_ts_millis",
                "unix_ts_micros",
                "unix_ts_nanos"
            ]
            "#;
        let date_time_formats: Vec<DateTimeFormat> =
            serde_json::from_str(date_time_formats_json).unwrap();
        let expected_date_time_formats = [
            DateTimeFormat::ISO8601,
            DateTimeFormat::RFC2822,
            DateTimeFormat::RCF3339,
            DateTimeFormat::TimestampSecs,
            DateTimeFormat::TimestampMillis,
            DateTimeFormat::TimestampMicros,
            DateTimeFormat::TimestampNanos,
        ];
        assert_eq!(date_time_formats, &expected_date_time_formats);
    }
}
