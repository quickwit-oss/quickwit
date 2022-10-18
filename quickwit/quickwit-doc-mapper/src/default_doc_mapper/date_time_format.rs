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

use ouroboros::self_referencing;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};
use time::format_description::FormatItem;
use time::{OffsetDateTime, PrimitiveDateTime};
use time_fmt::parse::time_format_item::parse_to_format_item;

/// A date time parser that holds the format specification `Vec<FormatItem>`.
#[self_referencing]
pub struct StrptimeParser {
    format: String,
    with_timezone: bool,
    #[borrows(format)]
    #[covariant]
    items: Vec<FormatItem<'this>>,
}

impl StrptimeParser {
    pub fn make(format_str: &str) -> Result<StrptimeParser, String> {
        StrptimeParser::try_new(
            format_str.to_string(),
            format_str.to_lowercase().contains("%z"),
            |format: &String| {
                parse_to_format_item(format).map_err(|err| {
                    format!("Invalid format specification `{format}`. Error: {err}.")
                })
            },
        )
    }

    pub fn parse_date_time(&self, date_time_str: &str) -> Result<OffsetDateTime, String> {
        if *self.borrow_with_timezone() {
            OffsetDateTime::parse(date_time_str, self.borrow_items()).map_err(|err| err.to_string())
        } else {
            PrimitiveDateTime::parse(date_time_str, self.borrow_items())
                .map(|date_time| date_time.assume_utc())
                .map_err(|err| err.to_string())
        }
    }
}

impl Clone for StrptimeParser {
    fn clone(&self) -> Self {
        // `self.format` is already known to be a valid format.
        Self::make(self.borrow_format()).unwrap()
    }
}

impl PartialEq for StrptimeParser {
    fn eq(&self, other: &Self) -> bool {
        self.borrow_format() == other.borrow_format()
    }
}

impl Eq for StrptimeParser {}

impl std::fmt::Debug for StrptimeParser {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StrptimeParser")
            .field("format", &self.borrow_format())
            .finish()
    }
}

impl std::hash::Hash for StrptimeParser {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.borrow_format().hash(state);
    }
}

/// Specifies the datetime and unix timestamp formats to use when parsing date strings.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum DateTimeFormat {
    ISO8601,
    RFC2822,
    RCF3339,
    Strptime(StrptimeParser),
    Timestamp,
}

impl DateTimeFormat {
    pub fn as_str(&self) -> &str {
        match self {
            DateTimeFormat::ISO8601 => "iso8601",
            DateTimeFormat::RFC2822 => "rfc2822",
            DateTimeFormat::RCF3339 => "rfc3339",
            DateTimeFormat::Strptime(parser) => parser.borrow_format(),
            DateTimeFormat::Timestamp => "unix_timestamp",
        }
    }
}

impl Display for DateTimeFormat {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for DateTimeFormat {
    type Err = String;

    fn from_str(date_time_format_str: &str) -> Result<Self, Self::Err> {
        let date_time_format = match date_time_format_str.to_lowercase().as_str() {
            "iso8601" => DateTimeFormat::ISO8601,
            "rfc2822" => DateTimeFormat::RFC2822,
            "rfc3339" => DateTimeFormat::RCF3339,
            "unix_timestamp" => DateTimeFormat::Timestamp,
            _ => DateTimeFormat::Strptime(StrptimeParser::make(date_time_format_str)?),
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
            DateTimeFormat::Timestamp,
        ])
        .unwrap();

        let expected_date_time_formats =
            serde_json::json!(["iso8601", "rfc2822", "rfc3339", "unix_timestamp",]);
        assert_eq!(date_time_formats_json, expected_date_time_formats);
    }

    #[test]
    fn test_date_time_format_deser() {
        let date_time_formats_json = r#"
            [
                "iso8601",
                "rfc2822",
                "rfc3339",
                "unix_timestamp"
            ]
            "#;
        let date_time_formats: Vec<DateTimeFormat> =
            serde_json::from_str(date_time_formats_json).unwrap();
        let expected_date_time_formats = [
            DateTimeFormat::ISO8601,
            DateTimeFormat::RFC2822,
            DateTimeFormat::RCF3339,
            DateTimeFormat::Timestamp,
        ];
        assert_eq!(date_time_formats, &expected_date_time_formats);
    }
}
