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
use std::fmt::Display;
use std::str::FromStr;

use derivative::Derivative;
use indexmap::IndexSet;
use itertools::Itertools;
use serde::{Deserialize, Deserializer, Serialize};
use tantivy::{DatePrecision as DateTimePrecision, DateTime};
use time::format_description::well_known::{Iso8601, Rfc2822, Rfc3339};
use time::OffsetDateTime;
use unwrap_infallible::UnwrapInfallible;

use super::default_as_true;

/// A struct holding DateTime field options.
#[derive(Clone, Serialize, Deserialize, Derivative)]
#[derivative(Debug, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QuickwitDateTimeOptions {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Accepted input formats.
    #[serde(default = "default_input_formats")]
    pub input_formats: IndexSet<DateTimeFormat>,

    /// Internal storage precision.
    #[serde(default)]
    pub precision: DateTimePrecision,

    #[serde(default = "default_as_true")]
    pub indexed: bool,

    #[serde(default = "default_as_true")]
    pub stored: bool,

    #[serde(default)]
    pub fast: bool,
}

impl Default for QuickwitDateTimeOptions {
    fn default() -> Self {
        Self {
            description: None,
            input_formats: default_input_formats(),
            precision: DateTimePrecision::default(),
            indexed: true,
            stored: true,
            fast: false,
        }
    }
}

impl QuickwitDateTimeOptions {
    pub(crate) fn parse_string(&self, value: String) -> Result<OffsetDateTime, String> {
        for format in self.input_formats.iter() {
            let result = match format {
                DateTimeFormat::RCF3339 => parse_rfc3339(&value),
                DateTimeFormat::RFC2822 => parse_rfc2822(&value),
                DateTimeFormat::ISO8601 => parse_iso8601(&value),
                DateTimeFormat::Strftime(strftime_format) => {
                    parse_strftime(&value, strftime_format)
                }
                _ => continue,
            };
            if result.is_ok() {
                return result;
            }
        }
        Err(format!(
            "Could not parse datetime `{}` using the specified formats `{}`.",
            value,
            self.input_formats
                .iter()
                .map(ToString::to_string)
                .join(", ")
        ))
    }

    pub(crate) fn parse_number(&self, value: i64) -> Result<OffsetDateTime, String> {
        for format in self.input_formats.iter() {
            if let DateTimeFormat::Timestamp(precision) = format {
                return parse_unix_timestamp(value, precision);
            }
        }

        // Use default number parser.
        parse_unix_timestamp(value, &DateTimePrecision::Seconds)
    }
}

/// Parses datetime strings using RFC3339 formatting.
fn parse_rfc3339(value: &str) -> Result<OffsetDateTime, String> {
    OffsetDateTime::parse(value, &Rfc3339).map_err(|error| error.to_string())
}

/// Parses DateTime strings using RFC2822 formatting.
fn parse_rfc2822(value: &str) -> Result<OffsetDateTime, String> {
    OffsetDateTime::parse(value, &Rfc2822).map_err(|error| error.to_string())
}

/// Parses DateTime strings using default ISO8601 formatting.
/// Examples: 2010-11-21T09:55:06.000000000+02:00, 2010-11-12 9:55:06 +2:00
fn parse_iso8601(value: &str) -> Result<OffsetDateTime, String> {
    OffsetDateTime::parse(value, &Iso8601::DEFAULT).map_err(|error| error.to_string())
}

/// Parses DateTime strings using the unix strftime formatting.
fn parse_strftime(value: &str, format: &str) -> Result<OffsetDateTime, String> {
    let date_time = if format.contains("%z") {
        chrono::DateTime::parse_from_str(value, format)
            .map_err(|error| error.to_string())
            .map(|date_time| date_time.naive_utc())?
    } else {
        chrono::NaiveDateTime::parse_from_str(value, format).map_err(|error| error.to_string())?
    };

    OffsetDateTime::from_unix_timestamp_nanos(date_time.timestamp_nanos() as i128)
        .map_err(|error| error.to_string())
}

/// Recognizes numbers as unix timestamp with a precision.
fn parse_unix_timestamp(
    value: i64,
    precision: &DateTimePrecision,
) -> Result<OffsetDateTime, String> {
    let date_time = match precision {
        DateTimePrecision::Seconds => OffsetDateTime::from_unix_timestamp(value),
        DateTimePrecision::Milliseconds => {
            OffsetDateTime::from_unix_timestamp_nanos((value as i128) * 1_000_000)
        }
        DateTimePrecision::Microseconds => {
            OffsetDateTime::from_unix_timestamp_nanos((value as i128) * 1000)
        }
    };
    date_time.map_err(|error| error.to_string())
}

// An enum specifying all supported datetime parsing format.
#[derive(Clone, Debug, Eq, Derivative)]
#[derivative(Hash, PartialEq)]
pub enum DateTimeFormat {
    RCF3339,
    RFC2822,
    ISO8601,
    Strftime(String),
    Timestamp(
        #[derivative(PartialEq = "ignore")]
        #[derivative(Hash = "ignore")]
        DateTimePrecision,
    ),
}

impl DateTimeFormat {
    pub fn as_str(&self) -> &str {
        match self {
            DateTimeFormat::RCF3339 => "rfc3339",
            DateTimeFormat::RFC2822 => "rfc2822",
            DateTimeFormat::ISO8601 => "iso8601",
            DateTimeFormat::Strftime(format) => format,
            DateTimeFormat::Timestamp(DateTimePrecision::Seconds) => "unix_ts_secs",
            DateTimeFormat::Timestamp(DateTimePrecision::Milliseconds) => "unix_ts_millis",
            DateTimeFormat::Timestamp(DateTimePrecision::Microseconds) => "unix_ts_micros",
        }
    }
}

impl Display for DateTimeFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for DateTimeFormat {
    type Err = Infallible;

    fn from_str(format: &str) -> Result<Self, Self::Err> {
        let dt_format = match format.to_lowercase().as_str() {
            "rfc3339" => DateTimeFormat::RCF3339,
            "rfc2822" => DateTimeFormat::RFC2822,
            "iso8601" => DateTimeFormat::ISO8601,
            "unix_ts_secs" => DateTimeFormat::Timestamp(DateTimePrecision::Seconds),
            "unix_ts_millis" => DateTimeFormat::Timestamp(DateTimePrecision::Milliseconds),
            "unix_ts_micros" => DateTimeFormat::Timestamp(DateTimePrecision::Microseconds),
            _ => DateTimeFormat::Strftime(format.to_string()),
        };
        Ok(dt_format)
    }
}

impl<'de> Deserialize<'de> for DateTimeFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let value = String::deserialize(deserializer)?;
        Ok(DateTimeFormat::from_str(&value).unwrap_infallible())
    }
}

impl Serialize for DateTimeFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        serializer.serialize_str(self.as_str())
    }
}

fn default_input_formats() -> IndexSet<DateTimeFormat> {
    let mut input_formats = IndexSet::new();
    input_formats.insert(DateTimeFormat::RCF3339);
    input_formats.insert(DateTimeFormat::Timestamp(DateTimePrecision::default()));
    input_formats
}

/// Converts a timestamp to displayable date time in available formats.
pub(crate) fn timestamp_to_datetime_str(
    timestamp: i64,
    precision: &DateTimePrecision,
) -> Result<String, String> {
    let date_time = match precision {
        DateTimePrecision::Seconds => DateTime::from_timestamp_secs(timestamp),
        DateTimePrecision::Milliseconds => DateTime::from_timestamp_millis(timestamp),
        DateTimePrecision::Microseconds => DateTime::from_timestamp_micros(timestamp),
    };

    date_time
        .into_utc()
        .format(&Rfc3339)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use indexmap::IndexSet;
    use tantivy::schema::Cardinality;
    use tantivy::DatePrecision as DateTimePrecision;
    use time::macros::{date, time};

    use super::DateTimeFormat;
    use crate::default_doc_mapper::date_time_type::{
        parse_strftime, parse_unix_timestamp, QuickwitDateTimeOptions,
    };
    use crate::default_doc_mapper::FieldMappingType;
    use crate::FieldMappingEntry;

    #[test]
    fn test_strftime_format_cannot_be_duplicated() {
        let mut formats = IndexSet::new();
        formats.insert(DateTimeFormat::Strftime(
            "%a %b %d %H:%M:%S %z %Y".to_string(),
        ));
        formats.insert(DateTimeFormat::Strftime("%Y %m %d".to_string()));
        formats.insert(DateTimeFormat::Strftime(
            "%a %b %d %H:%M:%S %z %Y".to_string(),
        ));
        formats.insert(DateTimeFormat::Timestamp(DateTimePrecision::Microseconds));
        assert_eq!(formats.len(), 3);
    }

    #[test]
    fn test_only_one_unix_ts_format_can_be_added() {
        let mut formats = IndexSet::new();
        formats.insert(DateTimeFormat::Timestamp(DateTimePrecision::Seconds));
        formats.insert(DateTimeFormat::Timestamp(DateTimePrecision::Microseconds));
        formats.insert(DateTimeFormat::Timestamp(DateTimePrecision::Milliseconds));
        assert_eq!(formats.len(), 1)
    }

    #[test]
    fn test_quickwit_date_time_options_default_consistent_with_default() {
        let quickwit_date_time_options: QuickwitDateTimeOptions =
            serde_json::from_str("{}").unwrap();
        assert_eq!(
            quickwit_date_time_options,
            QuickwitDateTimeOptions::default()
        );
    }

    #[test]
    fn test_parse_date_time_field_mapping_single_value() {
        let field_mapping_entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "updated_at",
                "type": "datetime",
                "description": "When was the record updated.",
                "input_formats": [
                    "rfc3339", "rfc2822", "unix_ts_millis", "%Y %m %d %H:%M:%S %z"
                ],
                "precision": "milliseconds",
                "indexed": true,
                "fast": true,
                "stored": false
            }
            "#,
        )
        .unwrap();
        assert_eq!(&field_mapping_entry.name, "updated_at");

        let mut input_formats = IndexSet::new();
        input_formats.insert(DateTimeFormat::RCF3339);
        input_formats.insert(DateTimeFormat::RFC2822);
        input_formats.insert(DateTimeFormat::Timestamp(DateTimePrecision::Milliseconds));
        input_formats.insert(DateTimeFormat::Strftime("%Y %m %d %H:%M:%S %z".to_string()));

        let expected_dt_opts = QuickwitDateTimeOptions {
            description: Some("When was the record updated.".to_string()),
            input_formats,
            precision: DateTimePrecision::Milliseconds,
            indexed: true,
            fast: true,
            stored: false,
        };

        assert!(
            matches!(field_mapping_entry.mapping_type, FieldMappingType::DateTime(date_time_opts,
            Cardinality::SingleValue) if date_time_opts == expected_dt_opts)
        );
    }

    #[test]
    fn test_parse_date_time_field_mapping_multi_value() {
        let field_mapping_entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "type": "array<datetime>",
                "name": "update_timeline",
                "stored": false
            }
            "#,
        )
        .unwrap();
        let expected_date_time_options = QuickwitDateTimeOptions {
            stored: false,
            ..Default::default()
        };
        assert_eq!(&field_mapping_entry.name, "update_timeline");
        assert!(
            matches!(field_mapping_entry.mapping_type, FieldMappingType::DateTime(dt_opts,
            Cardinality::MultiValues) if dt_opts == expected_date_time_options)
        );
    }

    #[test]
    fn test_serialize_date_time_field() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "updated_at",
                "type": "datetime",
                "description": "When was the record updated.",
                "input_formats": ["iso8601"]
            }"#,
        )
        .unwrap();

        let entry_json = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            entry_json,
            serde_json::json!({
                "name": "updated_at",
                "type": "datetime",
                "description": "When was the record updated.",
                "input_formats": ["iso8601"],
                "precision": "seconds",
                "indexed": true,
                "fast": false,
                "stored": true
            })
        );
    }

    #[test]
    fn test_deserialize_date_time_mapping_with_wrong_options() {
        assert_eq!(
            serde_json::from_str::<FieldMappingEntry>(
                r#"
            {
                "name": "updated_at",
                "type": "datetime",
                "tokenizer": "basic"
            }"#
            )
            .unwrap_err()
            .to_string(),
            "Error while parsing field `updated_at`: unknown field `tokenizer`, expected one of \
             `description`, `input_formats`, `precision`, `indexed`, `stored`, `fast`"
        );

        assert_eq!(
            serde_json::from_str::<FieldMappingEntry>(
                r#"
            {
                "name": "updated_at",
                "type": "datetime",
                "precision": "hours"
            }"#
            )
            .unwrap_err()
            .to_string(),
            "Error while parsing field `updated_at`: unknown variant `hours`, expected one of \
             `seconds`, `milliseconds`, `microseconds`"
        );
    }

    #[test]
    fn test_default_timestamp_parser_is_added_when_not_specified() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "updated_at",
                "type": "datetime",
                "input_formats": ["iso8601"]
            }"#,
        )
        .unwrap();

        match entry.mapping_type {
            FieldMappingType::DateTime(date_options, _) => {
                let now = time::OffsetDateTime::now_utc();
                let expected = date_options.parse_number(now.unix_timestamp()).unwrap();
                assert_eq!(now.to_calendar_date(), expected.to_calendar_date());
                assert_eq!(now.to_hms(), expected.to_hms());
            }
            _ => panic!("Expected `FieldMappingType::Date` variant."),
        }
    }

    #[test]
    fn test_parse_date_time() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "updated_at",
                "type": "datetime",
                "input_formats": ["iso8601", "rfc3339", "rfc2822", "%Y-%m-%d %H:%M:%S", "unix_ts_millis"]
            }"#,
        )
        .unwrap();

        let date_time_options = match entry.mapping_type {
            FieldMappingType::DateTime(date_time_options, _) => date_time_options,
            _ => panic!("Expected `FieldMappingType::Date` variant."),
        };
        {
            let date_time = date_time_options
                .parse_string("20120521T120914Z".to_string())
                .unwrap();
            assert_eq!(date_time.date(), date!(2012 - 05 - 21));
            assert_eq!(date_time.time(), time!(12:09:14));
        }
        {
            let date_time = date_time_options
                .parse_string("2012-05-21T12:09:14-00:00".to_string())
                .unwrap();
            assert_eq!(date_time.date(), date!(2012 - 05 - 21));
            assert_eq!(date_time.time(), time!(12:09:14));
        }
        {
            let date_time = date_time_options
                .parse_string("Mon, 21 May 2012 12:09:14 GMT".to_string())
                .unwrap();
            assert_eq!(date_time.date(), date!(2012 - 05 - 21));
            assert_eq!(date_time.time(), time!(12:09:14));
        }
        {
            let date_time = date_time_options
                .parse_string("2012-05-21 12:09:14".to_string())
                .unwrap();
            assert_eq!(date_time.date(), date!(2012 - 05 - 21));
            assert_eq!(date_time.time(), time!(12:09:14));
        }
        {
            let error = date_time_options
                .parse_string("foo".to_string())
                .unwrap_err();
            assert_eq!(
                error,
                "Could not parse datetime `foo` using the specified formats `iso8601, rfc3339, \
                 rfc2822, %Y-%m-%d %H:%M:%S, unix_ts_millis`.",
            );
        }
    }

    #[test]
    fn test_parse_strftime() {
        let date_time = parse_strftime("2012-05-21 12:09:14", "%Y-%m-%d %H:%M:%S").unwrap();
        assert_eq!(date_time.date(), date!(2012 - 05 - 21));
        assert_eq!(date_time.time(), time!(12:09:14));

        let date_time =
            parse_strftime("2012-05-21 12:09:14 -02:00", "%Y-%m-%d %H:%M:%S %z").unwrap();
        assert_eq!(date_time.date(), date!(2012 - 05 - 21));
        assert_eq!(date_time.time(), time!(14:09:14));
    }

    #[test]
    fn test_parse_unix_timestamp() {
        let now = time::OffsetDateTime::now_utc();

        let date_time =
            parse_unix_timestamp(now.unix_timestamp(), &DateTimePrecision::Seconds).unwrap();
        assert_eq!(date_time.date(), now.date());
        assert_eq!(date_time.time().as_hms(), now.time().as_hms());

        let ts_millis = now.unix_timestamp_nanos() / 1_000_000;
        let date_time =
            parse_unix_timestamp(ts_millis as i64, &DateTimePrecision::Milliseconds).unwrap();
        assert_eq!(date_time.date(), now.date());
        assert_eq!(date_time.time().as_hms_milli(), now.time().as_hms_milli());

        let ts_micros = now.unix_timestamp_nanos() / 1000;
        let date_time =
            parse_unix_timestamp(ts_micros as i64, &DateTimePrecision::Microseconds).unwrap();
        assert_eq!(date_time.date(), now.date());
        assert_eq!(date_time.time().as_hms_micro(), now.time().as_hms_micro());
    }
}
