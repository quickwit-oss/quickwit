// Copyright (C) 2021 Quickwit, Inc.
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

use std::collections::HashSet;

use chrono::NaiveDate;
use derivative::Derivative;
use serde::de::Error;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tantivy::{DateTimeFormat, DateTimeOptions, DateTimePrecision};
use time::format_description::well_known::{Iso8601, Rfc2822, Rfc3339};
use time::OffsetDateTime;

use super::default_as_true;

/// A struct holding datetime field options.
#[derive(Clone, Serialize, Deserialize, Derivative)]
#[derivative(Debug, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QuickwitDateTimeOptions {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Accepted input format.
    #[serde(default = "default_input_formats")]
    #[serde(serialize_with = "serialize_input_formats")]
    #[serde(deserialize_with = "deserialize_input_formats")]
    pub input_formats: HashSet<DateTimeFormat>,

    /// Internal storage precision.
    /// Used to avoid storing very large numbers when not needed.
    /// This optimizes compression.
    #[serde(default = "default_time_precision")]
    #[serde(serialize_with = "serialize_time_precision")]
    #[serde(deserialize_with = "deserialize_time_precision")]
    pub precision: DateTimePrecision,

    /// DateTime format used in query result.
    #[serde(default = "default_output_format")]
    #[serde(serialize_with = "serialize_input_format")]
    #[serde(deserialize_with = "deserialize_input_format")]
    pub output_format: DateTimeFormat,

    #[serde(default = "default_as_true")]
    pub indexed: bool,

    #[serde(default = "default_as_true")]
    pub stored: bool,

    #[serde(default = "default_as_true")]
    pub fast: bool,

    /// Tantivy's DateTimeOptions maintains internally a set of DateTime parsers
    /// created from the input_format and the precision.
    ///
    /// Since we are parsing documents outside of Tantivy,
    /// We need to hold on to one instance in order to perform
    /// DateTime parsing in Quickwit.
    #[serde(skip)]
    #[serde(default)]
    #[derivative(PartialEq = "ignore")]
    #[derivative(Debug = "ignore")]
    parsers_handle: Option<DateTimeOptions>,
}

impl Default for QuickwitDateTimeOptions {
    fn default() -> Self {
        Self {
            description: None,
            input_formats: default_input_formats(),
            precision: default_time_precision(),
            output_format: default_output_format(),
            indexed: true,
            stored: true,
            fast: true,
            parsers_handle: None,
        }
    }
}

impl QuickwitDateTimeOptions {
    pub fn into_with_parsers_handle_initialized(self) -> QuickwitDateTimeOptions {
        let parsers_handle = DateTimeOptions::default()
            .set_input_formats(self.input_formats.clone())
            .set_precision(self.precision);
        QuickwitDateTimeOptions {
            description: self.description,
            input_formats: self.input_formats,
            precision: self.precision,
            output_format: self.output_format,
            indexed: self.indexed,
            stored: self.stored,
            fast: self.fast,
            parsers_handle: Some(parsers_handle),
        }
    }

    pub fn parse_string(&self, value: String) -> Result<OffsetDateTime, String> {
        self.parsers_handle
            .as_ref()
            .ok_or("err".to_string())
            .map(|opts| opts.parse_string(value))?
    }

    pub fn parse_number(&self, value: i64) -> Result<OffsetDateTime, String> {
        self.parsers_handle
            .as_ref()
            .ok_or("err".to_string())
            .map(|opts| opts.parse_number(value))?
    }
}

pub(super) fn serialize_time_precision<S>(
    precision: &DateTimePrecision,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match precision {
        DateTimePrecision::Seconds => serializer.serialize_str("secs"),
        DateTimePrecision::Milliseconds => serializer.serialize_str("millis"),
        DateTimePrecision::Microseconds => serializer.serialize_str("micros"),
        DateTimePrecision::Nanoseconds => serializer.serialize_str("nanos"),
    }
}

pub(super) fn deserialize_time_precision<'de, D>(
    deserializer: D,
) -> Result<DateTimePrecision, D::Error>
where D: Deserializer<'de> {
    let time_precision: String = Deserialize::deserialize(deserializer)?;
    match time_precision.as_str() {
        "secs" => Ok(DateTimePrecision::Seconds),
        "millis" => Ok(DateTimePrecision::Milliseconds),
        "micros" => Ok(DateTimePrecision::Microseconds),
        "nanos" => Ok(DateTimePrecision::Nanoseconds),
        unknown => Err(D::Error::custom(format!(
            "Unknown precision value `{}` specified.",
            unknown
        ))),
    }
}

fn date_time_format_to_string(format: &DateTimeFormat) -> String {
    let format_str = match format {
        DateTimeFormat::RCF3339 => "rfc3339",
        DateTimeFormat::RFC2822 => "rfc2822",
        DateTimeFormat::ISO8601 => "iso8601",
        DateTimeFormat::Strftime(format) => format.as_str(),
        DateTimeFormat::Timestamp(precision) => match precision {
            DateTimePrecision::Seconds => "unix_ts_secs",
            DateTimePrecision::Milliseconds => "unix_ts_millis",
            DateTimePrecision::Microseconds => "unix_ts_micros",
            DateTimePrecision::Nanoseconds => "unix_ts_nanos",
        },
    };
    format_str.to_string()
}

fn date_time_format_from_string(format_str: String) -> DateTimeFormat {
    match format_str.to_lowercase().as_str() {
        "rfc3339" => DateTimeFormat::RCF3339,
        "rfc2822" => DateTimeFormat::RFC2822,
        "iso8601" => DateTimeFormat::ISO8601,
        "unix_ts_secs" => DateTimeFormat::Timestamp(DateTimePrecision::Seconds),
        "unix_ts_millis" => DateTimeFormat::Timestamp(DateTimePrecision::Milliseconds),
        "unix_ts_micros" => DateTimeFormat::Timestamp(DateTimePrecision::Microseconds),
        "unix_ts_nanos" => DateTimeFormat::Timestamp(DateTimePrecision::Nanoseconds),
        _ => DateTimeFormat::Strftime(format_str),
    }
}

pub(super) fn serialize_input_format<S>(
    input_format: &DateTimeFormat,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let format_str = date_time_format_to_string(input_format);
    serializer.serialize_str(&format_str)
}

pub(super) fn deserialize_input_format<'de, D>(
    deserializer: D,
) -> Result<DateTimeFormat, D::Error>
where D: Deserializer<'de> {
    let format_str = String::deserialize(deserializer)?;
    Ok(date_time_format_from_string(format_str))
}

pub(super) fn serialize_input_formats<S>(
    input_formats: &HashSet<DateTimeFormat>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut seq = serializer.serialize_seq(Some(input_formats.len()))?;
    for input_format in input_formats {
        let format_str = date_time_format_to_string(input_format);
        seq.serialize_element(&format_str)?;
    }
    seq.end()
}

pub(super) fn deserialize_input_formats<'de, D>(
    deserializer: D,
) -> Result<HashSet<DateTimeFormat>, D::Error>
where D: Deserializer<'de> {
    let date_formats = Vec::<String>::deserialize(deserializer)?
        .into_iter()
        .map(date_time_format_from_string)
        .collect();
    Ok(date_formats)
}

fn default_input_formats() -> HashSet<DateTimeFormat> {
    let mut input_formats = HashSet::new();
    input_formats.insert(DateTimeFormat::ISO8601);
    input_formats.insert(DateTimeFormat::Timestamp(DateTimePrecision::Milliseconds));
    input_formats
}

fn default_time_precision() -> DateTimePrecision {
    DateTimePrecision::Milliseconds
}

fn default_output_format() -> DateTimeFormat {
    DateTimeFormat::ISO8601
}

/// Converts a timestamp to displayable date_time in available formats.
pub fn timestamp_to_datetime_str(
    timestamp: i64,
    precision: &DateTimePrecision,
    format: &DateTimeFormat,
) -> Result<String, String> {
    let date_time = match precision {
        DateTimePrecision::Seconds => OffsetDateTime::from_unix_timestamp(timestamp),
        DateTimePrecision::Milliseconds => {
            OffsetDateTime::from_unix_timestamp_nanos((timestamp as i128) * 1_000_000)
        }
        DateTimePrecision::Microseconds => {
            OffsetDateTime::from_unix_timestamp_nanos((timestamp as i128) * 1000)
        }
        DateTimePrecision::Nanoseconds => {
            OffsetDateTime::from_unix_timestamp_nanos(timestamp as i128)
        }
    }
    .map_err(|error| error.to_string())?;

    match format {
        DateTimeFormat::RCF3339 => date_time
            .format(&Rfc3339)
            .map_err(|error| error.to_string()),
        DateTimeFormat::RFC2822 => date_time
            .format(&Rfc2822)
            .map_err(|error| error.to_string()),
        DateTimeFormat::ISO8601 => date_time
            .format(&Iso8601::DEFAULT)
            .map_err(|error| error.to_string()),
        DateTimeFormat::Strftime(str_fmt) => {
            let date = date_time.to_calendar_date();
            let time = date_time.to_hms_nano();
            NaiveDate::from_ymd(date.0, date.1 as u32, date.2 as u32)
                .and_hms_nano_opt(time.0 as u32, time.1 as u32, time.2 as u32, time.3)
                .ok_or_else(|| "Couldn't create NaiveDate from OffsetDateTime".to_string())
                .map(|datetime| datetime.format(str_fmt).to_string())
        }
        DateTimeFormat::Timestamp(_) => Ok(timestamp.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    use chrono_tz::Tz;
    use tantivy::schema::Cardinality;
    use tantivy::DateTimePrecision;
    use time::macros::{date, time};

    use super::DateTimeFormat;
    use crate::default_doc_mapper::date_time_type::QuickwitDateTimeOptions;
    use crate::default_doc_mapper::FieldMappingType;
    use crate::FieldMappingEntry;

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
                "precision": "millis",
                "output_format": "rfc3339",
                "indexed": true,
                "fast": true,
                "stored": false
            }
            "#,
        )
        .unwrap();
        assert_eq!(&field_mapping_entry.name, "updated_at");

        let mut input_formats = HashSet::new();
        input_formats.insert(DateTimeFormat::RCF3339);
        input_formats.insert(DateTimeFormat::RFC2822);
        input_formats.insert(DateTimeFormat::Timestamp(DateTimePrecision::Milliseconds));
        input_formats.insert(DateTimeFormat::Strftime("%Y %m %d %H:%M:%S %z".to_string()));

        let expected_dt_opts = QuickwitDateTimeOptions {
            description: Some("When was the record updated.".to_string()),
            input_formats,
            precision: DateTimePrecision::Milliseconds,
            output_format: DateTimeFormat::RCF3339,
            indexed: true,
            fast: true,
            stored: false,
            parsers_handle: None,
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
                "precision": "millis",
                "output_format": "iso8601",
                "indexed": true,
                "fast": true,
                "stored": true
            })
        );
    }

    #[test]
    fn test_deserialize_datetime_mapping_with_wrong_options() {
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
             `description`, `input_formats`, `precision`, `output_format`, `indexed`, `stored`, \
             `fast`"
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
            "Error while parsing field `updated_at`: Unknown precision value `hours` specified."
        );
    }
}
