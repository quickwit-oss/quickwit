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

use std::collections::{HashSet, VecDeque};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use chrono::{NaiveDate, TimeZone};
use chrono_tz::Tz;
use derivative::Derivative;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
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
    pub input_formats: HashSet<DateTimeFormat>,

    /// Default timezone used when the timezone cannot be
    /// extracted or implied from the input.
    #[serde(default = "default_input_timezone")]
    #[serde(serialize_with = "serialize_time_zone")]
    #[serde(deserialize_with = "deserialize_time_zone")]
    pub input_timezone: Tz,

    /// Internal storage precision.
    /// Used to avoid storing very large numbers when not needed.
    /// This optimizes compression.
    #[serde(default = "default_time_precision")]
    pub precision: TimePrecision,

    /// DateTime format used in query result.
    #[serde(default = "default_output_format")]
    pub output_format: DateTimeFormat,

    #[serde(default = "default_as_true")]
    pub indexed: bool,

    #[serde(default = "default_as_true")]
    pub stored: bool,

    #[serde(default = "default_as_true")]
    pub fast: bool,

    /// DateTime inputs parsers
    #[serde(skip)]
    #[derivative(Debug = "ignore")]
    #[derivative(PartialEq = "ignore")]
    parsers: Arc<Mutex<Option<DateTimeParsersHolder>>>,
    // TODO: handle year-month-day indexing params
}

impl Default for QuickwitDateTimeOptions {
    fn default() -> Self {
        Self {
            description: None,
            input_formats: default_input_formats(),
            input_timezone: default_input_timezone(),
            precision: default_time_precision(),
            output_format: default_output_format(),
            indexed: true,
            stored: true,
            fast: true,
            parsers: Arc::new(Mutex::new(None)),
        }
    }
}

impl QuickwitDateTimeOptions {
    pub fn get_parsers(&self) -> Arc<Mutex<Option<DateTimeParsersHolder>>> {
        let mut parser_guard = self.parsers.lock().unwrap();
        if parser_guard.is_none() {
            *parser_guard = Some(DateTimeParsersHolder::from(self.clone()));
        }
        self.parsers.clone()
    }
}

// Parser function types
type StringDateTimeParser = Arc<dyn Fn(&str) -> Result<OffsetDateTime, String> + Sync + Send>;
type NumberDateTimeParser = Arc<dyn Fn(i64) -> Result<OffsetDateTime, String> + Sync + Send>;

/// A struct for holding datetime parsing functions.
#[derive(Clone)]
pub struct DateTimeParsersHolder {
    /// Functions for parsing string input.
    string_parsers: VecDeque<StringDateTimeParser>,
    /// Function for parsing number input as timestamp.
    number_parser: NumberDateTimeParser,
}

impl Default for DateTimeParsersHolder {
    fn default() -> Self {
        Self {
            string_parsers: VecDeque::new(),
            number_parser: make_unix_timestamp_parser(TimePrecision::Seconds),
        }
    }
}

impl DateTimeParsersHolder {
    /// Parses string input.
    pub fn parse_string(&mut self, value: String) -> Result<OffsetDateTime, String> {
        for (index, parser) in self.string_parsers.iter().enumerate() {
            if let Ok(date_time) = parser(&value) {
                // Move successful parser in front of queue.
                self.string_parsers.swap(0, index);
                return Ok(date_time);
            }
        }
        Err(format!(
            "Could not parse datetime `{}` trying all specified parsers.",
            value
        ))
    }

    /// Parses number input.
    pub fn parse_number(&self, value: i64) -> Result<OffsetDateTime, String> {
        (self.number_parser)(value)
    }
}

impl From<QuickwitDateTimeOptions> for DateTimeParsersHolder {
    fn from(opts: QuickwitDateTimeOptions) -> Self {
        let mut parser = DateTimeParsersHolder::default();
        for input_format in opts.input_formats {
            match input_format {
                DateTimeFormat::RCF3339 => {
                    parser.string_parsers.push_back(Arc::new(rfc3339_parser))
                }
                DateTimeFormat::RFC2822 => {
                    parser.string_parsers.push_back(Arc::new(rfc2822_parser))
                }
                DateTimeFormat::ISO8601 => {
                    parser.string_parsers.push_back(Arc::new(iso8601_parser))
                }
                DateTimeFormat::Strftime(str_format) => parser
                    .string_parsers
                    .push_back(make_strftime_parser(str_format, opts.input_timezone)),
                DateTimeFormat::UnixTimestamp(precision) => {
                    parser.number_parser = make_unix_timestamp_parser(precision)
                }
            }
        }
        parser
    }
}

/// Parses datetime strings using RFC3339 formatting.
fn rfc3339_parser(value: &str) -> Result<OffsetDateTime, String> {
    OffsetDateTime::parse(value, &Rfc3339).map_err(|error| error.to_string())
}

/// Parses DateTime strings using RFC2822 formatting.
fn rfc2822_parser(value: &str) -> Result<OffsetDateTime, String> {
    OffsetDateTime::parse(value, &Rfc2822).map_err(|error| error.to_string())
}

/// Parses DateTime strings using default ISO8601 formatting.
/// Examples: 2010-11-21T09:55:06.000000000+02:00, 2010-11-12 9:55:06 +2:00
fn iso8601_parser(value: &str) -> Result<OffsetDateTime, String> {
    OffsetDateTime::parse(value, &Iso8601::DEFAULT).map_err(|error| error.to_string())
}

/// Configures and returns a function for parsing datetime strings
/// using strftime formatting.
fn make_strftime_parser(format: String, default_timezone: Tz) -> StringDateTimeParser {
    Arc::new(move |value: &str| {
        // expect timezone
        let date_time = if format.contains("%z") {
            chrono::DateTime::parse_from_str(value, &format)
                .map_err(|error| error.to_string())
                .map(|date_time| date_time.naive_utc())?
        } else {
            chrono::NaiveDateTime::parse_from_str(value, &format)
                .map_err(|error| error.to_string())
                .map(|date_time| {
                    default_timezone
                        .from_local_datetime(&date_time)
                        .unwrap()
                        .naive_utc()
                })?
        };

        OffsetDateTime::from_unix_timestamp_nanos(date_time.timestamp_nanos() as i128)
            .map_err(|error| error.to_string())
    })
}

/// Configures and returns a function for interpreting numbers
/// as unix timestamp with a precision.
fn make_unix_timestamp_parser(precision: TimePrecision) -> NumberDateTimeParser {
    Arc::new(move |value: i64| {
        let date_time = match precision {
            TimePrecision::Seconds => OffsetDateTime::from_unix_timestamp(value),
            TimePrecision::Milliseconds => {
                OffsetDateTime::from_unix_timestamp_nanos((value as i128) * 1_000_000)
            }
            TimePrecision::Microseconds => {
                OffsetDateTime::from_unix_timestamp_nanos((value as i128) * 1000)
            }
            TimePrecision::Nanoseconds => OffsetDateTime::from_unix_timestamp_nanos(value as i128),
        };
        date_time.map_err(|error| error.to_string())
    })
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum TimePrecision {
    Seconds,
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

impl<'de> Deserialize<'de> for TimePrecision {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let time_precision: String = Deserialize::deserialize(deserializer)?;
        match time_precision.as_str() {
            "secs" => Ok(TimePrecision::Seconds),
            "millis" => Ok(TimePrecision::Milliseconds),
            "micros" => Ok(TimePrecision::Microseconds),
            "nanos" => Ok(TimePrecision::Nanoseconds),
            unknown => Err(D::Error::custom(format!(
                "Unknown precision value `{}` specified.",
                unknown
            ))),
        }
    }
}

impl Serialize for TimePrecision {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match self {
            TimePrecision::Seconds => serializer.serialize_str("secs"),
            TimePrecision::Milliseconds => serializer.serialize_str("millis"),
            TimePrecision::Microseconds => serializer.serialize_str("micros"),
            TimePrecision::Nanoseconds => serializer.serialize_str("nanos"),
        }
    }
}

/// An enum specifying all supported datetime parsing format.
#[derive(Clone, Debug, Eq, Derivative)]
#[derivative(Hash, PartialEq)]
pub enum DateTimeFormat {
    RCF3339,
    RFC2822,
    ISO8601,
    Strftime(String),
    UnixTimestamp(
        #[derivative(PartialEq = "ignore")]
        #[derivative(Hash = "ignore")]
        TimePrecision,
    ),
}

impl<'de> Deserialize<'de> for DateTimeFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let value = String::deserialize(deserializer)?;
        match value.to_lowercase().as_str() {
            "rfc3339" => Ok(DateTimeFormat::RCF3339),
            "rfc2822" => Ok(DateTimeFormat::RFC2822),
            "iso8601" => Ok(DateTimeFormat::ISO8601),
            "unix_ts_secs" => Ok(DateTimeFormat::UnixTimestamp(TimePrecision::Seconds)),
            "unix_ts_millis" => Ok(DateTimeFormat::UnixTimestamp(TimePrecision::Milliseconds)),
            "unix_ts_micros" => Ok(DateTimeFormat::UnixTimestamp(TimePrecision::Microseconds)),
            "unix_ts_nanos" => Ok(DateTimeFormat::UnixTimestamp(TimePrecision::Nanoseconds)),
            _ => Ok(DateTimeFormat::Strftime(value)),
        }
    }
}

impl Serialize for DateTimeFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match self {
            DateTimeFormat::RCF3339 => serializer.serialize_str("rfc3339"),
            DateTimeFormat::RFC2822 => serializer.serialize_str("rfc2822"),
            DateTimeFormat::ISO8601 => serializer.serialize_str("iso8601"),
            DateTimeFormat::Strftime(format) => serializer.serialize_str(format),
            DateTimeFormat::UnixTimestamp(precision) => match precision {
                TimePrecision::Seconds => serializer.serialize_str("unix_ts_secs"),
                TimePrecision::Milliseconds => serializer.serialize_str("unix_ts_millis"),
                TimePrecision::Microseconds => serializer.serialize_str("unix_ts_micros"),
                TimePrecision::Nanoseconds => serializer.serialize_str("unix_ts_nanos"),
            },
        }
    }
}

pub(super) fn deserialize_time_zone<'de, D>(deserializer: D) -> Result<Tz, D::Error>
where D: Deserializer<'de> {
    let time_zone_name: String = Deserialize::deserialize(deserializer)?;
    Tz::from_str(&time_zone_name).map_err(D::Error::custom)
}

pub(super) fn serialize_time_zone<S>(time_zone: &Tz, s: S) -> Result<S::Ok, S::Error>
where S: Serializer {
    s.serialize_str(&time_zone.to_string())
}

fn default_input_formats() -> HashSet<DateTimeFormat> {
    let mut input_formats = HashSet::new();
    input_formats.insert(DateTimeFormat::RCF3339);
    input_formats.insert(DateTimeFormat::UnixTimestamp(TimePrecision::Seconds));
    input_formats
}

fn default_input_timezone() -> Tz {
    Tz::UTC
}

fn default_time_precision() -> TimePrecision {
    TimePrecision::Seconds
}

fn default_output_format() -> DateTimeFormat {
    DateTimeFormat::RCF3339
}

/// Converts a timestamp to displayable date_time in available formats.
pub fn timestamp_to_datetime_str(
    timestamp: i64,
    precision: &TimePrecision,
    format: &DateTimeFormat,
) -> Result<String, String> {
    let date_time = match precision {
        TimePrecision::Seconds => OffsetDateTime::from_unix_timestamp(timestamp),
        TimePrecision::Milliseconds => {
            OffsetDateTime::from_unix_timestamp_nanos((timestamp as i128) * 1_000_000)
        }
        TimePrecision::Microseconds => {
            OffsetDateTime::from_unix_timestamp_nanos((timestamp as i128) * 1000)
        }
        TimePrecision::Nanoseconds => OffsetDateTime::from_unix_timestamp_nanos(timestamp as i128),
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
        DateTimeFormat::UnixTimestamp(_) => Ok(timestamp.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    use chrono_tz::Tz;
    use tantivy::schema::Cardinality;
    use time::macros::{date, time};

    use super::{make_strftime_parser, DateTimeFormat};
    use crate::default_doc_mapper::date_time_type::{
        make_unix_timestamp_parser, QuickwitDateTimeOptions, TimePrecision,
    };
    use crate::default_doc_mapper::FieldMappingType;
    use crate::FieldMappingEntry;

    #[test]
    fn test_strftime_format_cannot_be_duplicated() {
        let mut formats = HashSet::new();
        formats.insert(DateTimeFormat::Strftime(
            "%a %b %d %H:%M:%S %z %Y".to_string(),
        ));
        formats.insert(DateTimeFormat::Strftime("%Y %m %d".to_string()));
        formats.insert(DateTimeFormat::Strftime(
            "%a %b %d %H:%M:%S %z %Y".to_string(),
        ));
        formats.insert(DateTimeFormat::UnixTimestamp(TimePrecision::Microseconds));
        assert_eq!(formats.len(), 3);
    }

    #[test]
    fn test_only_one_unix_ts_format_can_be_added() {
        let mut formats = HashSet::new();
        formats.insert(DateTimeFormat::UnixTimestamp(TimePrecision::Seconds));
        formats.insert(DateTimeFormat::UnixTimestamp(TimePrecision::Microseconds));
        formats.insert(DateTimeFormat::UnixTimestamp(TimePrecision::Milliseconds));
        formats.insert(DateTimeFormat::UnixTimestamp(TimePrecision::Nanoseconds));
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
                "input_timezone": "Africa/Lagos",
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
        input_formats.insert(DateTimeFormat::UnixTimestamp(TimePrecision::Milliseconds));
        input_formats.insert(DateTimeFormat::Strftime("%Y %m %d %H:%M:%S %z".to_string()));

        let expected_dt_opts = QuickwitDateTimeOptions {
            description: Some("When was the record updated.".to_string()),
            input_formats,
            input_timezone: Tz::Africa__Lagos,
            precision: TimePrecision::Milliseconds,
            output_format: DateTimeFormat::RCF3339,
            indexed: true,
            fast: true,
            stored: false,
            parsers: Arc::new(Mutex::new(None)),
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
                "description": "When was the record updated."
            }"#,
        )
        .unwrap();

        // re-order the input-formats array
        let mut entry_json = serde_json::to_value(&entry).unwrap();
        let mut formats = entry_json
            .get("input_formats")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|val| val.as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        formats.sort();
        let input_formats = entry_json.get_mut("input_formats").unwrap();
        *input_formats = serde_json::to_value(formats).unwrap();

        assert_eq!(
            entry_json,
            serde_json::json!({
                "name": "updated_at",
                "type": "datetime",
                "description": "When was the record updated.",
                "input_formats": ["rfc3339", "unix_ts_secs"],
                "input_timezone": "UTC",
                "precision": "secs",
                "output_format": "rfc3339",
                "indexed": true,
                "fast": true,
                "stored": true
            })
        );
    }

    // test config errors
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
             `description`, `input_formats`, `input_timezone`, `precision`, `output_format`, \
             `indexed`, `stored`, `fast`"
        );

        assert_eq!(
            serde_json::from_str::<FieldMappingEntry>(
                r#"
            {
                "name": "updated_at",
                "type": "datetime",
                "input_timezone": "Africa/Paris"
            }"#
            )
            .unwrap_err()
            .to_string(),
            "Error while parsing field `updated_at`: 'Africa/Paris' is not a valid timezone"
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

    #[test]
    fn test_strftime_parser() {
        let parse_without_timezone =
            make_strftime_parser("%Y-%m-%d %H:%M:%S".to_string(), Tz::Africa__Lagos);

        let date_time = parse_without_timezone("2012-05-21 12:09:14").unwrap();
        assert_eq!(date_time.date(), date!(2012 - 05 - 21));
        assert_eq!(date_time.time(), time!(11:09:14));

        let parse_with_timezone =
            make_strftime_parser("%Y-%m-%d %H:%M:%S %z".to_string(), Tz::Africa__Lagos);
        let date_time = parse_with_timezone("2012-05-21 12:09:14 -02:00").unwrap();
        assert_eq!(date_time.date(), date!(2012 - 05 - 21));
        assert_eq!(date_time.time(), time!(14:09:14));
    }

    #[test]
    fn test_unix_timestamp_parser() {
        let now = time::OffsetDateTime::now_utc();

        let parse_with_secs = make_unix_timestamp_parser(TimePrecision::Seconds);
        let date_time = parse_with_secs(now.unix_timestamp()).unwrap();
        assert_eq!(date_time.date(), now.date());
        assert_eq!(date_time.time().as_hms(), now.time().as_hms());

        let parse_with_millis = make_unix_timestamp_parser(TimePrecision::Milliseconds);
        let ts_millis = now.unix_timestamp_nanos() / 1_000_000;
        let date_time = parse_with_millis(ts_millis as i64).unwrap();
        assert_eq!(date_time.date(), now.date());
        assert_eq!(date_time.time().as_hms_milli(), now.time().as_hms_milli());

        let parse_with_micros = make_unix_timestamp_parser(TimePrecision::Microseconds);
        let ts_micros = now.unix_timestamp_nanos() / 1000;
        let date_time = parse_with_micros(ts_micros as i64).unwrap();
        assert_eq!(date_time.date(), now.date());
        assert_eq!(date_time.time().as_hms_micro(), now.time().as_hms_micro());

        let parse_with_nanos = make_unix_timestamp_parser(TimePrecision::Nanoseconds);
        let date_time = parse_with_nanos(now.unix_timestamp_nanos() as i64).unwrap();
        assert_eq!(date_time.date(), now.date());
        assert_eq!(date_time.time(), now.time());
    }
}
