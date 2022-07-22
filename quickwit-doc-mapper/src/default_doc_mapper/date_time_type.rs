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

use std::collections::BTreeSet;
use std::sync::Arc;

use derivative::Derivative;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tantivy::{DatePrecision, DateTime};
use time::format_description::well_known::{Iso8601, Rfc2822, Rfc3339};
use time::OffsetDateTime;

use super::default_as_true;

/// A struct holding DateTime field options.
#[derive(Clone, Serialize, Deserialize, Derivative)]
#[derivative(Debug, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
#[serde(from = "QuickwitDateOptionsDeser")]
pub struct QuickwitDateOptions {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Accepted input formats.
    #[serde(default = "default_input_formats")]
    #[serde(serialize_with = "serialize_input_formats")]
    #[serde(deserialize_with = "deserialize_input_formats")]
    pub input_formats: BTreeSet<DateTimeFormat>,

    /// Internal storage precision.
    #[serde(default)]
    pub precision: DatePrecision,

    #[serde(default = "default_as_true")]
    pub indexed: bool,

    #[serde(default = "default_as_true")]
    pub stored: bool,

    #[serde(default)]
    pub fast: bool,

    /// A handle on DateTime parsing functions.
    #[serde(skip)]
    #[serde(default)]
    #[derivative(PartialEq = "ignore")]
    #[derivative(Debug = "ignore")]
    parsers: DateTimeParsersHolder,
}

impl Default for QuickwitDateOptions {
    fn default() -> Self {
        Self {
            description: None,
            input_formats: default_input_formats(),
            precision: DatePrecision::default(),
            indexed: true,
            stored: true,
            fast: false,
            parsers: DateTimeParsersHolder::default(),
        }
    }
}

impl QuickwitDateOptions {
    pub fn parse_string(&self, value: String) -> Result<OffsetDateTime, String> {
        self.parsers.parse_string(value)
    }

    pub fn parse_number(&self, value: i64) -> Result<OffsetDateTime, String> {
        self.parsers.parse_number(value)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QuickwitDateOptionsDeser {
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default = "default_input_formats")]
    #[serde(deserialize_with = "deserialize_input_formats")]
    pub input_formats: BTreeSet<DateTimeFormat>,
    #[serde(default)]
    pub precision: DatePrecision,
    #[serde(default = "default_as_true")]
    pub indexed: bool,
    #[serde(default = "default_as_true")]
    pub stored: bool,
    #[serde(default)]
    pub fast: bool,
}

impl From<QuickwitDateOptionsDeser> for QuickwitDateOptions {
    fn from(options: QuickwitDateOptionsDeser) -> Self {
        let parsers = DateTimeParsersHolder::new(&options.input_formats);
        QuickwitDateOptions {
            description: options.description,
            input_formats: options.input_formats,
            precision: options.precision,
            indexed: options.indexed,
            stored: options.stored,
            fast: options.fast,
            parsers,
        }
    }
}

// Parser function types
type StringDateTimeParser = Arc<dyn Fn(&str) -> Result<OffsetDateTime, String> + Sync + Send>;
type NumberDateTimeParser = Arc<dyn Fn(i64) -> Result<OffsetDateTime, String> + Sync + Send>;

/// A struct for holding DateTime parsing functions.
#[derive(Clone)]
pub struct DateTimeParsersHolder {
    /// Functions for parsing string input.
    string_parsers: Vec<StringDateTimeParser>,
    /// Function for parsing number input as timestamp.
    number_parser: NumberDateTimeParser,
}

impl Default for DateTimeParsersHolder {
    fn default() -> Self {
        Self::new(&default_input_formats())
    }
}

impl DateTimeParsersHolder {
    fn new(input_formats: &BTreeSet<DateTimeFormat>) -> Self {
        let mut parser = Self {
            string_parsers: Vec::new(),
            number_parser: make_unix_timestamp_parser(DatePrecision::default()),
        };
        for input_format in input_formats {
            match input_format {
                DateTimeFormat::RCF3339 => parser.string_parsers.push(Arc::new(rfc3339_parser)),
                DateTimeFormat::RFC2822 => parser.string_parsers.push(Arc::new(rfc2822_parser)),
                DateTimeFormat::ISO8601 => parser.string_parsers.push(Arc::new(iso8601_parser)),
                DateTimeFormat::Strftime(str_format) => parser
                    .string_parsers
                    .push(make_strftime_parser(str_format.clone())),
                DateTimeFormat::Timestamp(precision) => {
                    parser.number_parser = make_unix_timestamp_parser(*precision)
                }
            }
        }
        parser
    }

    /// Parses string input.
    pub fn parse_string(&self, value: String) -> Result<OffsetDateTime, String> {
        for parser in self.string_parsers.iter() {
            if let Ok(date_time) = parser(&value) {
                return Ok(date_time);
            }
        }
        Err(format!(
            "Could not parse datetime `{}` using all specified formats.",
            value
        ))
    }

    /// Parses number input.
    pub fn parse_number(&self, value: i64) -> Result<OffsetDateTime, String> {
        (self.number_parser)(value)
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
fn make_strftime_parser(format: String) -> StringDateTimeParser {
    Arc::new(move |value: &str| {
        let date_time = chrono::DateTime::parse_from_str(value, &format)
            .map_err(|error| error.to_string())
            .map(|date_time| date_time.naive_utc())?;

        OffsetDateTime::from_unix_timestamp_nanos(date_time.timestamp_nanos() as i128)
            .map_err(|error| error.to_string())
    })
}

/// Configures and returns a function for interpreting numbers
/// as unix timestamp with a precision.
fn make_unix_timestamp_parser(precision: DatePrecision) -> NumberDateTimeParser {
    Arc::new(move |value: i64| {
        let date_time = match precision {
            DatePrecision::Seconds => OffsetDateTime::from_unix_timestamp(value),
            DatePrecision::Milliseconds => {
                OffsetDateTime::from_unix_timestamp_nanos((value as i128) * 1_000_000)
            }
            DatePrecision::Microseconds => {
                OffsetDateTime::from_unix_timestamp_nanos((value as i128) * 1000)
            }
        };
        date_time.map_err(|error| error.to_string())
    })
}

// An enum specifying all supported datetime parsing format.
#[derive(Clone, Debug, Eq, Ord, PartialOrd, Derivative)]
#[derivative(Hash, PartialEq)]
pub enum DateTimeFormat {
    RCF3339,
    RFC2822,
    ISO8601,
    Strftime(String),
    Timestamp(
        #[derivative(PartialEq = "ignore")]
        #[derivative(Hash = "ignore")]
        DatePrecision,
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
            "unix_ts_secs" => Ok(DateTimeFormat::Timestamp(DatePrecision::Seconds)),
            "unix_ts_millis" => Ok(DateTimeFormat::Timestamp(DatePrecision::Milliseconds)),
            "unix_ts_micros" => Ok(DateTimeFormat::Timestamp(DatePrecision::Microseconds)),
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
            DateTimeFormat::Timestamp(precision) => match precision {
                DatePrecision::Seconds => serializer.serialize_str("unix_ts_secs"),
                DatePrecision::Milliseconds => serializer.serialize_str("unix_ts_millis"),
                DatePrecision::Microseconds => serializer.serialize_str("unix_ts_micros"),
            },
        }
    }
}

fn date_time_format_to_string(format: &DateTimeFormat) -> String {
    let format_str = match format {
        DateTimeFormat::RCF3339 => "rfc3339",
        DateTimeFormat::RFC2822 => "rfc2822",
        DateTimeFormat::ISO8601 => "iso8601",
        DateTimeFormat::Strftime(format) => format.as_str(),
        DateTimeFormat::Timestamp(precision) => match precision {
            DatePrecision::Seconds => "unix_ts_secs",
            DatePrecision::Milliseconds => "unix_ts_millis",
            DatePrecision::Microseconds => "unix_ts_micros",
        },
    };
    format_str.to_string()
}

fn date_time_format_from_string(format_str: String) -> DateTimeFormat {
    match format_str.to_lowercase().as_str() {
        "rfc3339" => DateTimeFormat::RCF3339,
        "rfc2822" => DateTimeFormat::RFC2822,
        "iso8601" => DateTimeFormat::ISO8601,
        "unix_ts_secs" => DateTimeFormat::Timestamp(DatePrecision::Seconds),
        "unix_ts_millis" => DateTimeFormat::Timestamp(DatePrecision::Milliseconds),
        "unix_ts_micros" => DateTimeFormat::Timestamp(DatePrecision::Microseconds),
        _ => DateTimeFormat::Strftime(format_str),
    }
}

pub(super) fn serialize_input_formats<S>(
    input_formats: &BTreeSet<DateTimeFormat>,
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
) -> Result<BTreeSet<DateTimeFormat>, D::Error>
where D: Deserializer<'de> {
    let date_formats = Vec::<String>::deserialize(deserializer)?
        .into_iter()
        .map(date_time_format_from_string)
        .collect();
    Ok(date_formats)
}

fn default_input_formats() -> BTreeSet<DateTimeFormat> {
    let mut input_formats = BTreeSet::new();
    input_formats.insert(DateTimeFormat::RCF3339);
    input_formats.insert(DateTimeFormat::Timestamp(DatePrecision::default()));
    input_formats
}

/// Converts a timestamp to displayable date_time in available formats.
pub fn timestamp_to_datetime_str(
    timestamp: i64,
    precision: &DatePrecision,
) -> Result<String, String> {
    let date_time = match precision {
        DatePrecision::Seconds => DateTime::from_timestamp_secs(timestamp),
        DatePrecision::Milliseconds => DateTime::from_timestamp_millis(timestamp),
        DatePrecision::Microseconds => DateTime::from_timestamp_micros(timestamp),
    };

    date_time
        .into_utc()
        .format(&Rfc3339)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use tantivy::schema::Cardinality;
    use tantivy::DatePrecision;

    use super::DateTimeFormat;
    use crate::default_doc_mapper::date_time_type::{DateTimeParsersHolder, QuickwitDateOptions};
    use crate::default_doc_mapper::FieldMappingType;
    use crate::FieldMappingEntry;

    #[test]
    fn test_quickwit_date_time_options_default_consistent_with_default() {
        let quickwit_date_time_options: QuickwitDateOptions = serde_json::from_str("{}").unwrap();
        assert_eq!(quickwit_date_time_options, QuickwitDateOptions::default());
    }

    #[test]
    fn test_parse_date_time_field_mapping_single_value() {
        let field_mapping_entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "updated_at",
                "type": "date",
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

        let mut input_formats = BTreeSet::new();
        input_formats.insert(DateTimeFormat::RCF3339);
        input_formats.insert(DateTimeFormat::RFC2822);
        input_formats.insert(DateTimeFormat::Timestamp(DatePrecision::Milliseconds));
        input_formats.insert(DateTimeFormat::Strftime("%Y %m %d %H:%M:%S %z".to_string()));

        let expected_dt_opts = QuickwitDateOptions {
            description: Some("When was the record updated.".to_string()),
            input_formats,
            precision: DatePrecision::Milliseconds,
            indexed: true,
            fast: true,
            stored: false,
            parsers: DateTimeParsersHolder::default(),
        };

        assert!(
            matches!(field_mapping_entry.mapping_type, FieldMappingType::Date(date_time_opts,
            Cardinality::SingleValue) if date_time_opts == expected_dt_opts)
        );
    }

    #[test]
    fn test_parse_date_time_field_mapping_multi_value() {
        let field_mapping_entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "type": "array<date>",
                "name": "update_timeline",
                "stored": false
            }
            "#,
        )
        .unwrap();
        let expected_date_time_options = QuickwitDateOptions {
            stored: false,
            ..Default::default()
        };
        assert_eq!(&field_mapping_entry.name, "update_timeline");
        assert!(
            matches!(field_mapping_entry.mapping_type, FieldMappingType::Date(dt_opts,
            Cardinality::MultiValues) if dt_opts == expected_date_time_options)
        );
    }

    #[test]
    fn test_serialize_date_time_field() {
        let entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "updated_at",
                "type": "date",
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
                "type": "date",
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
    fn test_deserialize_datetime_mapping_with_wrong_options() {
        assert_eq!(
            serde_json::from_str::<FieldMappingEntry>(
                r#"
            {
                "name": "updated_at",
                "type": "date",
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
                "type": "date",
                "precision": "hours"
            }"#
            )
            .unwrap_err()
            .to_string(),
            "Error while parsing field `updated_at`: unknown variant `hours`, expected one of \
             `seconds`, `milliseconds`, `microseconds`"
        );
    }
}
