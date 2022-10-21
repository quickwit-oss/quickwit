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

use indexmap::IndexSet;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as JsonValue;
use tantivy::schema::Value as TantivyValue;
use tantivy::DatePrecision as DateTimePrecision;

use super::date_time_format::DateTimeFormat;
use super::date_time_parsing::{parse_date_time, parse_timestamp};
use super::default_as_true;

/// A struct holding DateTime field options.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuickwitDateTimeOptions {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Accepted input formats.
    #[serde(default)]
    pub input_formats: InputFormats,

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
            input_formats: InputFormats::default(),
            precision: DateTimePrecision::default(),
            indexed: true,
            stored: true,
            fast: false,
        }
    }
}

impl QuickwitDateTimeOptions {
    pub(crate) fn parse_json(&self, json_value: JsonValue) -> Result<TantivyValue, String> {
        let date_time = match json_value {
            JsonValue::Number(number) => {
                let timestamp = number.as_i64().ok_or_else(|| {
                    format!("Failed to parse datetime. Expected an integer, got `{number:?}`.")
                })?;
                parse_timestamp(timestamp)?
            }
            JsonValue::String(date_time_str) => {
                parse_date_time(&date_time_str, &self.input_formats.0)?
            }
            _ => {
                return Err(format!(
                    "Failed to parse datetime. Expected an integer or a string, got \
                     `{json_value}`."
                ))
            }
        };
        Ok(TantivyValue::Date(date_time))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InputFormats(Vec<DateTimeFormat>);

impl Default for InputFormats {
    fn default() -> Self {
        Self(vec![DateTimeFormat::RCF3339, DateTimeFormat::Timestamp])
    }
}

impl<'de> Deserialize<'de> for InputFormats {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let date_time_formats = IndexSet::<DateTimeFormat>::deserialize(deserializer)?;

        if date_time_formats.is_empty() {
            return Ok(InputFormats::default());
        }
        Ok(InputFormats(date_time_formats.into_iter().collect()))
    }
}

#[cfg(test)]
mod tests {

    use tantivy::schema::Cardinality;
    use time::macros::datetime;

    use super::*;
    use crate::default_doc_mapper::FieldMappingType;
    use crate::FieldMappingEntry;

    #[test]
    fn test_date_time_options_single_value_deser() {
        let field_mapping_entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "updated_at",
                "type": "datetime",
                "description": "When the record was last updated.",
                "input_formats": [
                    "rfc3339"
                ],
                "precision": "milliseconds",
                "indexed": true,
                "fast": true,
                "stored": false
            }
            "#,
        )
        .unwrap();

        assert_eq!(field_mapping_entry.name, "updated_at");

        let date_time_options = match field_mapping_entry.mapping_type {
            FieldMappingType::DateTime(date_time_options, Cardinality::SingleValue) => {
                date_time_options
            }
            _ => panic!("Expected a date time field mapping."),
        };
        let expected_input_formats = InputFormats(vec![DateTimeFormat::RCF3339]);
        let expected_date_time_options = QuickwitDateTimeOptions {
            description: Some("When the record was last updated.".to_string()),
            input_formats: expected_input_formats,
            precision: DateTimePrecision::Milliseconds,
            indexed: true,
            fast: true,
            stored: false,
        };
        assert_eq!(date_time_options, expected_date_time_options);
    }

    #[test]
    fn test_date_time_options_multi_values_deser() {
        let field_mapping_entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "updated_at",
                "type": "array<datetime>",
                "description": "When the record was last updated.",
                "input_formats": [
                    "rfc3339"
                ],
                "precision": "milliseconds",
                "indexed": true,
                "fast": true,
                "stored": false
            }
            "#,
        )
        .unwrap();

        assert_eq!(field_mapping_entry.name, "updated_at");

        let date_time_options = match field_mapping_entry.mapping_type {
            FieldMappingType::DateTime(date_time_options, Cardinality::MultiValues) => {
                date_time_options
            }
            _ => panic!("Expected a date time field mapping."),
        };
        let expected_input_formats = InputFormats(vec![DateTimeFormat::RCF3339]);
        let expected_date_time_options = QuickwitDateTimeOptions {
            description: Some("When the record was last updated.".to_string()),
            input_formats: expected_input_formats,
            precision: DateTimePrecision::Milliseconds,
            indexed: true,
            fast: true,
            stored: false,
        };
        assert_eq!(date_time_options, expected_date_time_options);
    }

    #[test]
    fn test_date_time_options_deser_default() {
        let date_time_options = serde_json::from_str::<QuickwitDateTimeOptions>("{}").unwrap();
        assert_eq!(date_time_options, QuickwitDateTimeOptions::default());
        assert_eq!(
            date_time_options.input_formats.0,
            &[DateTimeFormat::RCF3339, DateTimeFormat::Timestamp]
        );
        assert_eq!(date_time_options.precision, DateTimePrecision::Seconds);
        assert!(date_time_options.indexed);
        assert!(date_time_options.stored);
        assert!(!date_time_options.fast);
    }

    #[test]
    fn test_date_time_options_deser_denies_unknown_fields() {
        let error = serde_json::from_str::<QuickwitDateTimeOptions>(
            r#"
            {
                "tokenizer": "raw",
            }
            "#,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("unknown field `tokenizer`"));

        let error = serde_json::from_str::<QuickwitDateTimeOptions>(
            r#"
            {
                "precision": "hours",
            }
            "#,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("unknown variant `hours`"));
    }

    #[test]
    fn test_test_date_time_options_ser() {
        let field_mapping_entry = serde_json::from_str::<FieldMappingEntry>(
            r#"
            {
                "name": "updated_at",
                "type": "datetime",
                "description": "When the record was last updated.",
                "input_formats": ["iso8601"]
            }"#,
        )
        .unwrap();

        let entry_json = serde_json::to_value(&field_mapping_entry).unwrap();
        assert_eq!(
            entry_json,
            serde_json::json!({
                "name": "updated_at",
                "type": "datetime",
                "description": "When the record was last updated.",
                "input_formats": ["iso8601"],
                "precision": "seconds",
                "indexed": true,
                "fast": false,
                "stored": true
            })
        );
    }

    #[test]
    fn test_deserialize_input_formats_deser() {
        {
            let input_formats_json = r#"[]"#;
            let input_formats: InputFormats = serde_json::from_str(input_formats_json).unwrap();
            assert_eq!(
                input_formats.0,
                &[DateTimeFormat::RCF3339, DateTimeFormat::Timestamp]
            );
        }
        {
            let input_formats_json = r#"["rfc3339", "unix_timestamp", "unix_timestamp"]"#;
            let input_formats: InputFormats = serde_json::from_str(input_formats_json).unwrap();
            assert_eq!(
                input_formats.0,
                &[DateTimeFormat::RCF3339, DateTimeFormat::Timestamp]
            );
        }
    }

    #[test]
    fn test_deserialize_invalid_input_formats_should_error() {
        {
            let input_formats_json = r#"["rfc3339", "%Y-%Q-%d"]"#;
            let error = serde_json::from_str::<InputFormats>(input_formats_json)
                .unwrap_err()
                .to_string();
            assert!(error.contains("Invalid format specification"));
        }
    }

    #[test]
    fn test_date_time_options_parse_json() {
        let date_time_options = QuickwitDateTimeOptions {
            input_formats: InputFormats(vec![DateTimeFormat::RCF3339, DateTimeFormat::Timestamp]),
            ..Default::default()
        };
        let expected_timestamp = datetime!(2012-05-21 12:09:14 UTC).unix_timestamp();
        {
            let json_value = serde_json::json!("2012-05-21T12:09:14-00:00");
            let tantivy_value = date_time_options.parse_json(json_value).unwrap();
            let date_time = match tantivy_value {
                TantivyValue::Date(date_time) => date_time,
                _ => panic!("Expected a tantivy date time."),
            };
            assert_eq!(date_time.into_timestamp_secs(), expected_timestamp,);
        }
        {
            let json_value = serde_json::json!(expected_timestamp);
            let tantivy_value = date_time_options.parse_json(json_value).unwrap();
            let date_time = match tantivy_value {
                TantivyValue::Date(date_time) => date_time,
                _ => panic!("Expected a tantivy date time."),
            };
            assert_eq!(date_time.into_timestamp_secs(), expected_timestamp,);
        }
        {
            let json_value = serde_json::json!(expected_timestamp as f64);
            date_time_options.parse_json(json_value).unwrap_err();
        }
    }
}
