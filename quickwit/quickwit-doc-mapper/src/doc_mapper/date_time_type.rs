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

use indexmap::IndexSet;
use quickwit_common::true_fn;
use quickwit_datetime::{DateTimeInputFormat, DateTimeOutputFormat, TantivyDateTime};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as JsonValue;
use tantivy::schema::{DateTimePrecision, OwnedValue as TantivyValue};

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

    /// Output format
    #[serde(default)]
    pub output_format: DateTimeOutputFormat,

    /// Internal storage precision.
    #[serde(default)]
    #[serde(alias = "precision")]
    pub fast_precision: DateTimePrecision,

    #[serde(default = "true_fn")]
    pub indexed: bool,

    #[serde(default = "true_fn")]
    pub stored: bool,

    #[serde(default)]
    pub fast: bool,
}

impl Default for QuickwitDateTimeOptions {
    fn default() -> Self {
        Self {
            description: None,
            input_formats: InputFormats::default(),
            output_format: DateTimeOutputFormat::default(),
            fast_precision: DateTimePrecision::default(),
            indexed: true,
            stored: true,
            fast: false,
        }
    }
}

impl QuickwitDateTimeOptions {
    pub(crate) fn validate_json(
        &self,
        json_value: &serde_json_borrow::Value,
    ) -> Result<(), String> {
        match json_value {
            serde_json_borrow::Value::Number(timestamp) => {
                // `.as_f64()` actually converts floats to integers, so we must check for integers
                // first.
                if let Some(timestamp_i64) = timestamp.as_i64() {
                    quickwit_datetime::parse_timestamp_int(timestamp_i64, &self.input_formats.0)?;
                    Ok(())
                } else if let Some(timestamp_f64) = timestamp.as_f64() {
                    quickwit_datetime::parse_timestamp_float(timestamp_f64, &self.input_formats.0)?;
                    Ok(())
                } else {
                    Err(format!(
                        "failed to convert timestamp to f64 ({:?}). this should never happen",
                        serde_json::Number::from(*timestamp)
                    ))
                }
            }
            serde_json_borrow::Value::Str(date_time_str) => {
                quickwit_datetime::parse_date_time_str(date_time_str, &self.input_formats.0)?;
                Ok(())
            }
            _ => Err(format!(
                "failed to parse datetime: expected a float, integer, or string, got \
                 `{json_value}`"
            )),
        }
    }

    pub(crate) fn parse_json(&self, json_value: &JsonValue) -> Result<TantivyValue, String> {
        let date_time = match json_value {
            JsonValue::Number(timestamp) => {
                // `.as_f64()` actually converts floats to integers, so we must check for integers
                // first.
                if let Some(timestamp_i64) = timestamp.as_i64() {
                    quickwit_datetime::parse_timestamp_int(timestamp_i64, &self.input_formats.0)?
                } else if let Some(timestamp_f64) = timestamp.as_f64() {
                    quickwit_datetime::parse_timestamp_float(timestamp_f64, &self.input_formats.0)?
                } else {
                    return Err(format!(
                        "failed to parse datetime `{timestamp:?}`: value is larger than i64::MAX",
                    ));
                }
            }
            JsonValue::String(date_time_str) => {
                quickwit_datetime::parse_date_time_str(date_time_str, &self.input_formats.0)?
            }
            _ => {
                return Err(format!(
                    "failed to parse datetime: expected a float, integer, or string, got \
                     `{json_value}`"
                ));
            }
        };
        Ok(TantivyValue::Date(date_time))
    }

    pub(crate) fn reparse_tantivy_value(
        &self,
        tantivy_value: &TantivyValue,
    ) -> Option<TantivyDateTime> {
        match tantivy_value {
            TantivyValue::Date(date) => Some(*date),
            TantivyValue::Str(date_time_str) => {
                quickwit_datetime::parse_date_time_str(date_time_str, &self.input_formats.0).ok()
            }
            TantivyValue::U64(timestamp_u64) => {
                let timestamp_i64 = (*timestamp_u64).try_into().ok()?;
                quickwit_datetime::parse_timestamp_int(timestamp_i64, &self.input_formats.0).ok()
            }
            TantivyValue::I64(timestamp_i64) => {
                quickwit_datetime::parse_timestamp_int(*timestamp_i64, &self.input_formats.0).ok()
            }
            TantivyValue::F64(timestamp_f64) => {
                quickwit_datetime::parse_timestamp_float(*timestamp_f64, &self.input_formats.0).ok()
            }
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InputFormats(Vec<DateTimeInputFormat>);

impl Default for InputFormats {
    fn default() -> Self {
        Self(vec![
            DateTimeInputFormat::Rfc3339,
            DateTimeInputFormat::Timestamp,
        ])
    }
}

impl<'de> Deserialize<'de> for InputFormats {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let date_time_formats = IndexSet::<DateTimeInputFormat>::deserialize(deserializer)?;

        if date_time_formats.is_empty() {
            return Ok(InputFormats::default());
        }
        Ok(InputFormats(date_time_formats.into_iter().collect()))
    }
}

#[cfg(test)]
mod tests {

    use time::macros::datetime;

    use super::*;
    use crate::doc_mapper::FieldMappingType;
    use crate::{Cardinality, FieldMappingEntry};

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
                "fast_precision": "milliseconds",
                "indexed": true,
                "fast": true,
                "stored": false
            }
            "#,
        )
        .unwrap();

        assert_eq!(field_mapping_entry.name, "updated_at");

        let date_time_options = match field_mapping_entry.mapping_type {
            FieldMappingType::DateTime(date_time_options, Cardinality::SingleValued) => {
                date_time_options
            }
            _ => panic!("Expected a date time field mapping"),
        };
        let expected_input_formats = InputFormats(vec![DateTimeInputFormat::Rfc3339]);
        let expected_date_time_options = QuickwitDateTimeOptions {
            description: Some("When the record was last updated.".to_string()),
            input_formats: expected_input_formats,
            output_format: DateTimeOutputFormat::Rfc3339,
            fast_precision: DateTimePrecision::Milliseconds,
            indexed: true,
            fast: true,
            stored: false,
        };
        assert_eq!(date_time_options, expected_date_time_options);
    }

    #[test]
    fn test_backward_compatibility_after_fast_precision_rename() {
        let field_mapping_entry: FieldMappingEntry = serde_json::from_str(
            r#"
        {
            "name": "updated_at",
            "type": "datetime",
            "description": "When the record was last updated.",
            "input_formats": ["rfc3339"],
            "precision": "milliseconds",
            "indexed": true,
            "fast": true,
            "stored": false
        }
    "#,
        )
        .unwrap();

        if let FieldMappingType::DateTime(date_time_options, _) = field_mapping_entry.mapping_type {
            assert_eq!(
                date_time_options.fast_precision,
                DateTimePrecision::Milliseconds
            );
        } else {
            panic!("Expected a date time field mapping");
        }
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
                "output_format": "unix_timestamp_secs",
                "fast_precision": "milliseconds",
                "indexed": true,
                "fast": true,
                "stored": false
            }
            "#,
        )
        .unwrap();

        assert_eq!(field_mapping_entry.name, "updated_at");

        let date_time_options = match field_mapping_entry.mapping_type {
            FieldMappingType::DateTime(date_time_options, Cardinality::MultiValued) => {
                date_time_options
            }
            _ => panic!("Expected a date time field mapping."),
        };
        let expected_input_formats = InputFormats(vec![DateTimeInputFormat::Rfc3339]);
        let expected_date_time_options = QuickwitDateTimeOptions {
            description: Some("When the record was last updated.".to_string()),
            input_formats: expected_input_formats,
            output_format: DateTimeOutputFormat::TimestampSecs,
            fast_precision: DateTimePrecision::Milliseconds,
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
            &[DateTimeInputFormat::Rfc3339, DateTimeInputFormat::Timestamp]
        );
        assert_eq!(
            date_time_options.output_format,
            DateTimeOutputFormat::Rfc3339
        );
        assert_eq!(date_time_options.fast_precision, DateTimePrecision::Seconds);
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
                "fast_precision": "hours",
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
                "output_format": "rfc3339",
                "fast_precision": "seconds",
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
                &[DateTimeInputFormat::Rfc3339, DateTimeInputFormat::Timestamp]
            );
        }
        {
            let input_formats_json = r#"["rfc3339", "unix_timestamp", "unix_timestamp"]"#;
            let input_formats: InputFormats = serde_json::from_str(input_formats_json).unwrap();
            assert_eq!(
                input_formats.0,
                &[DateTimeInputFormat::Rfc3339, DateTimeInputFormat::Timestamp]
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
            assert!(error.contains("invalid strptime format"));
        }
    }

    #[test]
    fn test_date_time_options_parse_json() {
        let date_time_options = QuickwitDateTimeOptions {
            input_formats: InputFormats(vec![
                DateTimeInputFormat::Rfc3339,
                DateTimeInputFormat::Timestamp,
            ]),
            ..Default::default()
        };
        let expected_timestamp = datetime!(2012-05-21 12:09:14 UTC).unix_timestamp();
        {
            let json_value = serde_json::json!("2012-05-21T12:09:14-00:00");
            let tantivy_value = date_time_options.parse_json(&json_value).unwrap();
            let date_time = match tantivy_value {
                TantivyValue::Date(date_time) => date_time,
                other => panic!("Expected a tantivy date time, got `{other:?}`."),
            };
            assert_eq!(date_time.into_timestamp_secs(), expected_timestamp);
        }
        {
            let json_value = serde_json::json!(expected_timestamp);
            let tantivy_value = date_time_options.parse_json(&json_value).unwrap();
            let date_time = match tantivy_value {
                TantivyValue::Date(date_time) => date_time,
                other => panic!("Expected a tantivy date time, got `{other:?}`."),
            };
            assert_eq!(date_time.into_timestamp_secs(), expected_timestamp);
        }
        {
            let json_value = serde_json::json!(expected_timestamp as f64);
            let tantivy_value = date_time_options.parse_json(&json_value).unwrap();
            let date_time = match tantivy_value {
                TantivyValue::Date(date_time) => date_time,
                other => panic!("Expected a tantivy date time, got `{other:?}`."),
            };
            assert_eq!(date_time.into_timestamp_secs(), expected_timestamp);
        }
    }
}
