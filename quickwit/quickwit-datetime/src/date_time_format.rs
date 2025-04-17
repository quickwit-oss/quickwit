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

use std::fmt::Display;
use std::str::FromStr;

use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as JsonValue;
use time::Month;
use time::format_description::well_known::{Iso8601, Rfc2822, Rfc3339};

use crate::java_date_time_format::is_strftime_formatting;
use crate::{StrptimeParser, TantivyDateTime};

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
            DateTimeInputFormat::Strptime(parser) => parser.strptime_format.as_str(),
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
                DateTimeInputFormat::Strptime(StrptimeParser::from_strptime(date_time_format_str)?)
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
            DateTimeOutputFormat::Strptime(parser) => parser.strptime_format.as_str(),
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
                DateTimeOutputFormat::Strptime(StrptimeParser::from_strptime(date_time_format_str)?)
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

/// Infers the year of a parsed date time. It assumes that events appear more often delayed than in
/// the future and, as a result, skews towards the past year.
pub(super) fn infer_year(
    parsed_month_opt: Option<Month>,
    this_month: Month,
    this_year: i32,
) -> i32 {
    let Some(parsed_month) = parsed_month_opt else {
        return this_year;
    };
    if parsed_month as u8 > this_month as u8 + 3 {
        return this_year - 1;
    }
    this_year
}

#[cfg(test)]
mod tests {
    use time::Month;

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
    fn test_infer_year() {
        let inferred_year = infer_year(None, Month::January, 2024);
        assert_eq!(inferred_year, 2024);

        let inferred_year = infer_year(Some(Month::December), Month::January, 2024);
        assert_eq!(inferred_year, 2023);

        let inferred_year = infer_year(Some(Month::January), Month::January, 2024);
        assert_eq!(inferred_year, 2024);

        let inferred_year = infer_year(Some(Month::February), Month::January, 2024);
        assert_eq!(inferred_year, 2024);

        let inferred_year = infer_year(Some(Month::March), Month::January, 2024);
        assert_eq!(inferred_year, 2024);

        let inferred_year = infer_year(Some(Month::April), Month::January, 2024);
        assert_eq!(inferred_year, 2024);

        let inferred_year = infer_year(Some(Month::May), Month::January, 2024);
        assert_eq!(inferred_year, 2023);
    }
}
