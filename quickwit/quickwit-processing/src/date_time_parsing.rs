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

use itertools::Itertools;
use std::fmt::Display;

use time::OffsetDateTime;
use time::format_description::well_known::{Iso8601, Rfc2822, Rfc3339};

// Helper methods to retrieve Unix timestamps with millisecond and microsecond precision.
// Implemented as an extension trait on `OffsetDateTime`.
#[allow(dead_code)]
pub trait OffsetDateTimeUnixTs {
    fn unix_timestamp_millis(&self) -> i64;
    fn unix_timestamp_micros(&self) -> i64;
}

impl OffsetDateTimeUnixTs for OffsetDateTime {
    fn unix_timestamp_millis(&self) -> i64 {
        (self.unix_timestamp_nanos() / 1_000_000) as i64
    }

    fn unix_timestamp_micros(&self) -> i64 {
        (self.unix_timestamp_nanos() / 1_000) as i64
    }
}

/// Specifies the datetime and unix timestamp formats to use when parsing date strings.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Default)]
#[allow(dead_code)]
pub enum DateTimeInputFormat {
    Iso8601,
    Rfc2822,
    #[default]
    Rfc3339,
    Timestamp,
}

impl DateTimeInputFormat {
    pub fn as_str(&self) -> &str {
        match self {
            DateTimeInputFormat::Iso8601 => "iso8601",
            DateTimeInputFormat::Rfc2822 => "rfc2822",
            DateTimeInputFormat::Rfc3339 => "rfc3339",
            DateTimeInputFormat::Timestamp => "unix_timestamp",
        }
    }
}

impl Display for DateTimeInputFormat {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

// Minimum supported timestamp value in seconds (13 Apr 1972 23:59:55 GMT).
const MIN_TIMESTAMP_SECONDS: i64 = 72_057_595;

// Maximum supported timestamp value in seconds (16 Mar 2242 12:56:31 GMT).
const MAX_TIMESTAMP_SECONDS: i64 = 8_589_934_591;

pub fn parse_date_time_str(
    date_time_str: &str,
    date_time_formats: &[DateTimeInputFormat],
) -> Result<OffsetDateTime, String> {
    let trimmed_date_time_str = date_time_str.trim_ascii();

    for date_time_format in date_time_formats {
        let date_time_opt = match date_time_format {
            DateTimeInputFormat::Iso8601 => parse_iso8601(trimmed_date_time_str).ok(),
            DateTimeInputFormat::Rfc2822 => parse_rfc2822(trimmed_date_time_str).ok(),
            DateTimeInputFormat::Rfc3339 => parse_rfc3339(trimmed_date_time_str).ok(),
            DateTimeInputFormat::Timestamp => parse_timestamp_str(trimmed_date_time_str),
        };
        if let Some(date_time) = date_time_opt {
            return Ok(date_time);
        }
    }
    Err(format!(
        "failed to parse datetime `{date_time_str}` using the following formats: `{}`",
        date_time_formats
            .iter()
            .map(|date_time_format| date_time_format.as_str())
            .join("`, `")
    ))
}

pub fn parse_timestamp_str(timestamp_str: &str) -> Option<OffsetDateTime> {
    if let Ok(timestamp) = timestamp_str.parse::<i64>() {
        return parse_timestamp(timestamp).ok();
    }
    None
}

/// Parses a ISO8601 date.
fn parse_iso8601(value: &str) -> Result<OffsetDateTime, String> {
    OffsetDateTime::parse(value, &Iso8601::DEFAULT).map_err(|error| error.to_string())
}

/// Parses a RFC2822 date.
fn parse_rfc2822(value: &str) -> Result<OffsetDateTime, String> {
    OffsetDateTime::parse(value, &Rfc2822).map_err(|error| error.to_string())
}

/// Parses a RFC3339 date.
fn parse_rfc3339(value: &str) -> Result<OffsetDateTime, String> {
    OffsetDateTime::parse(value, &Rfc3339).map_err(|error| error.to_string())
}

/// Returns the appropriate [`OffsetDateTime`] for the specified Unix timestamp.
///
/// This function will choose the timestamp precision based on the value range.
/// The tradeoff is that we can only support dates ranging:
/// - from `13 Apr 1972 23:59:55`: smallest value that can be converted to all precisions.
/// - to: `16 Mar 2242 12:56:31`: greatest value that can be converted to all precisions.
pub fn parse_timestamp(timestamp: i64) -> Result<OffsetDateTime, String> {
    const MIN_TIMESTAMP_MILLIS: i64 = MIN_TIMESTAMP_SECONDS * 1000;
    const MAX_TIMESTAMP_MILLIS: i64 = MAX_TIMESTAMP_SECONDS * 1000;

    const MIN_TIMESTAMP_MICROS: i64 = MIN_TIMESTAMP_SECONDS * 1_000_000;
    const MAX_TIMESTAMP_MICROS: i64 = MAX_TIMESTAMP_SECONDS * 1_000_000;

    const MIN_TIMESTAMP_NANOS: i64 = MIN_TIMESTAMP_SECONDS * 1_000_000_000;
    const MAX_TIMESTAMP_NANOS: i64 = MAX_TIMESTAMP_SECONDS * 1_000_000_000;

    match timestamp {
        MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS => {
            OffsetDateTime::from_unix_timestamp(timestamp).map_err(|error| error.to_string())
        }
        MIN_TIMESTAMP_MILLIS..=MAX_TIMESTAMP_MILLIS => {
            OffsetDateTime::from_unix_timestamp_nanos((timestamp as i128) * 1_000_000)
                .map_err(|error| error.to_string())
        }
        MIN_TIMESTAMP_MICROS..=MAX_TIMESTAMP_MICROS => {
            OffsetDateTime::from_unix_timestamp_nanos((timestamp as i128) * 1_000)
                .map_err(|error| error.to_string())
        }
        MIN_TIMESTAMP_NANOS..=MAX_TIMESTAMP_NANOS => {
            OffsetDateTime::from_unix_timestamp_nanos(timestamp as i128)
                .map_err(|error| error.to_string())
        }
        _ => Err(format!(
            "failed to parse unix timestamp `{timestamp}`. Quickwit only support timestamp values \
             ranging from `13 Apr 1972 23:59:55` to `16 Mar 2242 12:56:31`"
        )),
    }
}

#[cfg(test)]
mod tests {
    use time::macros::datetime;

    use super::*;

    #[test]
    fn test_parse_iso8601() {
        let date_time = parse_iso8601("20120521T120914Z").unwrap();
        assert_eq!(date_time, datetime!(2012-05-21 12:09:14 UTC));
    }

    #[test]
    fn test_parse_rfc2822() {
        let date_time = parse_rfc2822("Mon, 21 May 2012 12:09:14 GMT").unwrap();
        assert_eq!(date_time, datetime!(2012-05-21 12:09:14 UTC));
    }

    #[test]
    fn test_parse_rfc3339() {
        let date_time = parse_rfc3339("2012-05-21T12:09:14-00:00").unwrap();
        assert_eq!(date_time, datetime!(2012-05-21 12:09:14 UTC));
    }

    #[test]
    fn test_parse_date_time_str() {
        for date_time_str in [
            "20120521T120914Z ",
            " Mon, 21 May 2012 12:09:14 GMT",
            " 2012-05-21T12:09:14-00:00 ",
            " 1337602154 ",
        ] {
            let date_time = parse_date_time_str(
                date_time_str,
                &[
                    DateTimeInputFormat::Iso8601,
                    DateTimeInputFormat::Rfc2822,
                    DateTimeInputFormat::Rfc3339,
                    DateTimeInputFormat::Timestamp,
                ],
            )
            .unwrap();
            assert_eq!(
                date_time.unix_timestamp(),
                datetime!(2012-05-21 12:09:14 UTC).unix_timestamp()
            );
        }
        let error = parse_date_time_str(
            "foo",
            &[DateTimeInputFormat::Iso8601, DateTimeInputFormat::Rfc2822],
        )
        .unwrap_err();
        assert_eq!(
            error,
            "failed to parse datetime `foo` using the following formats: `iso8601`, `rfc2822`"
        );
    }

    #[test]
    fn test_parse_timestamp_str() {
        let date_time = parse_timestamp_str("123456789").unwrap();
        assert_eq!(date_time.unix_timestamp(), 123456789);

        assert_eq!(parse_timestamp_str("123456789."), None);
        assert_eq!(parse_timestamp_str("123456789.0"), None);
    }

    #[test]
    fn test_parse_date_time_millis() {
        for date_time_str in ["20120521T120914.12Z", "2012-05-21T12:09:14.12-00:00"] {
            let date_time = parse_date_time_str(
                date_time_str,
                &[DateTimeInputFormat::Iso8601, DateTimeInputFormat::Rfc3339],
            )
            .unwrap();
            assert_eq!(
                date_time.unix_timestamp_micros() as i128,
                datetime!(2012-05-21 12:09:14.12 UTC).unix_timestamp_nanos() / 1_000
            );
        }
    }

    #[test]
    fn test_parse_timestamp() {
        let now = OffsetDateTime::now_utc();
        {
            let unix_ts_secs = now.unix_timestamp();
            let date_time = parse_timestamp(unix_ts_secs).unwrap();
            assert_eq!(date_time.unix_timestamp(), unix_ts_secs);
        }
        {
            let unix_ts_millis = (now.unix_timestamp_nanos() / 1_000_000) as i64;
            let date_time = parse_timestamp(unix_ts_millis).unwrap();
            assert_eq!(date_time.unix_timestamp_millis(), unix_ts_millis);
        }
        {
            let unix_ts_micros = (now.unix_timestamp_nanos() / 1_000) as i64;
            let date_time = parse_timestamp(unix_ts_micros).unwrap();
            assert_eq!(date_time.unix_timestamp_micros(), unix_ts_micros);
        }
        {
            let unix_ts_nanos = now.unix_timestamp_nanos();
            let date_time = parse_timestamp(unix_ts_nanos as i64).unwrap();
            assert_eq!(date_time.unix_timestamp_nanos(), unix_ts_nanos);
        }
        {
            let min_supported_date =
                OffsetDateTime::parse("1972-04-13T23:59:55.00Z", &Rfc3339).unwrap();
            let parsed_date_time = parse_timestamp(min_supported_date.unix_timestamp()).unwrap();
            assert_eq!(
                parsed_date_time.unix_timestamp(),
                min_supported_date.unix_timestamp()
            );
            assert_eq!(
                parsed_date_time.unix_timestamp_nanos(),
                min_supported_date.unix_timestamp_nanos()
            );
        }
        {
            let max_supported_date =
                OffsetDateTime::parse("2242-03-16T12:56:31.00Z", &Rfc3339).unwrap();
            let parsed_date_time = parse_timestamp(max_supported_date.unix_timestamp()).unwrap();
            assert_eq!(
                parsed_date_time.unix_timestamp(),
                max_supported_date.unix_timestamp()
            );
            assert_eq!(
                parsed_date_time.unix_timestamp_nanos(),
                max_supported_date.unix_timestamp_nanos()
            );
        }
        {
            let less_than_supported_date = MIN_TIMESTAMP_SECONDS - 1;
            let parse_err = parse_timestamp(less_than_supported_date).unwrap_err();
            assert!(parse_err.contains("failed to parse unix timestamp"));
        }
        {
            let greater_than_supported_date = MAX_TIMESTAMP_SECONDS + 1;
            let parse_err = parse_timestamp(greater_than_supported_date).unwrap_err();
            assert!(parse_err.contains("failed to parse unix timestamp"));
        }
        {
            let unix_epoch = 0;
            let parse_err = parse_timestamp(unix_epoch).unwrap_err();
            assert!(parse_err.contains("failed to parse unix timestamp"));

            let parse_err = parse_timestamp(MIN_TIMESTAMP_SECONDS << 7).unwrap_err();
            assert!(parse_err.contains("failed to parse unix timestamp"));

            let parse_err = parse_timestamp(MIN_TIMESTAMP_SECONDS << 17).unwrap_err();
            assert!(parse_err.contains("failed to parse unix timestamp"));

            let parse_err = parse_timestamp(MIN_TIMESTAMP_SECONDS << 27).unwrap_err();
            assert!(parse_err.contains("failed to parse unix timestamp"));
        }
    }

    #[test]
    fn test_parse_timestamp_min_max_values() {
        {
            let min_ts_millis = MIN_TIMESTAMP_SECONDS * 1_000;
            let date_time = parse_timestamp(min_ts_millis).unwrap();
            assert_eq!(
                date_time.unix_timestamp_nanos() as i64 / 1_000_000,
                min_ts_millis
            );

            let min_ts_micros = MIN_TIMESTAMP_SECONDS * 1_000_000;
            let date_time = parse_timestamp(min_ts_micros).unwrap();
            assert_eq!(
                date_time.unix_timestamp_nanos() as i64 / 1_000,
                min_ts_micros
            );

            let min_ts_nanos = MIN_TIMESTAMP_SECONDS * 1_000_000_000;
            let date_time = parse_timestamp(min_ts_nanos).unwrap();
            assert_eq!(date_time.unix_timestamp_micros() * 1000, min_ts_nanos);
        }
        {
            let max_ts_seconds = MAX_TIMESTAMP_SECONDS;
            let date_time = parse_timestamp(max_ts_seconds).unwrap();
            assert_eq!(date_time.unix_timestamp(), max_ts_seconds);

            let max_ts_millis = MAX_TIMESTAMP_SECONDS * 1_000;
            let date_time = parse_timestamp(max_ts_millis).unwrap();
            assert_eq!(date_time.unix_timestamp_millis(), max_ts_millis);

            let max_ts_micros = MAX_TIMESTAMP_SECONDS * 1_000_000;
            let date_time = parse_timestamp(max_ts_micros).unwrap();
            assert_eq!(date_time.unix_timestamp_micros(), max_ts_micros);
        }
    }
}
