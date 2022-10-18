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

use itertools::Itertools;
use tantivy::{DatePrecision as DateTimePrecision, DateTime as TantivyDateTime};
use time::format_description::well_known::{Iso8601, Rfc2822, Rfc3339};
use time::OffsetDateTime;

use super::date_time_format::DateTimeFormat;

// Minimum supported timestamp value in seconds (13 Apr 1972 23:59:55 GMT).
const MIN_TIMESTAMP_SECONDS: i64 = 72_057_595;

// Maximum supported timestamp value in seconds (16 Mar 2242 12:56:31 GMT).
const MAX_TIMESTAMP_SECONDS: i64 = 8_589_934_591;

pub(super) fn parse_date_time(
    date_time_str: &str,
    date_time_formats: &[DateTimeFormat],
) -> Result<TantivyDateTime, String> {
    for date_time_format in date_time_formats {
        let date_time_res = match date_time_format {
            DateTimeFormat::ISO8601 => parse_iso8601(date_time_str).map(TantivyDateTime::from_utc),
            DateTimeFormat::RFC2822 => parse_rfc2822(date_time_str).map(TantivyDateTime::from_utc),
            DateTimeFormat::RCF3339 => parse_rfc3339(date_time_str).map(TantivyDateTime::from_utc),
            DateTimeFormat::Strptime(parser) => parser
                .parse_date_time(date_time_str)
                .map(TantivyDateTime::from_utc),
            _ => continue,
        };
        if date_time_res.is_ok() {
            return date_time_res;
        }
    }
    Err(format!(
        "Failed to parse datetime `{date_time_str}` using the following formats: `{}`.",
        date_time_formats
            .iter()
            .map(|date_time_format| date_time_format.as_str())
            .join("`, `")
    ))
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

/// Returns the appropriate [`TantivyDateTime`] for the specified Unix timestamp.
///
/// This function will choose the timestamp precision based on the value range.
/// The tradeoff is that we can only support dates ranging:
/// - from `13 Apr 1972 23:59:55`: smallest value that can be converted to all precisions.
/// - to: `16 Mar 2242 12:56:31`: greatest value that can be converted to all precisions.
pub(super) fn parse_timestamp(timestamp: i64) -> Result<TantivyDateTime, String> {
    const MIN_TIMESTAMP_MILLIS: i64 = MIN_TIMESTAMP_SECONDS * 1000;
    const MAX_TIMESTAMP_MILLIS: i64 = MAX_TIMESTAMP_SECONDS * 1000;

    const MIN_TIMESTAMP_MICROS: i64 = MIN_TIMESTAMP_SECONDS * 1_000_000;
    const MAX_TIMESTAMP_MICROS: i64 = MAX_TIMESTAMP_SECONDS * 1_000_000;

    const MIN_TIMESTAMP_NANOS: i64 = MIN_TIMESTAMP_SECONDS * 1_000_000_000;
    const MAX_TIMESTAMP_NANOS: i64 = MAX_TIMESTAMP_SECONDS * 1_000_000_000;

    match timestamp {
        MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS => {
            Ok(TantivyDateTime::from_timestamp_secs(timestamp))
        }
        MIN_TIMESTAMP_MILLIS..=MAX_TIMESTAMP_MILLIS => {
            Ok(TantivyDateTime::from_timestamp_millis(timestamp))
        }
        MIN_TIMESTAMP_MICROS..=MAX_TIMESTAMP_MICROS => {
            Ok(TantivyDateTime::from_timestamp_micros(timestamp))
        }
        MIN_TIMESTAMP_NANOS..=MAX_TIMESTAMP_NANOS => {
            Ok(TantivyDateTime::from_timestamp_micros(timestamp / 1_000))
        }
        _ => Err(format!(
            "Failed to parse unix timestamp `{timestamp}`. Quickwit only support timestamp values \
             ranging from `13 Apr 1972 23:59:55` to `16 Mar 2242 12:56:31`."
        )),
    }
}

/// Formats the specified timestamp as a RFC3339 date string.
pub(crate) fn format_timestamp(
    timestamp: i64,
    precision: &DateTimePrecision,
) -> Result<String, String> {
    let date_time = match precision {
        DateTimePrecision::Seconds => OffsetDateTime::from_unix_timestamp(timestamp),
        DateTimePrecision::Milliseconds => {
            OffsetDateTime::from_unix_timestamp_nanos((timestamp * 1_000_000) as i128)
        }
        DateTimePrecision::Microseconds => {
            OffsetDateTime::from_unix_timestamp_nanos((timestamp * 1_000) as i128)
        }
    };
    date_time
        .expect("The datetime should be valid.")
        .format(&Rfc3339)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use time::macros::datetime;

    use super::*;
    use crate::default_doc_mapper::date_time_format::StrptimeParser;

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
    fn test_parse_strptime() {
        let test_data = vec![
            (
                "%Y-%m-%d %H:%M:%S",
                "2012-05-21 12:09:14",
                datetime!(2012-05-21 12:09:14 UTC),
            ),
            (
                "%Y-%m-%d %H:%M:%S %z",
                "2012-05-21 12:09:14 +0000",
                datetime!(2012-05-21 12:09:14 UTC),
            ),
            (
                "%Y-%m-%d %H:%M:%S %z",
                "2012-05-21 12:09:14 +0200",
                datetime!(2012-05-21 10:09:14 UTC),
            ),
            (
                "%Y-%m-%d %H:%M:%S %z",
                "2012-05-21 12:09:14 -0300",
                datetime!(2012-05-21 15:09:14 UTC),
            ),
            (
                "%Y-%m-%d %H:%M:%S %z",
                "2012-05-21 12:09:14 -03:00",
                datetime!(2012-05-21 15:09:14 UTC),
            ),
        ];
        for (fmt, date_time_str, expected) in test_data {
            let parser = StrptimeParser::make(fmt).unwrap();
            let result = parser.parse_date_time(date_time_str);
            if let Err(error) = &result {
                println!("Failed to parse: {} {} {}", date_time_str, fmt, error)
            }
            assert_eq!(result.unwrap(), expected);
        }
    }

    #[test]
    fn test_parse_date_time() {
        for date_time_str in [
            "20120521T120914Z",
            "Mon, 21 May 2012 12:09:14 GMT",
            "2012-05-21T12:09:14-00:00",
            "2012-05-21 12:09:14",
            "2012/05/21 12:09:14",
            "2012/05/21 12:09:14 +00:00",
        ] {
            let date_time = parse_date_time(
                date_time_str,
                &[
                    DateTimeFormat::ISO8601,
                    DateTimeFormat::RFC2822,
                    DateTimeFormat::RCF3339,
                    DateTimeFormat::Strptime(StrptimeParser::make("%Y-%m-%d %H:%M:%S").unwrap()),
                    DateTimeFormat::Strptime(StrptimeParser::make("%Y/%m/%d %H:%M:%S").unwrap()),
                    DateTimeFormat::Strptime(StrptimeParser::make("%Y/%m/%d %H:%M:%S %z").unwrap()),
                ],
            )
            .unwrap();
            assert_eq!(
                date_time.into_timestamp_secs(),
                datetime!(2012-05-21 12:09:14 UTC).unix_timestamp()
            );
        }
        let error = parse_date_time("foo", &[DateTimeFormat::ISO8601, DateTimeFormat::RFC2822])
            .unwrap_err();
        assert_eq!(
            error,
            "Failed to parse datetime `foo` using the following formats: `iso8601`, `rfc2822`."
        );
    }

    #[test]
    fn test_parse_date_time_millis() {
        for date_time_str in [
            "20120521T120914.12Z",
            "2012-05-21T12:09:14.12-00:00",
            "2012-05-21 12:09:14.120",
        ] {
            let date_time = parse_date_time(
                date_time_str,
                &[
                    DateTimeFormat::ISO8601,
                    DateTimeFormat::RCF3339,
                    DateTimeFormat::Strptime(StrptimeParser::make("%Y-%m-%d %H:%M:%S.%f").unwrap()),
                ],
            )
            .unwrap();
            assert_eq!(
                date_time.into_timestamp_micros() as i128,
                datetime!(2012-05-21 12:09:14.12 UTC).unix_timestamp_nanos() / 1_000
            );
        }
    }

    #[test]
    fn test_parse_timestamp() {
        let now = time::OffsetDateTime::now_utc();
        {
            let unix_ts_secs = now.unix_timestamp();
            let date_time = parse_timestamp(unix_ts_secs).unwrap();
            assert_eq!(date_time.into_timestamp_secs(), unix_ts_secs);
        }
        {
            let unix_ts_millis = (now.unix_timestamp_nanos() / 1_000_000) as i64;
            let date_time = parse_timestamp(unix_ts_millis).unwrap();
            assert_eq!(date_time.into_timestamp_millis(), unix_ts_millis);
        }
        {
            let unix_ts_micros = (now.unix_timestamp_nanos() / 1_000) as i64;
            let date_time = parse_timestamp(unix_ts_micros).unwrap();
            assert_eq!(date_time.into_timestamp_micros(), unix_ts_micros);
        }
        {
            let unix_ts_nanos = now.unix_timestamp_nanos() as i64;
            let date_time = parse_timestamp(unix_ts_nanos).unwrap();
            assert_eq!(date_time.into_timestamp_micros(), unix_ts_nanos / 1_000);
        }
        {
            let min_supported_date =
                OffsetDateTime::parse("1972-04-13T23:59:55.00Z", &Rfc3339).unwrap();
            let parsed_date_time = parse_timestamp(min_supported_date.unix_timestamp()).unwrap();
            assert_eq!(
                parsed_date_time.into_timestamp_secs(),
                min_supported_date.unix_timestamp()
            );
            assert_eq!(
                parsed_date_time.into_timestamp_micros(),
                min_supported_date.unix_timestamp_nanos() as i64 / 1_000
            );
        }
        {
            let max_supported_date =
                OffsetDateTime::parse("2242-03-16T12:56:31.00Z", &Rfc3339).unwrap();
            let parsed_date_time = parse_timestamp(max_supported_date.unix_timestamp()).unwrap();
            assert_eq!(
                parsed_date_time.into_timestamp_secs(),
                max_supported_date.unix_timestamp()
            );
            assert_eq!(
                parsed_date_time.into_timestamp_micros(),
                max_supported_date.unix_timestamp_nanos() as i64 / 1_000
            );
        }
        {
            let less_than_supported_date = MIN_TIMESTAMP_SECONDS - 1;
            let parse_err = parse_timestamp(less_than_supported_date).unwrap_err();
            assert!(parse_err.contains("Failed to parse unix timestamp"));
        }
        {
            let greater_than_supported_date = MAX_TIMESTAMP_SECONDS + 1;
            let parse_err = parse_timestamp(greater_than_supported_date).unwrap_err();
            assert!(parse_err.contains("Failed to parse unix timestamp"));
        }
        {
            let unix_epoch = 0;
            let parse_err = parse_timestamp(unix_epoch).unwrap_err();
            assert!(parse_err.contains("Failed to parse unix timestamp"));

            let parse_err = parse_timestamp(MIN_TIMESTAMP_SECONDS << 7).unwrap_err();
            assert!(parse_err.contains("Failed to parse unix timestamp"));

            let parse_err = parse_timestamp(MIN_TIMESTAMP_SECONDS << 17).unwrap_err();
            assert!(parse_err.contains("Failed to parse unix timestamp"));

            let parse_err = parse_timestamp(MIN_TIMESTAMP_SECONDS << 27).unwrap_err();
            assert!(parse_err.contains("Failed to parse unix timestamp"));
        }
    }

    #[test]
    fn test_parse_timestamp_min_max_values() {
        {
            let min_ts_millis = MIN_TIMESTAMP_SECONDS * 1_000;
            let date_time = parse_timestamp(min_ts_millis).unwrap();
            assert_eq!(date_time.into_timestamp_millis(), min_ts_millis);

            let min_ts_micros = MIN_TIMESTAMP_SECONDS * 1_000_000;
            let date_time = parse_timestamp(min_ts_micros).unwrap();
            assert_eq!(date_time.into_timestamp_micros(), min_ts_micros);

            let min_ts_nanos = MIN_TIMESTAMP_SECONDS * 1_000_000_000;
            let date_time = parse_timestamp(min_ts_nanos).unwrap();
            assert_eq!(date_time.into_timestamp_micros() * 1000, min_ts_nanos);
        }
        {
            let max_ts_seconds = MAX_TIMESTAMP_SECONDS;
            let date_time = parse_timestamp(max_ts_seconds).unwrap();
            assert_eq!(date_time.into_timestamp_secs(), max_ts_seconds);

            let max_ts_millis = MAX_TIMESTAMP_SECONDS * 1_000;
            let date_time = parse_timestamp(max_ts_millis).unwrap();
            assert_eq!(date_time.into_timestamp_millis(), max_ts_millis);

            let max_ts_micros = MAX_TIMESTAMP_SECONDS * 1_000_000;
            let date_time = parse_timestamp(max_ts_micros).unwrap();
            assert_eq!(date_time.into_timestamp_micros(), max_ts_micros);
        }
    }

    #[test]
    fn test_format_timestamp() {
        let date_time_str = format_timestamp(
            datetime!(2012-05-21 12:09:14 UTC).unix_timestamp(),
            &DateTimePrecision::Seconds,
        )
        .unwrap();
        assert_eq!(date_time_str, "2012-05-21T12:09:14Z");
    }
}
