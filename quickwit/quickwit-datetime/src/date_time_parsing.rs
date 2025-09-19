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

use std::time::Duration;

use itertools::Itertools;
use time::OffsetDateTime;
use time::format_description::well_known::{Iso8601, Rfc2822, Rfc3339};

use super::date_time_format::DateTimeInputFormat;
use crate::TantivyDateTime;

// Minimum supported timestamp value in seconds (13 Apr 1972 23:59:55 GMT).
const MIN_TIMESTAMP_SECONDS: i64 = 72_057_595;

// Maximum supported timestamp value in seconds (16 Mar 2242 12:56:31 GMT).
const MAX_TIMESTAMP_SECONDS: i64 = 8_589_934_591;

pub fn parse_date_time_str(
    date_time_str: &str,
    date_time_formats: &[DateTimeInputFormat],
) -> Result<TantivyDateTime, String> {
    let trimmed_date_time_str = date_time_str.trim_ascii();

    for date_time_format in date_time_formats {
        let date_time_opt = match date_time_format {
            DateTimeInputFormat::Iso8601 => parse_iso8601(trimmed_date_time_str)
                .map(TantivyDateTime::from_utc)
                .ok(),
            DateTimeInputFormat::Rfc2822 => parse_rfc2822(trimmed_date_time_str)
                .map(TantivyDateTime::from_utc)
                .ok(),
            DateTimeInputFormat::Rfc3339 => parse_rfc3339(trimmed_date_time_str)
                .map(TantivyDateTime::from_utc)
                .ok(),
            DateTimeInputFormat::Strptime(parser) => parser
                .parse_date_time(trimmed_date_time_str)
                .map(TantivyDateTime::from_utc)
                .ok(),
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

pub fn parse_timestamp_float(
    timestamp: f64,
    date_time_formats: &[DateTimeInputFormat],
) -> Result<TantivyDateTime, String> {
    if !date_time_formats.contains(&DateTimeInputFormat::Timestamp) {
        return Err(format!(
            "failed to parse datetime `{timestamp}` using the following formats: `{}`",
            date_time_formats
                .iter()
                .map(|date_time_format| date_time_format.as_str())
                .join("`, `")
        ));
    }
    let duration_since_epoch = Duration::try_from_secs_f64(timestamp)
        .map_err(|error| format!("failed to parse datetime `{timestamp}`: {error}"))?;
    let timestamp_nanos = duration_since_epoch.as_nanos() as i64;
    Ok(TantivyDateTime::from_timestamp_nanos(timestamp_nanos))
}

pub fn parse_timestamp_int(
    timestamp: i64,
    date_time_formats: &[DateTimeInputFormat],
) -> Result<TantivyDateTime, String> {
    if !date_time_formats.contains(&DateTimeInputFormat::Timestamp) {
        return Err(format!(
            "failed to parse datetime `{timestamp}` using the following formats: `{}`",
            date_time_formats
                .iter()
                .map(|date_time_format| date_time_format.as_str())
                .join("`, `")
        ));
    }
    parse_timestamp(timestamp)
}

pub fn parse_timestamp_str(timestamp_str: &str) -> Option<TantivyDateTime> {
    if let Ok(timestamp) = timestamp_str.parse::<i64>() {
        return parse_timestamp(timestamp).ok();
    }
    if let Some((timestamp_secs_str, subsecond_digits_str)) = timestamp_str.split_once('.') {
        if subsecond_digits_str.is_empty() {
            return parse_timestamp_str(timestamp_secs_str);
        }
        if let Ok(timestamp_secs @ MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS) =
            timestamp_secs_str.parse::<i64>()
        {
            let num_subsecond_digits = subsecond_digits_str.len().min(9);

            if let Ok(subsecond_digits) =
                subsecond_digits_str[..num_subsecond_digits].parse::<i64>()
            {
                let nanos = subsecond_digits * 10i64.pow(9 - num_subsecond_digits as u32);
                let timestamp_nanos = timestamp_secs * 1_000_000_000 + nanos;
                return Some(TantivyDateTime::from_timestamp_nanos(timestamp_nanos));
            }
        }
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

/// Returns the appropriate [`TantivyDateTime`] for the specified Unix timestamp.
///
/// This function will choose the timestamp precision based on the value range.
/// The tradeoff is that we can only support dates ranging:
/// - from `13 Apr 1972 23:59:55`: smallest value that can be converted to all precisions.
/// - to: `16 Mar 2242 12:56:31`: greatest value that can be converted to all precisions.
pub fn parse_timestamp(timestamp: i64) -> Result<TantivyDateTime, String> {
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
            Ok(TantivyDateTime::from_timestamp_nanos(timestamp))
        }
        _ => Err(format!(
            "failed to parse unix timestamp `{timestamp}`. Quickwit only support timestamp values \
             ranging from `13 Apr 1972 23:59:55` to `16 Mar 2242 12:56:31`"
        )),
    }
}

#[cfg(test)]
mod tests {
    use time::Month;
    use time::macros::datetime;

    use super::*;
    use crate::StrptimeParser;
    use crate::date_time_format::infer_year;

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
                " %Y-%m-%d %H:%M:%S ",
                "2012-05-21 12:09:14",
                datetime!(2012-05-21 12:09:14 UTC),
            ),
            (
                "%Y-%m-%d %H:%M:%S %z",
                " 2012-05-21 12:09:14 +0000 ",
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
            (
                "%Y-%m-%d %H:%M:%S.%f",
                "2024-01-31 18:40:19.950",
                datetime!(2024-01-31 18:40:19.950000000 UTC),
            ),
            (
                "%Y-%m-%d %H:%M:%S.%f",
                "2024-01-31 18:40:19.950188",
                datetime!(2024-01-31 18:40:19.950188000 UTC),
            ),
            (
                "%Y-%m-%d %H:%M:%S.%f",
                "2024-01-31 18:40:19.950188123",
                datetime!(2024-01-31 18:40:19.950188123 UTC),
            ),
            ("%b %d %H:%M:%S", "Mar  6 17:40:02", {
                let dt = datetime!(1900-03-06 17:40:02 UTC);
                let now = OffsetDateTime::now_utc();
                let year = infer_year(Some(Month::March), now.month(), now.year());
                dt.replace_year(year).unwrap()
            }),
            (
                "%Y-%m-%dT%H:%M:%S.%f%z",
                "2024-03-21T03:45:02.561820768-0400",
                datetime!(2024-03-21 03:45:02.561820768 -04:00),
            ),
        ];
        for (fmt, date_time_str, expected) in test_data {
            let parser = DateTimeInputFormat::Strptime(StrptimeParser::from_strptime(fmt).unwrap());
            let result = parse_date_time_str(date_time_str, &[parser]);
            if let Err(error) = &result {
                panic!(
                    "failed to parse `{date_time_str}` using the following strptime format \
                     `{fmt}`: {error}"
                )
            }
            assert_eq!(result.unwrap(), TantivyDateTime::from_utc(expected));
        }
    }

    #[test]
    fn test_parse_date_without_time() {
        let strptime_parser = StrptimeParser::from_strptime("%Y-%m-%d").unwrap();
        let date = strptime_parser.parse_date_time("2012-05-21").unwrap();
        assert_eq!(date, datetime!(2012-05-21 00:00:00 UTC));
    }

    #[test]
    fn test_parse_date_am_pm_hour_not_zeroed() {
        let strptime_parser = StrptimeParser::from_strptime("%Y-%m-%d %I:%M:%S %p").unwrap();
        let date = strptime_parser
            .parse_date_time("2012-05-21 10:05:12 pm")
            .unwrap();
        assert_eq!(date, datetime!(2012-05-21 22:05:12 UTC));
    }

    #[test]
    fn test_parse_date_time_str() {
        for date_time_str in [
            "20120521T120914Z ",
            " Mon, 21 May 2012 12:09:14 GMT",
            " 2012-05-21T12:09:14-00:00 ",
            "2012-05-21 12:09:14",
            " 2012/05/21 12:09:14",
            "2012/05/21 12:09:14 +00:00",
            "1337602154 ",
            " 1337602154.0 ",
        ] {
            let date_time = parse_date_time_str(
                date_time_str,
                &[
                    DateTimeInputFormat::Iso8601,
                    DateTimeInputFormat::Rfc2822,
                    DateTimeInputFormat::Rfc3339,
                    DateTimeInputFormat::Strptime(
                        StrptimeParser::from_strptime("%Y-%m-%d %H:%M:%S").unwrap(),
                    ),
                    DateTimeInputFormat::Strptime(
                        StrptimeParser::from_strptime("%Y/%m/%d %H:%M:%S").unwrap(),
                    ),
                    DateTimeInputFormat::Strptime(
                        StrptimeParser::from_strptime("%Y/%m/%d %H:%M:%S %z").unwrap(),
                    ),
                    DateTimeInputFormat::Timestamp,
                ],
            )
            .unwrap();
            assert_eq!(
                date_time.into_timestamp_secs(),
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
    fn test_parse_timestamp_float() {
        let unix_ts_secs = OffsetDateTime::now_utc().unix_timestamp();
        {
            let date_time = parse_timestamp_float(
                unix_ts_secs as f64,
                &[DateTimeInputFormat::Iso8601, DateTimeInputFormat::Timestamp],
            )
            .unwrap();
            assert_eq!(date_time.into_timestamp_millis(), unix_ts_secs * 1_000);
        }
        {
            let date_time = parse_timestamp_float(
                unix_ts_secs as f64 + 0.1230,
                &[DateTimeInputFormat::Iso8601, DateTimeInputFormat::Timestamp],
            )
            .unwrap();
            assert!((date_time.into_timestamp_millis() - (unix_ts_secs * 1_000 + 123)).abs() <= 1);
        }
        {
            let date_time = parse_timestamp_float(
                unix_ts_secs as f64 + 0.1234560,
                &[DateTimeInputFormat::Iso8601, DateTimeInputFormat::Timestamp],
            )
            .unwrap();
            assert!(
                (date_time.into_timestamp_micros() - (unix_ts_secs * 1_000_000 + 123_456)).abs()
                    <= 1
            );
        }
        {
            let date_time = parse_timestamp_float(
                unix_ts_secs as f64 + 0.123456789,
                &[DateTimeInputFormat::Iso8601, DateTimeInputFormat::Timestamp],
            )
            .unwrap();
            assert!(
                (date_time.into_timestamp_nanos() - (unix_ts_secs * 1_000_000_000 + 123_456_789))
                    .abs()
                    <= 100
            );
        }
        {
            let error = parse_timestamp_float(
                1668730394917.01,
                &[DateTimeInputFormat::Iso8601, DateTimeInputFormat::Rfc2822],
            )
            .unwrap_err();
            assert_eq!(
                error,
                "failed to parse datetime `1668730394917.01` using the following formats: \
                 `iso8601`, `rfc2822`"
            );
        }
    }

    #[test]
    fn test_parse_timestamp_int() {
        {
            let unix_ts_secs = OffsetDateTime::now_utc().unix_timestamp();
            let date_time = parse_timestamp_int(
                unix_ts_secs,
                &[DateTimeInputFormat::Iso8601, DateTimeInputFormat::Timestamp],
            )
            .unwrap();
            assert_eq!(date_time.into_timestamp_secs(), unix_ts_secs);
        }
        {
            let error = parse_timestamp_int(
                1668730394917,
                &[DateTimeInputFormat::Iso8601, DateTimeInputFormat::Rfc2822],
            )
            .unwrap_err();
            assert_eq!(
                error,
                "failed to parse datetime `1668730394917` using the following formats: `iso8601`, \
                 `rfc2822`"
            );
        }
    }

    #[test]
    fn test_parse_timestamp_str() {
        let date_time = parse_timestamp_str("123456789").unwrap();
        assert_eq!(date_time.into_timestamp_secs(), 123456789);

        let date_time = parse_timestamp_str("123456789.").unwrap();
        assert_eq!(date_time.into_timestamp_secs(), 123456789);

        let date_time = parse_timestamp_str("123456789.0").unwrap();
        assert_eq!(date_time.into_timestamp_secs(), 123456789);

        let date_time = parse_timestamp_str("123456789.1").unwrap();
        assert_eq!(date_time.into_timestamp_millis(), 123456789100);

        let date_time = parse_timestamp_str("123456789.100000001").unwrap();
        assert_eq!(date_time.into_timestamp_nanos(), 123456789100000001);

        let date_time = parse_timestamp_str("123456789.1000000011").unwrap();
        assert_eq!(date_time.into_timestamp_nanos(), 123456789100000001);
    }

    #[test]
    fn test_parse_date_time_millis() {
        for date_time_str in [
            "20120521T120914.12Z",
            "2012-05-21T12:09:14.12-00:00",
            "2012-05-21 12:09:14.120",
        ] {
            let date_time = parse_date_time_str(
                date_time_str,
                &[
                    DateTimeInputFormat::Iso8601,
                    DateTimeInputFormat::Rfc3339,
                    DateTimeInputFormat::Strptime(
                        StrptimeParser::from_strptime("%Y-%m-%d %H:%M:%S.%f").unwrap(),
                    ),
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
        let now = OffsetDateTime::now_utc();
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
            assert_eq!(date_time.into_timestamp_nanos(), unix_ts_nanos);
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
}
