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

// Minimum supported timestamp value in seconds (01 Jan 1974 00:00:00 GMT).
const MIN_TIMESTAMP_SECONDS: i64 = 126230400;

// Maximum supported timestamp value in nanoseconds (31 Dec 2241 23:59:59 GMT).
const MAX_TIMESTAMP_NANOSECONDS: i64 = 8_583_494_399_000_000_000;

pub(super) fn parse_date_time(
    date_time_str: &str,
    date_time_formats: &[DateTimeFormat],
) -> Result<TantivyDateTime, String> {
    for date_time_format in date_time_formats {
        let date_time_res = match date_time_format {
            DateTimeFormat::ISO8601 => parse_iso8601(date_time_str).map(TantivyDateTime::from_utc),
            DateTimeFormat::RFC2822 => parse_rfc2822(date_time_str).map(TantivyDateTime::from_utc),
            DateTimeFormat::RCF3339 => parse_rfc3339(date_time_str).map(TantivyDateTime::from_utc),
            DateTimeFormat::Strftime {
                strftime_format,
                with_timezone: true,
            } => parse_strftime_with_tz(date_time_str, strftime_format)
                .map(|dt| TantivyDateTime::from_timestamp_micros(dt.timestamp_micros())),
            DateTimeFormat::Strftime {
                strftime_format,
                with_timezone: false,
            } => parse_strftime(date_time_str, strftime_format)
                .map(|dt| TantivyDateTime::from_timestamp_micros(dt.timestamp_micros())),
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

/// Parses a date using the specified strftime format without a timezone.
fn parse_strftime(
    date_time_str: &str,
    strftime_format: &str,
) -> Result<chrono::NaiveDateTime, String> {
    chrono::NaiveDateTime::parse_from_str(date_time_str, strftime_format)
        .map_err(|error| error.to_string())
}

/// Parses a date using the specified strftime format with a timezone (`%z` or `%Z`).
fn parse_strftime_with_tz(
    date_time_str: &str,
    strftime_format: &str,
) -> Result<chrono::NaiveDateTime, String> {
    chrono::DateTime::parse_from_str(date_time_str, strftime_format)
        .map(|date_time| date_time.naive_utc())
        .map_err(|error| error.to_string())
}

/// Returns the appropriate tantivy datetime for the specified unix timestamp.
///
/// This function will choose the timestamp precision based on the
/// most significant bit position in the timestamp value.
/// The tradeoff is that we can only support dates ranging from
/// `01 Jan 1974 00:00:00` to `31 Dec 2241 23:59:59`
pub(super) fn parse_timestamp(timestamp: i64) -> Result<TantivyDateTime, String> {
    if !(MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_NANOSECONDS).contains(&timestamp) {
        return Err(format!(
            "Failed to parse unix timestamp `{timestamp}`. Quickwit only support timestamp values \
             ranging form `01 Jan 1974 00:00:00` to `31 Dec 2241 23:59:59`."
        ));
    }
    match i64::BITS - timestamp.leading_zeros() {
        27..=33 => Ok(TantivyDateTime::from_timestamp_secs(timestamp)),
        37..=43 => Ok(TantivyDateTime::from_timestamp_millis(timestamp)),
        47..=53 => Ok(TantivyDateTime::from_timestamp_micros(timestamp)),
        57..=63 => Ok(TantivyDateTime::from_timestamp_micros(timestamp / 1_000)),
        _ => unreachable!(),
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
    fn test_parse_strftime() {
        let date_time = parse_strftime("2012-05-21 12:09:14", "%Y-%m-%d %H:%M:%S").unwrap();
        assert_eq!(
            date_time.timestamp(),
            datetime!(2012-05-21 12:09:14 UTC).unix_timestamp()
        );
    }

    #[test]
    fn test_parse_strftime_with_tz() {
        let date_time =
            parse_strftime_with_tz("2012-05-21 12:09:14 +00:00", "%Y-%m-%d %H:%M:%S %z").unwrap();
        assert_eq!(
            date_time.timestamp(),
            datetime!(2012-05-21 12:09:14 UTC).unix_timestamp()
        );
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
                    DateTimeFormat::Strftime {
                        strftime_format: "%Y-%m-%d %H:%M:%S".to_string(),
                        with_timezone: false,
                    },
                    DateTimeFormat::Strftime {
                        strftime_format: "%Y/%m/%d %H:%M:%S".to_string(),
                        with_timezone: false,
                    },
                    DateTimeFormat::Strftime {
                        strftime_format: "%Y/%m/%d %H:%M:%S %z".to_string(),
                        with_timezone: true,
                    },
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
                    DateTimeFormat::Strftime {
                        strftime_format: "%Y-%m-%d %H:%M:%S.%3f".to_string(),
                        with_timezone: false,
                    },
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
                OffsetDateTime::parse("1974-01-01T00:00:00.00Z", &Rfc3339).unwrap();
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
                OffsetDateTime::parse("2241-12-31T23:59:59.00Z", &Rfc3339).unwrap();
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
            let greater_than_supported_date = MAX_TIMESTAMP_NANOSECONDS + 1;
            let parse_err = parse_timestamp(greater_than_supported_date).unwrap_err();
            assert!(parse_err.contains("Failed to parse unix timestamp"));
        }
        {
            let unix_epoch = 0;
            let parse_err = parse_timestamp(unix_epoch).unwrap_err();
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
            let max_ts_seconds = MAX_TIMESTAMP_NANOSECONDS / 1_000_000_000;
            let date_time = parse_timestamp(max_ts_seconds).unwrap();
            assert_eq!(date_time.into_timestamp_secs(), max_ts_seconds);

            let max_ts_millis = MAX_TIMESTAMP_NANOSECONDS / 1_000_000;
            let date_time = parse_timestamp(max_ts_millis).unwrap();
            assert_eq!(date_time.into_timestamp_millis(), max_ts_millis);

            let max_ts_micros = MAX_TIMESTAMP_NANOSECONDS / 1_000;
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
