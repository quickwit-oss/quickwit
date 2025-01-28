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

use std::collections::HashMap;
use std::num::NonZeroU8;
use std::sync::OnceLock;

use time::error::{Format, TryFromParsed};
use time::format_description::modifier::{
    Day, Hour, Minute, Month as MonthModifier, Padding, Second, Subsecond, SubsecondDigits,
    WeekNumber, WeekNumberRepr, Weekday, WeekdayRepr, Year, YearRepr,
};
use time::format_description::{Component, OwnedFormatItem};
use time::parsing::Parsed;
use time::{Month, OffsetDateTime, PrimitiveDateTime, UtcOffset};
use time_fmt::parse::time_format_item::parse_to_format_item;

use crate::date_time_format;

const JAVA_DATE_FORMAT_TOKENS: &[&str] = &[
    "yyyy",
    "xxxx",
    "SSSSSSSSS", // For nanoseconds
    "SSSSSSS",   // For microseconds
    "SSSSSS",    // For fractional seconds up to six digits
    "SSSSS",
    "SSSS",
    "SSS",
    "SS",
    "ZZ",
    "ww",
    "w[w]",
    "MM",
    "dd",
    "HH",
    "hh",
    "kk",
    "mm",
    "ss",
    "aa",
    "a",
    "w",
    "M",
    "d",
    "H",
    "h",
    "k",
    "m",
    "s",
    "S",
    "Z",
    "e",
];

fn literal(s: &[u8]) -> OwnedFormatItem {
    // builds a boxed slice from a slice
    let boxed_slice: Box<[u8]> = s.to_vec().into_boxed_slice();
    OwnedFormatItem::Literal(boxed_slice)
}

#[inline]
fn get_padding(ptn: &str) -> Padding {
    if ptn.len() == 2 {
        Padding::Zero
    } else {
        Padding::None
    }
}

fn build_zone_offset(_: &str) -> Option<OwnedFormatItem> {
    // 'Z' literal to represent UTC offset
    let z_literal = OwnedFormatItem::Literal(Box::from(b"Z".as_ref()));

    // Offset in '+/-HH:MM' format
    let offset_with_delimiter_items: Box<[OwnedFormatItem]> = vec![
        OwnedFormatItem::Component(Component::OffsetHour(Default::default())),
        OwnedFormatItem::Literal(Box::from(b":".as_ref())),
        OwnedFormatItem::Component(Component::OffsetMinute(Default::default())),
    ]
    .into_boxed_slice();
    let offset_with_delimiter_compound = OwnedFormatItem::Compound(offset_with_delimiter_items);

    // Offset in '+/-HHMM' format
    let offset_items: Box<[OwnedFormatItem]> = vec![
        OwnedFormatItem::Component(Component::OffsetHour(Default::default())),
        OwnedFormatItem::Component(Component::OffsetMinute(Default::default())),
    ]
    .into_boxed_slice();
    let offset_compound = OwnedFormatItem::Compound(offset_items);

    Some(OwnedFormatItem::First(
        vec![z_literal, offset_with_delimiter_compound, offset_compound].into_boxed_slice(),
    ))
}

// There is a `YearRepr::LastTwo` representation in the time crate, but the parser is unreliable, so
// we only support `YearRepr::Full` for now. See also https://github.com/time-rs/time/issues/649.
const fn year_item() -> Option<OwnedFormatItem> {
    let mut year_component = Year::default();
    year_component.repr = YearRepr::Full;
    Some(OwnedFormatItem::Component(Component::Year(year_component)))
}

fn build_month_item(ptn: &str) -> Option<OwnedFormatItem> {
    let mut month: MonthModifier = Default::default();
    month.padding = get_padding(ptn);
    Some(OwnedFormatItem::Component(Component::Month(month)))
}

fn build_day_item(ptn: &str) -> Option<OwnedFormatItem> {
    let mut day = Day::default();
    day.padding = get_padding(ptn);
    Some(OwnedFormatItem::Component(Component::Day(day)))
}

fn build_day_of_week_item(_: &str) -> Option<OwnedFormatItem> {
    let mut weekday = Weekday::default();
    weekday.repr = WeekdayRepr::Monday;
    weekday.one_indexed = false;
    Some(OwnedFormatItem::Component(Component::Weekday(weekday)))
}

fn build_week_of_year_item(ptn: &str) -> Option<OwnedFormatItem> {
    let mut week_number = WeekNumber::default();
    week_number.repr = WeekNumberRepr::Monday;
    week_number.padding = get_padding(ptn);
    Some(OwnedFormatItem::Component(Component::WeekNumber(
        week_number,
    )))
}

fn build_hour_item(ptn: &str) -> Option<OwnedFormatItem> {
    let mut hour = Hour::default();
    hour.padding = get_padding(ptn);
    hour.is_12_hour_clock = false;
    Some(OwnedFormatItem::Component(Component::Hour(hour)))
}

fn build_minute_item(ptn: &str) -> Option<OwnedFormatItem> {
    let mut minute: Minute = Default::default();
    minute.padding = get_padding(ptn);
    Some(OwnedFormatItem::Component(Component::Minute(minute)))
}

fn build_second_item(ptn: &str) -> Option<OwnedFormatItem> {
    let mut second: Second = Default::default();
    second.padding = get_padding(ptn);
    Some(OwnedFormatItem::Component(Component::Second(second)))
}

fn build_fraction_of_second_item(_ptn: &str) -> Option<OwnedFormatItem> {
    let mut subsecond: Subsecond = Default::default();
    subsecond.digits = SubsecondDigits::OneOrMore;
    Some(OwnedFormatItem::Component(Component::Subsecond(subsecond)))
}

fn parse_java_datetime_format_items_recursive(
    chars: &mut std::iter::Peekable<std::str::Chars>,
) -> Result<Vec<OwnedFormatItem>, String> {
    let mut items = Vec::new();

    while let Some(&c) = chars.peek() {
        match c {
            '[' => {
                chars.next();
                let optional_items = parse_java_datetime_format_items_recursive(chars)?;
                items.push(OwnedFormatItem::Optional(Box::new(
                    OwnedFormatItem::Compound(optional_items.into_boxed_slice()),
                )));
            }
            ']' => {
                chars.next();
                break;
            }
            '\'' => {
                chars.next();
                let mut literal_str = String::new();
                while let Some(&next_c) = chars.peek() {
                    if next_c == '\'' {
                        chars.next();
                        break;
                    } else {
                        literal_str.push(next_c);
                        chars.next();
                    }
                }
                items.push(literal(literal_str.as_bytes()));
            }
            _ => {
                if let Some(format_item) = match_java_date_format_token(chars)? {
                    items.push(format_item);
                } else {
                    // Treat as a literal character
                    items.push(literal(c.to_string().as_bytes()));
                    chars.next();
                }
            }
        }
    }

    Ok(items)
}

// Elasticsearch/OpenSearch uses a set of preconfigured formats, more information could be found
// here https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html
fn match_java_date_format_token(
    chars: &mut std::iter::Peekable<std::str::Chars>,
) -> Result<Option<OwnedFormatItem>, String> {
    if chars.peek().is_none() {
        return Ok(None);
    }

    let remaining: String = chars.clone().collect();

    // Try to match the longest possible token
    for token in JAVA_DATE_FORMAT_TOKENS {
        if remaining.starts_with(token) {
            for _ in 0..token.len() {
                chars.next();
            }

            let format_item = match *token {
                "yyyy" | "xxxx" => year_item(),
                "MM" | "M" => build_month_item(token),
                "dd" | "d" => build_day_item(token),
                "HH" | "H" => build_hour_item(token),
                "mm" | "m" => build_minute_item(token),
                "ss" | "s" => build_second_item(token),
                "SSSSSSSSS" | "SSSSSSS" | "SSSSSS" | "SSSSS" | "SSSS" | "SSS" | "SS" | "S" => {
                    build_fraction_of_second_item(token)
                }
                "Z" => build_zone_offset(token),
                "ww" | "w[w]" | "w" => build_week_of_year_item(token),
                "e" => build_day_of_week_item(token),
                _ => return Err(format!("unrecognized token '{token}'")),
            };
            return Ok(format_item);
        }
    }

    Ok(None)
}

// Check if the given date time format is a common alias and replace it with the
// Java date format it is mapped to, if any.
// If the java_datetime_format is not an alias, it is expected to be a
// java date time format and should be returned as is.
fn resolve_java_datetime_format_alias(java_datetime_format: &str) -> &str {
    static JAVA_DATE_FORMAT_ALIASES: OnceLock<HashMap<&'static str, &'static str>> =
        OnceLock::new();
    let java_datetime_format_map = JAVA_DATE_FORMAT_ALIASES.get_or_init(|| {
        let mut m = HashMap::new();
        m.insert("date_optional_time", "yyyy-MM-dd['T'HH:mm:ss.SSSZ]");
        m.insert(
            "strict_date_optional_time",
            "yyyy[-MM[-dd['T'HH[:mm[:ss[.SSS[Z]]]]]]]",
        );
        m.insert(
            "strict_date_optional_time_nanos",
            "yyyy[-MM[-dd['T'HH:mm:ss.SSSSSSZ]]]",
        );
        m.insert("basic_date", "yyyyMMdd");

        m.insert("strict_basic_week_date", "xxxx'W'wwe");
        m.insert("basic_week_date", "xxxx'W'wwe");

        m.insert("strict_basic_week_date_time", "xxxx'W'wwe'T'HHmmss.SSSZ");
        m.insert("basic_week_date_time", "xxxx'W'wwe'T'HHmmss.SSSZ");

        m.insert(
            "strict_basic_week_date_time_no_millis",
            "xxxx'W'wwe'T'HHmmssZ",
        );
        m.insert("basic_week_date_time_no_millis", "xxxx'W'wwe'T'HHmmssZ");

        m.insert("strict_week_date", "xxxx-'W'ww-e");
        m.insert("week_date", "xxxx-'W'w[w]-e");
        m
    });
    java_datetime_format_map
        .get(java_datetime_format)
        .copied()
        .unwrap_or(java_datetime_format)
}

/// A date time parser that holds the format specification `Vec<FormatItem>`.
#[derive(Clone)]
pub struct StrptimeParser {
    pub(crate) strptime_format: String,
    items: Box<[OwnedFormatItem]>,
}

pub fn parse_java_datetime_format_items(
    java_datetime_format: &str,
) -> Result<Box<[OwnedFormatItem]>, String> {
    let mut chars = java_datetime_format.chars().peekable();
    let items = parse_java_datetime_format_items_recursive(&mut chars)?;
    Ok(items.into_boxed_slice())
}

impl StrptimeParser {
    /// Parse a date assume UTC if unspecified.
    /// See `parse_date_time_with_default_timezone` for more details.
    pub fn parse_date_time(&self, date_time_str: &str) -> Result<OffsetDateTime, String> {
        self.parse_date_time_with_default_timezone(date_time_str, UtcOffset::UTC)
    }

    /// Parse a date. If no timezone is specified we will assume the timezone passed as
    /// `default_offset`. If the date is missing, it will be automatically set to 00:00:00.
    pub fn parse_date_time_with_default_timezone(
        &self,
        date_time_str: &str,
        default_offset: UtcOffset,
    ) -> Result<OffsetDateTime, String> {
        let mut parsed = Parsed::new();
        if !parsed
            .parse_items(date_time_str.as_bytes(), &self.items)
            .map_err(|err| err.to_string())?
            .is_empty()
        {
            return Err(format!(
                "datetime string `{date_time_str}` does not match strptime format `{}`",
                self.strptime_format
            ));
        }

        // The parsed datetime contains a date but seems to be missing "time".
        // We complete it artificially with 00:00:00.
        if parsed.hour_24().is_none()
            && !(parsed.hour_12().is_some() && parsed.hour_12_is_pm().is_some())
        {
            parsed.set_hour_24(0u8);
            parsed.set_minute(0u8);
            parsed.set_second(0u8);
        }

        if parsed.year().is_none() {
            let now = OffsetDateTime::now_utc();
            let year = date_time_format::infer_year(parsed.month(), now.month(), now.year());
            parsed.set_year(year);
        }

        if parsed.day().is_none() && parsed.monday_week_number().is_none() {
            parsed.set_day(NonZeroU8::try_from(1u8).unwrap());
        }

        if parsed.month().is_none() && parsed.monday_week_number().is_none() {
            parsed.set_month(Month::January);
        }

        if parsed.offset_hour().is_some() {
            let offset_datetime: OffsetDateTime = parsed
                .try_into()
                .map_err(|err: TryFromParsed| err.to_string())?;
            return Ok(offset_datetime);
        }
        let primitive_date_time: PrimitiveDateTime = parsed
            .try_into()
            .map_err(|err: TryFromParsed| err.to_string())?;
        Ok(primitive_date_time.assume_offset(default_offset))
    }

    pub fn format_date_time(&self, date_time: &OffsetDateTime) -> Result<String, Format> {
        date_time.format(&self.items)
    }

    pub fn from_strptime(strptime_format: &str) -> Result<StrptimeParser, String> {
        let items: Box<[OwnedFormatItem]> = parse_to_format_item(strptime_format)
            .map_err(|err| format!("invalid strptime format `{strptime_format}`: {err}"))?
            .into_iter()
            .map(|item| item.into())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(StrptimeParser::new(strptime_format.to_string(), items))
    }

    pub fn from_java_datetime_format(java_datetime_format: &str) -> Result<StrptimeParser, String> {
        let java_datetime_format_resolved =
            resolve_java_datetime_format_alias(java_datetime_format);
        let items: Box<[OwnedFormatItem]> =
            parse_java_datetime_format_items(java_datetime_format_resolved)?;
        Ok(StrptimeParser::new(java_datetime_format.to_string(), items))
    }

    fn new(strptime_format: String, items: Box<[OwnedFormatItem]>) -> Self {
        StrptimeParser {
            strptime_format,
            items,
        }
    }
}

impl PartialEq for StrptimeParser {
    fn eq(&self, other: &Self) -> bool {
        self.strptime_format == other.strptime_format
    }
}

impl Eq for StrptimeParser {}

impl std::fmt::Debug for StrptimeParser {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter
            .debug_struct("StrptimeParser")
            .field("format", &self.strptime_format)
            .finish()
    }
}

impl std::hash::Hash for StrptimeParser {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.strptime_format.hash(state);
    }
}

// `Strftime` format special characters.
// These characters are taken from the parsing crate we use for compatibility.
const STRFTIME_FORMAT_MARKERS: [&str; 36] = [
    "%a", "%A", "%b", "%B", "%c", "%C", "%d", "%D", "%e", "%f", "%F", "%h", "%H", "%I", "%j", "%k",
    "%l", "%m", "%M", "%n", "%p", "%P", "%r", "%R", "%S", "%t", "%T", "%U", "%w", "%W", "%x", "%X",
    "%y", "%Y", "%z", "%Z",
];

// Checks if a format contains `strftime` special characters.
pub fn is_strftime_formatting(format_str: &str) -> bool {
    STRFTIME_FORMAT_MARKERS
        .iter()
        .any(|marker| format_str.contains(marker))
}

#[cfg(test)]
mod tests {
    use time::macros::datetime;

    use super::*;
    use crate::java_date_time_format::parse_java_datetime_format_items;

    #[test]
    fn test_parse_datetime_format_missing_time() {
        let parser = StrptimeParser::from_strptime("%Y-%m-%d").unwrap();
        assert_eq!(
            parser.parse_date_time("2021-01-01").unwrap(),
            datetime!(2021-01-01 00:00:00 UTC)
        );
    }

    #[test]
    fn test_parse_datetime_format_strict_on_trailing_data() {
        let parser = StrptimeParser::from_strptime("%Y-%m-%d").unwrap();
        let error = parser.parse_date_time("2021-01-01TABC").unwrap_err();
        assert_eq!(
            error,
            "datetime string `2021-01-01TABC` does not match strptime format `%Y-%m-%d`"
        );
    }

    #[test]
    fn test_parse_strptime_with_timezone() {
        let parser = StrptimeParser::from_strptime("%Y-%m-%dT%H:%M:%S %z").unwrap();
        let offset_datetime = parser
            .parse_date_time("2021-01-01T11:00:03 +07:00")
            .unwrap();
        assert_eq!(offset_datetime, datetime!(2021-01-01 11:00:03 +7));
    }

    #[track_caller]
    fn test_parse_java_datetime_aux(
        java_date_time_format: &str,
        date_str: &str,
        expected_datetime: OffsetDateTime,
    ) {
        let parser = StrptimeParser::from_java_datetime_format(java_date_time_format).unwrap();
        let datetime = parser.parse_date_time(date_str).unwrap();
        assert_eq!(datetime, expected_datetime);
    }

    #[test]
    fn test_parse_java_datetime_format() {
        test_parse_java_datetime_aux("yyyyMMdd", "20210101", datetime!(2021-01-01 00:00:00 UTC));
        test_parse_java_datetime_aux(
            "yyyy MM dd",
            "2021 01 01",
            datetime!(2021-01-01 00:00:00 UTC),
        );
        test_parse_java_datetime_aux(
            "yyyy!MM?dd",
            "2021!01?01",
            datetime!(2021-01-01 00:00:00 UTC),
        );
        test_parse_java_datetime_aux(
            "yyyy!MM?dd'T'HH:",
            "2021!01?01T13:",
            datetime!(2021-01-01 13:00:00 UTC),
        );
        test_parse_java_datetime_aux(
            "yyyy!MM?dd['T'[HH:]]",
            "2021!01?01",
            datetime!(2021-01-01 00:00:00 UTC),
        );
        test_parse_java_datetime_aux(
            "yyyy!MM?dd['T'[HH:]",
            "2021!01?01T",
            datetime!(2021-01-01 00:00:00 UTC),
        );
        test_parse_java_datetime_aux(
            "yyyy!MM?dd['T'[HH:]]",
            "2021!01?01T13:",
            datetime!(2021-01-01 13:00:00 UTC),
        );
    }

    #[test]
    fn test_parse_java_missing_time() {
        test_parse_java_datetime_aux(
            "yyyy-MM-dd",
            "2021-01-01",
            datetime!(2021-01-01 00:00:00 UTC),
        );
    }

    #[test]
    fn test_parse_java_optional_missing_time() {
        test_parse_java_datetime_aux(
            "yyyy-MM-dd[ HH:mm:ss]",
            "2021-01-01",
            datetime!(2021-01-01 00:00:00 UTC),
        );
        test_parse_java_datetime_aux(
            "yyyy-MM-dd[ HH:mm:ss]",
            "2021-01-01 12:34:56",
            datetime!(2021-01-01 12:34:56 UTC),
        );
    }

    #[test]
    fn test_parse_java_datetime_format_aliases() {
        test_parse_java_datetime_aux(
            "date_optional_time",
            "2021-01-01",
            datetime!(2021-01-01 00:00:00 UTC),
        );
        test_parse_java_datetime_aux(
            "date_optional_time",
            "2021-01-21T03:01:22.312+01:00",
            datetime!(2021-01-21 03:01:22.312 +1),
        );
    }

    #[test]
    fn test_parse_java_week_formats() {
        test_parse_java_datetime_aux(
            "basic_week_date",
            "2024W313",
            datetime!(2024-08-01 0:00:00.0 +00:00:00),
        );
        let parser = StrptimeParser::from_java_datetime_format("basic_week_date").unwrap();
        parser.parse_date_time("24W313").unwrap_err();

        let parser = StrptimeParser::from_java_datetime_format("basic_week_date").unwrap();
        parser.parse_date_time("1W313").unwrap_err();

        test_parse_java_datetime_aux(
            "basic_week_date_time",
            "2018W313T121212.1Z",
            datetime!(2018-08-02 12:12:12.1 +00:00:00),
        );
        test_parse_java_datetime_aux(
            "basic_week_date_time",
            "2018W313T121212.123Z",
            datetime!(2018-08-02 12:12:12.123 +00:00:00),
        );
        test_parse_java_datetime_aux(
            "basic_week_date_time",
            "2018W313T121212.123456789Z",
            datetime!(2018-08-02 12:12:12.123456789 +00:00:00),
        );
        test_parse_java_datetime_aux(
            "basic_week_date_time",
            "2018W313T121212.123+0100",
            datetime!(2018-08-02 12:12:12.123 +01:00:00),
        );
        test_parse_java_datetime_aux(
            "basic_week_date_time_no_millis",
            "2018W313T121212Z",
            datetime!(2018-08-02 12:12:12.0 +00:00:00),
        );
        test_parse_java_datetime_aux(
            "basic_week_date_time_no_millis",
            "2018W313T121212+0100",
            datetime!(2018-08-02 12:12:12.0 +01:00:00),
        );
        test_parse_java_datetime_aux(
            "basic_week_date_time_no_millis",
            "2018W313T121212+01:00",
            datetime!(2018-08-02 12:12:12.0 +01:00:00),
        );

        test_parse_java_datetime_aux(
            "week_date",
            "2012-W48-6",
            datetime!(2012-12-02 0:00:00.0 +00:00:00),
        );

        test_parse_java_datetime_aux(
            "week_date",
            "2012-W01-6",
            datetime!(2012-01-08 0:00:00.0 +00:00:00),
        );

        test_parse_java_datetime_aux(
            "week_date",
            "2012-W1-6",
            datetime!(2012-01-08 0:00:00.0 +00:00:00),
        );
    }

    #[test]
    fn test_parse_java_strict_week_formats() {
        test_parse_java_datetime_aux(
            "strict_basic_week_date",
            "2024W313",
            datetime!(2024-08-01 0:00:00.0 +00:00:00),
        );

        test_parse_java_datetime_aux(
            "strict_week_date",
            "2012-W48-6",
            datetime!(2012-12-02 0:00:00.0 +00:00:00),
        );

        test_parse_java_datetime_aux(
            "strict_week_date",
            "2012-W01-6",
            datetime!(2012-01-08 0:00:00.0 +00:00:00),
        );
    }

    #[test]
    fn test_parse_strict_date_optional_time() {
        let parser =
            StrptimeParser::from_java_datetime_format("strict_date_optional_time").unwrap();
        let dates = [
            "2019",
            "2019-03",
            "2019-03-23",
            "2019-03-23T21:34",
            "2019-03-23T21:34:46",
            "2019-03-23T21:34:46.123Z",
            "2019-03-23T21:35:46.123+00:00",
            "2019-03-23T21:36:46.123+03:00",
            "2019-03-23T21:37:46.123+0300",
        ];
        let expected = [
            datetime!(2019-01-01 00:00:00 UTC),
            datetime!(2019-03-01 00:00:00 UTC),
            datetime!(2019-03-23 00:00:00 UTC),
            datetime!(2019-03-23 21:34 UTC),
            datetime!(2019-03-23 21:34:46 UTC),
            datetime!(2019-03-23 21:34:46.123 UTC),
            datetime!(2019-03-23 21:35:46.123 UTC),
            datetime!(2019-03-23 21:36:46.123 +03:00:00),
            datetime!(2019-03-23 21:37:46.123 +03:00:00),
        ];
        for (date_str, &expected_dt) in dates.iter().zip(expected.iter()) {
            let parsed_dt = parser
                .parse_date_time(date_str)
                .unwrap_or_else(|error| panic!("failed to parse {date_str}: {error}"));
            assert_eq!(parsed_dt, expected_dt);
        }
    }

    #[test]
    fn test_parse_strict_date_optional_time_nanos() {
        let parser =
            StrptimeParser::from_java_datetime_format("strict_date_optional_time_nanos").unwrap();
        let dates = [
            "2019",
            "2019-03",
            "2019-03-23",
            "2019-03-23T21:34:46.123456789Z",
            "2019-03-23T21:35:46.123456789+00:00",
            "2019-03-23T21:36:46.123456789+03:00",
            "2019-03-23T21:37:46.123456789+0300",
        ];
        let expected = [
            datetime!(2019-01-01 00:00:00 UTC),
            datetime!(2019-03-01 00:00:00 UTC),
            datetime!(2019-03-23 00:00:00 UTC),
            datetime!(2019-03-23 21:34:46.123456789 UTC),
            datetime!(2019-03-23 21:35:46.123456789 UTC),
            datetime!(2019-03-23 21:36:46.123456789 +03:00:00),
            datetime!(2019-03-23 21:37:46.123456789 +03:00:00),
        ];
        for (date_str, &expected_dt) in dates.iter().zip(expected.iter()) {
            let parsed_dt = parser
                .parse_date_time(date_str)
                .unwrap_or_else(|error| panic!("failed to parse {date_str}: {error}"));
            assert_eq!(parsed_dt, expected_dt);
        }
    }

    #[test]
    fn test_parse_java_datetime_format_items() {
        let format_str = "xxxx'W'wwe";
        let result = parse_java_datetime_format_items(format_str).unwrap();

        // We expect the tokens to be parsed as:
        // - 'xxxx' (week-based year)
        // - 'W' (literal)
        // - 'ww' (week of year)
        // - 'e' (day of week)

        assert_eq!(result.len(), 4);

        // Verify each token
        match &result[0] {
            OwnedFormatItem::Component(Component::Year(year)) => {
                assert_eq!(year.repr, YearRepr::Full);
            }
            unexpected => panic!("expected Year, but found: {unexpected:?}",),
        }
        match &result[1] {
            OwnedFormatItem::Literal(lit) => assert_eq!(lit.as_ref(), b"W"),
            unexpected => panic!("expected literal 'W', but found: {unexpected:?}"),
        }
        match &result[2] {
            OwnedFormatItem::Component(Component::WeekNumber(_)) => {}
            unexpected => panic!("expected WeekNumber component, but found: {unexpected:?}"),
        }
        match &result[3] {
            OwnedFormatItem::Component(Component::Weekday(_)) => {}
            unexpected => panic!("expected Weekday component, but found: {unexpected:?}"),
        }
    }

    #[test]
    fn test_parse_java_datetime_format_with_literals() {
        let format = "yyyy'T'Z-HHuu";
        let parser = StrptimeParser::from_java_datetime_format(format).unwrap();

        let test_cases = [
            ("2023TZ-14uu", datetime!(2023-01-01 14:00:00 UTC)),
            ("2024TZ-05uu", datetime!(2024-01-01 05:00:00 UTC)),
            ("2025TZ-23uu", datetime!(2025-01-01 23:00:00 UTC)),
        ];

        for (input, expected) in test_cases.iter() {
            let result = parser.parse_date_time(input).unwrap();
            assert_eq!(result, *expected, "failed to parse {input}");
        }

        // Test error case
        let error_case = "2023-1430";
        assert!(
            parser.parse_date_time(error_case).is_err(),
            "expected error for input: {error_case}",
        );
    }
}
