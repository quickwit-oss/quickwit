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
    Day, Hour, Minute, Month as MonthModifier, OffsetHour, OffsetMinute, Padding, Second,
    Subsecond, SubsecondDigits, WeekNumber, WeekNumberRepr, Weekday, WeekdayRepr, Year, YearRepr,
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
    "XXXXX", // ISO 8601 offset with colon or 'Z'
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

    // Offset in '+/-HH' format (abbreviated, hour only)
    let offset_hour_only = OwnedFormatItem::Component(Component::OffsetHour(Default::default()));

    Some(OwnedFormatItem::First(
        vec![
            z_literal,
            offset_with_delimiter_compound,
            offset_compound,
            offset_hour_only,
        ]
        .into_boxed_slice(),
    ))
}

/// Build the ES specific formatting: always outputs '+HH:MM' format
///
/// NOTE: Unfortunately we cannot have a conditional forwarding that replaces
/// +00:00 with 'Z' for UTC as ES does. We perform this replacement after the
/// formatting.
fn build_iso8601_zone_offset_for_formatting() -> Option<OwnedFormatItem> {
    // Configure OffsetHour to always show the sign
    let mut offset_hour_mod = OffsetHour::default();
    offset_hour_mod.sign_is_mandatory = true;
    offset_hour_mod.padding = Padding::Zero;

    let mut offset_minute_mod = OffsetMinute::default();
    offset_minute_mod.padding = Padding::Zero;

    let offset_with_delimiter_items: Box<[OwnedFormatItem]> = vec![
        OwnedFormatItem::Component(Component::OffsetHour(offset_hour_mod)),
        OwnedFormatItem::Literal(Box::from(b":".as_ref())),
        OwnedFormatItem::Component(Component::OffsetMinute(offset_minute_mod)),
    ]
    .into_boxed_slice();
    Some(OwnedFormatItem::Compound(offset_with_delimiter_items))
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

fn build_fraction_of_second_item_for_parsing() -> Option<OwnedFormatItem> {
    let mut subsecond: Subsecond = Default::default();
    // For parsing, use OneOrMore to accept variable precision
    subsecond.digits = SubsecondDigits::OneOrMore;
    Some(OwnedFormatItem::Component(Component::Subsecond(subsecond)))
}

// Build fractional seconds with fixed precision based on pattern length
fn build_fraction_of_second_item_for_formatting(ptn: &str) -> Option<OwnedFormatItem> {
    use time::format_description::modifier::SubsecondDigits;

    let mut subsecond: Subsecond = Default::default();
    // Use pattern length to determine fixed precision for formatting
    subsecond.digits = match ptn.len() {
        1 => SubsecondDigits::One,
        2 => SubsecondDigits::Two,
        3 => SubsecondDigits::Three,
        4 => SubsecondDigits::Four,
        5 => SubsecondDigits::Five,
        6 => SubsecondDigits::Six,
        7 => SubsecondDigits::Seven,
        8 => SubsecondDigits::Eight,
        9 => SubsecondDigits::Nine,
        _ => SubsecondDigits::OneOrMore,
    };
    Some(OwnedFormatItem::Component(Component::Subsecond(subsecond)))
}

fn parse_java_datetime_format_items_recursive(
    chars: &mut std::iter::Peekable<std::str::Chars>,
    for_formatting: bool,
) -> Result<Vec<OwnedFormatItem>, String> {
    let mut items = Vec::new();

    while let Some(&c) = chars.peek() {
        match c {
            '[' => {
                chars.next();
                let optional_items =
                    parse_java_datetime_format_items_recursive(chars, for_formatting)?;
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
                if let Some(format_item) = match_java_date_format_token(chars, for_formatting)? {
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
    for_formatting: bool,
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
                    if for_formatting {
                        build_fraction_of_second_item_for_formatting(token)
                    } else {
                        build_fraction_of_second_item_for_parsing()
                    }
                }
                "XXXXX" => {
                    if for_formatting {
                        build_iso8601_zone_offset_for_formatting()
                    } else {
                        return Err("XXXXX pattern is only supported for formatting.".to_string());
                    }
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
fn resolve_java_datetime_format_alias_for_parsing(java_datetime_format: &str) -> &str {
    static JAVA_DATE_FORMAT_ALIASES: OnceLock<HashMap<&'static str, &'static str>> =
        OnceLock::new();
    let java_datetime_format_map = JAVA_DATE_FORMAT_ALIASES.get_or_init(|| {
        let mut m = HashMap::new();
        m.insert(
            "date_optional_time",
            "yyyy[-MM[-dd['T'HH[Z][:mm[Z][:ss[.SSS][Z]]]]]]",
        );
        m.insert(
            "strict_date_optional_time",
            "yyyy[-MM[-dd['T'HH[Z][:mm[Z][:ss[.SSS][Z]]]]]]",
        );
        m.insert(
            "strict_date_optional_time_nanos",
            "yyyy[-MM[-dd['T'HH[Z][:mm[Z][:ss[.SSSSSS][Z]]]]]]",
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

fn resolve_java_datetime_format_alias_for_formatting(java_datetime_format: &str) -> &str {
    static JAVA_DATE_FORMAT_ALIASES_FORMATTING: OnceLock<HashMap<&'static str, &'static str>> =
        OnceLock::new();
    let java_datetime_format_map = JAVA_DATE_FORMAT_ALIASES_FORMATTING.get_or_init(|| {
        let mut m = HashMap::new();
        // For strict_date_optional_time, format with full date-time and milliseconds
        m.insert(
            "strict_date_optional_time",
            "yyyy-MM-dd'T'HH:mm:ss.SSSXXXXX",
        );
        // For strict_date_optional_time_nanos, format with full date-time and nanoseconds
        m.insert(
            "strict_date_optional_time_nanos",
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXXXX",
        );
        // date_optional_time uses the same format as strict variant
        m.insert("date_optional_time", "yyyy-MM-dd'T'HH:mm:ss.SSSXXXXX");
        // Other formats that don't have complex optional structures can use their parse patterns
        m.insert("basic_date", "yyyyMMdd");
        m.insert("strict_basic_week_date", "xxxx'W'wwe");
        m.insert("basic_week_date", "xxxx'W'wwe");
        m.insert(
            "strict_basic_week_date_time",
            "xxxx'W'wwe'T'HHmmss.SSSXXXXX",
        );
        m.insert("basic_week_date_time", "xxxx'W'wwe'T'HHmmss.SSSXXXXX");
        m.insert(
            "strict_basic_week_date_time_no_millis",
            "xxxx'W'wwe'T'HHmmssXXXXX",
        );
        m.insert("basic_week_date_time_no_millis", "xxxx'W'wwe'T'HHmmssXXXXX");
        m.insert("strict_week_date", "xxxx-'W'ww-e");
        m.insert("week_date", "xxxx-'W'ww-e");
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
    format_items: Box<[OwnedFormatItem]>,
}

pub fn parse_java_datetime_format_items(
    java_datetime_format: &str,
) -> Result<Box<[OwnedFormatItem]>, String> {
    let mut chars = java_datetime_format.chars().peekable();
    let items = parse_java_datetime_format_items_recursive(&mut chars, false)?;
    Ok(items.into_boxed_slice())
}

// Parse format items with fixed precision for formatting output
fn parse_java_datetime_format_items_for_formatting(
    java_datetime_format: &str,
) -> Result<Box<[OwnedFormatItem]>, String> {
    let mut chars = java_datetime_format.chars().peekable();
    let items = parse_java_datetime_format_items_recursive(&mut chars, true)?;
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
        // Elasticsearch/OpenSearch support comma as a decimal separator for fractional seconds
        // (in addition to period). The time crate only supports period, so we normalize
        // comma to period before parsing. This is safe because comma doesn't appear in
        // other parts of the format.
        let normalized_date_time_str;
        let date_time_str_to_parse = if date_time_str.contains(',') {
            normalized_date_time_str = date_time_str.replace(',', ".");
            &normalized_date_time_str
        } else {
            date_time_str
        };

        let mut parsed = Parsed::new();
        if !parsed
            .parse_items(date_time_str_to_parse.as_bytes(), &self.items)
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
        let mut formatted = date_time.format(&self.format_items)?;
        // For ES/Java ISO 8601 compatibility: replace '+00:00' with 'Z' for UTC.
        // The time crate doesn't support conditional 'Z' in format items, so we handle it manually
        // here. Since the offset is always a suffix, we can efficiently truncate and append.
        if date_time.offset() == UtcOffset::UTC && formatted.ends_with("+00:00") {
            formatted.truncate(formatted.len() - 6);
            formatted.push('Z');
        }
        Ok(formatted)
    }

    pub fn from_strptime(strptime_format: &str) -> Result<StrptimeParser, String> {
        let items: Box<[OwnedFormatItem]> = parse_to_format_item(strptime_format)
            .map_err(|err| format!("invalid strptime format `{strptime_format}`: {err}"))?
            .into_iter()
            .map(|item| item.into())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        // For strptime, use the same items for both parsing and formatting
        let format_items = items.clone();
        Ok(StrptimeParser::new(
            strptime_format.to_string(),
            items,
            format_items,
        ))
    }

    pub fn from_java_datetime_format(java_datetime_format: &str) -> Result<StrptimeParser, String> {
        let java_datetime_format_resolved =
            resolve_java_datetime_format_alias_for_parsing(java_datetime_format);
        let items: Box<[OwnedFormatItem]> =
            parse_java_datetime_format_items(java_datetime_format_resolved)?;

        // Get format-specific pattern and create format items
        let java_datetime_format_for_formatting =
            resolve_java_datetime_format_alias_for_formatting(java_datetime_format);
        let format_items: Box<[OwnedFormatItem]> =
            parse_java_datetime_format_items_for_formatting(java_datetime_format_for_formatting)?;

        Ok(StrptimeParser::new(
            java_datetime_format.to_string(),
            items,
            format_items,
        ))
    }

    fn new(
        strptime_format: String,
        items: Box<[OwnedFormatItem]>,
        format_items: Box<[OwnedFormatItem]>,
    ) -> Self {
        StrptimeParser {
            strptime_format,
            items,
            format_items,
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
            "2019-03-23T21:38:46+00:00",
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
            datetime!(2019-03-23 21:38:46 UTC),
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
            "2019-03-23T21:38:46+00:00",
        ];
        let expected = [
            datetime!(2019-01-01 00:00:00 UTC),
            datetime!(2019-03-01 00:00:00 UTC),
            datetime!(2019-03-23 00:00:00 UTC),
            datetime!(2019-03-23 21:34:46.123456789 UTC),
            datetime!(2019-03-23 21:35:46.123456789 UTC),
            datetime!(2019-03-23 21:36:46.123456789 +03:00:00),
            datetime!(2019-03-23 21:37:46.123456789 +03:00:00),
            datetime!(2019-03-23 21:38:46 UTC),
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

/// Tests ported from Elasticsearch's `DateFormattersTests.java` to ensure maximum
/// compatibility with their date parsing behavior.
#[cfg(test)]
mod tests_parsing_ported_from_es {
    use time::macros::datetime;

    use super::*;

    #[test]
    fn test_strict_date_optional_time_comprehensive() {
        let parser =
            StrptimeParser::from_java_datetime_format("strict_date_optional_time").unwrap();

        // Comprehensive test cases from Elasticsearch
        let test_cases = [
            // Date only
            ("2018-12-31", datetime!(2018-12-31 00:00:00 UTC)),
            // Date with time
            ("2010-01-05T02:00", datetime!(2010-01-05 02:00:00 UTC)),
            ("2018-12-31T10:15:30", datetime!(2018-12-31 10:15:30 UTC)),
            // With UTC timezone
            ("2018-12-31T10:15:30Z", datetime!(2018-12-31 10:15:30 UTC)),
            ("2015-01-04T00:00Z", datetime!(2015-01-04 00:00:00 UTC)),
            // With numeric timezones (compact)
            ("2016-11-30T00+01", datetime!(2016-11-30 00:00:00 +01:00:00)),
            (
                "2016-11-30T00+0100",
                datetime!(2016-11-30 00:00:00 +01:00:00),
            ),
            (
                "2018-12-31T10:15:30+0100",
                datetime!(2018-12-31 10:15:30 +01:00:00),
            ),
            // With numeric timezones (colon-separated)
            (
                "2016-11-30T00+01:00",
                datetime!(2016-11-30 00:00:00 +01:00:00),
            ),
            (
                "2018-12-31T10:15:30+01:00",
                datetime!(2018-12-31 10:15:30 +01:00:00),
            ),
            // With milliseconds
            (
                "2018-12-31T10:15:30.1Z",
                datetime!(2018-12-31 10:15:30.1 UTC),
            ),
            (
                "2018-12-31T10:15:30.123Z",
                datetime!(2018-12-31 10:15:30.123 UTC),
            ),
            (
                "2018-12-31T10:15:30.1+0100",
                datetime!(2018-12-31 10:15:30.1 +01:00:00),
            ),
            (
                "2018-12-31T10:15:30.123+0100",
                datetime!(2018-12-31 10:15:30.123 +01:00:00),
            ),
            (
                "2018-12-31T10:15:30.123+01:00",
                datetime!(2018-12-31 10:15:30.123 +01:00:00),
            ),
            // Partial dates
            ("2001", datetime!(2001-01-01 00:00:00 UTC)),
            ("2001-01", datetime!(2001-01-01 00:00:00 UTC)),
            ("2001-01-01", datetime!(2001-01-01 00:00:00 UTC)),
        ];

        for (input, expected) in test_cases.iter() {
            let parsed = parser
                .parse_date_time(input)
                .unwrap_or_else(|err| panic!("failed to parse {input}: {err}"));
            assert_eq!(parsed, *expected, "mismatch for input: {input}");
        }
    }

    #[test]
    fn test_strict_date_optional_time_nanos_comprehensive() {
        let parser =
            StrptimeParser::from_java_datetime_format("strict_date_optional_time_nanos").unwrap();

        // Comprehensive test cases with nanosecond precision
        let test_cases = [
            // From Elasticsearch test suite
            (
                "2016-01-01T00:00:00.000",
                datetime!(2016-01-01 00:00:00.0 UTC),
            ),
            ("2018-05-15T17:14:56", datetime!(2018-05-15 17:14:56 UTC)),
            ("2018-05-15T17:14:56Z", datetime!(2018-05-15 17:14:56 UTC)),
            (
                "2018-05-15T17:14:56+0100",
                datetime!(2018-05-15 17:14:56 +01:00:00),
            ),
            (
                "2018-05-15T17:14:56+01:00",
                datetime!(2018-05-15 17:14:56 +01:00:00),
            ),
            (
                "2022-12-16T10:00:57.149001+00:00",
                datetime!(2022-12-16 10:00:57.149001 UTC),
            ),
            (
                "2018-05-15T17:14:56.123456789+0100",
                datetime!(2018-05-15 17:14:56.123456789 +01:00:00),
            ),
            (
                "2018-05-15T17:14:56.123456789+01:00",
                datetime!(2018-05-15 17:14:56.123456789 +01:00:00),
            ),
            // Fractional second precision tests (1-9 digits)
            (
                "2019-05-06T14:52:37.1Z",
                datetime!(2019-05-06 14:52:37.1 UTC),
            ),
            (
                "2019-05-06T14:52:37.12Z",
                datetime!(2019-05-06 14:52:37.12 UTC),
            ),
            (
                "2019-05-06T14:52:37.123Z",
                datetime!(2019-05-06 14:52:37.123 UTC),
            ),
            (
                "2019-05-06T14:52:37.1234Z",
                datetime!(2019-05-06 14:52:37.1234 UTC),
            ),
            (
                "2019-05-06T14:52:37.12345Z",
                datetime!(2019-05-06 14:52:37.12345 UTC),
            ),
            (
                "2019-05-06T14:52:37.123456Z",
                datetime!(2019-05-06 14:52:37.123456 UTC),
            ),
            (
                "2019-05-06T14:52:37.1234567Z",
                datetime!(2019-05-06 14:52:37.1234567 UTC),
            ),
            (
                "2019-05-06T14:52:37.12345678Z",
                datetime!(2019-05-06 14:52:37.12345678 UTC),
            ),
            (
                "2019-05-06T14:52:37.123456789Z",
                datetime!(2019-05-06 14:52:37.123456789 UTC),
            ),
            // Edge case: 1 nanosecond
            (
                "1970-01-01T00:00:00.000000001",
                datetime!(1970-01-01 00:00:00.000000001 UTC),
            ),
        ];

        for (input, expected) in test_cases.iter() {
            let parsed = parser
                .parse_date_time(input)
                .unwrap_or_else(|err| panic!("failed to parse {input}: {err}"));
            assert_eq!(parsed, *expected, "mismatch for input: {input}");
        }
    }

    // Additional comprehensive tests including time variations
    #[test]
    fn test_strict_date_optional_time_variations() {
        let parser =
            StrptimeParser::from_java_datetime_format("strict_date_optional_time").unwrap();

        let test_cases = [
            // Time without timezone
            ("2016-11-30T12", datetime!(2016-11-30 12:00:00 UTC)),
            ("2016-11-30T12:00", datetime!(2016-11-30 12:00:00 UTC)),
            ("2016-11-30T12:00:00", datetime!(2016-11-30 12:00:00 UTC)),
            (
                "2016-11-30T12:00:00.000",
                datetime!(2016-11-30 12:00:00.0 UTC),
            ),
            // Hour with timezone (abbreviated formats)
            ("2016-11-30T12+01", datetime!(2016-11-30 12:00:00 +01:00:00)),
            (
                "2016-11-30T12+0100",
                datetime!(2016-11-30 12:00:00 +01:00:00),
            ),
            (
                "2016-11-30T12+01:00",
                datetime!(2016-11-30 12:00:00 +01:00:00),
            ),
            // Hour:minute with timezone
            (
                "2016-11-30T12:00+01",
                datetime!(2016-11-30 12:00:00 +01:00:00),
            ),
            (
                "2016-11-30T12:00+0100",
                datetime!(2016-11-30 12:00:00 +01:00:00),
            ),
            (
                "2016-11-30T12:00+01:00",
                datetime!(2016-11-30 12:00:00 +01:00:00),
            ),
            // Full time with timezone
            (
                "2016-11-30T12:00:00+01",
                datetime!(2016-11-30 12:00:00 +01:00:00),
            ),
            (
                "2016-11-30T12:00:00+0100",
                datetime!(2016-11-30 12:00:00 +01:00:00),
            ),
            (
                "2016-11-30T12:00:00+01:00",
                datetime!(2016-11-30 12:00:00 +01:00:00),
            ),
            // Milliseconds with timezone
            (
                "2016-11-30T12:00:00.000+01",
                datetime!(2016-11-30 12:00:00.0 +01:00:00),
            ),
            (
                "2016-11-30T12:00:00.000+0100",
                datetime!(2016-11-30 12:00:00.0 +01:00:00),
            ),
            (
                "2016-11-30T12:00:00.000+01:00",
                datetime!(2016-11-30 12:00:00.0 +01:00:00),
            ),
        ];

        for (input, expected) in test_cases.iter() {
            let parsed = parser
                .parse_date_time(input)
                .unwrap_or_else(|err| panic!("failed to parse {input}: {err}"));
            assert_eq!(parsed, *expected, "mismatch for input: {input}");
        }
    }

    // Test error cases - strict parsing should reject malformed inputs
    #[test]
    fn test_strict_date_optional_time_error_cases() {
        let parser =
            StrptimeParser::from_java_datetime_format("strict_date_optional_time").unwrap();

        // These should all fail to parse
        let error_cases = [
            // Timezone without time component
            "2016-11-30T+01",
            // Non-zero-padded time components
            "2018-12-31T9:15:30", // hour not padded
            "2018-12-31T10:5:30", // minute not padded
            "2018-12-31T10:15:3", // second not padded
            // Non-zero-padded date components
            "2018-12-1", // day not padded
            "2018-1-31", // month not padded
            // 5-digit year (out of range)
            "10000-01-31",
        ];

        for input in error_cases.iter() {
            assert!(
                parser.parse_date_time(input).is_err(),
                "Expected parsing to fail for input: {input}"
            );
        }
    }

    #[test]
    fn test_strict_date_optional_time_fractional_seconds() {
        // the difference between strict_date_optional_time and
        // strict_date_optional_time_nanos is formatting, not parsing, ES
        // accepts all precisions even with strict_date_optional_time (see
        // testFractionalSeconds in ES's DateFormattersTests.java)
        let parser =
            StrptimeParser::from_java_datetime_format("strict_date_optional_time").unwrap();

        // Test various fractional second precisions (1-9 digits)
        let test_cases = [
            ("2019-05-06T14:52:37.1Z", 100_000_000),
            ("2019-05-06T14:52:37.12Z", 120_000_000),
            ("2019-05-06T14:52:37.123Z", 123_000_000),
            ("2019-05-06T14:52:37.1234Z", 123_400_000),
            ("2019-05-06T14:52:37.12345Z", 123_450_000),
            ("2019-05-06T14:52:37.123456Z", 123_456_000),
            ("2019-05-06T14:52:37.1234567Z", 123_456_700),
            ("2019-05-06T14:52:37.12345678Z", 123_456_780),
            ("2019-05-06T14:52:37.123456789Z", 123_456_789),
        ];

        for (input, expected_nanos) in test_cases.iter() {
            let parsed = parser
                .parse_date_time(input)
                .unwrap_or_else(|err| panic!("failed to parse {input}: {err}"));
            assert_eq!(
                parsed.nanosecond(),
                *expected_nanos,
                "mismatch for input: {input}"
            );
        }
    }

    // Test decimal point variations (comma vs period)
    #[test]
    fn test_decimal_point_parsing() {
        let parser =
            StrptimeParser::from_java_datetime_format("strict_date_optional_time").unwrap();

        // Period as decimal separator (standard)
        let result = parser.parse_date_time("2001-01-01T00:00:00.123Z");
        assert!(result.is_ok(), "Failed to parse with period separator");
        assert_eq!(result.unwrap(), datetime!(2001-01-01 00:00:00.123 UTC));

        // Comma as decimal separator (some locales)
        // Elasticsearch/OpenSearch support both period and comma
        let result = parser.parse_date_time("2001-01-01T00:00:00,123Z");
        assert!(result.is_ok(), "Failed to parse with comma separator");
        assert_eq!(result.unwrap(), datetime!(2001-01-01 00:00:00.123 UTC));

        // Test comma with different precision levels
        let test_cases = [
            (
                "2019-05-06T14:52:37,1Z",
                datetime!(2019-05-06 14:52:37.1 UTC),
            ),
            (
                "2019-05-06T14:52:37,12Z",
                datetime!(2019-05-06 14:52:37.12 UTC),
            ),
            (
                "2019-05-06T14:52:37,123Z",
                datetime!(2019-05-06 14:52:37.123 UTC),
            ),
            (
                "2018-12-31T10:15:30,123+01:00",
                datetime!(2018-12-31 10:15:30.123 +01:00:00),
            ),
        ];

        for (input, expected) in test_cases.iter() {
            let parsed = parser
                .parse_date_time(input)
                .unwrap_or_else(|err| panic!("failed to parse {input}: {err}"));
            assert_eq!(parsed, *expected, "mismatch for input: {input}");
        }
    }

    #[test]
    fn test_basic_date() {
        let parser = StrptimeParser::from_java_datetime_format("basic_date").unwrap();

        let test_cases = [
            ("20181126", datetime!(2018-11-26 00:00:00 UTC)),
            ("20210101", datetime!(2021-01-01 00:00:00 UTC)),
            ("19991231", datetime!(1999-12-31 00:00:00 UTC)),
        ];

        for (input, expected) in test_cases.iter() {
            let parsed = parser
                .parse_date_time(input)
                .unwrap_or_else(|err| panic!("failed to parse {input}: {err}"));
            assert_eq!(parsed, *expected, "mismatch for input: {input}");
        }
    }

    #[test]
    fn test_week_date_formats() {
        // basic_week_date with 4-digit years
        let basic_parser = StrptimeParser::from_java_datetime_format("basic_week_date").unwrap();

        let basic_cases = [
            ("2018W313", datetime!(2018-08-02 00:00:00 UTC)),
            ("2024W011", datetime!(2024-01-02 00:00:00 UTC)),
        ];

        for (input, expected) in basic_cases.iter() {
            let parsed = basic_parser
                .parse_date_time(input)
                .unwrap_or_else(|err| panic!("failed to parse {input}: {err}"));
            assert_eq!(parsed, *expected, "mismatch for input: {input}");
        }

        // strict_basic_week_date requires exactly 4-digit year
        let strict_parser =
            StrptimeParser::from_java_datetime_format("strict_basic_week_date").unwrap();

        assert!(strict_parser.parse_date_time("2018W313").is_ok());
        assert!(strict_parser.parse_date_time("2024W011").is_ok());
        assert!(strict_parser.parse_date_time("18W313").is_err());

        // ES allows 1-2 digit years for basic_week_date but our implementation
        // currently requires 4 digits like the strict variant
        // TODO: implement flexible year parsing to match ES behavior for:
        // - "1W313" (1-digit year)
        // - "18W313" (2-digit year)
    }

    #[test]
    fn test_week_date_time_formats() {
        let basic_parser =
            StrptimeParser::from_java_datetime_format("basic_week_date_time").unwrap();

        let test_cases = [
            ("2018W313T121212.1Z", datetime!(2018-08-02 12:12:12.1 UTC)),
            (
                "2018W313T121212.1+0100",
                datetime!(2018-08-02 12:12:12.1 +01:00:00),
            ),
            (
                "2018W313T121212.123Z",
                datetime!(2018-08-02 12:12:12.123 UTC),
            ),
            (
                "2018W313T121212.123456789Z",
                datetime!(2018-08-02 12:12:12.123456789 UTC),
            ),
            (
                "2018W313T121212.123+0100",
                datetime!(2018-08-02 12:12:12.123 +01:00:00),
            ),
            (
                "2018W313T121212.123+01:00",
                datetime!(2018-08-02 12:12:12.123 +01:00:00),
            ),
        ];

        for (input, expected) in test_cases.iter() {
            let parsed = basic_parser
                .parse_date_time(input)
                .unwrap_or_else(|err| panic!("failed to parse {input}: {err}"));
            assert_eq!(parsed, *expected, "mismatch for input: {input}");
        }

        // Test strict variant
        let strict_parser =
            StrptimeParser::from_java_datetime_format("strict_basic_week_date_time").unwrap();

        // Additional test case from ES testStrictParsing
        let parsed = strict_parser
            .parse_date_time("2018W313T121212.1+01:00")
            .unwrap();
        assert_eq!(parsed, datetime!(2018-08-02 12:12:12.1 +01:00:00));

        for (input, expected) in test_cases.iter() {
            let parsed = strict_parser
                .parse_date_time(input)
                .unwrap_or_else(|err| panic!("failed to parse {input}: {err}"));
            assert_eq!(parsed, *expected, "mismatch for input: {input}");
        }

        // Test strict error cases - invalid time components
        assert!(
            strict_parser
                .parse_date_time("2018W313T12128.123Z")
                .is_err()
        );
        assert!(
            strict_parser
                .parse_date_time("2018W313T81212.123Z")
                .is_err()
        );
        assert!(
            strict_parser
                .parse_date_time("2018W313T12812.123Z")
                .is_err()
        );
        assert!(strict_parser.parse_date_time("2018W313T12812.1Z").is_err());
        assert!(
            strict_parser
                .parse_date_time("2018W313T12128.123456789Z")
                .is_err()
        );
    }

    #[test]
    fn test_week_date_time_no_millis() {
        let basic_parser =
            StrptimeParser::from_java_datetime_format("basic_week_date_time_no_millis").unwrap();

        let test_cases = [
            ("2018W313T121212Z", datetime!(2018-08-02 12:12:12 UTC)),
            (
                "2018W313T121212+0100",
                datetime!(2018-08-02 12:12:12 +01:00:00),
            ),
            (
                "2018W313T121212+01:00",
                datetime!(2018-08-02 12:12:12 +01:00:00),
            ),
        ];

        for (input, expected) in test_cases.iter() {
            let parsed = basic_parser
                .parse_date_time(input)
                .unwrap_or_else(|err| panic!("failed to parse {input}: {err}"));
            assert_eq!(parsed, *expected, "mismatch for input: {input}");
        }

        // Test strict variant
        let strict_parser =
            StrptimeParser::from_java_datetime_format("strict_basic_week_date_time_no_millis")
                .unwrap();

        for (input, expected) in test_cases.iter() {
            let parsed = strict_parser
                .parse_date_time(input)
                .unwrap_or_else(|err| panic!("failed to parse {input}: {err}"));
            assert_eq!(parsed, *expected, "mismatch for input: {input}");
        }

        // Test strict error cases - invalid time components
        assert!(strict_parser.parse_date_time("2018W313T12128Z").is_err());
        assert!(strict_parser.parse_date_time("2018W313T81212Z").is_err());
        assert!(strict_parser.parse_date_time("2018W313T12812Z").is_err());
        assert!(
            strict_parser
                .parse_date_time("2018W313T12128+0100")
                .is_err()
        );
        assert!(
            strict_parser
                .parse_date_time("2018W313T81212+01:00")
                .is_err()
        );
        assert!(
            strict_parser
                .parse_date_time("2018W313T12128+01:00")
                .is_err()
        );
        assert!(
            strict_parser
                .parse_date_time("2018W313T81212+0100")
                .is_err()
        );
        assert!(
            strict_parser
                .parse_date_time("2018W313T12812+0100")
                .is_err()
        );
        assert!(
            strict_parser
                .parse_date_time("2018W313T12812+01:00")
                .is_err()
        );
    }

    #[test]
    fn test_strict_week_date_with_separators() {
        let parser = StrptimeParser::from_java_datetime_format("strict_week_date").unwrap();

        let test_cases = [
            ("2012-W48-6", datetime!(2012-12-02 00:00:00 UTC)),
            ("2012-W01-6", datetime!(2012-01-08 00:00:00 UTC)),
            ("2018-W31-3", datetime!(2018-08-02 00:00:00 UTC)),
        ];

        for (input, expected) in test_cases.iter() {
            let parsed = parser
                .parse_date_time(input)
                .unwrap_or_else(|err| panic!("failed to parse {input}: {err}"));
            assert_eq!(parsed, *expected, "mismatch for input: {input}");
        }

        // Test error cases - non-padded week
        assert!(parser.parse_date_time("2012-W1-6").is_err());
        // Invalid day of week (should be 1-7)
        assert!(parser.parse_date_time("2012-W01-8").is_err());
        // Both errors at once
        assert!(parser.parse_date_time("2012-W1-8").is_err());
    }

    #[test]
    fn test_week_date_non_strict() {
        let parser = StrptimeParser::from_java_datetime_format("week_date").unwrap();

        let test_cases = [
            ("2012-W48-6", datetime!(2012-12-02 00:00:00 UTC)),
            ("2012-W01-6", datetime!(2012-01-08 00:00:00 UTC)),
            ("2012-W1-6", datetime!(2012-01-08 00:00:00 UTC)), // Non-strict allows W1
        ];

        for (input, expected) in test_cases.iter() {
            let parsed = parser
                .parse_date_time(input)
                .unwrap_or_else(|err| panic!("failed to parse {input}: {err}"));
            assert_eq!(parsed, *expected, "mismatch for input: {input}");
        }

        // Invalid day of week should still fail
        assert!(parser.parse_date_time("2012-W1-8").is_err());
    }

    // Test date_optional_time format (non-strict variant)
    #[test]
    fn test_date_optional_time() {
        let parser = StrptimeParser::from_java_datetime_format("date_optional_time").unwrap();

        let test_cases = [
            // Date only formats
            ("2018-05", datetime!(2018-05-01 00:00:00 UTC)),
            ("2018-05-30", datetime!(2018-05-30 00:00:00 UTC)),
            // With time components (no timezone)
            ("2018-05-30T20", datetime!(2018-05-30 20:00:00 UTC)),
            ("2018-05-30T20:21", datetime!(2018-05-30 20:21:00 UTC)),
            ("2018-05-30T20:21:23", datetime!(2018-05-30 20:21:23 UTC)),
            // With fractional seconds (no timezone)
            (
                "2018-05-30T20:21:23.1",
                datetime!(2018-05-30 20:21:23.1 UTC),
            ),
            (
                "2018-05-30T20:21:23.123",
                datetime!(2018-05-30 20:21:23.123 UTC),
            ),
            (
                "2018-05-30T20:21:23.123456789",
                datetime!(2018-05-30 20:21:23.123456789 UTC),
            ),
            // With timezone
            (
                "2018-05-30T20:21:23.123Z",
                datetime!(2018-05-30 20:21:23.123 UTC),
            ),
            (
                "2018-05-30T20:21:23.123456789Z",
                datetime!(2018-05-30 20:21:23.123456789 UTC),
            ),
            (
                "2018-05-30T20:21:23.1+0100",
                datetime!(2018-05-30 20:21:23.1 +01:00:00),
            ),
            (
                "2018-05-30T20:21:23.123+0100",
                datetime!(2018-05-30 20:21:23.123 +01:00:00),
            ),
            (
                "2018-05-30T20:21:23.1+01:00",
                datetime!(2018-05-30 20:21:23.1 +01:00:00),
            ),
            (
                "2018-05-30T20:21:23.123+01:00",
                datetime!(2018-05-30 20:21:23.123 +01:00:00),
            ),
            // Padded time components
            ("2018-12-31T10:15:30", datetime!(2018-12-31 10:15:30 UTC)),
        ];

        for (input, expected) in test_cases.iter() {
            let parsed = parser
                .parse_date_time(input)
                .unwrap_or_else(|err| panic!("failed to parse {input}: {err}"));
            assert_eq!(parsed, *expected, "mismatch for input: {input}");
        }

        // ES allows non-padded date and time components for date_optional_time but our
        // current implementation uses strict padding (dd, HH, mm, ss require 2 digits)
        // TODO: implement optional padding to match ES behavior for:
        // - "2018-12-1" (non-padded day)
        // - "2018-12-31T10:15:3" (non-padded second)
        // - "2018-12-31T10:5:30" (non-padded minute)
        // - "2018-12-31T1:15:30" (non-padded hour)
    }
}

/// Tests for formatting datetime to strings, ported from Elasticsearch's
/// `DateFormattersTests.java` to ensure maximum compatibility with their
/// date formatting behavior.
#[cfg(test)]
mod tests_formatting_ported_from_es {
    use time::macros::datetime;

    use super::*;

    #[test]
    fn test_strict_date_optional_time_formats_milliseconds() {
        // From ES testMinNanos() and testMaxNanos() - strict_date_optional_time
        // should format with 3 digits (milliseconds precision)
        let formatter =
            StrptimeParser::from_java_datetime_format("strict_date_optional_time").unwrap();

        // Test with various nanosecond values
        let test_cases = [
            // Full milliseconds
            (
                datetime!(2019-02-08 11:43:00.123 UTC),
                "2019-02-08T11:43:00.123Z",
            ),
            // Zero milliseconds should show .000
            (
                datetime!(2019-02-08 11:43:00.0 UTC),
                "2019-02-08T11:43:00.000Z",
            ),
            // Partial milliseconds - should round/truncate to 3 digits
            (
                datetime!(2019-02-08 11:43:00.1 UTC),
                "2019-02-08T11:43:00.100Z",
            ),
            (
                datetime!(2019-02-08 11:43:00.12 UTC),
                "2019-02-08T11:43:00.120Z",
            ),
            // With microseconds - should truncate to milliseconds
            (
                datetime!(2019-02-08 11:43:00.123456 UTC),
                "2019-02-08T11:43:00.123Z",
            ),
            // With nanoseconds - should truncate to milliseconds
            (
                datetime!(2019-02-08 11:43:00.123456789 UTC),
                "2019-02-08T11:43:00.123Z",
            ),
        ];

        for (input, expected) in test_cases.iter() {
            let formatted = formatter
                .format_date_time(input)
                .unwrap_or_else(|err| panic!("failed to format {input}: {err}"));
            assert_eq!(
                &formatted, expected,
                "formatting mismatch for {input}: got {formatted}"
            );
        }
    }

    #[test]
    fn test_strict_date_optional_time_nanos_formats_nanoseconds() {
        // From ES testMinNanos() and testMaxNanos() - strict_date_optional_time_nanos
        // should format with 9 digits (nanosecond precision)
        let formatter =
            StrptimeParser::from_java_datetime_format("strict_date_optional_time_nanos").unwrap();

        // Test with various nanosecond values
        let test_cases = [
            // Full nanoseconds
            (
                datetime!(2019-02-08 11:43:00.123456789 UTC),
                "2019-02-08T11:43:00.123456789Z",
            ),
            // Zero nanoseconds should show .000000000
            (
                datetime!(2019-02-08 11:43:00.0 UTC),
                "2019-02-08T11:43:00.000000000Z",
            ),
            // Milliseconds only - should pad to 9 digits
            (
                datetime!(2019-02-08 11:43:00.123 UTC),
                "2019-02-08T11:43:00.123000000Z",
            ),
            // Microseconds - should pad to 9 digits
            (
                datetime!(2019-02-08 11:43:00.123456 UTC),
                "2019-02-08T11:43:00.123456000Z",
            ),
            // Partial nanoseconds
            (
                datetime!(2019-02-08 11:43:00.1 UTC),
                "2019-02-08T11:43:00.100000000Z",
            ),
            (
                datetime!(2019-02-08 11:43:00.12 UTC),
                "2019-02-08T11:43:00.120000000Z",
            ),
        ];

        for (input, expected) in test_cases.iter() {
            let formatted = formatter
                .format_date_time(input)
                .unwrap_or_else(|err| panic!("failed to format {input}: {err}"));
            assert_eq!(
                &formatted, expected,
                "formatting mismatch for {input}: got {formatted}"
            );
        }
    }

    #[test]
    fn test_strict_date_optional_time_formats_with_timezone() {
        let formatter =
            StrptimeParser::from_java_datetime_format("strict_date_optional_time").unwrap();

        let test_cases = [
            // UTC timezone
            (
                datetime!(2018-12-31 10:15:30.123 UTC),
                "2018-12-31T10:15:30.123Z",
            ),
            // Positive offset
            (
                datetime!(2018-12-31 10:15:30.123 +01:00:00),
                "2018-12-31T10:15:30.123+01:00",
            ),
            // Negative offset
            (
                datetime!(2018-12-31 10:15:30.123 -05:00:00),
                "2018-12-31T10:15:30.123-05:00",
            ),
        ];

        for (input, expected) in test_cases.iter() {
            let formatted = formatter
                .format_date_time(input)
                .unwrap_or_else(|err| panic!("failed to format {input}: {err}"));
            assert_eq!(
                &formatted, expected,
                "formatting mismatch for {input}: got {formatted}"
            );
        }
    }

    #[test]
    fn test_format_and_parse_roundtrip() {
        // Verify that formatting and parsing are inverse operations
        let formatter =
            StrptimeParser::from_java_datetime_format("strict_date_optional_time").unwrap();

        let test_datetimes = [
            datetime!(2018-12-31 10:15:30.123 UTC),
            datetime!(2019-02-08 11:43:00.0 UTC),
            datetime!(2020-01-01 00:00:00.001 UTC),
            datetime!(2018-12-31 10:15:30.123 +01:00:00),
        ];

        for original in test_datetimes.iter() {
            let formatted = formatter
                .format_date_time(original)
                .unwrap_or_else(|err| panic!("failed to format {original}: {err}"));
            let parsed = formatter
                .parse_date_time(&formatted)
                .unwrap_or_else(|err| panic!("failed to parse {formatted}: {err}"));
            assert_eq!(
                parsed, *original,
                "roundtrip failed for {original}: formatted as {formatted}, parsed as {parsed}"
            );
        }
    }

    #[test]
    fn test_format_nanos_roundtrip() {
        // Verify that formatting and parsing are inverse operations for nanos format
        let formatter =
            StrptimeParser::from_java_datetime_format("strict_date_optional_time_nanos").unwrap();

        let test_datetimes = [
            datetime!(2018-12-31 10:15:30.123456789 UTC),
            datetime!(2019-02-08 11:43:00.0 UTC),
            datetime!(2020-01-01 00:00:00.000000001 UTC),
            datetime!(2018-12-31 10:15:30.123456 UTC),
        ];

        for original in test_datetimes.iter() {
            let formatted = formatter
                .format_date_time(original)
                .unwrap_or_else(|err| panic!("failed to format {original}: {err}"));
            let parsed = formatter
                .parse_date_time(&formatted)
                .unwrap_or_else(|err| panic!("failed to parse {formatted}: {err}"));
            assert_eq!(
                parsed, *original,
                "roundtrip failed for {original}: formatted as {formatted}, parsed as {parsed}"
            );
        }
    }

    #[test]
    fn test_zero_millis_formatted_with_trailing_zeros() {
        // From ES test0MillisAreFormatted() - even when milliseconds are zero,
        // they should be formatted with .000
        let formatter =
            StrptimeParser::from_java_datetime_format("strict_date_optional_time").unwrap();

        let dt = datetime!(2019-02-08 11:43:00.0 UTC);
        let formatted = formatter.format_date_time(&dt).unwrap();

        // Should format as .000, not omit the fractional seconds
        assert_eq!(formatted, "2019-02-08T11:43:00.000Z");
    }
}
