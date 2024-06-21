// Copyright (C) 2024 Quickwit, Inc.
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

use std::str::FromStr;

use ouroboros::self_referencing;
use time::error::Format;
use time::format_description::well_known::{Iso8601, Rfc2822, Rfc3339};
use time::format_description::FormatItem;
use time::parsing::Parsed;
use time::{OffsetDateTime, PrimitiveDateTime, UtcOffset};
use time_fmt::parse::time_format_item::parse_to_format_item;

use crate::date_time_format::infer_year;
use crate::TantivyDateTime;

/// A date time parser that holds the format specification `Vec<FormatItem>`.
#[self_referencing]
pub struct StrptimeParser {
    pub strptime_format: String,
    with_timezone: bool,
    #[borrows(strptime_format)]
    #[covariant]
    items: Vec<FormatItem<'this>>,
}

impl FromStr for StrptimeParser {
    type Err = String;

    fn from_str(strptime_format: &str) -> Result<Self, Self::Err> {
        StrptimeParser::try_new(
            strptime_format.to_string(),
            strptime_format.to_lowercase().contains("%z"),
            |strptime_format: &String| {
                parse_to_format_item(strptime_format).map_err(|error| {
                    format!("invalid strptime format `{strptime_format}`: {error}")
                })
            },
        )
    }
}

impl StrptimeParser {
    /// Parse a given date according to the datetime format specified during the StrptimeParser
    /// creation. If the date format does not provide a specific a time, the time will be set to
    /// 00:00:00.
    fn parse_primitive_date_time(&self, date_time_str: &str) -> anyhow::Result<PrimitiveDateTime> {
        let mut parsed = Parsed::new();
        if !parsed
            .parse_items(date_time_str.as_bytes(), self.borrow_items())?
            .is_empty()
        {
            anyhow::bail!(
                "datetime string `{}` does not match strptime format `{}`",
                date_time_str,
                self.borrow_strptime_format()
            );
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
            let year = infer_year(parsed.month(), now.month(), now.year());
            parsed.set_year(year);
        }
        let date_time = parsed.try_into()?;
        Ok(date_time)
    }

    pub fn parse_date_time(&self, date_time_str: &str) -> Result<OffsetDateTime, String> {
        if *self.borrow_with_timezone() {
            OffsetDateTime::parse(date_time_str, self.borrow_items()).map_err(|error| error.to_string())
        } else {
            self.parse_primitive_date_time(date_time_str)
                .map(|date_time| date_time.assume_utc())
                .map_err(|error| error.to_string())
        }
    }

    pub fn format_date_time(&self, date_time: &OffsetDateTime) -> Result<String, Format> {
        date_time.format(self.borrow_items())
    }
}

impl Clone for StrptimeParser {
    fn clone(&self) -> Self {
        // `self.format` is already known to be a valid format.
        Self::from_str(self.borrow_strptime_format().as_str()).unwrap()
    }
}

impl PartialEq for StrptimeParser {
    fn eq(&self, other: &Self) -> bool {
        self.borrow_strptime_format() == other.borrow_strptime_format()
    }
}

impl Eq for StrptimeParser {}

impl std::fmt::Debug for StrptimeParser {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StrptimeParser")
            .field("format", &self.borrow_strptime_format())
            .finish()
    }
}

impl std::hash::Hash for StrptimeParser {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.borrow_strptime_format().hash(state);
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
pub(crate) fn is_strftime_formatting(format_str: &str) -> bool {
    STRFTIME_FORMAT_MARKERS
        .iter()
        .any(|marker| format_str.contains(marker))
}
