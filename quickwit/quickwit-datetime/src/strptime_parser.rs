// Copyright (C) 2023 Quickwit, Inc.
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

use std::fmt;
use std::hash::{Hash, Hasher};

use ouroboros::self_referencing;
use time::error::Format;
use time::format_description::FormatItem;
use time::parsing::Parsed;
use time::{OffsetDateTime, PrimitiveDateTime, UtcOffset};

// impl FromStr for StrptimeParser {
//     type Err = String;

//     fn from_str(strptime_format_str: &str) -> Result<Self, Self::Err> {
//         StrptimeParser::try_new(
//             strptime_format_str.to_string(),
//             strptime_format_str.to_lowercase().contains("%z"),
//             UtcOffset::UTC,
//             |strptime_format: &String| {
//                 parse_to_format_item(strptime_format).map_err(|err| {
//                     format!("Invalid format specification `{strptime_format}`. Error: {err}.")
//                 })
//             },
//         )
//     }
// }

/// `strptime` field descriptors.
/// This list of descriptors is coming from the `time-fmt` crate.
const STRFTIME_FIELD_DESCRIPTORS: [&str; 36] = [
    "%a", "%A", "%b", "%B", "%c", "%C", "%d", "%D", "%e", "%f", "%F", "%h", "%H", "%I", "%j", "%k",
    "%l", "%m", "%M", "%n", "%p", "%P", "%r", "%R", "%S", "%t", "%T", "%U", "%w", "%W", "%x", "%X",
    "%y", "%Y", "%z", "%Z",
];

// Checks whether a format contains `strftime` field descriptors.
pub(crate) fn is_strftime_format(format_str: &str) -> bool {
    STRFTIME_FIELD_DESCRIPTORS
        .iter()
        .any(|marker| format_str.contains(marker))
}

#[self_referencing]
pub(crate) struct StrptimeParser {
    strptime_format: String,
    has_offset: bool,
    default_offset: UtcOffset,
    #[borrows(strptime_format)]
    #[covariant]
    format_items: Vec<FormatItem<'this>>,
}

impl StrptimeParser {
    /// Parses a given date according to the datetime format specified during the StrptimeParser
    /// creation. If the date format does not provide a specific a time, the time will be set to
    /// 00:00:00.
    fn parse_primitive_date_time(
        &self,
        date_time_str: &str,
    ) -> Result<PrimitiveDateTime, time::error::Parse> {
        let mut parsed = Parsed::new();
        parsed.parse_items(date_time_str.as_bytes(), self.borrow_format_items())?;
        // If the parsed date time contains a date but not a "time" component, we complete it
        // artificially with "00:00:00".
        if parsed.hour_24().is_none()
            && !(parsed.hour_12().is_some() && parsed.hour_12_is_pm().is_some())
        {
            parsed.set_hour_24(0);
            parsed.set_minute(0);
            parsed.set_second(0);
        }
        let date_time = parsed.try_into()?;
        Ok(date_time)
    }

    pub fn parse_date_time(&self, date_time_str: &str) -> Result<OffsetDateTime, String> {
        if *self.borrow_has_offset() {
            OffsetDateTime::parse(date_time_str, self.borrow_format_items())
                .map_err(|error| error.to_string())
        } else {
            self.parse_primitive_date_time(date_time_str)
                .map(|date_time| date_time.assume_offset(*self.borrow_default_offset()))
                .map_err(|error| error.to_string())
        }
    }

    pub fn format_date_time(&self, date_time: &OffsetDateTime) -> Result<String, Format> {
        date_time.format(self.borrow_format_items())
    }
}

impl fmt::Debug for StrptimeParser {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let debug_struct = f
            .debug_struct("StrptimeParser")
            .field("format", &self.borrow_strptime_format());

        if !self.borrow_has_offset() {
            debug_struct.field("offset", &self.borrow_default_offset());
        }
        debug_struct.finish()
    }
}

impl Clone for StrptimeParser {
    fn clone(&self) -> Self {
        Self::new(
            self.borrow_strptime_format().clone(),
            *self.borrow_has_offset(),
            *self.borrow_default_offset(),
            |_| self.borrow_format_items().clone(),
        )
    }
}

impl PartialEq for StrptimeParser {
    fn eq(&self, other: &Self) -> bool {
        self.borrow_strptime_format() == other.borrow_strptime_format()
            && self.borrow_default_offset() == other.borrow_default_offset()
    }
}

impl Eq for StrptimeParser {}

impl Hash for StrptimeParser {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.borrow_strptime_format().hash(state);
    }
}

// impl<'de> Deserialize<'de> for StrptimeParser {
//     fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
//         struct StrptimeFormatVisitor;

//         impl<'de> Visitor<'de> for StrptimeFormatVisitor {
//             type Value = StrptimeFormat;

//             fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
//                 formatter.write_str("string or map containing the parameters of a match query.")
//             }

//             fn visit_str<E>(self, strptime_format: &str) -> Result<Self::Value, E>
//             where E: serde::de::Error {
//                 Ok(StrptimeFormat {
//                     strptime_format: strptime_format.to_string(),
//                     default_timezone: TimeZone::Local,
//                 })
//             }

//             fn visit_map<M>(self, map: M) -> Result<StrptimeFormat, M::Error>
//             where M: MapAccess<'de> {
//                 Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))
//             }
//         }

//         let strptime_input_format = deserializer.deserialize_any(StrptimeFormatVisitor)?;
//         let has_offset = strptime_input_format.strptime_format.contains("%z");
//         let default_offset = strptime_input_format
//             .default_timezone
//             .utc_offset()
//             .map_err(de::Error::custom)?;
//         let strptime_parser = StrptimeParser::try_new(
//             strptime_input_format.strptime_format,
//             has_offset,
//             default_offset,
//             |strptime_format| parse_to_format_item(strptime_format),
//         )
//         .map_err(de::Error::custom)?;
//         Ok(strptime_parser)
//     }
// }
