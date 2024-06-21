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

use std::borrow::Cow;
use std::str::FromStr;

use chrono::Local;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize, Serializer};
use time::UtcOffset;

#[derive(Debug, Default, Eq, PartialEq)]
pub(crate) enum TimeZone {
    #[default]
    Local,
    Offset(String),
}

impl TimeZone {
    pub fn as_str(&self) -> &str {
        match self {
            TimeZone::Local => "local",
            TimeZone::Offset(offset) => offset,
        }
    }

    pub fn utc_offset(&self) -> Result<UtcOffset, String> {
        match self {
            TimeZone::Local => Ok(local_utc_offset()),
            TimeZone::Offset(offset) => parse_offset(offset),
        }
    }
}

fn local_utc_offset() -> UtcOffset {
    // We use the `chrono` implementation to determine the local UTC offset here because `time`'s
    // always returns `IndeterminateOffset`.
    static LOCAL_UTC_OFFSET: Lazy<UtcOffset> = Lazy::new(|| {
        let local_now = Local::now();
        let offset = local_now.offset();
        UtcOffset::from_whole_seconds(offset.local_minus_utc())
            .expect("local offset should be valid")
    });
    *LOCAL_UTC_OFFSET
}

fn parse_offset(offset: &str) -> Result<UtcOffset, String> {
    static OFFSET_REGEX: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"[+-](?P<hours>[0-9]{2})(:?(?P<minutes>[0-9]{2}))?")
            .expect("regular expression should compile")
    });
    let captures = OFFSET_REGEX
        .captures(offset)
        .ok_or_else(|| format!("failed to parse time zone offset `{offset}`."))?;
    let hours = captures
        .name("hours")
        .expect("`hours` capture should match")
        .as_str()
        .parse::<i32>()
        .expect("`hours` capture should be an integer");
    let minutes = captures
        .name("minutes")
        .map(|minutes| minutes.as_str())
        .unwrap_or("0")
        .parse::<i32>()
        .expect("`minutes` capture should be an integer");
    let sign = if offset.starts_with('+') { 1 } else { -1 };

    UtcOffset::from_whole_seconds(sign * (hours * 3600 + minutes * 60))
        .map_err(|_| format!("time zone offset `{offset}` is invalid"))
}

impl Serialize for TimeZone {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TimeZone {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let time_zone_str: Cow<'de, str> = Cow::deserialize(deserializer)?;
        TimeZone::from_str(&time_zone_str).map_err(serde::de::Error::custom)
    }
}

impl FromStr for TimeZone {
    type Err = String;

    fn from_str(time_zone_str: &str) -> Result<Self, Self::Err> {
        if time_zone_str.eq_ignore_ascii_case("local") {
            return Ok(TimeZone::Local);
        }
        if time_zone_str.starts_with('+') || time_zone_str.starts_with('-') {
            return Ok(TimeZone::Offset(time_zone_str.to_string()));
        }
        Err(format!(
            "failed to parse time zone `{time_zone_str}`: only `local` and UTC offsets ±hh[:mm] \
             are supported"
        ))
    }
}

#[cfg(test)]
mod tests {
    use chrono::Local;

    use super::*;

    #[test]
    fn test_timezone_from_str() {
        let time_zone = TimeZone::from_str("local").unwrap();
        assert_eq!(time_zone, TimeZone::Local);

        let time_zone = TimeZone::from_str("+0200").unwrap();
        assert_eq!(time_zone, TimeZone::Offset("+0200".to_string()));

        let time_zone = TimeZone::from_str("-0200").unwrap();
        assert_eq!(time_zone, TimeZone::Offset("-0200".to_string()));

        TimeZone::from_str("Europe/Paris").unwrap_err();
    }

    #[test]
    fn test_timezone_utc_offset() {
        assert_eq!(
            TimeZone::Local.utc_offset().unwrap().whole_seconds(),
            Local::now().offset().local_minus_utc()
        );
        for (offset, expected_offset) in [
            ("-02:30", -9000),
            ("-0230", -9000),
            ("-0200", -7200),
            ("-0100", -3600),
            ("-0000", 0),
            ("-00:00", 0),
            ("+00:00", 0),
            ("+0000", 0),
            ("+0100", 3600),
            ("+0200", 7200),
            ("+02:30", 9000),
        ] {
            let actual_offset = TimeZone::Offset(offset.to_string())
                .utc_offset()
                .unwrap()
                .whole_seconds();
            assert_eq!(
                actual_offset, expected_offset,
                "Test UTC offset `{offset}` failed: expected offset `{expected_offset}`, got \
                 `{actual_offset}`.",
            );
        }
    }
}
