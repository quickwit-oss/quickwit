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
use std::str::FromStr;

use once_cell::sync::Lazy;
use ouroboros::self_referencing;
use regex::{CaptureMatches, Match, Regex};
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Serialize, Serializer};
use time::util::local_offset;
use time::{OffsetDateTime, UtcOffset};
use time_fmt::parse::time_format_item::parse_to_format_item;

use crate::time_zone::TimeZone;

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrptimeFormat {
    #[serde(rename = "format")]
    strptime_format: String,
    #[serde(default)]
    default_timezone: TimeZone,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strptime_format_serde() {
        {
            let strptime_format =
                serde_json::from_str::<StrptimeFormat>(r#""format": "%Y-%m-%dT%H:%M:%S%z""#)
                    .unwrap();
            assert_eq!(
                strptime_format,
                StrptimeFormat {
                    strptime_format: "%Y-%m-%dT%H:%M:%S%z".to_string(),
                    default_timezone: TimeZone::Local,
                }
            );
        }
        {
            let strptime_format = serde_json::from_str::<StrptimeFormat>(
                r#"{
                "format": "%Y-%m-%dT%H:%M:%S%z"
            }"#,
            )
            .unwrap();
            assert_eq!(
                strptime_format,
                StrptimeFormat {
                    strptime_format: "%Y-%m-%dT%H:%M:%S%z".to_string(),
                    default_timezone: TimeZone::Local,
                }
            );
        }
        {
            let strptime_format = serde_json::from_str::<StrptimeFormat>(
                r#"{
                "format": "%Y-%m-%dT%H:%M:%S%z",
                "default_timezone": "+0200"
            }"#,
            )
            .unwrap();
            assert_eq!(
                strptime_format,
                StrptimeFormat {
                    strptime_format: "%Y-%m-%dT%H:%M:%S%z".to_string(),
                    default_timezone: TimeZone::Offset("+0200".to_string()),
                }
            );
        }
    }
}
