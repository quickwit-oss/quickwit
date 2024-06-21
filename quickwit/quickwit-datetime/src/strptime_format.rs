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

use std::fmt;
use std::hash::Hasher;

use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use crate::time_zone::TimeZone;

#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StrptimeFormat {
    #[serde(rename = "format")]
    strptime_format: String,
    #[serde(default)]
    default_timezone: TimeZone,
}

impl<'de> Deserialize<'de> for StrptimeFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        deserializer.deserialize_any(StrptimeFormatVisitor)
    }
}

struct StrptimeFormatVisitor;

impl<'de> Visitor<'de> for StrptimeFormatVisitor {
    type Value = StrptimeFormat;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string or object representing a strptime format")
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        let strptime_format = StrptimeFormat {
            strptime_format: value.to_string(),
            default_timezone: TimeZone::default(),
        };
        Ok(strptime_format)
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where M: MapAccess<'de> {
        let mut strptime_format = None;
        let mut default_timezone = None;

        while let Some(key) = map.next_key()? {
            match key {
                "format" => {
                    if strptime_format.is_some() {
                        return Err(de::Error::duplicate_field("format"));
                    }
                    strptime_format = Some(map.next_value()?);
                }
                "default_timezone" => {
                    if default_timezone.is_some() {
                        return Err(de::Error::duplicate_field("default_timezone"));
                    }
                    default_timezone = Some(map.next_value()?);
                }
                _ => {
                    return Err(de::Error::unknown_field(
                        key,
                        &["format", "default_timezone"],
                    ));
                }
            }
        }
        let strptime_format = strptime_format.ok_or_else(|| de::Error::missing_field("format"))?;
        let default_timezone = default_timezone.unwrap_or_default();
        Ok(StrptimeFormat {
            strptime_format,
            default_timezone,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strptime_format_serde() {
        {
            let strptime_format =
                serde_json::from_str::<StrptimeFormat>(r#""%Y-%m-%dT%H:%M:%S%z""#).unwrap();
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
