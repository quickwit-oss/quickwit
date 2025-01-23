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

use prost_types::{Duration as ProstDuration, Timestamp as ProstTimestamp};

pub(crate) fn parse_duration_with_units(duration_string: String) -> anyhow::Result<ProstDuration> {
    parse_duration_nanos(&duration_string)
        .map(to_well_known_timestamp)
        .map(|timestamp| ProstDuration {
            seconds: timestamp.seconds,
            nanos: timestamp.nanos,
        })
        .map_err(|error| anyhow::anyhow!("Failed to parse duration: {:?}", error))
}

pub(crate) fn to_well_known_timestamp(timestamp_nanos: i64) -> ProstTimestamp {
    let seconds = timestamp_nanos / 1_000_000_000;
    let nanos = (timestamp_nanos % 1_000_000_000) as i32;
    ProstTimestamp { seconds, nanos }
}

/// Parses a duration string and return duration in nanoseconds.
/// A duration string is a possibly signed sequence of decimal numbers, each
/// with optional fraction and a unit suffix, such as "300ms", "-1.5h".
///
/// Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".
fn parse_duration_nanos(input: &str) -> anyhow::Result<i64> {
    let mut num_str = String::new();
    for ch in input.trim().chars() {
        if ch.is_ascii_digit() || ch == '.' || ch == '-' {
            num_str.push(ch);
            continue;
        }
        if ch.is_alphabetic() {
            let unit = &input[num_str.len()..];
            let num: f64 = num_str.parse()?;
            let duration: f64 = match unit {
                "ns" => num,
                "us" | "µs" => num * 1000.0,
                "ms" => num * 1_000_000.0,
                "s" => num * 1_000_000_000.0,
                "m" => num * 60.0 * 1_000_000_000.0,
                "h" => num * 3600.0 * 1_000_000_000.0,
                _ => anyhow::bail!("Invalid time unit: {}", unit),
            };
            if num < i64::MIN as f64 || num > i64::MAX as f64 {
                anyhow::bail!("Invalid duration: {}", num_str)
            }
            return Ok(duration.round() as i64);
        } else {
            anyhow::bail!("Invalid duration string")
        }
    }
    anyhow::bail!("Invalid duration string")
}

#[cfg(test)]
mod tests {
    use crate::jaeger_api::parse_duration::parse_duration_nanos;

    #[test]
    fn test_parse_duration_nanos() {
        // Test valid duration strings
        assert_eq!(parse_duration_nanos("300ns").unwrap(), 300);
        assert_eq!(parse_duration_nanos("1us").unwrap(), 1000);
        assert_eq!(parse_duration_nanos("2.5ms").unwrap(), 2500000);
        assert_eq!(parse_duration_nanos("3s").unwrap(), 3000000000);
        assert_eq!(parse_duration_nanos("4m").unwrap(), 240000000000);
        assert_eq!(parse_duration_nanos("5h").unwrap(), 18000000000000);
        assert_eq!(parse_duration_nanos("-100ns").unwrap(), -100);
        assert_eq!(parse_duration_nanos("-2us").unwrap(), -2000);
        assert_eq!(parse_duration_nanos("-3.5ms").unwrap(), -3500000);
        assert_eq!(parse_duration_nanos("-4s").unwrap(), -4000000000);
        assert_eq!(parse_duration_nanos("-5m").unwrap(), -300000000000);
        assert_eq!(parse_duration_nanos("-6h").unwrap(), -21600000000000);

        // Test invalid duration strings
        assert!(parse_duration_nanos("abc").is_err());
        assert!(parse_duration_nanos("1.2.3s").is_err());
        assert!(parse_duration_nanos("1-.23s").is_err());
    }
}
