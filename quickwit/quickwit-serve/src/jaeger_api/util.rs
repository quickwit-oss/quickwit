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

use go_parse_duration::parse_duration;
use prost_types::{Duration as WellKnownDuration, Timestamp as WellKnownTimestamp};

// TODO move to `TraceId` and simplify if possible
pub fn hex_string_to_bytes(hex_string: &str) -> Vec<u8> {
    if hex_string.len() % 2 != 0 {
        panic!("Hex string must have an even number of characters");
    }
    (0..hex_string.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex_string[i..i + 2], 16).expect("Failed to parse hex"))
        .collect()
}

pub fn bytes_to_hex_string(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

pub fn to_well_known_timestamp(timestamp_nanos: i64) -> WellKnownTimestamp {
    let seconds = timestamp_nanos / 1_000_000;
    let nanos = (timestamp_nanos % 1_000_000) as i32;
    WellKnownTimestamp { seconds, nanos }
}

pub fn from_well_known_timestamp(timestamp_opt: &Option<WellKnownTimestamp>) -> i64 {
    match timestamp_opt {
        Some(timestamp) => timestamp.seconds * 1_000_000 + i64::from(timestamp.nanos / 1000),
        None => 0i64,
    }
}

pub fn parse_duration_with_units(duration_string_opt: Option<String>) -> Option<WellKnownDuration> {
    duration_string_opt
        .and_then(|duration_string| parse_duration(duration_string.as_str()).ok())
        .map(to_well_known_duration)
}

fn to_well_known_duration(timestamp_nanos: i64) -> WellKnownDuration {
    let seconds = timestamp_nanos / 1_000_000;
    let nanos = (timestamp_nanos % 1_000_000) as i32;
    WellKnownDuration { seconds, nanos }
}

pub fn from_well_known_duration(duration_opt: &Option<WellKnownDuration>) -> i64 {
    match duration_opt {
        Some(duration) => duration.seconds * 1_000_000 + i64::from(duration.nanos / 1000),
        None => 0i64,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_to_create() {}
}
