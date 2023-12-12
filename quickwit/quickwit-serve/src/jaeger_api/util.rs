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

// TODO: remove go_parse_duration dependency
use go_parse_duration::parse_duration;
use prost_types::{Duration, Timestamp};

pub fn bytes_to_hex_string(bytes: &[u8]) -> String {
    format!("{:0>16}", hex::encode(bytes))
}

pub fn to_well_known_timestamp(timestamp_nanos: i64) -> Timestamp {
    let seconds = timestamp_nanos / 1_000_000;
    let nanos = (timestamp_nanos % 1_000_000) as i32;
    Timestamp { seconds, nanos }
}

pub fn parse_duration_with_units(duration_string: String) -> anyhow::Result<Duration> {
    parse_duration(&duration_string)
        .map(|duration_nanos| to_well_known_timestamp(duration_nanos))
        .map(|timestamp| Duration {
            seconds: timestamp.seconds,
            nanos: timestamp.nanos,
        })
        .map_err(|error| anyhow::anyhow!("Failed to parse duration: {:?}", error))
}

pub fn convert_timestamp_to_microsecs(timestamp: &Timestamp) -> i64 {
    timestamp.seconds * 1_000_000 + i64::from(timestamp.nanos / 1000)
}

pub fn convert_duration_to_microsecs(duration: Duration) -> i64 {
    duration.seconds * 1_000_000 + i64::from(duration.nanos / 1000)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_to_create() {}
}
