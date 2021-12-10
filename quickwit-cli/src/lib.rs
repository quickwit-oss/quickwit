// Copyright (C) 2021 Quickwit, Inc.
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

use std::time::Duration;

pub mod index;
pub mod service;
pub mod split;
pub mod stats;

/// Throughput calculation window size.
const THROUGHPUT_WINDOW_SIZE: usize = 5;

/// This environment variable can be set to send telemetry events to a jaeger instance.
pub const QUICKWIT_JAEGER_ENABLED_ENV_KEY: &str = "QUICKWIT_JAEGER_ENABLED";

/// Parse duration with unit.
/// examples: 1s 2m 3h 5d
pub fn parse_duration_with_unit(duration: &str) -> anyhow::Result<Duration> {
    let mut value = "".to_string();
    let mut unit = "".to_string();
    for character in duration.chars() {
        if character.is_numeric() {
            value.push(character);
        } else {
            unit.push(character);
        }
    }

    match value.parse::<u64>() {
        Ok(value) => match unit.as_str() {
            "s" => Ok(Duration::from_secs(value)),
            "m" => Ok(Duration::from_secs(value * 60)),
            "h" => Ok(Duration::from_secs(value * 60 * 60)),
            "d" => Ok(Duration::from_secs(value * 60 * 60 * 24)),
            _ => Err(anyhow::anyhow!("Invalid duration format: `[0-9]+[smhd]`")),
        },
        Err(err) => Err(anyhow::anyhow!(err)),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::parse_duration_with_unit;

    #[test]
    fn test_parse_duration_with_unit() -> anyhow::Result<()> {
        assert_eq!(parse_duration_with_unit("8s")?, Duration::from_secs(8));
        assert_eq!(parse_duration_with_unit("5m")?, Duration::from_secs(5 * 60));
        assert_eq!(
            parse_duration_with_unit("2h")?,
            Duration::from_secs(2 * 60 * 60)
        );
        assert_eq!(
            parse_duration_with_unit("3d")?,
            Duration::from_secs(3 * 60 * 60 * 24)
        );

        assert!(parse_duration_with_unit("").is_err());
        assert!(parse_duration_with_unit("a2d").is_err());
        assert!(parse_duration_with_unit("3 d").is_err());
        assert!(parse_duration_with_unit("3").is_err());
        Ok(())
    }
}
