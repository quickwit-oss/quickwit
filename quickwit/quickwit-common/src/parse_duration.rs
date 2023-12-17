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

#[derive(Debug, PartialEq)]
pub enum DurationError {
    DurationParseError(String),
    InvalidDuration(String),
    InvalidDurationUnit(String),
    Overflow(String),
}

impl fmt::Display for DurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DurationError::DurationParseError(message) => write!(formatter, "{}", message),
            DurationError::InvalidDuration(message) => write!(formatter, "{}", message),
            DurationError::InvalidDurationUnit(message) => write!(formatter, "{}", message),
            DurationError::Overflow(message) => write!(formatter, "{}", message),
        }
    }
}

/// parses a duration string and return duration in nanoseconds.
/// A duration string is a possibly signed sequence of decimal numbers, each
/// with optional fraction and a unit suffix, such as "300ms", "-1.5h", or
/// "2h45m".
///
/// Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".
/// Taken from https://crates.io/crates/go-parse-duration
pub fn parse_duration(string: &str) -> Result<i64, DurationError> {
    let mut s = string;
    let mut duration: i64 = 0;
    let mut is_negative = false;

    if let Some(c) = s.chars().next() {
        if c == '-' || c == '+' {
            is_negative = c == '-';
            s = &s[1..];
        }
    } else {
        return Err(DurationError::InvalidDuration(
            "duration can not be empty".to_string(),
        ));
    }
    // Special case: if all that is left is "0", this is zero.
    if s == "0" {
        return Ok(0);
    }

    while !s.is_empty() {
        // The next character must be [0-9.]
        match s.chars().nth(0).unwrap_or('\0') {
            '.' | '0'..='9' => (),
            _ => {
                return Err(DurationError::InvalidDuration(format!(
                    "invalid duration character: '{}'",
                    string
                )));
            }
        }
        let pl = s.len();

        let mut v: i64;
        let mut f: i64 = 0;
        // value = v + f / scale
        let mut scale: f64 = 1f64;
        match leading_int(s) {
            Ok((_v, _s)) => {
                v = _v;
                s = _s;
            }
            Err(e) => {
                return Err(e);
            }
        }
        let pre = pl != s.len(); // whether we consume anything before a period

        // Consume (\.[0-9]*)?
        let mut post = false;
        if s != "" && s.chars().nth(0) == Some('.') {
            s = &s[1..];
            let pl = s.len();
            match leading_fraction(s) {
                Ok((f_, scale_, s_)) => {
                    f = f_;
                    scale = scale_;
                    s = s_;
                }
                Err(_) => {
                    return Err(DurationError::InvalidDuration(format!(
                        "invalid duration character: '{}'",
                        string
                    )));
                }
            }
            post = pl != s.len();
        }
        if !pre && !post {
            // no digits (e.g. ".s" or "-.s")
            return Err(DurationError::DurationParseError(format!(
                "invalid duration: {}",
                string
            )));
        }

        // Consume unit.
        let mut i = 0;
        while i < s.len() {
            let c = s.chars().nth(i).unwrap();
            if c == '.' || '0' <= c && c <= '9' {
                break;
            }
            i += 1;
        }
        if i == 0 {
            return Err(DurationError::InvalidDurationUnit(format!(
                "missing unit in duration '{}'",
                string
            )));
        }
        let u = &s[..i];
        s = &s[i..];
        let unit = match u {
            "ns" => 1i64,
            "us" | "µs" | "μs" => 1000i64,
            "ms" => 1000000i64,
            "s" => 1000000000i64,
            "m" => 60000000000i64,
            "h" => 3600000000000i64,
            _ => {
                return Err(DurationError::InvalidDurationUnit(format!(
                    "unknown unit '{}' in duration '{}'",
                    u, string
                )));
            }
        };
        if v > (1 << 63 - 1) / unit {
            return Err(DurationError::Overflow(format!(
                "invalid duration '{}'",
                string
            )));
        }
        v *= unit;
        if f > 0 {
            // f64 is needed to be nanosecond accurate for fractions of hours.
            // v >= 0 && (f*unit/scale) <= 3.6e+12 (ns/h, h is the largest unit)
            v += (f as f64 * (unit as f64 / scale)) as i64;
            if v < 0 {
                return Err(DurationError::Overflow(format!(
                    "invalid duration {}",
                    string
                )));
            }
        }
        duration += v;
        if duration < 0 {
            return Err(DurationError::Overflow(format!(
                "invalid duration {}",
                string
            )));
        }
    }
    if is_negative {
        duration = -duration;
    }
    Ok(duration)
}

// leading_int consumes the leading [0-9]* from s.
fn leading_int(s: &str) -> Result<(i64, &str), DurationError> {
    let mut x = 0;
    let mut i = 0;
    while i < s.len() {
        let Some(c) = s.chars().nth(i) else {
            return Err(DurationError::InvalidDuration(format!(
                "invalid duration character: '{}'",
                s
            )));
        };
        if c < '0' || c > '9' {
            break;
        }
        if x > (1 << 63 - 1) / 10 {
            return Err(DurationError::Overflow(format!("invalid duration '{}'", s)));
        }
        let Some(f) = c.to_digit(10) else {
            return Err(DurationError::InvalidDuration(format!(
                "invalid duration number: '{}'",
                s
            )));
        };

        let d = i64::from(f);
        x = x * 10 + d;
        if x < 0 {
            return Err(DurationError::Overflow(format!("invalid duration '{}'", s)));
        }
        i += 1;
    }
    Ok((x, &s[i..]))
}

// leading_fraction consumes the leading [0-9]* from s.
fn leading_fraction(s: &str) -> Result<(i64, f64, &str), DurationError> {
    let mut i = 0;
    let mut x = 0i64;
    let mut scale = 1f64;
    let mut has_overflow = false;
    while i < s.len() {
        let Some(c) = s.chars().nth(i) else {
            return Err(DurationError::InvalidDuration(format!(
                "invalid duration character: '{}'",
                s
            )));
        };
        if c < '0' || c > '9' {
            break;
        }
        if has_overflow {
            continue;
        }
        if x > (1 << 63 - 1) / 10 {
            // It's possible for overflow to give a positive number, so take care.
            has_overflow = true;
            continue;
        }
        let Some(f) = c.to_digit(10) else {
            break;
        };

        let d = i64::from(f);
        let y = x * 10 + d;
        if y < 0 {
            has_overflow = true;
            continue;
        }
        x = y;
        scale *= 10f64;
        i += 1;
    }
    Ok((x, scale, &s[i..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_ok_cases() -> Result<(), DurationError> {
        assert_eq!(parse_duration("0")?, 0);
        assert_eq!(parse_duration("50ns")?, 50);
        assert_eq!(parse_duration("3ms")?, 3000000);
        assert_eq!(parse_duration("2us")?, 2000);
        assert_eq!(parse_duration("4s")?, 4000000000);
        assert_eq!(parse_duration("1h45m")?, 6300000000000);
        assert_eq!(parse_duration("+3m")?, 180000000000);
        assert_eq!(parse_duration("-3m")?, -180000000000);
        Ok(())
    }

    #[test]
    fn test_parse_duration_error_cases() -> Result<(), DurationError> {
        assert_eq!(
            parse_duration("").unwrap_err(),
            DurationError::InvalidDuration(String::from("duration can not be empty")),
        );
        assert_eq!(
            parse_duration("c").unwrap_err(),
            DurationError::InvalidDuration(String::from("invalid duration character: 'c'")),
        );
        assert_eq!(
            parse_duration("1").unwrap_err(),
            DurationError::InvalidDurationUnit(String::from("missing unit in duration '1'")),
        );
        assert_eq!(
            parse_duration("1ks").unwrap_err(),
            DurationError::InvalidDurationUnit(String::from("unknown unit 'ks' in duration '1ks'")),
        );
        Ok(())
    }
}
