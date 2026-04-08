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

//! Shared window math for time-windowed compaction.
//!
//! These pure functions are the single source of truth for window arithmetic,
//! used by both stateright models and production code.

/// Compute window_start for a timestamp.
///
/// Uses `rem_euclid` for correct handling of negative timestamps (before Unix
/// epoch). Standard `%` truncates toward zero: `-1 % 900 = -1` (wrong).
/// `rem_euclid` always returns non-negative: `(-1i64).rem_euclid(900) = 899`.
///
/// Mirrors TLA+ `WindowStart(t) == t - (t % WindowDuration)`.
pub fn window_start_secs(timestamp_secs: i64, duration_secs: i64) -> i64 {
    timestamp_secs - timestamp_secs.rem_euclid(duration_secs)
}

/// TW-2: window_duration must evenly divide one hour (3600 seconds).
///
/// Returns true if the duration is a positive divisor of 3600. This ensures
/// window boundaries align across hours and days regardless of starting point.
pub fn is_valid_window_duration(duration_secs: u32) -> bool {
    duration_secs > 0 && 3600 % duration_secs == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_start_basic() {
        assert_eq!(window_start_secs(0, 2), 0);
        assert_eq!(window_start_secs(1, 2), 0);
        assert_eq!(window_start_secs(2, 2), 2);
        assert_eq!(window_start_secs(3, 2), 2);
        assert_eq!(window_start_secs(5, 3), 3);
    }

    #[test]
    fn window_start_negative_timestamps() {
        assert_eq!(window_start_secs(-1, 900), -900);
        assert_eq!(window_start_secs(-3601, 3600), -7200);
    }

    #[test]
    fn window_start_on_boundary() {
        assert_eq!(window_start_secs(900, 900), 900);
        assert_eq!(window_start_secs(899, 900), 0);
    }

    #[test]
    fn valid_window_durations() {
        let valid = [
            1, 2, 3, 4, 5, 6, 8, 9, 10, 12, 15, 16, 18, 20, 24, 25, 30, 36, 40, 45, 48, 50, 60, 72,
            75, 80, 90, 100, 120, 144, 150, 180, 200, 225, 240, 300, 360, 400, 450, 600, 720, 900,
            1200, 1800, 3600,
        ];
        for dur in valid {
            assert!(
                is_valid_window_duration(dur),
                "expected {} to be valid",
                dur
            );
        }
    }

    #[test]
    fn invalid_window_durations() {
        assert!(!is_valid_window_duration(0));
        assert!(!is_valid_window_duration(7));
        assert!(!is_valid_window_duration(11));
        assert!(!is_valid_window_duration(7200));
    }
}
