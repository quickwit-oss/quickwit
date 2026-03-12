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

//! Canonical time-window functions for metrics compaction.
//!
//! `window_start` is the foundational time-partitioning function used by
//! ingestion (Phase 32), merge policy (Phase 33), and compaction planning
//! (Phase 35). Correctness at boundary conditions -- especially negative
//! timestamps and zero-crossing -- is critical because an off-by-one error
//! silently misroutes data to wrong windows.
//!
//! The implementation uses `rem_euclid` instead of the `%` operator to handle
//! negative timestamps correctly. Standard `%` truncates toward zero, which
//! gives wrong results for negative inputs:
//!   - `-1 % 900 = -1` (wrong: would compute window_start as 0)
//!   - `(-1i64).rem_euclid(900) = 899` (correct: window_start = -1 - 899 = -900)

use chrono::{DateTime, Utc};

use super::SortFieldsError;

/// Validate that a window duration evenly divides one hour (3600 seconds).
///
/// This is ADR-003 invariant TW-2: all windows within an hour must have
/// identical boundaries regardless of when counting starts. 3600 has 45
/// positive divisors (1, 2, 3, ..., 1800, 3600). Any of these are accepted.
/// In practice, metrics systems use durations >= 60s: 60, 120, 180, 240,
/// 300, 360, 600, 720, 900, 1200, 1800, 3600.
///
/// A duration of 0 is rejected as nonsensical. Durations that do not evenly
/// divide 3600 are rejected because they would produce inconsistent window
/// boundaries across different starting points within an hour.
pub fn validate_window_duration(duration_secs: u32) -> Result<(), SortFieldsError> {
    if duration_secs == 0 {
        return Err(SortFieldsError::InvalidWindowDuration {
            duration_secs,
            reason: "must be positive",
        });
    }
    if 3600 % duration_secs != 0 {
        return Err(SortFieldsError::InvalidWindowDuration {
            duration_secs,
            reason: "must evenly divide 3600 (one hour)",
        });
    }
    Ok(())
}

/// Compute the start of the time window containing the given timestamp.
///
/// Uses `rem_euclid` for correct handling of negative timestamps (before Unix
/// epoch). Standard `%` truncates toward zero: `-1 % 900 = -1` (wrong).
/// `rem_euclid` always returns non-negative: `(-1i64).rem_euclid(900) = 899`.
/// So `window_start(-1, 900) = -1 - 899 = -900` (correct: timestamp -1 is in
/// window [-900, 0)).
///
/// # Invariants (verified by proptest)
/// - Window start is aligned: `window_start % duration == 0`
/// - Timestamp is contained: `window_start <= timestamp < window_start + duration`
/// - Deterministic: same inputs always produce same output
///
/// # Errors
/// Returns `SortFieldsError::WindowStartOutOfRange` if the computed start
/// timestamp cannot be represented as a `DateTime<Utc>`.
pub fn window_start(
    timestamp_secs: i64,
    duration_secs: i64,
) -> Result<DateTime<Utc>, SortFieldsError> {
    use quickwit_dst::check_invariant;
    use quickwit_dst::invariants::InvariantId;

    check_invariant!(InvariantId::TW2, duration_secs > 0, ": duration_secs must be positive");
    check_invariant!(
        InvariantId::TW2,
        3600 % duration_secs == 0,
        ": duration_secs={} does not divide 3600", duration_secs
    );
    let start_secs = quickwit_dst::invariants::window::window_start_secs(
        timestamp_secs,
        duration_secs,
    );
    DateTime::from_timestamp(start_secs, 0).ok_or(SortFieldsError::WindowStartOutOfRange {
        timestamp_secs: start_secs,
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    // -----------------------------------------------------------------------
    // Proptest properties
    // -----------------------------------------------------------------------

    proptest! {
        #[test]
        fn window_start_is_aligned(
            ts in -1_000_000_000i64..2_000_000_000i64,
            dur in prop::sample::select(vec![60i64, 120, 180, 240, 300, 360,
                                             600, 720, 900, 1200, 1800, 3600])
        ) {
            let ws = window_start(ts, dur).unwrap();
            let ws_secs = ws.timestamp();
            // window_start is aligned to duration
            prop_assert_eq!(ws_secs.rem_euclid(dur), 0);
            // timestamp is within [window_start, window_start + duration)
            prop_assert!(ws_secs <= ts);
            prop_assert!(ts < ws_secs + dur);
        }

        #[test]
        fn window_start_is_deterministic(
            ts in -1_000_000_000i64..2_000_000_000i64,
            dur in prop::sample::select(vec![60i64, 300, 900, 3600])
        ) {
            let ws1 = window_start(ts, dur).unwrap();
            let ws2 = window_start(ts, dur).unwrap();
            prop_assert_eq!(ws1, ws2);
        }

        #[test]
        fn adjacent_windows_do_not_overlap(
            ts in 0i64..1_000_000_000i64,
            dur in prop::sample::select(vec![60i64, 300, 900, 3600])
        ) {
            let ws = window_start(ts, dur).unwrap();
            let next_ws = window_start(ws.timestamp() + dur, dur).unwrap();
            // Next window starts exactly at current window end
            prop_assert_eq!(next_ws.timestamp(), ws.timestamp() + dur);
        }
    }

    // -----------------------------------------------------------------------
    // Unit tests: edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_negative_timestamp_crossing() {
        let ws = window_start(-1, 900).unwrap();
        assert_eq!(ws.timestamp(), -900);
    }

    #[test]
    fn test_zero_timestamp() {
        let ws = window_start(0, 900).unwrap();
        assert_eq!(ws.timestamp(), 0);
    }

    #[test]
    fn test_exactly_on_boundary() {
        let ws = window_start(900, 900).unwrap();
        assert_eq!(ws.timestamp(), 900);
    }

    #[test]
    fn test_one_before_boundary() {
        let ws = window_start(899, 900).unwrap();
        assert_eq!(ws.timestamp(), 0);
    }

    #[test]
    fn test_large_negative_timestamp() {
        let ws = window_start(-3601, 3600).unwrap();
        assert_eq!(ws.timestamp(), -7200);
    }

    #[test]
    fn test_60s_window() {
        let ws = window_start(1_700_000_042, 60).unwrap();
        assert_eq!(ws.timestamp(), 1_700_000_040);
    }

    // -----------------------------------------------------------------------
    // Validation tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_valid_window_durations() {
        let valid = [60, 120, 180, 240, 300, 360, 600, 720, 900, 1200, 1800, 3600];
        for dur in valid {
            assert!(
                validate_window_duration(dur).is_ok(),
                "duration {} should be valid",
                dur
            );
        }
    }

    #[test]
    fn test_invalid_window_durations() {
        // None of these evenly divide 3600.
        let invalid = [0, 7, 11, 13, 17, 700, 1000, 1500, 2000, 2400, 7200];
        for dur in invalid {
            assert!(
                validate_window_duration(dur).is_err(),
                "duration {} should be invalid",
                dur
            );
        }
    }

    #[test]
    fn test_small_valid_divisors_also_accepted() {
        // The function accepts all positive divisors of 3600, not just >= 60.
        let small_valid = [
            1, 2, 3, 4, 5, 6, 8, 9, 10, 12, 15, 16, 18, 20, 24, 25, 30, 36, 40, 45, 48, 50,
        ];
        for dur in small_valid {
            assert!(
                validate_window_duration(dur).is_ok(),
                "duration {} should be valid (divides 3600)",
                dur
            );
        }
    }

    #[test]
    fn test_zero_duration_error_message() {
        let err = validate_window_duration(0).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("must be positive"), "got: {msg}");
    }

    #[test]
    fn test_non_divisor_error_message() {
        let err = validate_window_duration(7).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("must evenly divide 3600"), "got: {msg}");
    }
}
