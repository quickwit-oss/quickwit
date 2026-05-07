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

//! Pluggable invariant recorder — Layer 4 of the verification stack.
//!
//! Every call to [`check_invariant!`](crate::check_invariant) evaluates the
//! condition in **all** build profiles (debug and release). The result is
//! forwarded to a recorder function that can emit metrics (StatsD/DogStatsD,
//! Prometheus, etc.), log violations, or take any other action.
//!
//! # Wiring up a metrics recorder
//!
//! Call [`set_invariant_recorder`] once at process startup. The OSS Quickwit
//! binary wires a Prometheus-backed recorder in `quickwit_cli::logger`; the
//! example below shows the minimal recorder shape:
//!
//! ```rust
//! use quickwit_dst::invariants::{InvariantId, set_invariant_recorder};
//!
//! fn my_recorder(id: InvariantId, passed: bool) {
//!     if !passed {
//!         eprintln!("{} violated in production", id);
//!     }
//! }
//!
//! set_invariant_recorder(my_recorder);
//! ```

use std::sync::OnceLock;

use super::InvariantId;

/// Signature for an invariant recorder function.
///
/// Called on every `check_invariant!` invocation with the invariant ID and
/// whether the check passed. Implementations must be cheap — this is called
/// on hot paths.
pub type InvariantRecorder = fn(InvariantId, bool);

/// Global recorder. When unset, [`record_invariant_check`] is a no-op.
static RECORDER: OnceLock<InvariantRecorder> = OnceLock::new();

/// Register a global invariant recorder.
///
/// Should be called once at process startup. Subsequent calls are ignored
/// (first writer wins). This is safe to call from any thread.
pub fn set_invariant_recorder(recorder: InvariantRecorder) {
    // OnceLock::set returns Err if already initialized — that's fine.
    let _ = RECORDER.set(recorder);
}

/// Record an invariant check result.
///
/// Called by [`check_invariant!`](crate::check_invariant) on every invocation,
/// in both debug and release builds. If no recorder has been registered via
/// [`set_invariant_recorder`], this is a no-op (single atomic load).
#[inline]
pub fn record_invariant_check(id: InvariantId, passed: bool) {
    if let Some(recorder) = RECORDER.get() {
        recorder(id, passed);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    // Note: these tests use a process-global OnceLock, so only the first
    // test to run can set the recorder. We test the no-recorder path and
    // the recorder path in a single test to avoid ordering issues.

    #[test]
    fn record_without_recorder_is_noop() {
        // Before any recorder is set, this should not panic.
        // (In the test binary, another test may have set it, so we just
        // verify it doesn't panic either way.)
        record_invariant_check(InvariantId::SS1, true);
        record_invariant_check(InvariantId::SS1, false);
    }

    #[test]
    fn recorder_receives_calls() {
        static CHECKS: AtomicU32 = AtomicU32::new(0);
        static VIOLATIONS: AtomicU32 = AtomicU32::new(0);

        fn test_recorder(_id: InvariantId, passed: bool) {
            CHECKS.fetch_add(1, Ordering::Relaxed);
            if !passed {
                VIOLATIONS.fetch_add(1, Ordering::Relaxed);
            }
        }

        // May fail if another test already set the recorder — that's OK,
        // the test still verifies the function doesn't panic.
        let _ = RECORDER.set(test_recorder);

        let before_checks = CHECKS.load(Ordering::Relaxed);
        let before_violations = VIOLATIONS.load(Ordering::Relaxed);

        record_invariant_check(InvariantId::TW2, true);
        record_invariant_check(InvariantId::TW2, false);

        // If our recorder was set, we should see the increments.
        // If another recorder was set first, we can't assert on counts.
        if RECORDER.get() == Some(&(test_recorder as InvariantRecorder)) {
            assert_eq!(CHECKS.load(Ordering::Relaxed), before_checks + 2);
            assert_eq!(VIOLATIONS.load(Ordering::Relaxed), before_violations + 1);
        }
    }
}
