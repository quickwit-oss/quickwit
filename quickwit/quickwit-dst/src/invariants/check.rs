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

//! Invariant checking macro — Layers 3 + 4 of the verification stack.
//!
//! The condition is **always evaluated** (debug and release). Results are:
//!
//! - **Debug builds (Layer 3 — Prevention):** panics on violation via `debug_assert!`, catching
//!   bugs during development and testing.
//! - **All builds (Layer 4 — Production):** forwards the result to the registered
//!   [`InvariantRecorder`](super::recorder::InvariantRecorder) for metrics emission. No-op if no
//!   recorder is set.

/// Check an invariant condition in all build profiles.
///
/// The condition is always evaluated. In debug builds, a violation panics.
/// In all builds, the result is forwarded to the registered invariant
/// recorder for metrics emission (see [`set_invariant_recorder`]).
///
/// [`set_invariant_recorder`]: crate::invariants::set_invariant_recorder
///
/// # Examples
///
/// ```
/// use quickwit_dst::check_invariant;
/// use quickwit_dst::invariants::InvariantId;
///
/// let duration_secs = 900u32;
/// check_invariant!(InvariantId::TW2, 3600 % duration_secs == 0, ": duration={}", duration_secs);
/// ```
#[macro_export]
macro_rules! check_invariant {
    ($id:expr, $cond:expr) => {{
        let passed = $cond;
        $crate::invariants::record_invariant_check($id, passed);
        debug_assert!(passed, "{} violated", $id);
    }};
    ($id:expr, $cond:expr, $fmt:literal $($arg:tt)*) => {{
        let passed = $cond;
        $crate::invariants::record_invariant_check($id, passed);
        debug_assert!(passed, concat!("{} violated", $fmt), $id $($arg)*);
    }};
}
