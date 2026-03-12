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

//! Invariant checking macro — Layers 3 + 4 of the verification stack.
//!
//! The condition is **always evaluated** (debug and release). Results are:
//!
//! - **Debug builds (Layer 3 — Prevention):** panics on violation via
//!   `debug_assert!`, catching bugs during development and testing.
//! - **All builds (Layer 4 — Production):** forwards the result to the
//!   registered [`InvariantRecorder`](super::recorder::InvariantRecorder)
//!   for Datadog metrics emission. No-op if no recorder is set.

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
