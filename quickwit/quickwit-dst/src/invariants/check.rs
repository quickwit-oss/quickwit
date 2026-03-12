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

//! Invariant checking macro.
//!
//! Wraps `debug_assert!` with the invariant ID, providing a single hook point
//! for future Datadog metrics emission (Layer 4 of the verification stack).

/// Check an invariant condition. In debug builds, panics on violation.
/// In release builds, currently a no-op (future: emit Datadog metric).
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
    ($id:expr, $cond:expr) => {
        debug_assert!($cond, "{} violated", $id);
    };
    ($id:expr, $cond:expr, $fmt:literal $($arg:tt)*) => {
        debug_assert!($cond, concat!("{} violated", $fmt), $id $($arg)*);
    };
}
