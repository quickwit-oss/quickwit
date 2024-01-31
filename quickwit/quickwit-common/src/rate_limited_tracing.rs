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

#[macro_export]
macro_rules! rate_limited_tracing {
    ($log_fn:ident, limit=$limit:literal, $($args:tt)*) => {{
        use ::std::sync::atomic::{AtomicU32, Ordering};
        use ::std::sync::Mutex;
        use ::std::time::{Instant, Duration};

        static COUNT: AtomicU32 = AtomicU32::new(0);
        // we can't build an Instant from const context, so we pinitialize with a None
        static LAST_RESET: Mutex<Option<Instant>> = Mutex::new(None);

        let count = COUNT.fetch_add(1, Ordering::Relaxed);
        if count == 0 {
            // this can only be reached the very 1st time we log
            *LAST_RESET.lock().unwrap() = Some(Instant::now());
        }

        let do_log = if count >= $limit {
            let mut last_reset = LAST_RESET.lock().unwrap();
            let current_time = Instant::now();
            let should_reset = last_reset
                .map(|last_reset| current_time.duration_since(last_reset) >= Duration::from_secs(60))
                .unwrap_or(true);

            if should_reset {
                *last_reset = Some(current_time);
                // we store 1 because we are already about to log something
                COUNT.store(1, Ordering::Relaxed);
                true
            } else {
                // we are over-limit and not far enough in time to reset: don't log
                false
            }
        } else {
            true
        };

        if do_log {
            ::tracing::$log_fn!($($args)*);
        }
    }};
}

#[macro_export]
macro_rules! rate_limited_trace {
    (limit=$limit:literal, $($args:tt)*) => {
        $crate::rate_limited_tracing::rate_limited_tracing!(trace, limit=$limit, $($args)*)
    };
}
#[macro_export]
macro_rules! rate_limited_debug {
    (limit=$limit:literal, $($args:tt)*) => {
        $crate::rate_limited_tracing::rate_limited_tracing!(debug, limit=$limit, $($args)*)
    };
}
#[macro_export]
macro_rules! rate_limited_info {
    (limit=$limit:literal, $($args:tt)*) => {
        $crate::rate_limited_tracing::rate_limited_tracing!(info, limit=$limit, $($args)*)
    };
}
#[macro_export]
macro_rules! rate_limited_warn {
    (limit=$limit:literal, $($args:tt)*) => {
        $crate::rate_limited_tracing::rate_limited_tracing!(warn, limit=$limit, $($args)*)
    };
}
#[macro_export]
macro_rules! rate_limited_error {
    (limit=$limit:literal, $($args:tt)*) => {
        $crate::rate_limited_tracing::rate_limited_tracing!(error, limit=$limit, $($args)*)
    };
}

#[doc(hidden)]
pub use rate_limited_tracing;
pub use {
    rate_limited_debug, rate_limited_error, rate_limited_info, rate_limited_trace,
    rate_limited_warn,
};
