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

// TODO coasetime has a recent() instead of now() which is essentially free (atomic read instead of
// vdso call), but needs us to spawn a future/thread updating that value regularly

#[macro_export]
macro_rules! rate_limited_tracing {
    ($log_fn:ident, limit_per_min=$limit:literal, $($args:tt)*) => {{
        use ::std::sync::atomic::{AtomicU64, Ordering};

        use $crate::rate_limited_tracing::coarsetime::{Instant, Duration};

        //  This is treated as 2 u32: upper bits count "generation", lower bits count number of
        //  calls since LAST_RESET. We assume there won't be 2**32 calls to this log in ~60s.
        //  Generation is free to wrap arround.
        static COUNT: AtomicU64 = AtomicU64::new(0);
        const MASK: u64 = 0xffffffff;
        // we can't get time from constant context, so we pre-initialize with zero
        static LAST_RESET: AtomicU64 = AtomicU64::new(0);

        let count = COUNT.fetch_add(1, Ordering::Acquire);
        if count == 0 {
            // this can only be reached the very 1st time we log
            LAST_RESET.store(Instant::now().as_ticks(), Ordering::Release);
        }

        let do_log = if count & MASK >= $limit {
            let current_time = Duration::from_ticks(Instant::now().as_ticks());
            let last_reset = Duration::from_ticks(LAST_RESET.load(Ordering::Acquire));

            let should_reset = current_time.abs_diff(last_reset) >= Duration::from_secs(60);

            if should_reset {
                let generation = count >> 32;
                let mut update_time = false;
                let mut can_log = false;

                let _ = COUNT.fetch_update(Ordering::Release, Ordering::Acquire, |current_count| {
                    let current_generation = current_count >> 32;
                    if generation == current_generation {
                        // we can update generation&time, so we can definitely log
                        update_time = true;
                        can_log = true;
                        let generation_part = (generation + 1) << 32;
                        let count_part = 1;
                        Some(generation_part + count_part)
                    } else {
                        // we can't update generation&time, but maybe we can still log?
                        update_time = false;
                        if current_generation & MASK < $limit {
                            // we can log, update the count
                            can_log = true;
                            Some(current_count + 1)
                        } else {
                            // we can't log, save some contention by not recording that we tried to
                            // log
                            can_log = false;
                            None
                        }
                    }
                });

                // technically there is a race condition if we stay stuck *here* for > 60s, which
                // could cause us to log more than required. This is unlikely to happen, and not
                // really a big issue.

                if update_time {
                    // *we* updated generation, so we must update last_reset too
                    LAST_RESET.store(current_time.as_ticks(), Ordering::Release);
                }
                can_log
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
    ($unit:ident=$limit:literal, $($args:tt)*) => {
        $crate::rate_limited_tracing::rate_limited_tracing!(trace, $unit=$limit, $($args)*)
    };
}
#[macro_export]
macro_rules! rate_limited_debug {
    ($unit:ident=$limit:literal, $($args:tt)*) => {
        $crate::rate_limited_tracing::rate_limited_tracing!(debug, $unit=$limit, $($args)*)
    };
}
#[macro_export]
macro_rules! rate_limited_info {
    ($unit:ident=$limit:literal, $($args:tt)*) => {
        $crate::rate_limited_tracing::rate_limited_tracing!(info, $unit=$limit, $($args)*)
    };
}
#[macro_export]
macro_rules! rate_limited_warn {
    ($unit:ident=$limit:literal, $($args:tt)*) => {
        $crate::rate_limited_tracing::rate_limited_tracing!(warn, $unit=$limit, $($args)*)
    };
}
#[macro_export]
macro_rules! rate_limited_error {
    ($unit:literal=$limit:literal, $($args:tt)*) => {
        $crate::rate_limited_tracing::rate_limited_tracing!(error, $unit=$limit, $($args)*)
    };
}

fn _check_macro_works() {
    rate_limited_info!(limit_per_min = 10, "test {}", "test");
}

#[doc(hidden)]
pub use coarsetime;
#[doc(hidden)]
pub use rate_limited_tracing;
pub use {
    rate_limited_debug, rate_limited_error, rate_limited_info, rate_limited_trace,
    rate_limited_warn,
};
