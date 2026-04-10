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

// TODO coasetime has a recent() instead of now() which is essentially free (atomic read instead of
// vdso call), but needs us to spawn a future/thread updating that value regularly

use std::sync::atomic::{AtomicU64, Ordering};

use coarsetime::{Duration, Instant};

/// Metadata for a log site. This is stored inside a single AtomicU64 when not in use.
///
/// `call_count` is the number of calls since the last upgrade of generation, it's stored
/// in the lower 32b of the atomic, so it can just be incremented on the fast path.
/// `generation` is the number of time we reset the `call_count`. It isn't used as is, and
/// is just compared to itself to detect and handle properly concurrent resets from multiple
/// threads.
#[derive(Clone, Copy)]
struct LogSiteMetadata {
    generation: u32,
    call_count: u32,
}

impl From<u64> for LogSiteMetadata {
    fn from(val: u64) -> LogSiteMetadata {
        LogSiteMetadata {
            generation: (val >> 32) as u32,
            call_count: (val & ((1 << 32) - 1)) as u32,
        }
    }
}

impl From<LogSiteMetadata> for u64 {
    fn from(count: LogSiteMetadata) -> u64 {
        ((count.generation as u64) << 32) + count.call_count as u64
    }
}

/// Helper function used in [`rate_limited_tracing`] to determine if this line should log,
/// and update the related counters.
pub fn should_log<F: Fn() -> Instant>(
    count_atomic: &AtomicU64,
    last_reset_atomic: &AtomicU64,
    limit: u32,
    now: F,
) -> bool {
    //  count_atomic is treated as 2 u32: upper bits count "generation", lower bits count number of
    //  calls since LAST_RESET. We assume there won't be 2**32 calls to this log in ~60s.
    //  Generation is free to wrap around.

    // Because the lower 32 bits are storing the log count, we can
    // increment the entire u64 to record this log call.
    let logsite_meta_u64 = count_atomic.fetch_add(1, Ordering::Acquire);
    if logsite_meta_u64 == 0 {
        // this can only be reached the very 1st time we log
        last_reset_atomic.store(now().as_ticks(), Ordering::Release);
    }

    let LogSiteMetadata {
        generation,
        call_count,
    } = logsite_meta_u64.into();

    if call_count < limit {
        return true;
    }

    let current_time = Duration::from_ticks(now().as_ticks());
    let last_reset = Duration::from_ticks(last_reset_atomic.load(Ordering::Acquire));

    let should_reset = current_time.abs_diff(last_reset) >= Duration::from_secs(60);

    if !should_reset {
        // we are over-limit and not far enough in time to reset: don't log
        return false;
    }

    let mut update_time = false;

    let update_res =
        count_atomic.fetch_update(Ordering::Release, Ordering::Acquire, |current_count| {
            let mut current_count: LogSiteMetadata = current_count.into();
            if generation == current_count.generation {
                // we can update generation&time, so we can definitely log
                update_time = true;
                let new_count = LogSiteMetadata {
                    generation: generation.wrapping_add(1),
                    call_count: 1,
                };
                Some(new_count.into())
            } else {
                // we can't update generation&time, but maybe we can still log?
                update_time = false;
                if current_count.call_count < limit {
                    // we can log, update the count
                    current_count.call_count += 1;
                    Some(current_count.into())
                } else {
                    // we can't log, save some contention by not recording that we tried to
                    // log, and exit in error
                    None
                }
            }
        });
    let can_log = update_res.is_ok();

    // technically there is a race condition if we stay stuck *here* for > 60s, which
    // could cause us to log more than required. This is unlikely to happen, and not
    // really a big issue.

    if update_time {
        // *we* updated generation, so we must update last_reset too
        last_reset_atomic.store(current_time.as_ticks(), Ordering::Release);
    }
    can_log
}

#[macro_export]
macro_rules! rate_limited_tracing {
    ($log_fn:ident, limit_per_min=$limit:literal, $($args:tt)*) => {{
        use ::std::sync::atomic::AtomicU64;
        use $crate::rate_limited_tracing::CoarsetimeInstant;

        static COUNT: AtomicU64 = AtomicU64::new(0);
        // we can't get time from constant context, so we pre-initialize with zero
        static LAST_RESET: AtomicU64 = AtomicU64::new(0);

        if $crate::rate_limited_tracing::should_log(&COUNT, &LAST_RESET, $limit, CoarsetimeInstant::now) {
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
    ($unit:ident=$limit:literal, $($args:tt)*) => {
        $crate::rate_limited_tracing::rate_limited_tracing!(error, $unit=$limit, $($args)*)
    };
}

fn _check_macro_works() {
    rate_limited_info!(limit_per_min = 10, "test {}", "test");
}

#[doc(hidden)]
pub use coarsetime::Instant as CoarsetimeInstant;
#[doc(hidden)]
pub use rate_limited_tracing;
pub use {
    rate_limited_debug, rate_limited_error, rate_limited_info, rate_limited_trace,
    rate_limited_warn,
};

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use coarsetime::{Duration, Instant};

    use super::should_log;

    // TODO as this is atomic code, we should test it with multiple threads to verify it behaves
    // like we'd expect, maybe using something like `loom`?

    #[test]
    fn test_rate_limited_log_single_thread() {
        let count = AtomicU64::new(0);
        let last_reset = AtomicU64::new(0);
        let limit = 5u64;

        let mut simulated_time = Instant::now();
        let simulation_step = Duration::from_secs(1);

        assert!(should_log(&count, &last_reset, limit as _, || {
            simulated_time
        }));
        assert_eq!(count.load(Ordering::Relaxed), 1);
        let reset_timestamp = last_reset.load(Ordering::Relaxed);
        assert_ne!(reset_timestamp, 0);

        simulated_time += simulation_step;

        for i in 1..limit {
            // we log as many time as expected
            assert!(should_log(&count, &last_reset, limit as _, || {
                simulated_time
            }));
            assert_eq!(count.load(Ordering::Relaxed), i + 1);
            assert_eq!(last_reset.load(Ordering::Relaxed), reset_timestamp);
            simulated_time += simulation_step;
        }

        for i in limit..(limit * 2) {
            // we don't log, nor update
            assert!(!should_log(&count, &last_reset, limit as _, || {
                simulated_time
            }));
            assert_eq!(count.load(Ordering::Relaxed), i + 1);
            assert_eq!(last_reset.load(Ordering::Relaxed), reset_timestamp);
            simulated_time += simulation_step;
        }

        // advance enough to reset counter
        simulated_time += simulation_step * 60;

        assert!(should_log(&count, &last_reset, limit as _, || {
            simulated_time
        }));
        // counter got reset, generation increased
        assert_eq!(count.load(Ordering::Relaxed), 1 + (1 << 32));
        // last reset changed too
        assert_ne!(last_reset.load(Ordering::Relaxed), reset_timestamp);
        let reset_timestamp = last_reset.load(Ordering::Relaxed);

        for i in 1..limit {
            // we log as many time as expected
            assert!(should_log(&count, &last_reset, limit as _, || {
                simulated_time
            }));
            assert_eq!(count.load(Ordering::Relaxed), i + 1 + (1 << 32));
            assert_eq!(last_reset.load(Ordering::Relaxed), reset_timestamp);
            simulated_time += simulation_step;
        }

        for i in limit..(limit * 2) {
            // we don't log, nor update
            assert!(!should_log(&count, &last_reset, limit as _, || {
                simulated_time
            }));
            assert_eq!(count.load(Ordering::Relaxed), i + 1 + (1 << 32));
            assert_eq!(last_reset.load(Ordering::Relaxed), reset_timestamp);
            simulated_time += simulation_step;
        }
    }
}
