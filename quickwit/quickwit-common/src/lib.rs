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

#![deny(clippy::disallowed_methods)]

mod coolid;

pub mod binary_heap;
pub mod fs;
pub mod io;
mod kill_switch;
pub mod metrics;
pub mod net;
mod path_hasher;
pub mod pretty;
mod progress;
pub mod pubsub;
pub mod rand;
pub mod rate_limited_tracing;
pub mod rate_limiter;
pub mod rendezvous_hasher;
pub mod retry;
pub mod runtimes;
pub mod shared_consts;
pub mod sorted_iter;
pub mod stream_utils;
pub mod temp_dir;
#[cfg(any(test, feature = "testsuite"))]
pub mod test_utils;
pub mod thread_pool;
pub mod tower;
pub mod type_map;
pub mod uri;

use std::env;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::ops::{Range, RangeInclusive};
use std::str::FromStr;

pub use coolid::new_coolid;
pub use kill_switch::KillSwitch;
pub use path_hasher::PathHasher;
pub use progress::{Progress, ProtectedZoneGuard};
pub use stream_utils::{BoxStream, ServiceStream};
use tracing::{error, info};

pub fn chunk_range(range: Range<usize>, chunk_size: usize) -> impl Iterator<Item = Range<usize>> {
    range.clone().step_by(chunk_size).map(move |block_start| {
        let block_end = (block_start + chunk_size).min(range.end);
        block_start..block_end
    })
}

pub fn into_u64_range(range: Range<usize>) -> Range<u64> {
    range.start as u64..range.end as u64
}

pub fn setup_logging_for_tests() {
    let _ = env_logger::builder().format_timestamp(None).try_init();
}

pub fn split_file(split_id: impl Display) -> String {
    format!("{split_id}.split")
}

pub fn get_from_env<T: FromStr + Debug>(key: &str, default_value: T) -> T {
    if let Ok(value_str) = std::env::var(key) {
        if let Ok(value) = T::from_str(&value_str) {
            info!(value=?value, "setting `{}` from environment", key);
            return value;
        } else {
            error!(value_str=%value_str, "failed to parse `{}` from environment", key);
        }
    }
    info!(value=?default_value, "setting `{}` from default", key);
    default_value
}

pub fn truncate_str(text: &str, max_len: usize) -> &str {
    if max_len > text.len() {
        return text;
    }

    let mut truncation_index = max_len;
    while !text.is_char_boundary(truncation_index) {
        truncation_index -= 1;
    }
    &text[..truncation_index]
}

/// Extracts time range from optional start and end timestamps.
pub fn extract_time_range(
    start_timestamp_opt: Option<i64>,
    end_timestamp_opt: Option<i64>,
) -> Option<Range<i64>> {
    match (start_timestamp_opt, end_timestamp_opt) {
        (Some(start_timestamp), Some(end_timestamp)) => Some(Range {
            start: start_timestamp,
            end: end_timestamp,
        }),
        (_, Some(end_timestamp)) => Some(Range {
            start: i64::MIN,
            end: end_timestamp,
        }),
        (Some(start_timestamp), _) => Some(Range {
            start: start_timestamp,
            end: i64::MAX,
        }),
        _ => None,
    }
}

/// Takes 2 intervals and returns true iff their intersection is empty
pub fn is_disjoint(left: &Range<i64>, right: &RangeInclusive<i64>) -> bool {
    left.end <= *right.start() || *right.end() < left.start
}

/// For use with the `skip_serializing_if` serde attribute.
pub fn is_false(value: &bool) -> bool {
    !*value
}

pub fn no_color() -> bool {
    matches!(env::var("NO_COLOR"), Ok(value) if !value.is_empty())
}

#[macro_export]
macro_rules! ignore_error_kind {
    ($kind:path, $expr:expr) => {
        match $expr {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == $kind => Ok(()),
            Err(error) => Err(error),
        }
    };
}

#[inline]
pub const fn div_ceil_u32(lhs: u32, rhs: u32) -> u32 {
    let d = lhs / rhs;
    let r = lhs % rhs;
    if r > 0 {
        d + 1
    } else {
        d
    }
}

#[inline]
pub const fn div_ceil(lhs: i64, rhs: i64) -> i64 {
    let d = lhs / rhs;
    let r = lhs % rhs;
    if (r > 0 && rhs > 0) || (r < 0 && rhs < 0) {
        d + 1
    } else {
        d
    }
}

/// Return the number of vCPU/hyperthreads available.
/// This number is usually not equal to the number of cpu cores
pub fn num_cpus() -> usize {
    match std::thread::available_parallelism() {
        Ok(num_cpus) => num_cpus.get(),
        Err(io_error) => {
            error!(errror=?io_error, "failed to detect the number of threads available: arbitrarily returning 2");
            2
        }
    }
}

// The following are helpers to build named tasks.
//
// Named tasks require the tokio feature `tracing` to be enabled.
// If the `named_tasks` feature is disabled, this is no-op.
//
// By default, these function will just ignore the name passed and just act
// like a regular call to `tokio::spawn`.
//
// If the user compiles `quickwit-cli` with the `tokio-console` feature,
// then tasks will automatically be named. This is not just "visual sugar".
//
// Without names, tasks will only show their spawn site on tokio-console.
// This is a catastrophy for actors who all share the same spawn site.
//
// # Naming
//
// Actors will get named after their type, which is fine.
// For other tasks, please use `snake_case`.

#[cfg(not(all(tokio_unstable, feature = "named_tasks")))]
pub fn spawn_named_task<F>(future: F, _name: &'static str) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::task::spawn(future)
}

#[cfg(not(all(tokio_unstable, feature = "named_tasks")))]
pub fn spawn_named_task_on<F>(
    future: F,
    _name: &'static str,
    runtime: &tokio::runtime::Handle,
) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    runtime.spawn(future)
}

#[cfg(all(tokio_unstable, feature = "named_tasks"))]
pub fn spawn_named_task<F>(future: F, name: &'static str) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::task::Builder::new()
        .name(name)
        .spawn(future)
        .unwrap()
}

#[cfg(all(tokio_unstable, feature = "named_tasks"))]
pub fn spawn_named_task_on<F>(
    future: F,
    name: &'static str,
    runtime: &tokio::runtime::Handle,
) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::task::Builder::new()
        .name(name)
        .spawn_on(future, runtime)
        .unwrap()
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use super::*;

    #[test]
    fn test_get_from_env() {
        const TEST_KEY: &str = "TEST_KEY";
        assert_eq!(super::get_from_env(TEST_KEY, 10), 10);
        std::env::set_var(TEST_KEY, "15");
        assert_eq!(super::get_from_env(TEST_KEY, 10), 15);
        std::env::set_var(TEST_KEY, "1invalidnumber");
        assert_eq!(super::get_from_env(TEST_KEY, 10), 10);
    }

    #[test]
    fn test_truncate_str() {
        assert_eq!(truncate_str("", 0), "");
        assert_eq!(truncate_str("", 3), "");
        assert_eq!(truncate_str("hello", 0), "");
        assert_eq!(truncate_str("hello", 5), "hello");
        assert_eq!(truncate_str("hello", 6), "hello");
        assert_eq!(truncate_str("hello-world", 5), "hello");
        assert_eq!(truncate_str("hello-world", 6), "hello-");
        assert_eq!(truncate_str("hello🧑‍🔬world", 6), "hello");
        assert_eq!(truncate_str("hello🧑‍🔬world", 7), "hello");
    }

    #[test]
    fn test_ignore_io_error_macro() {
        ignore_error_kind!(
            ErrorKind::NotFound,
            std::fs::remove_file("file-does-not-exist")
        )
        .unwrap();
    }

    #[test]
    fn test_div_ceil() {
        assert_eq!(div_ceil(5, 1), 5);
        assert_eq!(div_ceil(5, 2), 3);
        assert_eq!(div_ceil(6, 2), 3);

        assert_eq!(div_ceil(3, 3), 1);
        assert_eq!(div_ceil(2, 3), 1);
        assert_eq!(div_ceil(1, 3), 1);
        assert_eq!(div_ceil(0, 3), 0);
        assert_eq!(div_ceil(-1, 3), 0);
        assert_eq!(div_ceil(-2, 3), 0);

        assert_eq!(div_ceil(-5, 1), -5);
        assert_eq!(div_ceil(-5, 2), -2);
        assert_eq!(div_ceil(-6, 2), -3);

        assert_eq!(div_ceil(5, -1), -5);
        assert_eq!(div_ceil(5, -2), -2);
        assert_eq!(div_ceil(6, -2), -3);

        assert_eq!(div_ceil(-5, -1), 5);
        assert_eq!(div_ceil(-5, -2), 3);
        assert_eq!(div_ceil(-6, -2), 3);
    }

    #[test]
    fn test_div_ceil_u32() {
        assert_eq!(div_ceil_u32(5, 1), 5);
        assert_eq!(div_ceil_u32(5, 2), 3);
        assert_eq!(div_ceil_u32(6, 2), 3);
        assert_eq!(div_ceil_u32(3, 3), 1);
        assert_eq!(div_ceil_u32(2, 3), 1);
        assert_eq!(div_ceil_u32(1, 3), 1);
        assert_eq!(div_ceil_u32(0, 3), 0);
    }
}
