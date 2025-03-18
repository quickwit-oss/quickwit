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
pub mod tracker;
pub mod type_map;
pub mod uri;

mod socket_addr_legacy_hash;

use std::env;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::ops::{Range, RangeInclusive};
use std::str::FromStr;

pub use coolid::new_coolid;
pub use kill_switch::KillSwitch;
pub use path_hasher::PathHasher;
pub use progress::{Progress, ProtectedZoneGuard};
pub use socket_addr_legacy_hash::SocketAddrLegacyHash;
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
            info!(value=?value, "using environment variable `{key}` value");
            return value;
        } else {
            error!(value=%value_str, "failed to parse environment variable `{key}` value");
        }
    }
    info!(value=?default_value, "using environment variable `{key}` default value");
    default_value
}

pub fn get_bool_from_env(key: &str, default_value: bool) -> bool {
    if let Ok(value_str) = std::env::var(key) {
        if let Some(value) = parse_bool_lenient(&value_str) {
            info!(value=%value, "using environment variable `{key}` value");
            return value;
        } else {
            error!(value=%value_str, "failed to parse environment variable `{key}` value");
        }
    }
    info!(value=?default_value, "using environment variable `{key}` default value");
    default_value
}

pub fn get_from_env_opt<T: FromStr + Debug>(key: &str) -> Option<T> {
    let Some(value_str) = std::env::var(key).ok() else {
        info!("environment variable `{key}` is not set");
        return None;
    };
    if let Ok(value) = T::from_str(&value_str) {
        info!(value=?value, "using environment variable `{key}` value");
        Some(value)
    } else {
        error!(value=%value_str, "failed to parse environment variable `{key}` value");
        None
    }
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
            error!(error=?io_error, "failed to detect the number of threads available: arbitrarily returning 2");
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

pub fn parse_bool_lenient(bool_str: &str) -> Option<bool> {
    let trimmed_bool_str = bool_str.trim();

    for truthy_value in ["true", "yes", "1"] {
        if trimmed_bool_str.eq_ignore_ascii_case(truthy_value) {
            return Some(true);
        }
    }
    for falsy_value in ["false", "no", "0"] {
        if trimmed_bool_str.eq_ignore_ascii_case(falsy_value) {
            return Some(false);
        }
    }
    None
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

    #[test]
    fn test_parse_bool_lenient() {
        assert_eq!(parse_bool_lenient("true"), Some(true));
        assert_eq!(parse_bool_lenient("TRUE"), Some(true));
        assert_eq!(parse_bool_lenient("True"), Some(true));
        assert_eq!(parse_bool_lenient("yes"), Some(true));
        assert_eq!(parse_bool_lenient(" 1"), Some(true));

        assert_eq!(parse_bool_lenient("false"), Some(false));
        assert_eq!(parse_bool_lenient("FALSE"), Some(false));
        assert_eq!(parse_bool_lenient("False"), Some(false));
        assert_eq!(parse_bool_lenient("no"), Some(false));
        assert_eq!(parse_bool_lenient("0 "), Some(false));

        assert_eq!(parse_bool_lenient("foo"), None);
    }
}
