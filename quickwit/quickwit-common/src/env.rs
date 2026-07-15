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

use std::fmt::Debug;
use std::str::FromStr;

use tracing::{error, info};

fn get_from_env_opt_aux<T: Debug>(
    key: &str,
    parse_fn: impl FnOnce(&str) -> Option<T>,
    sensitive: bool,
) -> Option<T> {
    let value_str = std::env::var(key).ok()?;
    let Some(value) = parse_fn(&value_str) else {
        error!(value=%value_str, "failed to parse environment variable `{key}` value");
        return None;
    };
    if sensitive {
        info!("using environment variable `{key}` value");
    } else {
        info!(value=?value, "using environment variable `{key}` value");
    }
    Some(value)
}

pub fn get_from_env<T: FromStr + Debug>(key: &str, default_value: T, sensitive: bool) -> T {
    if let Some(value) = get_from_env_opt(key, sensitive) {
        value
    } else {
        info!(default_value=?default_value, "using environment variable `{key}` default value");
        default_value
    }
}

pub fn get_from_env_opt<T: FromStr + Debug>(key: &str, sensitive: bool) -> Option<T> {
    get_from_env_opt_aux(key, |val_str| val_str.parse().ok(), sensitive)
}

pub fn get_bool_from_env_opt(key: &str) -> Option<bool> {
    get_from_env_opt_aux(key, parse_bool_lenient, false)
}

pub fn get_bool_from_env(key: &str, default_value: bool) -> bool {
    if let Some(flag_value) = get_bool_from_env_opt(key) {
        flag_value
    } else {
        info!(default_value=%default_value, "using environment variable `{key}` default value");
        default_value
    }
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

/// Reads and parses an environment variable exactly once, caching the result for the lifetime of
/// the process via a per-call-site [`std::sync::LazyLock`].
///
/// Prefer this over [`get_from_env`](crate::get_from_env) on paths that may run repeatedly (e.g.
/// constructors that are re-invoked): it avoids re-reading and, more importantly, re-logging the
/// same variable on every call.
///
/// The type is required because the backing `static` needs a concrete type. The key and default
/// must be `const` expressions or literals — a `static` initializer cannot capture locals.
///
/// ```no_run
/// # use quickwit_common::get_from_env_cached;
/// let max_concurrency: usize = get_from_env_cached!(usize, "QW_S3_MAX_CONCURRENCY", 10_000, false);
/// ```
#[macro_export]
macro_rules! get_from_env_cached {
    ($ty:ty, $key:expr, $default:expr, $sensitive:expr $(,)?) => {{
        static CACHED: ::std::sync::LazyLock<$ty> =
            ::std::sync::LazyLock::new(|| $crate::get_from_env::<$ty>($key, $default, $sensitive));
        // `LazyLock<T>` derefs to `T`; clone so callers receive an owned value, matching
        // `get_from_env`'s return type (a no-op copy for the common `Copy` cases).
        #[allow(clippy::clone_on_copy)]
        let value = (*CACHED).clone();
        value
    }};
}

/// Boolean counterpart of [`get_from_env_cached`], using the same lenient parsing as
/// [`get_bool_from_env`](crate::get_bool_from_env). See that macro for the caching semantics and
/// constraints.
///
/// ```no_run
/// # use quickwit_common::get_bool_from_env_cached;
/// let cors_debug: bool = get_bool_from_env_cached!("QW_ENABLE_CORS_DEBUG", false);
/// ```
#[macro_export]
macro_rules! get_bool_from_env_cached {
    ($key:expr, $default:expr $(,)?) => {{
        static CACHED: ::std::sync::LazyLock<bool> =
            ::std::sync::LazyLock::new(|| $crate::get_bool_from_env($key, $default));
        *CACHED
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_from_env() {
        // SAFETY: this test may not be entirely sound if not run with nextest or --test-threads=1
        // as this is only a test, and it would be extremely inconvenient to run it in a different
        // way, we are keeping it that way

        const TEST_KEY: &str = "TEST_KEY";
        assert_eq!(get_from_env(TEST_KEY, 10, false), 10);
        unsafe { std::env::set_var(TEST_KEY, "15") };
        assert_eq!(get_from_env(TEST_KEY, 10, false), 15);
        unsafe { std::env::set_var(TEST_KEY, "1invalidnumber") };
        assert_eq!(get_from_env(TEST_KEY, 10, false), 10);
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
