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
