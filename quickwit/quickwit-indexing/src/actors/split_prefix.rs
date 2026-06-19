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

//! Computes the S3 key prefix used to shard split object keys across S3 partitions and avoid
//! hotspots. See [`compute_split_key_prefix`].

use std::sync::LazyLock;

use quickwit_common::rate_limited_warn;
use tracing::warn;

/// Maximum prefix length: the ULID random portion (positions 10–25) is 16 characters long.
const MAX_SPLIT_KEY_PREFIX_LEN: u8 = 16;

/// Returns the number of characters from the ULID random portion used as an S3 key prefix
/// directory.
///
/// Controlled by the `QW_SPLIT_KEY_PREFIX_LEN` environment variable (default: 0, i.e. the legacy
/// flat scheme). Setting this to `2` creates 32^2 = 1024 buckets, which is sufficient for most
/// workloads. Values above [`MAX_SPLIT_KEY_PREFIX_LEN`] are clamped.
///
/// The value is read from the environment once and cached for the lifetime of the process.
fn split_key_prefix_len() -> u8 {
    static SPLIT_KEY_PREFIX_LEN: LazyLock<u8> = LazyLock::new(|| {
        let prefix_len: u8 = quickwit_common::get_from_env("QW_SPLIT_KEY_PREFIX_LEN", 0u8, false);
        if prefix_len > MAX_SPLIT_KEY_PREFIX_LEN {
            warn!(
                prefix_len,
                max = MAX_SPLIT_KEY_PREFIX_LEN,
                "`QW_SPLIT_KEY_PREFIX_LEN` exceeds maximum; clamping"
            );
            return MAX_SPLIT_KEY_PREFIX_LEN;
        }
        prefix_len
    });
    *SPLIT_KEY_PREFIX_LEN
}

/// Computes the S3 key prefix for a split from its ULID, using the process-wide
/// `QW_SPLIT_KEY_PREFIX_LEN` configuration (see [`split_key_prefix_len`]).
///
/// Extracts the configured number of characters starting at position 10 of the ULID (the first
/// characters of the random portion, after the 10-character timestamp). Returns an empty string
/// when the configured length is 0 (legacy flat scheme).
///
/// The returned prefix does NOT contain the trailing `/` separator; that separator is added by
/// [`quickwit_common::split_storage_path`] when building the full storage path.
///
/// Hidden contract: `split_id` must be a valid 26-character ULID. If it is too short to extract
/// the requested prefix, a rate-limited warning is logged and an empty string is returned, falling
/// back to the legacy flat scheme.
pub(crate) fn compute_split_key_prefix(split_id: &str) -> String {
    let prefix_len = split_key_prefix_len();
    if prefix_len == 0 {
        return String::new();
    }
    let end = 10 + prefix_len as usize;
    if split_id.len() < end {
        rate_limited_warn!(
            limit_per_min = 1,
            split_id = split_id,
            prefix_len = prefix_len,
            "split ID is too short to extract prefix; falling back to flat storage path"
        );
        return String::new();
    }
    split_id[10..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_ULID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    // The tests below mutate the `QW_SPLIT_KEY_PREFIX_LEN` environment variable, which feeds a
    // process-wide cached `LazyLock`. They rely on `cargo nextest` running each test in its own
    // process so the cache (and the environment) does not leak between tests.

    #[test]
    fn test_split_key_prefix_len_disabled_by_default() {
        assert_eq!(split_key_prefix_len(), 0);
    }

    #[test]
    fn test_compute_split_key_prefix_disabled_by_default() {
        assert_eq!(compute_split_key_prefix(SAMPLE_ULID), "");
    }

    #[test]
    fn test_compute_split_key_prefix_extracts_random_portion() {
        // SAFETY: under nextest each test runs in its own process, so this does not race.
        unsafe { std::env::set_var("QW_SPLIT_KEY_PREFIX_LEN", "4") };
        // Characters 10–13 of the ULID are the first 4 characters of the random portion.
        assert_eq!(compute_split_key_prefix(SAMPLE_ULID), "TSV4");
    }

    #[test]
    fn test_compute_split_key_prefix_too_short_falls_back() {
        // SAFETY: under nextest each test runs in its own process, so this does not race.
        unsafe { std::env::set_var("QW_SPLIT_KEY_PREFIX_LEN", "4") };
        assert_eq!(compute_split_key_prefix("01ARZ"), "");
    }
}
