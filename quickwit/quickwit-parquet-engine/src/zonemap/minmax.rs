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

//! MinMax builder for zonemap hash-range tracking.
//!
//! Tracks minimum and maximum FNV-1a hashes of registered values for
//! hash-based pruning at query time.
//!
//! Ported from Go: `logs-event-store/zonemap/minmax.go` and
//! `logs-event-store/hashutil/hashutil.go`.

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET_64: u64 = 14695981039346656037;

/// FNV-1a 64-bit prime.
const FNV_PRIME_64: u64 = 1099511628211;

/// Maximum number of string bytes to hash (matches Go `maxStringBytes`).
const MAX_STRING_BYTES: usize = 256;

/// Sentinel value used when FNV-1a produces zero (matches Go convention).
const ZERO_HASH_SENTINEL: u64 = 69;

/// Compute FNV-1a 64-bit hash for a `u64` value (little-endian byte order).
pub fn fnv1a_hash_u64(value: u64) -> u64 {
    let mut h = FNV_OFFSET_64;
    for i in 0..8u32 {
        let b = (value >> (8 * i)) as u8;
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME_64);
    }
    h
}

/// Compute FNV-1a 64-bit hash for a byte slice.
pub fn fnv1a_hash_bytes(data: &[u8]) -> u64 {
    let mut h = FNV_OFFSET_64;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME_64);
    }
    h
}

/// Hash an integer value, mapping zero hashes to the sentinel.
pub fn hash_int(value: i64) -> u64 {
    let h = fnv1a_hash_u64(value as u64);
    if h == 0 { ZERO_HASH_SENTINEL } else { h }
}

/// Hash a string value, mapping zero hashes to the sentinel.
/// Truncates to 256 bytes.
pub fn hash_string(value: &str) -> u64 {
    let bytes = if value.len() > MAX_STRING_BYTES {
        &value.as_bytes()[..MAX_STRING_BYTES]
    } else {
        value.as_bytes()
    };
    let h = fnv1a_hash_bytes(bytes);
    if h == 0 { ZERO_HASH_SENTINEL } else { h }
}

/// Result of a MinMax build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinMax {
    pub min_hash: u64,
    pub max_hash: u64,
    pub min_int: i64,
    pub max_int: i64,
}

/// Builder that tracks min/max hash values for strings and integers.
#[derive(Default)]
pub struct MinMaxBuilder {
    min_hash: u64,
    max_hash: u64,
    min_int: i64,
    max_int: i64,
    count: usize,
}

impl MinMaxBuilder {
    /// Create a new MinMaxBuilder.
    pub fn new() -> Self {
        MinMaxBuilder {
            min_hash: 0,
            max_hash: 0,
            min_int: 0,
            max_int: 0,
            count: 0,
        }
    }

    /// Reset the builder to its initial state.
    pub fn reset(&mut self) {
        self.min_hash = 0;
        self.max_hash = 0;
        self.min_int = 0;
        self.max_int = 0;
        self.count = 0;
    }

    /// Register a string value, updating min/max hashes.
    pub fn register(&mut self, value: &str) {
        let h = hash_string(value);
        self.count += 1;
        if self.count == 1 {
            self.min_hash = h;
            self.max_hash = h;
            return;
        }
        if h < self.min_hash {
            self.min_hash = h;
        }
        if h > self.max_hash {
            self.max_hash = h;
        }
    }

    /// Register an `i64` value, updating min/max hashes and min/max ints.
    pub fn register_int64(&mut self, value: i64) {
        let h = hash_int(value);
        self.count += 1;
        if self.count == 1 {
            self.min_hash = h;
            self.max_hash = h;
            self.min_int = value;
            self.max_int = value;
            return;
        }
        if h < self.min_hash {
            self.min_hash = h;
        }
        if h > self.max_hash {
            self.max_hash = h;
        }
        if value < self.min_int {
            self.min_int = value;
        }
        if value > self.max_int {
            self.max_int = value;
        }
    }

    /// Build and return the MinMax result.
    ///
    /// Returns `None` if no values were registered.
    pub fn build(&self) -> Option<MinMax> {
        if self.count == 0 {
            return None;
        }
        Some(MinMax {
            min_hash: self.min_hash,
            max_hash: self.max_hash,
            min_int: self.min_int,
            max_int: self.max_int,
        })
    }
}
