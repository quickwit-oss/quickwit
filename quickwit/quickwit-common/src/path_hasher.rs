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

use std::hash::Hasher;

/// We use 255 as a separator as it isn't used by utf-8.
///
/// Tantivy uses 1 because it is more convenient for range queries, but we don't
/// care about the sort order here.
///
/// Note: changing this is not retro-compatible!
const SEPARATOR: &[u8] = &[255];

/// Mini wrapper over the FnvHasher to incrementally hash nodes
/// in a tree.
///
/// Its purpose is to:
/// - work around the lack of Clone in the fnv Hasher
/// - enforce a 1 byte separator between segments
#[derive(Default)]
pub struct PathHasher {
    hasher: fnv::FnvHasher,
}

impl Clone for PathHasher {
    #[inline(always)]
    fn clone(&self) -> PathHasher {
        PathHasher {
            hasher: fnv::FnvHasher::with_key(self.hasher.finish()),
        }
    }
}

impl PathHasher {
    #[cfg(any(test, feature = "testsuite"))]
    pub fn hash_path(segments: &[&[u8]]) -> u64 {
        let mut hasher = Self::default();
        for segment in segments {
            hasher.append(segment);
        }
        hasher.finish_leaf()
    }

    /// Appends a new segment to our path.
    ///
    /// In order to avoid natural collisions, (e.g. &["ab", "c"] and &["a", "bc"]),
    /// we add a null byte between each segment as a separator.
    #[inline]
    pub fn append(&mut self, payload: &[u8]) {
        self.hasher.write(payload);
        self.hasher.write(SEPARATOR);
    }

    #[inline]
    pub fn finish_leaf(&self) -> u64 {
        self.hasher.finish()
    }

    #[inline]
    pub fn finish_intermediate(&self) -> u64 {
        let mut intermediate = fnv::FnvHasher::with_key(self.hasher.finish());
        intermediate.write(SEPARATOR);
        intermediate.finish()
    }
}
