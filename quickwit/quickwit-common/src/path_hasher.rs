// Copyright (C) 2023 Quickwit, Inc.
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

use std::hash::Hasher;

/// Mini wrapper over the FnvHasher to incrementally hash nodes
/// in a tree.
///
/// The wrapper does not do too much. Its main purpose to
/// work around the lack of Clone in the fnv Hasher
/// and enforce a 0 byte separator between segments.
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
    /// Helper function, mostly for tests.
    pub fn hash_path(segments: &[&[u8]]) -> u64 {
        let mut hasher = Self::default();
        for segment in segments {
            hasher.append(segment);
        }
        hasher.finish()
    }

    /// Appends a new segment to our path.
    ///
    /// In order to avoid natural collisions, (e.g. &["ab", "c"] and &["a", "bc"]),
    /// we add a null byte between each segment as a separator.
    #[inline]
    pub fn append(&mut self, payload: &[u8]) {
        self.hasher.write(payload);
        // We use 255 as a separator as all utf8 bytes contain a 0
        // in position 0-5.
        self.hasher.write(&[255u8]);
    }

    #[inline]
    pub fn finish(&self) -> u64 {
        self.hasher.finish()
    }
}
