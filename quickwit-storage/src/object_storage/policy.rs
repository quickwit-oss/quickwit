// Copyright (C) 2021 Quickwit, Inc.
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

/// The multipart policy defines when and how multipart upload / download should happen.
///
/// The right settings might be vendor specific, but if not available the default values
/// should be safe.
pub struct MultiPartPolicy {
    /// Ideal part size.
    /// Since S3 has a constraint on the number of parts, it cannot always be
    /// respected.
    pub target_part_num_bytes: usize,
    /// Maximum number of parts allowed.
    pub max_num_parts: usize,
    /// Threshold above which multipart is trigged.
    pub multipart_threshold_num_bytes: u64,
    /// Maximum size allowed for an object.
    pub max_object_num_bytes: u64,
    /// Maximum number of part to be upload concurrently.
    pub max_concurrent_upload: usize,
}

impl MultiPartPolicy {
    /// This function returns the size of the part that should
    /// be used. We should have `part_num_bytes(len)` <= `len`.
    ///
    /// If this function returns `len`, then multipart upload
    /// will not be used.
    pub fn part_num_bytes(&self, len: u64) -> u64 {
        assert!(
            len < self.max_object_num_bytes,
            "This objet storage does not support object of that size {}",
            self.max_object_num_bytes
        );
        assert!(
            self.max_num_parts > 0,
            "Misconfiguration: max_num_parts == 0 makes no sense."
        );
        if len < self.multipart_threshold_num_bytes || self.max_num_parts == 1 {
            return len;
        }
        let max_num_parts = self.max_num_parts as u64;
        // complete part is the smallest integer such that
        // <max_num_parts> * <min_part_len> >= len.
        let min_part_len = 1u64 + (len - 1u64) / max_num_parts;
        (min_part_len).max(self.target_part_num_bytes as u64)
    }

    /// Limits the number of parts that can be concurrently uploaded.
    pub fn max_concurrent_upload(&self) -> usize {
        self.max_concurrent_upload
    }
}

// Default values from https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/Constants.java
// The best default value may however differ depending on vendors.
impl Default for MultiPartPolicy {
    fn default() -> Self {
        MultiPartPolicy {
            // S3 limits part size from 5M to 5GB, we want to end up with as few parts as possible
            // since each part is charged as a put request.
            target_part_num_bytes: 5_000_000_000, // 5GB
            multipart_threshold_num_bytes: 128 * 1_024 * 1_024, // 128 MiB
            max_num_parts: 10_000,
            max_object_num_bytes: 5_000_000_000_000u64, // S3 allows up to 5TB objects
            max_concurrent_upload: 100,
        }
    }
}
