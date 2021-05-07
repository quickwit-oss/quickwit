/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

/// The multipart policy defines when and how multipart upload / download should happen.
///
/// The right settings might be vendor specific, but if not available using
/// [`S3MultiPartPolicy`] should be safe.
pub trait MultiPartPolicy: Sync + Send + 'static {
    /// This function returns the size of the part that should
    /// be used. We should have `part_num_bytes(len)` < `len`.
    ///
    /// If this function returns `len`, then multipart upload
    /// will not be used.
    fn part_num_bytes(&self, len: u64) -> u64;
    /// Limits the number of parts that can be concurrently uploaded.
    fn max_concurrent_upload(&self) -> usize;
}

pub struct S3MultiPartPolicy {
    pub target_part_num_bytes: usize,
    pub max_num_parts: usize,
    pub multipart_threshold_num_bytes: u64,
    pub max_object_num_bytes: u64,
    pub max_concurrent_upload: usize,
}

impl MultiPartPolicy for S3MultiPartPolicy {
    fn part_num_bytes(&self, len: u64) -> u64 {
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

    fn max_concurrent_upload(&self) -> usize {
        self.max_concurrent_upload
    }
}

// Default values from https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/Constants.java
// The best default value may however differ depending on vendors.
impl Default for S3MultiPartPolicy {
    fn default() -> Self {
        S3MultiPartPolicy {
            target_part_num_bytes: 64 * 1_024 * 1_024, // 64 MiB
            multipart_threshold_num_bytes: 128 * 1_024 * 1_024, // 128 MiB
            max_num_parts: 10_000,
            max_object_num_bytes: 5_000_000_000_000u64, // S3 allows up to 5TB objects
            max_concurrent_upload: 100,
        }
    }
}
