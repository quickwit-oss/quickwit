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
    /// Threshold above which multipart is triggered.
    pub multipart_threshold_num_bytes: u64,
    /// Maximum size allowed for an object.
    pub max_object_num_bytes: u64,
    /// Maximum number of parts to be upload concurrently.
    pub max_concurrent_uploads: usize,
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
            "This object storage does not support object of that size {}",
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
    pub fn max_concurrent_uploads(&self) -> usize {
        self.max_concurrent_uploads
    }
}

// The best default value may differ depending on vendors.
impl Default for MultiPartPolicy {
    fn default() -> Self {
        MultiPartPolicy {
            // S3 limits part size from 5M to 5GB, we want to end up with as few parts as possible
            // since each part is charged as a put request.
            target_part_num_bytes: 5_000_000_000, // 5GB
            multipart_threshold_num_bytes: 128 * 1_024 * 1_024, // 128 MiB
            max_num_parts: 10_000,
            max_object_num_bytes: 5_000_000_000_000u64, // S3 allows up to 5TB objects
            max_concurrent_uploads: 100,
        }
    }
}
