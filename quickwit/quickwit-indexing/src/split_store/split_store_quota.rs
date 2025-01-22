// Copyright (C) 2024 Quickwit, Inc.
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

use bytesize::ByteSize;
use quickwit_config::IndexerConfig;

/// A struct for keeping in check multiple SplitStore.
#[derive(Debug, Clone)]
pub struct SplitStoreQuota {
    /// Current number of splits in the cache.
    num_splits_in_cache: usize,
    /// Current size in bytes of splits in the cache.
    size_in_bytes_in_cache: ByteSize,
    /// Maximum number of files allowed in the cache.
    max_num_splits: usize,
    /// Maximum size in bytes allowed in the cache. 0 if max_num_splits=0.
    max_num_bytes: ByteSize,
}

impl Default for SplitStoreQuota {
    fn default() -> Self {
        Self {
            num_splits_in_cache: 0,
            size_in_bytes_in_cache: ByteSize::default(),
            max_num_bytes: IndexerConfig::default_split_store_max_num_bytes(),
            max_num_splits: IndexerConfig::default_split_store_max_num_splits(),
        }
    }
}

impl SplitStoreQuota {
    pub fn try_new(max_num_splits: usize, max_num_bytes: ByteSize) -> anyhow::Result<Self> {
        if max_num_splits == 0 && max_num_bytes.as_u64() > 0 {
            anyhow::bail!("max_num_bytes cannot be > 0 if max_num_splits is 0");
        }
        Ok(Self {
            max_num_splits,
            max_num_bytes,
            ..Default::default()
        })
    }

    /// Space quota that prevents any caching.
    pub fn no_caching() -> Self {
        Self::try_new(0, ByteSize::default()).unwrap()
    }

    pub fn can_fit_split(&self, split_size_in_bytes: ByteSize) -> bool {
        if self.num_splits_in_cache >= self.max_num_splits {
            return false;
        }
        if self.size_in_bytes_in_cache.as_u64() + split_size_in_bytes.as_u64()
            > self.max_num_bytes.as_u64()
        {
            return false;
        }
        true
    }

    pub fn add_split(&mut self, split_size_in_bytes: ByteSize) {
        self.num_splits_in_cache += 1;
        self.size_in_bytes_in_cache =
            ByteSize(self.size_in_bytes_in_cache.as_u64() + split_size_in_bytes.as_u64());
    }

    pub fn remove_split(&mut self, split_size_in_bytes: ByteSize) {
        self.size_in_bytes_in_cache =
            ByteSize(self.size_in_bytes_in_cache.as_u64() - split_size_in_bytes.as_u64());
        self.num_splits_in_cache -= 1;
    }

    pub fn max_num_bytes(&self) -> ByteSize {
        self.max_num_bytes
    }

    pub fn used_num_bytes(&self) -> ByteSize {
        self.size_in_bytes_in_cache
    }
}

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;

    use crate::split_store::SplitStoreQuota;

    #[test]
    fn test_invalid_quota() {
        SplitStoreQuota::try_new(0, ByteSize(100)).unwrap_err();
    }

    #[test]
    fn test_split_store_quota_max_bytes_accepted() {
        let split_store_quota = SplitStoreQuota::try_new(3, ByteSize(100)).unwrap();
        assert!(split_store_quota.can_fit_split(ByteSize(100)));
    }

    #[test]
    fn test_split_store_quota_exceeding_bytes() {
        let split_store_quota = SplitStoreQuota::try_new(3, ByteSize(100)).unwrap();
        assert!(!split_store_quota.can_fit_split(ByteSize(101)));
    }

    #[test]
    fn test_split_store_quota_max_num_files_accepted() {
        let mut split_store_quota = SplitStoreQuota::try_new(2, ByteSize(100)).unwrap();
        split_store_quota.add_split(ByteSize(1));
        assert!(split_store_quota.can_fit_split(ByteSize(1)));
    }

    #[test]
    fn test_split_store_quota_exceeding_max_num_files() {
        let mut split_store_quota = SplitStoreQuota::try_new(2, ByteSize(100)).unwrap();
        split_store_quota.add_split(ByteSize(1));
        split_store_quota.add_split(ByteSize(1));
        assert!(!split_store_quota.can_fit_split(ByteSize(1)));
    }
}
