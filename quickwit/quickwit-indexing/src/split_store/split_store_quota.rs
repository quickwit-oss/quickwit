// Copyright (C) 2022 Quickwit, Inc.
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

use byte_unit::Byte;
use quickwit_config::IndexerConfig;
use tracing::warn;

/// A struct for keeping in check multiple SplitStore.
#[derive(Debug)]
pub struct SplitStoreQuota {
    /// Current number of splits in the cache.
    num_splits_in_cache: usize,
    /// Current size in bytes of splits in the cache.
    size_in_bytes_in_cache: Byte,
    /// Maximum number of files allowed in the cache.
    max_num_splits: usize,
    /// Maximum size in bytes allowed in the cache.
    max_num_bytes: Byte,
}

impl Default for SplitStoreQuota {
    fn default() -> Self {
        Self {
            num_splits_in_cache: 0,
            size_in_bytes_in_cache: Byte::default(),
            max_num_bytes: IndexerConfig::default_split_store_max_num_bytes(),
            max_num_splits: IndexerConfig::default_split_store_max_num_splits(),
        }
    }
}

impl SplitStoreQuota {
    pub fn new(max_num_splits: usize, max_num_bytes: Byte) -> Self {
        Self {
            max_num_splits,
            max_num_bytes,
            ..Default::default()
        }
    }

    /// Space quota that prevents any caching.
    pub fn no_caching() -> Self {
        Self::new(0, Byte::default())
    }

    pub fn can_fit_split(&self, split_size_in_bytes: Byte) -> bool {
        if self.num_splits_in_cache >= self.max_num_splits {
            warn!("Failed to cache file: maximum number of files exceeded.");
            return false;
        }
        if self.size_in_bytes_in_cache.get_bytes() + split_size_in_bytes.get_bytes()
            > self.max_num_bytes.get_bytes()
        {
            warn!("Failed to cache file: maximum size in bytes of cache exceeded.");
            return false;
        }
        true
    }

    pub fn add_split(&mut self, split_size_in_bytes: Byte) {
        self.num_splits_in_cache += 1;
        self.size_in_bytes_in_cache = Byte::from_bytes(
            self.size_in_bytes_in_cache.get_bytes() + split_size_in_bytes.get_bytes(),
        );
    }

    pub fn remove_split(&mut self, split_size_in_bytes: Byte) {
        self.size_in_bytes_in_cache = Byte::from_bytes(
            self.size_in_bytes_in_cache.get_bytes() - split_size_in_bytes.get_bytes(),
        );
        self.num_splits_in_cache -= 1;
    }

    pub fn max_num_bytes(&self) -> Byte {
        self.max_num_bytes
    }
}

#[cfg(test)]
mod tests {
    use byte_unit::Byte;

    use crate::split_store::SplitStoreQuota;

    #[test]
    fn test_split_store_quota_max_bytes_accepted() {
        let split_store_quota = SplitStoreQuota::new(3, Byte::from_bytes(100));
        assert!(split_store_quota.can_fit_split(Byte::from_bytes(100)));
    }

    #[test]
    fn test_split_store_quota_exceeding_bytes() {
        let split_store_quota = SplitStoreQuota::new(3, Byte::from_bytes(100));
        assert!(!split_store_quota.can_fit_split(Byte::from_bytes(101)));
    }

    #[test]
    fn test_split_store_quota_max_num_files_accepted() {
        let mut split_store_quota = SplitStoreQuota::new(2, Byte::from_bytes(100));
        split_store_quota.add_split(Byte::from_bytes(1));
        assert!(split_store_quota.can_fit_split(Byte::from_bytes(1)));
    }

    #[test]
    fn test_split_store_quota_exceeding_max_num_files() {
        let mut split_store_quota = SplitStoreQuota::new(2, Byte::from_bytes(100));
        split_store_quota.add_split(Byte::from_bytes(1));
        split_store_quota.add_split(Byte::from_bytes(1));
        assert!(!split_store_quota.can_fit_split(Byte::from_bytes(1)));
    }
}
