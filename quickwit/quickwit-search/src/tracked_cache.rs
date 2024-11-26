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

use std::sync::{Arc, Mutex};

use quickwit_directories::DirectoryCache;
use quickwit_storage::ByteRangeCache;

use crate::search_permit_provider::SearchPermit;

/// A [`ByteRangeCache`] tied to a [`SearchPermit`].
#[derive(Clone)]
pub struct TrackedByteRangeCache {
    inner: Arc<Inner>,
}

struct Inner {
    cache: ByteRangeCache,
    search_permit: Mutex<SearchPermit>,
}

impl TrackedByteRangeCache {
    pub fn new(cache: ByteRangeCache, search_permit: SearchPermit) -> TrackedByteRangeCache {
        TrackedByteRangeCache {
            inner: Arc::new(Inner {
                cache,
                search_permit: Mutex::new(search_permit),
            }),
        }
    }

    pub fn warmup_completed(&self) {
        self.inner
            .search_permit
            .lock()
            .unwrap()
            .warmup_completed(self.get_num_bytes());
    }

    pub fn get_num_bytes(&self) -> u64 {
        self.inner.cache.get_num_bytes()
    }
}

impl DirectoryCache for TrackedByteRangeCache {
    fn get_slice(
        &self,
        path: &std::path::Path,
        byte_range: std::ops::Range<usize>,
    ) -> Option<quickwit_storage::OwnedBytes> {
        self.inner.cache.get_slice(path, byte_range)
    }
    fn put_slice(
        &self,
        path: std::path::PathBuf,
        byte_range: std::ops::Range<usize>,
        bytes: quickwit_storage::OwnedBytes,
    ) {
        self.inner.cache.put_slice(path, byte_range, bytes)
    }
}
