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

use tantivy::directory::OwnedBytes;
use tokio::time::Instant;

/// A cached value together with the memory charged for the key it is stored under.
///
/// The key itself lives in the backing cache's own map, out of reach of the value, so every
/// backend memorizes the key footprint at insertion time rather than recomputing it from the key
/// on the way out. That serves two purposes: it keeps `MemUsage::mem_usage` off the eviction path,
/// and it guarantees an entry is debited exactly what it was credited — any divergence would make
/// a cache's `num_bytes` counter drift, and underflow on the way down.
#[derive(Clone)]
pub(super) struct KeyedEntry<V = OwnedBytes> {
    value: V,
    key_mem_usage: usize,
}

impl<V> KeyedEntry<V> {
    pub fn new(value: V, key_mem_usage: usize) -> Self {
        KeyedEntry {
            value,
            key_mem_usage,
        }
    }

    pub fn value(&self) -> &V {
        &self.value
    }
}

impl<V: ValueLen> KeyedEntry<V> {
    /// Number of bytes of payload, as served to a caller on a cache hit.
    pub fn payload_num_bytes(&self) -> usize {
        self.value.len()
    }

    /// Number of bytes this entry occupies, key included. This is what a cache capacity is
    /// enforced on.
    pub fn entry_num_bytes(&self) -> usize {
        self.key_mem_usage + self.value.len()
    }
}

/// It is a bit overkill to put this in its own module, but I
/// wanted to ensure that no one would access payload without updating `last_access_time`.
pub(super) struct StoredItem<V = OwnedBytes> {
    last_access_time: Instant,
    entry: KeyedEntry<V>,
}

impl<V> StoredItem<V> {
    pub fn new(payload: V, key_mem_usage: usize, now: Instant) -> Self {
        StoredItem {
            last_access_time: now,
            entry: KeyedEntry::new(payload, key_mem_usage),
        }
    }
}

impl<V: ValueLen + Clone> StoredItem<V> {
    pub fn payload(&mut self) -> V {
        self.last_access_time = Instant::now();
        self.entry.value().clone()
    }

    /// Number of bytes of payload, as served to a caller on a cache hit.
    pub fn payload_num_bytes(&self) -> usize {
        self.entry.payload_num_bytes()
    }

    /// Number of bytes this entry occupies, key included. This is what the cache capacity is
    /// enforced on.
    pub fn entry_num_bytes(&self) -> usize {
        self.entry.entry_num_bytes()
    }

    pub fn last_access_time(&self) -> Instant {
        self.last_access_time
    }
}

pub(crate) trait ValueLen {
    fn len(&self) -> usize;
}

impl ValueLen for OwnedBytes {
    fn len(&self) -> usize {
        OwnedBytes::len(self)
    }
}
