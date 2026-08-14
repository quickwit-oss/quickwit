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

use std::collections::BTreeMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rustc_hash::FxHashMap;
use tantivy::directory::OwnedBytes;

struct CacheValue {
    range_end: usize,
    bytes: OwnedBytes,
}

struct FileByteRangeCacheState {
    blocks: BTreeMap<usize, CacheValue>,
    num_bytes: u64,
}

impl FileByteRangeCacheState {
    fn with_infinite_capacity() -> Self {
        FileByteRangeCacheState {
            blocks: BTreeMap::new(),
            num_bytes: 0,
        }
    }

    fn get_slice(&mut self, byte_range: Range<usize>) -> Option<OwnedBytes> {
        if byte_range.start == byte_range.end {
            return Some(OwnedBytes::empty());
        }

        if let Some((block_start, value)) =
            Self::get_block(&self.blocks, byte_range.start, byte_range.end)
        {
            let start = byte_range.start - block_start;
            let end = byte_range.end - block_start;
            return Some(value.bytes.slice(start..end));
        }
        Self::merge_ranges(&mut self.blocks, byte_range.start, byte_range.end)?;
        let (block_start, value) = Self::get_block(&self.blocks, byte_range.start, byte_range.end)?;
        let start = byte_range.start - block_start;
        let end = byte_range.end - block_start;
        Some(value.bytes.slice(start..end))
    }

    fn put_slice(&mut self, byte_range: Range<usize>, bytes: OwnedBytes) {
        let len = byte_range.end - byte_range.start;
        assert_eq!(len, bytes.len());
        if len == 0 {
            return;
        }

        // Try to find a block with which we overlap (and not just touch).
        let first_matching_block =
            Self::get_block(&self.blocks, byte_range.start, byte_range.start + 1)
                .map(|(block_start, _)| *block_start);
        let last_matching_block = Self::get_block(&self.blocks, byte_range.end - 1, byte_range.end)
            .map(|(block_start, _)| *block_start);

        if first_matching_block.is_some() && first_matching_block == last_matching_block {
            return;
        }

        let first_matching_block = first_matching_block.unwrap_or(byte_range.start);
        let last_matching_block = last_matching_block.unwrap_or(byte_range.end - 1);
        let overlapping: Vec<Range<usize>> = self
            .blocks
            .range(first_matching_block..=last_matching_block)
            .map(|(block_start, value)| *block_start..value.range_end)
            .collect();

        let can_drop_first = match overlapping.first() {
            Some(range) => byte_range.start <= range.start,
            None => true,
        };
        let can_drop_last = match overlapping.last() {
            Some(range) => byte_range.end >= range.end,
            None => true,
        };

        let (final_range, final_bytes) = if can_drop_first && can_drop_last {
            (byte_range, bytes)
        } else {
            let start = if can_drop_first {
                byte_range.start
            } else {
                overlapping[0].start
            };
            let end = if can_drop_last {
                byte_range.end
            } else {
                overlapping[overlapping.len() - 1].end
            };
            let mut buffer = Vec::with_capacity(end - start);

            if !can_drop_first {
                let first_range = &overlapping[0];
                let block = &self.blocks[&first_range.start];
                let prefix_len = first_range.end.min(byte_range.start) - first_range.start;
                buffer.extend_from_slice(&block.bytes[..prefix_len]);
            }
            buffer.extend_from_slice(&bytes);
            if !can_drop_last {
                let last_range = &overlapping[overlapping.len() - 1];
                let block = &self.blocks[&last_range.start];
                let suffix_start = last_range.start.max(byte_range.end) - last_range.start;
                buffer.extend_from_slice(&block.bytes[suffix_start..]);
            }
            debug_assert_eq!(end - start, buffer.len());
            (start..end, OwnedBytes::new(buffer))
        };

        for range in overlapping {
            self.blocks.remove(&range.start);
            self.num_bytes -= (range.end - range.start) as u64;
        }
        let value = CacheValue {
            range_end: final_range.end,
            bytes: final_bytes,
        };
        self.blocks.insert(final_range.start, value);
        self.num_bytes += (final_range.end - final_range.start) as u64;
    }

    /// Returns a block containing everything between `range_start` and `range_end`.
    fn get_block(
        blocks: &BTreeMap<usize, CacheValue>,
        range_start: usize,
        range_end: usize,
    ) -> Option<(&usize, &CacheValue)> {
        blocks
            .range(..=range_start)
            .next_back()
            .filter(|(_, value)| range_end <= value.range_end)
    }

    /// Tries to merge all blocks in the given range. Fails if some bytes are not stored.
    fn merge_ranges(
        blocks: &mut BTreeMap<usize, CacheValue>,
        range_start: usize,
        range_end: usize,
    ) -> Option<()> {
        let (first_start, _) = Self::get_block(blocks, range_start, range_start)?;
        let first_start = *first_start;
        let overlapping: Vec<Range<usize>> = blocks
            .range(first_start..)
            .take_while(|(block_start, _)| **block_start <= range_end)
            .map(|(block_start, value)| *block_start..value.range_end)
            .collect();

        let mut previous_end = overlapping.first()?.end;
        for range in overlapping.iter().skip(1) {
            if range.start != previous_end {
                return None;
            }
            previous_end = range.end;
        }
        if previous_end < range_end {
            return None;
        }

        let mut buffer = Vec::with_capacity(previous_end - first_start);
        for range in &overlapping {
            let value = blocks.get(&range.start)?;
            buffer.extend_from_slice(&value.bytes);
        }
        assert_eq!(buffer.len(), previous_end - first_start);

        for range in &overlapping {
            blocks.remove(&range.start);
        }
        let value = CacheValue {
            range_end: previous_end,
            bytes: OwnedBytes::new(buffer),
        };
        blocks.insert(first_start, value);
        Some(())
    }
}

/// Cache for ranges of bytes in one immutable file.
///
/// Clones share the same cached ranges. Obtain this cache from
/// [`ByteRangeCache::get_file_cache`] so that all handles for one path share it.
#[derive(Clone)]
pub struct FileByteRangeCache {
    state: Arc<Mutex<FileByteRangeCacheState>>,
    total_num_stored_bytes: Arc<AtomicU64>,
}

impl FileByteRangeCache {
    fn with_total_num_stored_bytes(total_num_stored_bytes: Arc<AtomicU64>) -> Self {
        FileByteRangeCache {
            state: Arc::new(Mutex::new(FileByteRangeCacheState::with_infinite_capacity())),
            total_num_stored_bytes,
        }
    }

    /// Returns the cached view of the slice if it is available.
    pub fn get_slice(&self, byte_range: Range<usize>) -> Option<OwnedBytes> {
        self.state
            .lock()
            .expect("file byte range cache mutex is poisoned")
            .get_slice(byte_range)
    }

    /// Stores the given slice in the cache.
    pub fn put_slice(&self, byte_range: Range<usize>, bytes: OwnedBytes) {
        let mut state = self
            .state
            .lock()
            .expect("file byte range cache mutex is poisoned");
        let previous_num_bytes = state.num_bytes;
        state.put_slice(byte_range, bytes);
        let num_added_bytes = state.num_bytes - previous_num_bytes;
        self.total_num_stored_bytes
            .fetch_add(num_added_bytes, Ordering::Relaxed);
    }
}

/// Cache for ranges of bytes in files.
///
/// This cache is used in the contraption that makes it possible for Quickwit
/// to use tantivy while doing asynchronous io.
/// Quickwit manually populates this cache in an asynchronous "warmup" phase.
/// tantivy then gets its data from this cache without performing any IO.
///
/// Each path has a separate [`FileByteRangeCache`]. A caller binds a file cache
/// once when opening a file, avoiding a path lookup and cross-file lock
/// contention for each range read.
///
/// Contrary to `MemorySizedCache`, it's able to answer subset of known ranges,
/// does not have any eviction, and assumes an infinite capacity.
///
/// This cache assume immutable data: if you put a new slice and it overlap with
/// cached data, the changes may or may not get recorded.
///
/// At the moment this is hardly a cache as it features no eviction policy.
#[derive(Clone)]
pub struct ByteRangeCache {
    inner_arc: Arc<Inner>,
}

struct Inner {
    total_num_stored_bytes: Arc<AtomicU64>,
    // Paths identify virtual Tantivy files within this split, not storage object paths.
    file_caches: Mutex<FxHashMap<PathBuf, FileByteRangeCache>>,
}

impl ByteRangeCache {
    /// Creates a slice cache that never removes any entry.
    pub fn with_infinite_capacity() -> Self {
        let inner = Inner {
            total_num_stored_bytes: Arc::new(AtomicU64::default()),
            file_caches: Mutex::new(FxHashMap::default()),
        };
        ByteRangeCache {
            inner_arc: Arc::new(inner),
        }
    }

    /// Returns the shared byte-range cache for `path`.
    pub fn get_file_cache(&self, path: &Path) -> FileByteRangeCache {
        let mut file_caches = self
            .inner_arc
            .file_caches
            .lock()
            .expect("byte range cache mutex is poisoned");
        if let Some(file_cache) = file_caches.get(path) {
            return file_cache.clone();
        }
        let file_cache = FileByteRangeCache::with_total_num_stored_bytes(
            self.inner_arc.total_num_stored_bytes.clone(),
        );
        file_caches.insert(path.to_path_buf(), file_cache.clone());
        file_cache
    }

    /// Overall amount of bytes stored in the cache.
    pub fn get_num_bytes(&self) -> u64 {
        self.inner_arc
            .total_num_stored_bytes
            .load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Range;
    use std::path::Path;

    use proptest::prelude::*;

    use super::ByteRangeCache;
    use crate::OwnedBytes;

    #[derive(Debug)]
    enum Operation {
        Insert {
            range: Range<usize>,
            tag: &'static str,
        },
        Get {
            range: Range<usize>,
            tag: &'static str,
        },
    }

    fn tag_strategy() -> impl Strategy<Value = &'static str> {
        prop_oneof![Just("path1"), Just("path2"),]
    }

    #[allow(deprecated)]
    fn range_strategy() -> impl Strategy<Value = Range<usize>> {
        (0usize..11usize).prop_perturb(|start, mut rng| start..rng.gen_range(start..12usize))
    }

    fn op_strategy() -> impl Strategy<Value = Operation> {
        prop_oneof![
            (tag_strategy(), range_strategy())
                .prop_map(|(tag, range)| Operation::Insert { range, tag }),
            (tag_strategy(), range_strategy())
                .prop_map(|(tag, range)| Operation::Get { range, tag }),
        ]
    }

    fn ops_strategy() -> impl Strategy<Value = Vec<Operation>> {
        prop::collection::vec(op_strategy(), 1..100)
    }

    fn get_num_cached_blocks(cache: &ByteRangeCache) -> usize {
        let file_caches = cache.inner_arc.file_caches.lock().unwrap();
        let mut num_cached_blocks = 0;
        for file_cache in file_caches.values() {
            num_cached_blocks += file_cache.state.lock().unwrap().blocks.len();
        }
        num_cached_blocks
    }

    proptest::proptest! {
        #[test]
        fn test_proptest_byte_range_cache(ops in ops_strategy()) {
            let mut state: HashMap<&'static str, Vec<bool>> = HashMap::new();
            state.insert("path1", vec![false; 12]);
            state.insert("path2", vec![false; 12]);

            let cache = ByteRangeCache::with_infinite_capacity();

            for op in ops {
                match op {
                    Operation::Insert {
                        range,
                        tag,
                    } => {
                        state.get_mut(tag).unwrap()
                            [range.clone()].fill(true);
                        let bytes = range.clone().map(|i| (i%256) as u8).collect::<Vec<_>>();
                        cache
                            .get_file_cache(Path::new(tag))
                            .put_slice(range, OwnedBytes::new(bytes));

                        let expected_item_count: usize = state.values()
                            .map(|tagged_state| {
                                count_items(tagged_state)
                            })
                            .sum();
                        // in some case we have ranges touching each other, count_items count them
                        // as only one, but cache count them as 2.
                        assert!(get_num_cached_blocks(&cache) >= expected_item_count);

                        let expected_byte_count = state.values()
                            .flatten()
                            .filter(|stored| **stored)
                            .count();
                        assert_eq!(cache.get_num_bytes(), expected_byte_count as u64);
                    }
                    Operation::Get {
                        range,
                        tag,
                    } => {
                        let slice = cache.get_file_cache(Path::new(tag)).get_slice(range.clone());
                        if state[tag][range.clone()].iter().all(|t| *t) {
                            let slice = slice.unwrap();
                            let bytes = range.clone().map(|i| (i%256) as u8).collect::<Vec<_>>();
                            assert_eq!(slice[..], bytes[..]);

                        } else {
                            assert!(slice.is_none());
                        }
                    },
                }
            }
        }
    }

    fn count_items(state: &[bool]) -> usize {
        state
            .iter()
            .fold((false, 0), |(last_val, count), next| {
                if *next && !last_val {
                    (*next, count + 1)
                } else {
                    (*next, count)
                }
            })
            .1
    }

    #[test]
    fn test_byte_range_cache_is_shared_for_same_path() {
        let cache = ByteRangeCache::with_infinite_capacity();
        let first_file_cache = cache.get_file_cache(Path::new("path1"));
        let second_file_cache = cache.get_file_cache(Path::new("path1"));
        let other_file_cache = cache.get_file_cache(Path::new("path2"));

        first_file_cache.put_slice(0..3, OwnedBytes::new(vec![1, 2, 3]));

        assert_eq!(second_file_cache.get_slice(0..3).unwrap()[..], [1, 2, 3]);
        assert!(other_file_cache.get_slice(0..3).is_none());
        assert_eq!(cache.get_num_bytes(), 3);
    }

    #[test]
    fn test_byte_range_cache_doesnt_merge_unnecessarily() {
        let cache = ByteRangeCache::with_infinite_capacity();
        let file_cache = cache.get_file_cache(Path::new("key"));

        file_cache.put_slice(0..5, OwnedBytes::new((0..5).collect::<Vec<_>>()));
        file_cache.put_slice(5..10, OwnedBytes::new((5..10).collect::<Vec<_>>()));
        file_cache.put_slice(10..15, OwnedBytes::new((10..15).collect::<Vec<_>>()));
        file_cache.put_slice(15..20, OwnedBytes::new((15..20).collect::<Vec<_>>()));

        {
            let state = file_cache.state.lock().unwrap();
            assert_eq!(state.blocks.len(), 4);
            assert_eq!(state.num_bytes, 20);
        }

        file_cache.get_slice(3..12).unwrap();

        {
            // now they should've been merged, except the last one
            let state = file_cache.state.lock().unwrap();
            assert_eq!(state.blocks.len(), 2);
            assert_eq!(state.num_bytes, 20);
        }
    }
}
