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

use std::borrow::{Borrow, Cow};
use std::collections::BTreeMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tantivy::directory::OwnedBytes;

use crate::metrics::CacheMetrics;

#[derive(Clone, PartialOrd, Ord, PartialEq, Eq)]
struct CacheKey<'a, T: ToOwned + ?Sized> {
    tag: Cow<'a, T>,
    range_start: usize,
}

impl<T: ToOwned + ?Sized> CacheKey<'static, T> {
    fn from_owned(tag: T::Owned, range_start: usize) -> Self {
        CacheKey {
            tag: Cow::Owned(tag),
            range_start,
        }
    }
}

impl<'a, T: ToOwned + ?Sized> CacheKey<'a, T> {
    fn from_borrowed(tag: &'a T, range_start: usize) -> Self {
        CacheKey {
            tag: Cow::Borrowed(tag),
            range_start,
        }
    }
}

struct CacheValue {
    range_end: usize,
    bytes: OwnedBytes,
}

/// T is a tag, usually a file path.
struct NeedMutByteRangeCache<T: 'static + ToOwned + ?Sized> {
    cache: BTreeMap<CacheKey<'static, T>, CacheValue>,
    // this is hardly significant as items can get merged if they overlap
    num_items: u64,
    num_bytes: u64,
    cache_counters: &'static CacheMetrics,
}

impl<T: 'static + ToOwned + ?Sized + Ord> NeedMutByteRangeCache<T> {
    fn with_infinite_capacity(cache_counters: &'static CacheMetrics) -> Self {
        NeedMutByteRangeCache {
            cache: BTreeMap::new(),
            num_items: 0,
            num_bytes: 0,
            cache_counters,
        }
    }

    fn get_slice(&mut self, tag: &T, byte_range: Range<usize>) -> Option<OwnedBytes> {
        if byte_range.start == byte_range.end {
            return Some(OwnedBytes::empty());
        }

        let key = CacheKey::from_borrowed(tag, byte_range.start);
        let (k, v) = if let Some((k, v)) = self.get_block(&key, byte_range.end) {
            (k, v)
        } else if let Some((k, v)) = self.merge_ranges(&key, byte_range.end) {
            (k, v)
        } else {
            self.cache_counters.misses_num_items.inc();
            return None;
        };

        let start = byte_range.start - k.range_start;
        let end = byte_range.end - k.range_start;
        let result = v.bytes.slice(start..end);

        self.cache_counters.hits_num_items.inc();
        self.cache_counters
            .hits_num_bytes
            .inc_by((end - start) as u64);

        Some(result)
    }

    fn put_slice(&mut self, tag: T::Owned, byte_range: Range<usize>, bytes: OwnedBytes) {
        let len = byte_range.end - byte_range.start;
        assert_eq!(len, bytes.len());
        if len == 0 {
            return;
        }

        // try to find a block with which we overlap (and not just touch)
        let start_key = CacheKey::from_borrowed(tag.borrow(), byte_range.start);
        let first_matching_block = self
            .get_block(&start_key, byte_range.start + 1)
            .map(|(k, _v)| k);

        let end_key = CacheKey::from_borrowed(tag.borrow(), byte_range.end - 1);
        let last_matching_block = self.get_block(&end_key, byte_range.end).map(|(k, _v)| k);

        if first_matching_block.is_some() && first_matching_block == last_matching_block {
            // same start and end: all the range is already covered
            return;
        }

        let first_matching_block = first_matching_block.unwrap_or(&start_key);
        let last_matching_block = last_matching_block.unwrap_or(&end_key);

        let overlapping: Vec<Range<usize>> = self
            .cache
            .range(first_matching_block..=last_matching_block)
            .map(|(k, v)| k.range_start..v.range_end)
            .collect();

        let can_drop_first = overlapping
            .first()
            .map(|r| byte_range.start <= r.start)
            .unwrap_or(true);

        let can_drop_last = overlapping
            .last()
            .map(|r| byte_range.end >= r.end)
            .unwrap_or(true);

        let (final_range, final_bytes) = if can_drop_first && can_drop_last {
            // if we are here, either there was no overlapping block, or there was, but this buffer
            // covers entirely every block it overlapped with. There is no merging to do.
            (byte_range, bytes)
        } else {
            // if we are here, we have to do some merging

            // first find the final buffer start and end position.
            let start = if can_drop_first {
                byte_range.start
            } else {
                // if no first, can_drop_first is true
                overlapping.first().unwrap().start
            };
            let end = if can_drop_last {
                byte_range.end
            } else {
                // if no last, can_drop_last is true
                overlapping.last().unwrap().end
            };

            let mut buffer = Vec::with_capacity(end - start);

            // if this buffer overlap, but does not contain the 1st buffer, copy the
            // non-overlapping part at the start of the final buffer.
            if !can_drop_first {
                let first_range = overlapping.first().unwrap();
                let key = CacheKey::from_borrowed(tag.borrow(), first_range.start);
                let block = self.cache.get(&key).unwrap();

                let len = first_range.end.min(byte_range.start) - first_range.start;
                buffer.extend_from_slice(&block.bytes[..len]);
            }

            // copy the entire current buffer
            buffer.extend_from_slice(&bytes);

            // if this buffer overlap, but does not contain the last buffer, copy the
            // non-overlapping part ad the end of the final buffer.
            if !can_drop_last {
                let last_range = overlapping.last().unwrap();
                let key = CacheKey::from_borrowed(tag.borrow(), last_range.start);
                let block = self.cache.get(&key).unwrap();

                let start = last_range.start.max(byte_range.end) - last_range.start;
                buffer.extend_from_slice(&block.bytes[start..]);
            }

            // sanity check, we copied as much as expected
            debug_assert_eq!(end - start, buffer.len());

            (start..end, OwnedBytes::new(buffer))
        };

        // not sure why, but the borrow check gets unhappy if I create a borrowed
        // in the loop. It works with .get() instead of .remove() (?).
        let mut key = CacheKey::from_owned(tag, 0);
        for range in overlapping.into_iter() {
            // remove every block with which we overlapped, including the 1st and last, as they
            // were included as prefix/suffix to the final block.
            key.range_start = range.start;
            self.cache.remove(&key);
            self.update_counter_drop_item(range.end - range.start);
        }

        // and finally insert the newly added buffer
        key.range_start = final_range.start;
        let value = CacheValue {
            range_end: final_range.end,
            bytes: final_bytes,
        };
        self.cache.insert(key, value);
        self.update_counter_record_item(final_range.end - final_range.start);
    }

    // Return a block that contain everything between query.range_start and range_end
    fn get_block<'a>(
        &self,
        query: &CacheKey<'a, T>,
        range_end: usize,
    ) -> Option<(&CacheKey<'a, T>, &CacheValue)> {
        self.cache
            .range(..=query)
            .next_back()
            .filter(|(k, v)| k.tag == query.tag && range_end <= v.range_end)
    }

    /// Try to merge all blocks in the given range. Fails if some bytes were not already stored.
    fn merge_ranges<'a>(
        &mut self,
        start: &CacheKey<'a, T>,
        range_end: usize,
    ) -> Option<(&CacheKey<'a, T>, &CacheValue)> {
        let own_key = |key: &CacheKey<T>| {
            CacheKey::from_owned(T::borrow(&key.tag).to_owned(), key.range_start)
        };

        let first_block = self.get_block(start, start.range_start)?;

        // query cache for all blocks which overlap with our query
        let overlapping_blocks = self
            .cache
            .range(first_block.0..)
            .take_while(|(k, _)| k.tag == start.tag && k.range_start <= range_end);

        // verify there are no hole, and each range touches the next one. There can't be overlap
        // due to how we fill our data-structure.
        let mut last_block = first_block;
        for (k, v) in overlapping_blocks.clone().skip(1) {
            if k.range_start != last_block.1.range_end {
                return None;
            }

            last_block = (k, v);
        }
        if last_block.1.range_end < range_end {
            // we got a gap at the end
            return None;
        }

        // we have everything we need. Merge every sub-buffer into a single large buffer.
        let mut buffer = Vec::with_capacity(last_block.1.range_end - first_block.0.range_start);
        let mut part_count = 0i64;
        for (_, v) in overlapping_blocks {
            part_count += 1;
            buffer.extend_from_slice(&v.bytes);
        }
        assert_eq!(
            buffer.len(),
            (last_block.1.range_end - first_block.0.range_start)
        );

        let new_key = own_key(first_block.0);
        let new_value = CacheValue {
            range_end: last_block.1.range_end,
            bytes: OwnedBytes::new(buffer),
        };

        // cleanup is sub-optimal, we'd need a BTreeMap::drain_range or something like that
        let last_key = own_key(last_block.0);

        // remove previous buffers from the cache
        let blocks_to_remove: Vec<_> = self
            .cache
            .range(&new_key..=&last_key)
            .map(|(k, _)| own_key(k))
            .collect();
        for block in blocks_to_remove {
            self.cache.remove(&block);
        }

        // and insert the new merged buffer
        self.cache.insert(new_key, new_value);

        self.num_items -= (part_count - 1) as u64;
        self.cache_counters.in_cache_count.sub(part_count - 1);

        self.get_block(start, range_end)
    }

    fn update_counter_record_item(&mut self, num_bytes: usize) {
        self.num_items += 1;
        self.num_bytes += num_bytes as u64;
        self.cache_counters.in_cache_count.inc();
        self.cache_counters.in_cache_num_bytes.add(num_bytes as i64);
    }

    fn update_counter_drop_item(&mut self, num_bytes: usize) {
        self.num_items -= 1;
        self.num_bytes -= num_bytes as u64;
        self.cache_counters.in_cache_count.dec();
        self.cache_counters.in_cache_num_bytes.sub(num_bytes as i64);
        self.cache_counters.evict_num_items.inc();
        self.cache_counters.evict_num_bytes.inc_by(num_bytes as u64);
    }
}

impl<T: 'static + ToOwned + ?Sized> Drop for NeedMutByteRangeCache<T> {
    fn drop(&mut self) {
        self.cache_counters
            .in_cache_count
            .sub(self.num_items as i64);
        self.cache_counters
            .in_cache_num_bytes
            .sub(self.num_bytes as i64);
    }
}

/// Cache for ranges of bytes in files.
///
/// This cache is used in the contraption that makes it possible for Quickwit
/// to use tantivy while doing asynchronous io.
/// Quickwit manually populates this cache in an asynchronous "warmup" phase.
/// tantivy then gets its data from this cache without performing any IO.
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
    num_stored_bytes: AtomicU64,
    need_mut_byte_range_cache: Mutex<NeedMutByteRangeCache<Path>>,
}

impl ByteRangeCache {
    /// Creates a slice cache that never removes any entry.
    pub fn with_infinite_capacity(cache_counters: &'static CacheMetrics) -> Self {
        let need_mut_byte_range_cache =
            NeedMutByteRangeCache::with_infinite_capacity(cache_counters);
        let inner = Inner {
            num_stored_bytes: AtomicU64::default(),
            need_mut_byte_range_cache: Mutex::new(need_mut_byte_range_cache),
        };
        ByteRangeCache {
            inner_arc: Arc::new(inner),
        }
    }

    /// Overall amount of bytes stored in the cache.
    pub fn get_num_bytes(&self) -> u64 {
        self.inner_arc.num_stored_bytes.load(Ordering::Relaxed)
    }

    /// If available, returns the cached view of the slice.
    pub fn get_slice(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes> {
        self.inner_arc
            .need_mut_byte_range_cache
            .lock()
            .unwrap()
            .get_slice(path, byte_range)
    }

    /// Put the given amount of data in the cache.
    pub fn put_slice(&self, path: PathBuf, byte_range: Range<usize>, bytes: OwnedBytes) {
        let mut need_mut_byte_range_cache_locked =
            self.inner_arc.need_mut_byte_range_cache.lock().unwrap();
        need_mut_byte_range_cache_locked.put_slice(path, byte_range, bytes);
        let num_bytes = need_mut_byte_range_cache_locked.num_bytes;
        drop(need_mut_byte_range_cache_locked);
        self.inner_arc
            .num_stored_bytes
            .store(num_bytes, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Range;
    use std::path::Path;

    use once_cell::sync::Lazy;
    use proptest::prelude::*;

    use super::ByteRangeCache;
    use crate::OwnedBytes;
    use crate::metrics::{CACHE_METRICS_FOR_TESTS, CacheMetrics};

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

    proptest::proptest! {
        #[test]
        fn test_proptest_byte_range_cache(ops in ops_strategy()) {
            let mut state: HashMap<&'static str, Vec<bool>> = HashMap::new();
            state.insert("path1", vec![false; 12]);
            state.insert("path2", vec![false; 12]);

            let cache = ByteRangeCache::with_infinite_capacity(&CACHE_METRICS_FOR_TESTS);

            for op in ops {
                match op {
                    Operation::Insert {
                        range,
                        tag,
                    } => {
                        state.get_mut(tag).unwrap()
                            [range.clone()].fill(true);
                        let bytes = range.clone().map(|i| (i%256) as u8).collect::<Vec<_>>();
                        cache.put_slice(tag.into(), range, OwnedBytes::new(bytes));


                        let expected_item_count: usize = state.values()
                            .map(|tagged_state| {
                                count_items(tagged_state)
                            })
                            .sum();
                        // in some case we have ranges touching each other, count_items count them
                        // as only one, but cache count them as 2.
                        assert!(cache.inner_arc.need_mut_byte_range_cache.lock().unwrap().num_items >= expected_item_count as u64);

                        let expected_byte_count = state.values()
                            .flatten()
                            .filter(|stored| **stored)
                            .count();
                        assert_eq!(cache.inner_arc.need_mut_byte_range_cache.lock().unwrap().num_bytes, expected_byte_count as u64);
                    }
                    Operation::Get {
                        range,
                        tag,
                    } => {
                        let slice = cache.get_slice(Path::new(tag), range.clone());
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
    fn test_byte_range_cache_doesnt_merge_unnecessarily() {
        // we need to get a 'static ref to metrics, and want a dedicated metrics because we assert
        // on it
        static METRICS: Lazy<CacheMetrics> =
            Lazy::new(|| CacheMetrics::for_component("byterange_cache_test"));

        let cache = ByteRangeCache::with_infinite_capacity(&METRICS);

        let key: std::path::PathBuf = "key".into();

        cache.put_slice(
            key.clone(),
            0..5,
            OwnedBytes::new((0..5).collect::<Vec<_>>()),
        );
        cache.put_slice(
            key.clone(),
            5..10,
            OwnedBytes::new((5..10).collect::<Vec<_>>()),
        );
        cache.put_slice(
            key.clone(),
            10..15,
            OwnedBytes::new((10..15).collect::<Vec<_>>()),
        );
        cache.put_slice(
            key.clone(),
            15..20,
            OwnedBytes::new((15..20).collect::<Vec<_>>()),
        );

        {
            let mutable_cache = cache.inner_arc.need_mut_byte_range_cache.lock().unwrap();
            assert_eq!(mutable_cache.cache.len(), 4);
            assert_eq!(mutable_cache.num_items, 4);
            assert_eq!(mutable_cache.cache_counters.in_cache_count.get(), 4);
            assert_eq!(mutable_cache.num_bytes, 20);
            assert_eq!(mutable_cache.cache_counters.in_cache_num_bytes.get(), 20);
        }

        cache.get_slice(&key, 3..12).unwrap();

        {
            // now they should've been merged, except the last one
            let mutable_cache = cache.inner_arc.need_mut_byte_range_cache.lock().unwrap();
            assert_eq!(mutable_cache.cache.len(), 2);
            assert_eq!(mutable_cache.num_items, 2);
            assert_eq!(mutable_cache.cache_counters.in_cache_count.get(), 2);
            assert_eq!(mutable_cache.num_bytes, 20);
            assert_eq!(mutable_cache.cache_counters.in_cache_num_bytes.get(), 20);
        }
    }
}
