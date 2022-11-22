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

use std::borrow::{Borrow, Cow};
use std::collections::BTreeMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

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
        } else {
            self.cache_counters.misses_num_items.inc();
            return None;
        };

        let start = byte_range.start - k.range_start;
        let end = byte_range.end - k.range_start;

        self.cache_counters.hits_num_items.inc();
        self.cache_counters
            .hits_num_bytes
            .inc_by((end - start) as u64);

        Some(v.bytes.slice(start..end))
    }

    fn put_slice(&mut self, tag: T::Owned, byte_range: Range<usize>, bytes: OwnedBytes) {
        let len = byte_range.end - byte_range.start;
        assert_eq!(len, bytes.len());
        if len == 0 {
            return;
        }

        let start_key = CacheKey::from_borrowed(tag.borrow(), byte_range.start);
        let end_key = CacheKey::from_borrowed(tag.borrow(), byte_range.end);

        let first_matching_block = self
            .get_block(&start_key, byte_range.start)
            .map(|(k, _v)| k);

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
            (byte_range, bytes)
        } else {
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

            if !can_drop_first {
                let first_range = overlapping.first().unwrap();
                let key = CacheKey::from_borrowed(tag.borrow(), first_range.start);
                let block = self.cache.get(&key).unwrap();

                let len = first_range.end.min(byte_range.start) - first_range.start;
                buffer.extend_from_slice(&block.bytes[..len]);
            }

            buffer.extend_from_slice(&bytes);

            if !can_drop_last {
                let last_range = overlapping.last().unwrap();
                let key = CacheKey::from_borrowed(tag.borrow(), last_range.start);
                let block = self.cache.get(&key).unwrap();

                let start = last_range.start.max(byte_range.end) - last_range.start;
                buffer.extend_from_slice(&block.bytes[start..]);
            }

            debug_assert_eq!(end - start, buffer.len());

            (start..end, OwnedBytes::new(buffer))
        };

        // not sure why, but the borrow check gets unhappy if I create a borrowed
        // in the loop. It works with .get() instead of .remove() (?).
        let mut key = CacheKey::from_owned(tag, 0);
        for range in overlapping.into_iter() {
            key.range_start = range.start;
            self.cache.remove(&key);
            self.update_counter_drop_item(range.end - range.start);
        }

        key.range_start = final_range.start;
        let value = CacheValue {
            range_end: final_range.end,
            bytes: final_bytes,
        };
        self.cache.insert(key, value);
        self.update_counter_record_item(final_range.end - final_range.start);
    }

    // Return a block that contain everything between query.range_start and range_end
    fn get_block<'a, 'b: 'a>(
        &'a self,
        query: &CacheKey<'b, T>,
        range_end: usize,
    ) -> Option<(&CacheKey<'_, T>, &CacheValue)> {
        self.cache
            .range(..=query)
            .next_back()
            .filter(|(k, v)| k.tag == query.tag && range_end <= v.range_end)
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
pub struct ByteRangeCache {
    inner: Mutex<NeedMutByteRangeCache<Path>>,
}

impl ByteRangeCache {
    /// Creates a slice cache that nevers removes any entry.
    pub fn with_infinite_capacity(cache_counters: &'static CacheMetrics) -> Self {
        ByteRangeCache {
            inner: Mutex::new(NeedMutByteRangeCache::with_infinite_capacity(
                cache_counters,
            )),
        }
    }

    /// If available, returns the cached view of the slice.
    pub fn get_slice(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes> {
        self.inner.lock().unwrap().get_slice(path, byte_range)
    }

    /// Put the given amount of data in the cache.
    pub fn put_slice(&self, path: PathBuf, byte_range: Range<usize>, bytes: OwnedBytes) {
        self.inner
            .lock()
            .unwrap()
            .put_slice(path, byte_range, bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Range;
    use std::path::Path;

    use proptest::prelude::*;

    use super::ByteRangeCache;
    use crate::metrics::CACHE_METRICS_FOR_TESTS;
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
                        assert_eq!(cache.inner.lock().unwrap().num_items, expected_item_count as u64);

                        let expected_byte_count = state.values()
                            .flatten()
                            .filter(|stored| **stored)
                            .count();
                        assert_eq!(cache.inner.lock().unwrap().num_bytes, expected_byte_count as u64);
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
}
