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

use std::future::Future;
use std::ops::Range;

use futures::{StreamExt, stream};

use crate::OwnedBytes;

pub(super) const MAX_CACHE_RANGES: usize = 100;

/// Checks a synchronous cache, then reads all misses as one batch.
/// Preserves input order and duplicates. Empty ranges bypass cache and storage.
/// Batches with more than 100 ranges skip cache lookups and pass all nonempty ranges
/// to the read callback. The callback controls cache insertion.
///
/// Ranges must have `start <= end`. `read_missing_ranges` must return one buffer per range,
/// in input order, and may populate its cache after the whole read succeeds.
pub async fn read_batch_with_cache<ReadFuture, Error>(
    requested_ranges: Vec<Range<usize>>,
    mut get_cached: impl FnMut(Range<usize>) -> Option<OwnedBytes>,
    read_missing_ranges: impl FnOnce(Vec<Range<usize>>) -> ReadFuture,
) -> Result<Vec<OwnedBytes>, Error>
where
    ReadFuture: Future<Output = Result<Vec<OwnedBytes>, Error>>,
{
    let mut batch = BatchReadBuffers::new(requested_ranges.len());
    if batch.skip_cache_lookups(&requested_ranges) {
        return batch
            .read_missing_ranges(requested_ranges, read_missing_ranges)
            .await;
    }
    for (index, requested_range) in requested_ranges.iter().enumerate() {
        if !requested_range.is_empty() {
            batch.record_cache_lookup(index, get_cached(requested_range.clone()));
        }
    }
    batch
        .read_missing_ranges(requested_ranges, read_missing_ranges)
        .await
}

/// Like [`read_batch_with_cache`], with up to five concurrent asynchronous cache lookups.
/// Collects all misses before calling `read_missing_ranges`.
pub async fn read_batch_with_async_cache<GetFuture, ReadFuture, Error>(
    requested_ranges: Vec<Range<usize>>,
    mut get_cached: impl FnMut(Range<usize>) -> GetFuture,
    read_missing_ranges: impl FnOnce(Vec<Range<usize>>) -> ReadFuture,
) -> Result<Vec<OwnedBytes>, Error>
where
    GetFuture: Future<Output = Option<OwnedBytes>>,
    ReadFuture: Future<Output = Result<Vec<OwnedBytes>, Error>>,
{
    let mut batch = BatchReadBuffers::new(requested_ranges.len());
    if batch.skip_cache_lookups(&requested_ranges) {
        return batch
            .read_missing_ranges(requested_ranges, read_missing_ranges)
            .await;
    }
    let mut lookups = stream::iter(
        requested_ranges
            .iter()
            .cloned()
            .enumerate()
            .filter(|(_, requested_range)| !requested_range.is_empty()),
    )
    .map(|(index, requested_range)| {
        let lookup = get_cached(requested_range);
        async move { (index, lookup.await) }
    })
    .buffered(5);
    while let Some((index, bytes)) = lookups.next().await {
        batch.record_cache_lookup(index, bytes);
    }
    drop(lookups);
    batch
        .read_missing_ranges(requested_ranges, read_missing_ranges)
        .await
}

/// Buffers for a batch read, in input order, and indices of ranges missing from the cache.
/// Empty ranges and unread ranges initially contain empty bytes.
struct BatchReadBuffers {
    results: Vec<OwnedBytes>,
    missing_range_indices: Vec<usize>,
}

impl BatchReadBuffers {
    fn new(num_ranges: usize) -> Self {
        Self {
            results: vec![OwnedBytes::empty(); num_ranges],
            missing_range_indices: Vec::new(),
        }
    }

    /// Treats every nonempty range as a miss when the batch exceeds the lookup limit.
    fn skip_cache_lookups(&mut self, requested_ranges: &[Range<usize>]) -> bool {
        if requested_ranges.len() <= MAX_CACHE_RANGES {
            return false;
        }
        self.missing_range_indices = requested_ranges
            .iter()
            .enumerate()
            .filter(|(_, range)| !range.is_empty())
            .map(|(index, _)| index)
            .collect();
        true
    }

    /// Stores a cache hit or saves the range index for reading on a miss.
    fn record_cache_lookup(&mut self, range_index: usize, cached_bytes: Option<OwnedBytes>) {
        if let Some(bytes) = cached_bytes {
            self.results[range_index] = bytes;
        } else {
            self.missing_range_indices.push(range_index);
        }
    }

    /// Reads all recorded cache misses in one callback and combines them with cached hits in
    /// the original request order. Empty ranges keep their empty buffers. Skips the callback
    /// when there are no misses and forwards the original ranges directly when every range misses.
    /// The callback must return one buffer per missing range, in the order supplied.
    async fn read_missing_ranges<ReadFuture, Error>(
        mut self,
        requested_ranges: Vec<Range<usize>>,
        read_missing_ranges: impl FnOnce(Vec<Range<usize>>) -> ReadFuture,
    ) -> Result<Vec<OwnedBytes>, Error>
    where
        ReadFuture: Future<Output = Result<Vec<OwnedBytes>, Error>>,
    {
        if self.missing_range_indices.is_empty() {
            return Ok(self.results);
        }
        if self.missing_range_indices.len() == requested_ranges.len() {
            return read_missing_ranges(requested_ranges).await;
        }
        let missing_ranges = self
            .missing_range_indices
            .iter()
            .map(|&range_index| requested_ranges[range_index].clone())
            .collect();
        let downloaded = read_missing_ranges(missing_ranges).await?;
        for (range_index, bytes) in self.missing_range_indices.into_iter().zip(downloaded) {
            self.results[range_index] = bytes;
        }
        Ok(self.results)
    }
}

#[cfg(test)]
mod tests {
    use std::future::ready;

    use super::*;

    #[tokio::test]
    async fn test_cache_lookup_limit() {
        for async_cache in [false, true] {
            for num_ranges in [100, 101] {
                let mut requested_ranges: Vec<_> = (0..num_ranges - 2)
                    .rev()
                    .map(|index| index % 7..index % 7 + 1)
                    .collect();
                requested_ranges.insert(0, 0..0);
                requested_ranges.insert(50, 2..2);
                let expected_ranges: Vec<_> = requested_ranges
                    .iter()
                    .filter(|range| !range.is_empty())
                    .cloned()
                    .collect();
                let mut lookups = 0;
                let mut get_cached = |range: Range<usize>| {
                    lookups += 1;
                    Some(OwnedBytes::new(vec![range.start as u8]))
                };
                let read_missing_ranges = |ranges: Vec<Range<usize>>| {
                    assert_eq!(num_ranges, 101);
                    assert_eq!(ranges, expected_ranges);
                    ready(Ok::<_, ()>(
                        ranges
                            .iter()
                            .map(|range| OwnedBytes::new(vec![range.start as u8]))
                            .collect(),
                    ))
                };
                let results = if async_cache {
                    read_batch_with_async_cache(
                        requested_ranges.clone(),
                        |range| ready(get_cached(range)),
                        read_missing_ranges,
                    )
                    .await
                } else {
                    read_batch_with_cache(requested_ranges.clone(), get_cached, read_missing_ranges)
                        .await
                }
                .unwrap();
                assert_eq!(lookups, if num_ranges == 100 { 98 } else { 0 });
                assert_eq!(results.len(), requested_ranges.len());
                for (range, bytes) in requested_ranges.iter().zip(results) {
                    assert_eq!(bytes.as_slice(), vec![range.start as u8; range.len()]);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_large_batch_propagates_read_error() {
        for async_cache in [false, true] {
            let ranges = vec![0..1; 101];
            let read_missing_ranges = |ranges: Vec<Range<usize>>| {
                assert_eq!(ranges.len(), 101);
                ready(Err::<Vec<OwnedBytes>, _>("read failed"))
            };
            let result = if async_cache {
                read_batch_with_async_cache(
                    ranges,
                    |_| async { panic!("cache lookup must be skipped") },
                    read_missing_ranges,
                )
                .await
            } else {
                read_batch_with_cache(
                    ranges,
                    |_| panic!("cache lookup must be skipped"),
                    read_missing_ranges,
                )
                .await
            };
            assert_eq!(result.unwrap_err(), "read failed");
        }
    }

    #[tokio::test]
    async fn test_all_misses_forward_original_vector() {
        for async_cache in [false, true] {
            let requested_ranges = vec![4..6, 0..2, 4..6];
            let address = requested_ranges.as_ptr() as usize;
            let read_missing_ranges = |missing_ranges: Vec<Range<usize>>| async move {
                assert_eq!(missing_ranges.as_ptr() as usize, address);
                assert_eq!(missing_ranges, vec![4..6, 0..2, 4..6]);
                Ok::<_, ()>(
                    missing_ranges
                        .into_iter()
                        .map(|missing_range| {
                            OwnedBytes::new(vec![missing_range.start as u8; missing_range.len()])
                        })
                        .collect(),
                )
            };
            let results = if async_cache {
                read_batch_with_async_cache(requested_ranges, |_| ready(None), read_missing_ranges)
                    .await
            } else {
                read_batch_with_cache(requested_ranges, |_| None, read_missing_ranges).await
            }
            .unwrap();
            assert_eq!(
                results.iter().map(OwnedBytes::as_slice).collect::<Vec<_>>(),
                vec![&[4, 4][..], &[0, 0], &[4, 4]]
            );
        }
    }

    #[tokio::test]
    async fn test_hits_and_empty_ranges_need_no_read() {
        for requested_ranges in [
            vec![],
            vec![0..0, usize::MAX..usize::MAX],
            vec![0..0; 101],
            vec![4..6, 0..0, 0..2, 4..6],
        ] {
            let expected: Vec<_> = requested_ranges
                .iter()
                .map(|requested_range| vec![requested_range.start as u8; requested_range.len()])
                .collect();
            for async_cache in [false, true] {
                let get_cached = |requested_range: Range<usize>| {
                    assert!(!requested_range.is_empty());
                    Some(OwnedBytes::new(vec![
                        requested_range.start as u8;
                        requested_range.len()
                    ]))
                };
                let read_missing_ranges = |_| ready(Err::<Vec<OwnedBytes>, ()>(()));
                let results = if async_cache {
                    read_batch_with_async_cache(
                        requested_ranges.clone(),
                        |requested_range| ready(get_cached(requested_range)),
                        read_missing_ranges,
                    )
                    .await
                } else {
                    read_batch_with_cache(requested_ranges.clone(), get_cached, read_missing_ranges)
                        .await
                }
                .unwrap();
                assert_eq!(
                    results.iter().map(OwnedBytes::as_slice).collect::<Vec<_>>(),
                    expected
                );
            }
        }
    }
}
