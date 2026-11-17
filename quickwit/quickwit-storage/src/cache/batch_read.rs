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

pub(crate) const MAX_CACHE_RANGES: usize = 100;

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
    async fn test_cache_hits_empty_ranges_and_lookup_limit() {
        let boundary_ranges = |num_ranges: usize| {
            let mut ranges: Vec<_> = (0..num_ranges - 2)
                .rev()
                .map(|index| index % 7..index % 7 + 1)
                .collect();
            ranges.insert(0, 0..0);
            ranges.insert(50, 2..2);
            ranges
        };
        for (requested_ranges, expected_lookups) in [
            (vec![], 0),
            (vec![0..0, usize::MAX..usize::MAX], 0),
            (vec![0..0; 101], 0),
            (vec![4..6, 0..0, 0..2, 4..6], 3),
            (boundary_ranges(100), 98),
            (boundary_ranges(101), 0),
        ] {
            for async_cache in [false, true] {
                let expected_ranges: Vec<_> = requested_ranges
                    .iter()
                    .filter(|range| !range.is_empty())
                    .cloned()
                    .collect();
                let mut lookups = 0;
                let mut get_cached = |range: Range<usize>| {
                    assert!(!range.is_empty());
                    lookups += 1;
                    Some(OwnedBytes::new(vec![range.start as u8; range.len()]))
                };
                let mut fetched = false;
                let fetch_ranges = |ranges: Vec<Range<usize>>| {
                    assert_eq!(requested_ranges.len(), 101);
                    assert!(!ranges.is_empty());
                    assert_eq!(ranges, expected_ranges);
                    fetched = true;
                    ready(Ok::<_, ()>(
                        ranges
                            .iter()
                            .map(|range| OwnedBytes::new(vec![range.start as u8; range.len()]))
                            .collect(),
                    ))
                };
                let results = if async_cache {
                    read_batch_with_async_cache(
                        requested_ranges.clone(),
                        |range| ready(get_cached(range)),
                        fetch_ranges,
                    )
                    .await
                } else {
                    read_batch_with_cache(requested_ranges.clone(), get_cached, fetch_ranges).await
                }
                .unwrap();
                assert_eq!(lookups, expected_lookups);
                assert_eq!(
                    fetched,
                    requested_ranges.len() == 101 && !expected_ranges.is_empty()
                );
                assert_eq!(results.len(), requested_ranges.len());
                for (range, bytes) in requested_ranges.iter().zip(results) {
                    assert_eq!(bytes.as_slice(), vec![range.start as u8; range.len()]);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_cache_misses_forward_original_vector_and_read_result() {
        for ranges in [vec![4..6, 0..2, 4..6], vec![0..1; 101]] {
            for (async_cache, fail) in [(false, false), (false, true), (true, false), (true, true)]
            {
                let requested_ranges = ranges.clone();
                let address = requested_ranges.as_ptr();
                let mut lookups = 0;
                let mut get_cached = |_| {
                    lookups += 1;
                    None
                };
                let fetch_ranges = |missing_ranges: Vec<Range<usize>>| {
                    assert_eq!(missing_ranges.as_ptr(), address);
                    assert_eq!(missing_ranges, ranges);
                    ready(if fail {
                        Err("read failed")
                    } else {
                        Ok(missing_ranges
                            .iter()
                            .map(|range| OwnedBytes::new(vec![range.start as u8; range.len()]))
                            .collect())
                    })
                };
                let result = if async_cache {
                    read_batch_with_async_cache(
                        requested_ranges,
                        |range| ready(get_cached(range)),
                        fetch_ranges,
                    )
                    .await
                } else {
                    read_batch_with_cache(requested_ranges, get_cached, fetch_ranges).await
                };
                assert_eq!(lookups, if ranges.len() == 101 { 0 } else { ranges.len() });
                if fail {
                    assert_eq!(result.unwrap_err(), "read failed");
                } else {
                    let buffers = result.unwrap();
                    assert_eq!(buffers.len(), ranges.len());
                    for (buffer, range) in buffers.iter().zip(&ranges) {
                        assert_eq!(buffer.as_slice(), vec![range.start as u8; range.len()]);
                    }
                }
            }
        }
    }
}
