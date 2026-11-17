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

pub(crate) const MAX_NUM_RANGES_FOR_CACHE_LOOKUP: usize = 100;

/// Checks a synchronous cache, then reads all misses as one batch.
/// Preserves input order and duplicates. Empty ranges bypass cache and storage.
/// Batches with more than 100 ranges skip cache lookups and pass all nonempty ranges
/// to `fetch_ranges`, which controls cache insertion.
///
/// Ranges must have `start <= end`. `fetch_ranges` must return one buffer per range,
/// in input order, and may populate its cache after the whole read succeeds.
pub async fn read_batch_with_cache<FetchRangesFuture, Error>(
    requested_ranges: Vec<Range<usize>>,
    mut get_cached: impl FnMut(Range<usize>) -> Option<OwnedBytes>,
    fetch_ranges: impl FnOnce(Vec<Range<usize>>) -> FetchRangesFuture,
) -> Result<Vec<OwnedBytes>, Error>
where
    FetchRangesFuture: Future<Output = Result<Vec<OwnedBytes>, Error>>,
{
    let mut batch = CachedAndFetchedBytes::new(requested_ranges.len());
    if batch.skip_cache_lookups(&requested_ranges) {
        return batch.fetch_ranges(requested_ranges, fetch_ranges).await;
    }
    for (index, requested_range) in requested_ranges.iter().enumerate() {
        if !requested_range.is_empty() {
            batch.record_cache_lookup(index, get_cached(requested_range.clone()));
        }
    }
    batch.fetch_ranges(requested_ranges, fetch_ranges).await
}

/// Like [`read_batch_with_cache`], with up to five concurrent asynchronous cache lookups.
/// Collects all misses before calling `fetch_ranges`.
/// `get_cached` returns cached bytes or `None` for a range.
/// `fetch_ranges` returns the fetched buffers or an error.
pub async fn read_batch_with_async_cache<CacheLookupFuture, FetchRangesFuture, Error>(
    requested_ranges: Vec<Range<usize>>,
    mut get_cached: impl FnMut(Range<usize>) -> CacheLookupFuture,
    fetch_ranges: impl FnOnce(Vec<Range<usize>>) -> FetchRangesFuture,
) -> Result<Vec<OwnedBytes>, Error>
where
    CacheLookupFuture: Future<Output = Option<OwnedBytes>>,
    FetchRangesFuture: Future<Output = Result<Vec<OwnedBytes>, Error>>,
{
    let mut batch = CachedAndFetchedBytes::new(requested_ranges.len());
    if batch.skip_cache_lookups(&requested_ranges) {
        return batch.fetch_ranges(requested_ranges, fetch_ranges).await;
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
    batch.fetch_ranges(requested_ranges, fetch_ranges).await
}

/// Combines cached and fetched bytes in request order.
/// Empty ranges and unread ranges initially contain empty bytes.
struct CachedAndFetchedBytes {
    bytes_by_request: Vec<OwnedBytes>,
    request_indices_to_fetch: Vec<usize>,
}

impl CachedAndFetchedBytes {
    fn new(num_ranges: usize) -> Self {
        Self {
            bytes_by_request: vec![OwnedBytes::empty(); num_ranges],
            request_indices_to_fetch: Vec::new(),
        }
    }

    /// Treats every nonempty range as a miss when the batch exceeds the lookup limit.
    fn skip_cache_lookups(&mut self, requested_ranges: &[Range<usize>]) -> bool {
        if requested_ranges.len() <= MAX_NUM_RANGES_FOR_CACHE_LOOKUP {
            return false;
        }
        self.request_indices_to_fetch = requested_ranges
            .iter()
            .enumerate()
            .filter(|(_, range)| !range.is_empty())
            .map(|(index, _)| index)
            .collect();
        true
    }

    /// Stores cached bytes or saves the request index for fetching on a miss.
    fn record_cache_lookup(&mut self, request_index: usize, cached_bytes: Option<OwnedBytes>) {
        if let Some(bytes) = cached_bytes {
            self.bytes_by_request[request_index] = bytes;
        } else {
            self.request_indices_to_fetch.push(request_index);
        }
    }

    /// Reads all recorded cache misses in one callback and combines them with cached hits in
    /// the original request order. Empty ranges keep their empty buffers. Skips the callback
    /// when there are no misses and forwards the original ranges directly when every range misses.
    /// The callback must return one buffer per missing range, in the order supplied.
    async fn fetch_ranges<FetchRangesFuture, Error>(
        mut self,
        requested_ranges: Vec<Range<usize>>,
        fetch_ranges: impl FnOnce(Vec<Range<usize>>) -> FetchRangesFuture,
    ) -> Result<Vec<OwnedBytes>, Error>
    where
        FetchRangesFuture: Future<Output = Result<Vec<OwnedBytes>, Error>>,
    {
        if self.request_indices_to_fetch.is_empty() {
            return Ok(self.bytes_by_request);
        }
        if self.request_indices_to_fetch.len() == requested_ranges.len() {
            return fetch_ranges(requested_ranges).await;
        }
        let missing_ranges = self
            .request_indices_to_fetch
            .iter()
            .map(|&request_index| requested_ranges[request_index].clone())
            .collect();
        let downloaded = fetch_ranges(missing_ranges).await?;
        for (request_index, bytes) in self.request_indices_to_fetch.into_iter().zip(downloaded) {
            self.bytes_by_request[request_index] = bytes;
        }
        Ok(self.bytes_by_request)
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
            check_batch_read(requested_ranges, expected_lookups, true).await;
        }
    }

    #[tokio::test]
    async fn test_cache_misses_forward_original_vector_and_read_result() {
        for ranges in [vec![4..6, 0..2, 4..6], vec![0..1; 101]] {
            let expected_lookups = if ranges.len() == 101 { 0 } else { ranges.len() };
            check_batch_read(ranges, expected_lookups, false).await;
        }
    }

    // Exercise both lookup APIs with successful and failed fetches, preserving caller order.
    async fn check_batch_read(ranges: Vec<Range<usize>>, expected_lookups: usize, cache_hit: bool) {
        let missing_ranges: Vec<_> = ranges
            .iter()
            .filter(|range| !range.is_empty() && (!cache_hit || ranges.len() > 100))
            .cloned()
            .collect();
        for async_cache in [false, true] {
            for fail in [false, true] {
                let requested_ranges = ranges.clone();
                let address = requested_ranges.as_ptr();
                let mut lookups = 0;
                let mut get_cached = |range: Range<usize>| {
                    assert!(!range.is_empty());
                    lookups += 1;
                    cache_hit.then(|| OwnedBytes::new(vec![range.start as u8; range.len()]))
                };
                let mut fetches = 0;
                let fetch_ranges = |fetched_ranges: Vec<Range<usize>>| {
                    fetches += 1;
                    assert!(!fetched_ranges.is_empty());
                    assert_eq!(fetched_ranges, missing_ranges);
                    if missing_ranges.len() == ranges.len() {
                        assert_eq!(fetched_ranges.as_ptr(), address);
                    }
                    ready(if fail {
                        Err("read failed")
                    } else {
                        Ok(fetched_ranges
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
                assert_eq!(lookups, expected_lookups);
                assert_eq!(fetches, usize::from(!missing_ranges.is_empty()));
                if fail && !missing_ranges.is_empty() {
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
