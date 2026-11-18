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
/// Preserves input order and duplicates. An empty batch returns no buffers.
/// Batches with more than 100 ranges skip cache lookups and pass all ranges
/// to `fetch_ranges`, which controls cache insertion.
///
/// Ranges must have `start < end`. `fetch_ranges` must return one buffer per range,
/// in input order, and may populate its cache after the whole read succeeds.
pub async fn read_batch_with_cache<FetchRangesFuture, Error>(
    requested_ranges: Vec<Range<usize>>,
    mut get_cached: impl FnMut(Range<usize>) -> Option<OwnedBytes>,
    fetch_ranges: impl FnOnce(Vec<Range<usize>>) -> FetchRangesFuture,
) -> Result<Vec<OwnedBytes>, Error>
where
    FetchRangesFuture: Future<Output = Result<Vec<OwnedBytes>, Error>>,
{
    debug_assert!(requested_ranges.iter().all(|range| range.start < range.end));
    if requested_ranges.len() > MAX_NUM_RANGES_FOR_CACHE_LOOKUP {
        return fetch_ranges(requested_ranges).await;
    }
    let mut bytes_by_request = vec![OwnedBytes::empty(); requested_ranges.len()];
    let mut request_indices_to_fetch = Vec::new();
    for (index, requested_range) in requested_ranges.iter().enumerate() {
        let cached_bytes = get_cached(requested_range.clone());
        if let Some(bytes) = cached_bytes {
            bytes_by_request[index] = bytes;
        } else {
            request_indices_to_fetch.push(index);
        }
    }
    if request_indices_to_fetch.is_empty() {
        return Ok(bytes_by_request);
    }
    // Forward the original allocation when every range misses.
    if request_indices_to_fetch.len() == requested_ranges.len() {
        return fetch_ranges(requested_ranges).await;
    }
    let ranges_to_fetch = request_indices_to_fetch
        .iter()
        .map(|&index| requested_ranges[index].clone())
        .collect();
    let downloaded = fetch_ranges(ranges_to_fetch).await?;
    for (index, bytes) in request_indices_to_fetch.into_iter().zip(downloaded) {
        bytes_by_request[index] = bytes;
    }
    Ok(bytes_by_request)
}

/// Like [`read_batch_with_cache`], with up to 32 concurrent asynchronous cache lookups.
/// Collects all misses before calling `fetch_ranges`.
/// `get_cached` returns cached bytes or `None` for a range.
/// `fetch_ranges` returns the fetched buffers or an error.
pub async fn read_batch_with_async_cache<CacheLookupFuture, FetchRangesFuture, Error>(
    requested_ranges: Vec<Range<usize>>,
    get_cached: impl FnMut(Range<usize>) -> CacheLookupFuture,
    fetch_ranges: impl FnOnce(Vec<Range<usize>>) -> FetchRangesFuture,
) -> Result<Vec<OwnedBytes>, Error>
where
    CacheLookupFuture: Future<Output = Option<OwnedBytes>>,
    FetchRangesFuture: Future<Output = Result<Vec<OwnedBytes>, Error>>,
{
    debug_assert!(requested_ranges.iter().all(|range| range.start < range.end));
    let cached_bytes: Vec<_> = if requested_ranges.len() <= MAX_NUM_RANGES_FOR_CACHE_LOOKUP {
        stream::iter(requested_ranges.iter().cloned())
            .map(get_cached)
            .buffered(32)
            .collect()
            .await
    } else {
        Vec::new()
    };
    // `buffered` keeps lookup results in request order for the cache callback below.
    let mut cached_bytes = cached_bytes.into_iter();
    read_batch_with_cache(
        requested_ranges,
        |_| cached_bytes.next().expect("one lookup per range"),
        fetch_ranges,
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::future::ready;

    use super::*;

    #[tokio::test]
    async fn test_cache_hits_empty_batch_and_lookup_limit() {
        let boundary_ranges = |num_ranges: usize| {
            (0..num_ranges)
                .rev()
                .map(|index| index % 7..index % 7 + 1)
                .collect()
        };
        for (requested_ranges, expected_lookups) in [
            (vec![], 0),
            (vec![4..6, 0..2, 4..6], 3),
            (boundary_ranges(100), 100),
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
        let ranges_to_fetch: Vec<_> = ranges
            .iter()
            .filter(|_| !cache_hit || ranges.len() > 100)
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
                    assert_eq!(fetched_ranges, ranges_to_fetch);
                    if ranges_to_fetch.len() == ranges.len() {
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
                assert_eq!(fetches, usize::from(!ranges_to_fetch.is_empty()));
                if fail && !ranges_to_fetch.is_empty() {
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
