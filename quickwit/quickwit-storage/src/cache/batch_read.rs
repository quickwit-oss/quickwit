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

/// Checks a synchronous cache, then reads all misses as one batch.
/// Preserves input order and duplicates. Empty ranges bypass cache and storage.
///
/// Ranges must have `start <= end`. `read_misses` must return one buffer per range,
/// in input order, and may populate its cache after the whole read succeeds.
pub async fn read_batch_with_cache<ReadFuture, Error>(
    byte_ranges: Vec<Range<usize>>,
    mut get_cached: impl FnMut(Range<usize>) -> Option<OwnedBytes>,
    read_misses: impl FnOnce(Vec<Range<usize>>) -> ReadFuture,
) -> Result<Vec<OwnedBytes>, Error>
where
    ReadFuture: Future<Output = Result<Vec<OwnedBytes>, Error>>,
{
    let mut batch = BatchReadBuffers::new(byte_ranges.len());
    for (index, range) in byte_ranges.iter().enumerate() {
        if !range.is_empty() {
            batch.set_cached_bytes(index, get_cached(range.clone()));
        }
    }
    batch.read_missing_ranges(byte_ranges, read_misses).await
}

/// Like [`read_batch_with_cache`], with up to five concurrent asynchronous cache lookups.
/// Collects all misses before calling `read_misses`.
pub async fn read_batch_with_async_cache<GetFuture, ReadFuture, Error>(
    byte_ranges: Vec<Range<usize>>,
    mut get_cached: impl FnMut(Range<usize>) -> GetFuture,
    read_misses: impl FnOnce(Vec<Range<usize>>) -> ReadFuture,
) -> Result<Vec<OwnedBytes>, Error>
where
    GetFuture: Future<Output = Option<OwnedBytes>>,
    ReadFuture: Future<Output = Result<Vec<OwnedBytes>, Error>>,
{
    let mut batch = BatchReadBuffers::new(byte_ranges.len());
    let mut lookups = stream::iter(
        byte_ranges
            .iter()
            .cloned()
            .enumerate()
            .filter(|(_, range)| !range.is_empty()),
    )
    .map(|(index, range)| {
        let lookup = get_cached(range);
        async move { (index, lookup.await) }
    })
    .buffered(5);
    while let Some((index, bytes)) = lookups.next().await {
        batch.set_cached_bytes(index, bytes);
    }
    drop(lookups);
    batch.read_missing_ranges(byte_ranges, read_misses).await
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

    /// Saves the range index for reading when cached bytes are absent.
    fn set_cached_bytes(&mut self, range_index: usize, cached_bytes: Option<OwnedBytes>) {
        if let Some(bytes) = cached_bytes {
            self.results[range_index] = bytes;
        } else {
            self.missing_range_indices.push(range_index);
        }
    }

    async fn read_missing_ranges<ReadFuture, Error>(
        mut self,
        byte_ranges: Vec<Range<usize>>,
        read_misses: impl FnOnce(Vec<Range<usize>>) -> ReadFuture,
    ) -> Result<Vec<OwnedBytes>, Error>
    where
        ReadFuture: Future<Output = Result<Vec<OwnedBytes>, Error>>,
    {
        if self.missing_range_indices.is_empty() {
            return Ok(self.results);
        }
        if self.missing_range_indices.len() == byte_ranges.len() {
            return read_misses(byte_ranges).await;
        }
        let missing_ranges = self
            .missing_range_indices
            .iter()
            .map(|&range_index| byte_ranges[range_index].clone())
            .collect();
        let missing_range_bytes = read_misses(missing_ranges).await?;
        for (range_index, bytes) in self
            .missing_range_indices
            .into_iter()
            .zip(missing_range_bytes)
        {
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
    async fn test_all_misses_forward_original_vector() {
        for async_cache in [false, true] {
            let ranges = vec![4..6, 0..2, 4..6];
            let address = ranges.as_ptr() as usize;
            let read_misses = |misses: Vec<Range<usize>>| async move {
                assert_eq!(misses.as_ptr() as usize, address);
                assert_eq!(misses, vec![4..6, 0..2, 4..6]);
                Ok::<_, ()>(
                    misses
                        .into_iter()
                        .map(|range| OwnedBytes::new(vec![range.start as u8; range.len()]))
                        .collect(),
                )
            };
            let results = if async_cache {
                read_batch_with_async_cache(ranges, |_| ready(None), read_misses).await
            } else {
                read_batch_with_cache(ranges, |_| None, read_misses).await
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
        for ranges in [
            vec![],
            vec![0..0, usize::MAX..usize::MAX],
            vec![4..6, 0..0, 0..2, 4..6],
        ] {
            let expected: Vec<_> = ranges
                .iter()
                .map(|range| vec![range.start as u8; range.len()])
                .collect();
            for async_cache in [false, true] {
                let get_cached = |range: Range<usize>| {
                    assert!(!range.is_empty());
                    Some(OwnedBytes::new(vec![range.start as u8; range.len()]))
                };
                let read_misses = |_| ready(Err::<Vec<OwnedBytes>, ()>(()));
                let results = if async_cache {
                    read_batch_with_async_cache(
                        ranges.clone(),
                        |range| ready(get_cached(range)),
                        read_misses,
                    )
                    .await
                } else {
                    read_batch_with_cache(ranges.clone(), get_cached, read_misses).await
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
