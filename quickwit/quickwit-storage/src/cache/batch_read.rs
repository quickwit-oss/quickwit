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
/// Cache lookups run in a plain loop, without futures or buffering.
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
    let mut cached = CacheLookupResults::new(byte_ranges.len());
    for (index, range) in byte_ranges.iter().enumerate() {
        if !range.is_empty() {
            cached.record(index, get_cached(range.clone()));
        }
    }
    cached.read_misses(byte_ranges, read_misses).await
}

/// Like [`read_batch_with_cache`], with up to five concurrent asynchronous cache lookups.
/// All misses are collected before forwarding one complete batch to `read_misses`.
pub async fn read_batch_with_async_cache<GetFuture, ReadFuture, Error>(
    byte_ranges: Vec<Range<usize>>,
    mut get_cached: impl FnMut(Range<usize>) -> GetFuture,
    read_misses: impl FnOnce(Vec<Range<usize>>) -> ReadFuture,
) -> Result<Vec<OwnedBytes>, Error>
where
    GetFuture: Future<Output = Option<OwnedBytes>>,
    ReadFuture: Future<Output = Result<Vec<OwnedBytes>, Error>>,
{
    let mut cached = CacheLookupResults::new(byte_ranges.len());
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
        cached.record(index, bytes);
    }
    drop(lookups);
    cached.read_misses(byte_ranges, read_misses).await
}

struct CacheLookupResults {
    buffers: Vec<OwnedBytes>,
    misses: Vec<usize>,
}

impl CacheLookupResults {
    fn new(num_ranges: usize) -> Self {
        Self {
            buffers: vec![OwnedBytes::empty(); num_ranges],
            misses: Vec::new(),
        }
    }

    fn record(&mut self, index: usize, bytes: Option<OwnedBytes>) {
        if let Some(bytes) = bytes {
            self.buffers[index] = bytes;
        } else {
            self.misses.push(index);
        }
    }

    async fn read_misses<ReadFuture, Error>(
        mut self,
        byte_ranges: Vec<Range<usize>>,
        read_misses: impl FnOnce(Vec<Range<usize>>) -> ReadFuture,
    ) -> Result<Vec<OwnedBytes>, Error>
    where
        ReadFuture: Future<Output = Result<Vec<OwnedBytes>, Error>>,
    {
        if self.misses.is_empty() {
            return Ok(self.buffers);
        }
        if self.misses.len() == byte_ranges.len() {
            return read_misses(byte_ranges).await;
        }
        let ranges = self
            .misses
            .iter()
            .map(|&index| byte_ranges[index].clone())
            .collect();
        let payloads = read_misses(ranges).await?;
        for (index, bytes) in self.misses.into_iter().zip(payloads) {
            self.buffers[index] = bytes;
        }
        Ok(self.buffers)
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
