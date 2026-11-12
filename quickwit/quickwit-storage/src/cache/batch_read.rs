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

/// Checks up to five cache entries concurrently, then reads all misses as one batch.
/// Preserves input order and duplicates. Empty ranges bypass cache and storage.
/// Synchronous caches can return `std::future::ready` from `get_cached`.
///
/// Ranges must have `start <= end`. `read_misses` must return one buffer per range,
/// in input order, and may populate its cache after the whole read succeeds.
pub async fn read_batch_with_cache<GetFuture, ReadFuture, Error>(
    byte_ranges: Vec<Range<usize>>,
    mut get_cached: impl FnMut(Range<usize>) -> GetFuture,
    read_misses: impl FnOnce(Vec<Range<usize>>) -> ReadFuture,
) -> Result<Vec<OwnedBytes>, Error>
where
    GetFuture: Future<Output = Option<OwnedBytes>>,
    ReadFuture: Future<Output = Result<Vec<OwnedBytes>, Error>>,
{
    let mut results = vec![OwnedBytes::empty(); byte_ranges.len()];
    let mut misses = Vec::new();
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
        if let Some(bytes) = bytes {
            results[index] = bytes;
        } else {
            misses.push(index);
        }
    }
    drop(lookups);
    if misses.is_empty() {
        return Ok(results);
    }
    if misses.len() == byte_ranges.len() {
        return read_misses(byte_ranges).await;
    }
    let ranges = misses
        .iter()
        .map(|&index| byte_ranges[index].clone())
        .collect();
    let payloads = read_misses(ranges).await?;
    for (index, bytes) in misses.into_iter().zip(payloads) {
        results[index] = bytes;
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use std::future::ready;

    use super::*;

    #[tokio::test]
    async fn test_all_misses_forward_original_vector() {
        let ranges = vec![4..6, 0..2, 4..6];
        let address = ranges.as_ptr() as usize;
        let results = read_batch_with_cache(
            ranges,
            |_| ready(None),
            |misses| async move {
                assert_eq!(misses.as_ptr() as usize, address);
                assert_eq!(misses, vec![4..6, 0..2, 4..6]);
                Ok::<_, ()>(
                    misses
                        .into_iter()
                        .map(|range| OwnedBytes::new(vec![range.start as u8; range.len()]))
                        .collect(),
                )
            },
        )
        .await
        .unwrap();
        assert_eq!(
            results.iter().map(OwnedBytes::as_slice).collect::<Vec<_>>(),
            vec![&[4, 4][..], &[0, 0], &[4, 4]]
        );
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
            let results = read_batch_with_cache(
                ranges,
                |range| {
                    assert!(!range.is_empty());
                    ready(Some(OwnedBytes::new(vec![range.start as u8; range.len()])))
                },
                |_| ready(Err::<Vec<OwnedBytes>, ()>(())),
            )
            .await
            .unwrap();
            assert_eq!(
                results.iter().map(OwnedBytes::as_slice).collect::<Vec<_>>(),
                expected
            );
        }
    }
}
