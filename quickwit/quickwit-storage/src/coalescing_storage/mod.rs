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

use std::collections::HashMap;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::{BoxFuture, Shared};
use futures::{FutureExt, StreamExt, TryStreamExt, stream};
use quickwit_common::thread_pool::run_cpu_intensive;
use quickwit_common::uri::Uri;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::storage::SendableAsync;
use crate::{BulkDeleteError, ListObjectsStream, OwnedBytes, PutPayload, Storage, StorageResult};

// Collection ends after this duration, measured from the first caller.
const COLLECTION_DURATION: Duration = Duration::from_millis(1);
const MAX_GAP: usize = 4 * 1024 * 1024;

struct CollectedBatch {
    // Taking these ranges ends collection. Later callers start another batch because
    // the stream may have discarded bytes they need.
    ranges_to_read: Arc<Mutex<Option<Vec<Range<usize>>>>>,
    read_future: Shared<BoxFuture<'static, StorageResult<Arc<Vec<OwnedBytes>>>>>,
}

impl CollectedBatch {
    fn new(
        underlying: Arc<dyn Storage>,
        path: PathBuf,
        requested_ranges: Vec<Range<usize>>,
    ) -> Arc<Self> {
        let ranges_to_read = Arc::new(Mutex::new(Some(requested_ranges)));
        let collected_ranges = ranges_to_read.clone();
        let read_future = async move {
            tokio::time::sleep(COLLECTION_DURATION).await;
            let requested_ranges = collected_ranges
                .lock()
                .unwrap()
                .take()
                .expect("collected ranges are taken once");
            download_chunks_in_parallel(underlying.as_ref(), &path, requested_ranges)
                .await
                .map(Arc::new)
        }
        .boxed()
        .shared();
        Arc::new(Self {
            ranges_to_read,
            read_future,
        })
    }
}

/// Collects concurrent `get_slice`, `get_slice_into`, and `get_slices` calls for the same immutable
/// object for 1 ms, including singletons. Batch reads submit all ranges together, with one shared
/// read future per caller. Merges overlaps and gaps up to 4 MiB and runs at most five downloads per
/// collected group. Streams merged downloads, retaining requested slices and discarding gaps.
/// Batch ranges must have `start < end`; ranges may be in any order. Empty batches return no
/// buffers.
///
/// Place this below exact-range caches and above timeout/retry storage. Caches then see original
/// ranges, and only their misses wait for collection. Object paths are relative to `underlying`.
/// Callers own each batch. Dropping the last caller cancels its timer or downloads.
/// Other storage operations, including `get_slice_stream`, forward to `underlying`.
#[derive(Debug)]
pub struct CoalescingStorage {
    underlying: Arc<dyn Storage>,
    batches_by_object: Mutex<HashMap<PathBuf, Weak<CollectedBatch>>>,
}

impl CoalescingStorage {
    /// Wraps storage with a collector shared by callers using this instance.
    pub fn new(underlying: Arc<dyn Storage>) -> Self {
        Self {
            underlying,
            batches_by_object: Mutex::default(),
        }
    }

    fn collect_ranges(
        &self,
        path: &Path,
        requested_ranges: Vec<Range<usize>>,
    ) -> (Arc<CollectedBatch>, Range<usize>) {
        let mut batches_by_object = self.batches_by_object.lock().unwrap();
        if let Some(batch) = batches_by_object.get(path).and_then(Weak::upgrade)
            && let Some(ranges_to_read) = batch.ranges_to_read.lock().unwrap().as_mut()
        {
            let start = ranges_to_read.len();
            ranges_to_read.extend(requested_ranges);
            return (batch.clone(), start..ranges_to_read.len());
        }
        let caller_result_indices = 0..requested_ranges.len();
        let batch = CollectedBatch::new(self.underlying.clone(), path.to_owned(), requested_ranges);
        batches_by_object.insert(path.to_owned(), Arc::downgrade(&batch));
        (batch, caller_result_indices)
    }
}

#[async_trait]
impl Storage for CoalescingStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.underlying.check_connectivity().await
    }

    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()> {
        self.underlying.put(path, payload).await
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        self.underlying.copy_to(path, output).await
    }

    #[allow(clippy::single_range_in_vec_init)] // A single-range read.
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        if range.is_empty() {
            return Ok(OwnedBytes::empty());
        }
        Ok(self
            .get_slices(path, vec![range])
            .await?
            .pop()
            .expect("one buffer per requested range"))
    }

    async fn get_slice_into(
        &self,
        path: &Path,
        range: Range<usize>,
        buffer: &mut [u8],
    ) -> StorageResult<()> {
        // Collect this read too. This uses a temporary buffer before copying into the caller's
        // allocation, because collected reads return shared buffers to their callers.
        let bytes = self.get_slice(path, range).await?;
        buffer.copy_from_slice(&bytes);
        Ok(())
    }

    async fn get_slices(
        &self,
        path: &Path,
        requested_ranges: Vec<Range<usize>>,
    ) -> StorageResult<Vec<OwnedBytes>> {
        debug_assert!(requested_ranges.iter().all(|range| range.start < range.end));
        if requested_ranges.is_empty() {
            return Ok(Vec::new());
        }
        let (batch, caller_result_indices) = self.collect_ranges(path, requested_ranges);
        // Keep the batch alive while this caller awaits its results.
        let buffers = batch.read_future.clone().await?;
        Ok(buffers[caller_result_indices].to_vec())
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        self.underlying.get_slice_stream(path, range).await
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        self.underlying.get_all(path).await
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.underlying.delete(path).await
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        self.underlying.bulk_delete(paths).await
    }

    fn list(&self, prefix: &Path) -> ListObjectsStream {
        self.underlying.list(prefix)
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.underlying.file_num_bytes(path).await
    }

    fn uri(&self) -> &Uri {
        self.underlying.uri()
    }
}

async fn download_chunks_in_parallel(
    storage: &dyn Storage,
    path: &Path,
    requested_ranges: Vec<Range<usize>>,
) -> StorageResult<Vec<OwnedBytes>> {
    let mut buffers = vec![OwnedBytes::empty(); requested_ranges.len()];
    // Merge the collected ranges into download chunks, then download at most five concurrently.
    let (requests, download_chunks) = run_cpu_intensive(move || {
        let mut requests: Vec<_> = requested_ranges.into_iter().enumerate().collect();
        requests.sort_unstable_by_key(|(_, requested_range)| requested_range.start);
        let download_chunks = coalesce_ranges(
            requests
                .iter()
                .map(|(_, requested_range)| requested_range.clone()),
        );
        (requests, download_chunks)
    })
    .await
    .map_err(io::Error::other)?;
    // Sorted, disjoint download chunks contain consecutive groups of requested ranges.
    let mut remaining_requests = requests.as_slice();
    let mut downloads = stream::iter(download_chunks)
        .map(|download_chunk| {
            let end = remaining_requests
                .partition_point(|(_, requested_range)| requested_range.start < download_chunk.end);
            let (requests, remaining) = remaining_requests.split_at(end);
            remaining_requests = remaining;
            download_chunk_and_extract_slices(storage, path, download_chunk, requests)
        })
        .buffer_unordered(5);
    while let Some(downloaded) = downloads.try_next().await? {
        for (index, bytes) in downloaded {
            buffers[index] = bytes;
        }
    }
    Ok(buffers)
}

/// Ranges must have `start < end` and be sorted by start offset.
/// Merges overlaps and gaps up to 4 MiB. This limits each gap, not the total download size
/// or the total number of gap bytes downloaded.
fn coalesce_ranges(requested_ranges: impl Iterator<Item = Range<usize>>) -> Vec<Range<usize>> {
    let mut merged_ranges: Vec<Range<usize>> = Vec::new();
    for requested_range in requested_ranges {
        debug_assert!(requested_range.start < requested_range.end);
        if let Some(previous) = merged_ranges.last_mut()
            && requested_range.start.saturating_sub(previous.end) <= MAX_GAP
        {
            previous.end = previous.end.max(requested_range.end);
        } else {
            merged_ranges.push(requested_range);
        }
    }
    merged_ranges
}

// Requests are sorted, nonempty, and contained in the download chunk.
async fn download_chunk_and_extract_slices(
    storage: &dyn Storage,
    path: &Path,
    download_chunk: Range<usize>,
    mut requests: &[(usize, Range<usize>)],
) -> StorageResult<Vec<(usize, OwnedBytes)>> {
    if let [(index, _)] = requests {
        let bytes = storage.get_slice(path, download_chunk).await?;
        return Ok(vec![(*index, bytes)]);
    }
    let reader = storage
        .get_slice_stream(path, download_chunk.clone())
        .await?;
    let mut reader = tokio::io::BufReader::new(reader);
    let mut buffers = Vec::with_capacity(requests.len());
    let mut position = download_chunk.start;
    while !requests.is_empty() {
        // Read overlapping requests together so the stream never needs to seek backwards.
        let mut useful = requests[0].1.clone();
        let mut count = 1;
        while count < requests.len() && requests[count].1.start < useful.end {
            useful.end = useful.end.max(requests[count].1.end);
            count += 1;
        }
        let gap = (useful.start - position) as u64;
        tokio::io::copy_buf(&mut (&mut reader).take(gap), &mut tokio::io::sink()).await?;
        let mut bytes = vec![0; useful.len()];
        reader.read_exact(&mut bytes).await?;
        let bytes = OwnedBytes::new(bytes);
        for (index, requested_range) in &requests[..count] {
            let result = if *requested_range == useful {
                bytes.clone()
            } else {
                // Copy proper subranges so cached results don't retain the entire
                // overlapping-request buffer.
                OwnedBytes::new(
                    bytes[requested_range.start - useful.start..requested_range.end - useful.start]
                        .to_vec(),
                )
            };
            buffers.push((*index, result));
        }
        position = useful.end;
        requests = &requests[count..];
    }
    debug_assert_eq!(position, download_chunk.end);
    Ok(buffers)
}

#[cfg(test)]
mod plan_tests {
    use super::*;

    const KIB: usize = 1024;
    const MIB: usize = 1024 * KIB;

    fn coalesce_ranges(mut ranges: Vec<Range<usize>>) -> Vec<Range<usize>> {
        ranges.sort_unstable_by_key(|range| range.start);
        super::coalesce_ranges(ranges.into_iter())
    }

    #[test]
    fn test_coalesce_ranges_boundaries() {
        let cases: &[(&[Range<usize>], &[Range<usize>])] = &[
            (&[], &[]),
            (&[10..20], &[10..20]),
            // Unsorted, duplicate, nested, adjacent, and separated ranges.
            (
                &[
                    4 * MIB..5 * MIB,
                    0..MIB,
                    0..MIB,
                    10..20,
                    MIB..2 * MIB,
                    2 * MIB + 512 * KIB..3 * MIB,
                ],
                &[0..5 * MIB],
            ),
            (&[0..20 * MIB, 24 * MIB..44 * MIB], &[0..44 * MIB]),
            // Even large downloads cannot bridge a gap greater than 4 MiB.
            (
                &[0..20 * MIB, 24 * MIB + 1..44 * MIB + 1],
                &[0..20 * MIB, 24 * MIB + 1..44 * MIB + 1],
            ),
            (
                &[usize::MAX - 10..usize::MAX, 0..10],
                &[0..10, usize::MAX - 10..usize::MAX],
            ),
            (&[0..usize::MAX, 1..2], &[0..usize::MAX]),
        ];
        for (ranges, expected) in cases {
            assert_eq!(coalesce_ranges(ranges.to_vec()), *expected, "{ranges:?}");
        }
    }

    #[derive(serde::Deserialize)]
    struct QueryRanges {
        query: String,
        ranges: Vec<[usize; 2]>,
    }

    #[test]
    fn test_coalesce_ranges_from_postings() {
        // Reference plans independently calculated with a 4 MiB gap threshold.
        // Pattern, requested ranges/bytes, download chunks/bytes.
        for (fixture, json, expected) in [
            (
                "inline_8",
                include_str!("../../test-data/coalescing/requested_ranges.json"),
                [
                    ("*ab*", 203, 288_249, 9, 38_242_633),
                    ("*aba*", 16, 143_930, 2, 7_888_734),
                    ("*traaa*", 0, 0, 0, 0),
                    ("traaa*", 0, 0, 0, 0),
                    ("*player*", 21, 4_221_761, 6, 15_382_086),
                    ("*ms*", 27, 825_018, 8, 19_690_015),
                ],
            ),
            (
                // Original postings offsets, keeping the first 16,384 matching lists per query.
                "no_inline",
                include_str!("../../test-data/coalescing/requested_ranges_no_inline.json"),
                [
                    ("*ab*", 16_384, 82_276, 2, 107_392_855),
                    ("*aba*", 2_817, 157_561, 4, 237_355_128),
                    ("*traaa*", 2, 10, 2, 10),
                    ("traaa*", 0, 0, 0, 0),
                    ("*player*", 21, 4_221_761, 6, 17_345_872),
                    ("*ms*", 16_384, 200_211, 2, 159_383_608),
                ],
            ),
        ] {
            let queries: Vec<QueryRanges> = serde_json::from_str(json).unwrap();
            assert_eq!(queries.len(), expected.len(), "{fixture}");
            for (query, (pattern, range_count, requested_bytes, download_count, download_bytes)) in
                queries.into_iter().zip(expected)
            {
                assert_eq!(query.query, pattern, "{fixture}");
                let pattern = format!("{fixture}: {pattern}");
                let ranges: Vec<Range<usize>> = query
                    .ranges
                    .into_iter()
                    .map(|[start, end]| start..end)
                    .collect();
                assert_eq!(ranges.len(), range_count, "{pattern}");
                assert_eq!(
                    ranges.iter().map(|range| range.len()).sum::<usize>(),
                    requested_bytes,
                    "{pattern}"
                );
                let downloads = coalesce_ranges(ranges.clone());
                let fetched_bytes: usize = downloads.iter().map(|range| range.len()).sum();
                println!(
                    "{pattern}: {range_count} requested ranges, {requested_bytes} requested bytes \
                     -> {} downloads, {fetched_bytes} fetched bytes",
                    downloads.len()
                );
                assert_eq!(downloads.len(), download_count, "{pattern}");
                assert_eq!(fetched_bytes, download_bytes, "{pattern}");
                assert!(downloads.windows(2).all(|pair| pair[0].end < pair[1].start));
                for range in ranges {
                    assert!(downloads.iter().any(|download| {
                        download.start <= range.start && range.end <= download.end
                    }));
                }
            }
        }
    }
}

#[cfg(test)]
mod stream_tests;
#[cfg(test)]
mod tests;
