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
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use std::{fmt, io};

use async_trait::async_trait;
use futures::future::{BoxFuture, Shared};
use futures::{FutureExt, StreamExt, TryStreamExt, stream};
use quickwit_common::thread_pool::run_cpu_intensive;
use quickwit_common::uri::Uri;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::storage::SendableAsync;
use crate::{BulkDeleteError, ListObjectsStream, OwnedBytes, PutPayload, Storage, StorageResult};

mod plan;

// Collection ends after this duration, measured from the first caller.
const COLLECTION_DURATION: Duration = Duration::from_millis(1);

struct CollectedBatch {
    // Taking these ranges ends collection. Later callers start another batch because
    // the stream may have discarded bytes they need.
    ranges_to_read: Mutex<Option<Vec<Range<usize>>>>,
    read_future: Shared<BoxFuture<'static, StorageResult<Arc<Vec<OwnedBytes>>>>>,
}

impl CollectedBatch {
    fn new(underlying: Arc<dyn Storage>, path: PathBuf, ranges: Vec<Range<usize>>) -> Arc<Self> {
        Arc::new_cyclic(|weak_batch: &Weak<Self>| {
            let weak_batch = weak_batch.clone();
            let read_future = async move {
                tokio::time::sleep(COLLECTION_DURATION).await;
                // The upgraded Arc drops before awaiting the reads. Callers own the batch,
                // so dropping the last caller cancels its timer or downloads.
                let collected_ranges = weak_batch
                    .upgrade()
                    .expect("a caller owns the batch while polling its read future")
                    .ranges_to_read
                    .lock()
                    .unwrap()
                    .take()
                    .expect("collected ranges are taken once");
                read_collected_ranges(underlying.as_ref(), &path, collected_ranges)
                    .await
                    .map(Arc::new)
            }
            .boxed()
            .shared();
            Self {
                ranges_to_read: Mutex::new(Some(ranges)),
                read_future,
            }
        })
    }
}

/// Collects concurrent `get_slice` and `get_slices` calls for the same immutable object for 1 ms,
/// including singletons. Batches submit all descriptors together, with one shared read future
/// per caller. Merges overlaps and gaps up to 4 MiB and runs at most five downloads per collected
/// group. Streams merged downloads, retaining requested slices and discarding gaps.
///
/// Place this below exact-range caches and above timeout/retry storage. Caches then see original
/// ranges, and only their misses wait for collection. Object paths are relative to `underlying`.
/// Callers own each batch. Dropping the last caller cancels its timer or downloads.
/// Other storage operations, including `get_slice_stream`, forward to `underlying`.
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
        ranges: Vec<Range<usize>>,
    ) -> (Arc<CollectedBatch>, Range<usize>) {
        let mut batches_by_object = self.batches_by_object.lock().unwrap();
        if let Some(batch) = batches_by_object.get(path).and_then(Weak::upgrade) {
            let mut ranges_to_read = batch.ranges_to_read.lock().unwrap();
            if let Some(ranges_to_read) = ranges_to_read.as_mut() {
                let buffer_indices = ranges_to_read.len()..ranges_to_read.len() + ranges.len();
                ranges_to_read.extend(ranges);
                return (batch.clone(), buffer_indices);
            }
        }
        let buffer_indices = 0..ranges.len();
        let batch = CollectedBatch::new(self.underlying.clone(), path.to_owned(), ranges);
        batches_by_object.insert(path.to_owned(), Arc::downgrade(&batch));
        (batch, buffer_indices)
    }
}

impl fmt::Debug for CoalescingStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoalescingStorage")
            .field("underlying", &self.underlying)
            .finish_non_exhaustive()
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

    #[allow(clippy::single_range_in_vec_init)] // A scalar read submits one descriptor.
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        Ok(self
            .get_slices(path, vec![range])
            .await?
            .pop()
            .expect("one buffer per requested range"))
    }

    async fn get_slices(
        &self,
        path: &Path,
        ranges: Vec<Range<usize>>,
    ) -> StorageResult<Vec<OwnedBytes>> {
        crate::storage::validate_byte_ranges(&ranges)?;
        if ranges.iter().all(Range::is_empty) {
            return Ok(vec![OwnedBytes::empty(); ranges.len()]);
        }
        let (batch, buffer_indices) = self.collect_ranges(path, ranges);
        let buffers = batch.read_future.clone().await?;
        Ok(buffers[buffer_indices].to_vec())
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

async fn read_collected_ranges(
    storage: &dyn Storage,
    path: &Path,
    byte_ranges: Vec<Range<usize>>,
) -> StorageResult<Vec<OwnedBytes>> {
    let mut buffers = vec![OwnedBytes::empty(); byte_ranges.len()];
    // Coalesce the collected ranges, then run at most five downloads concurrently.
    let (requests, download_ranges) = run_cpu_intensive(move || {
        let mut requests: Vec<_> = byte_ranges
            .into_iter()
            .enumerate()
            .filter(|(_, range)| range.start != range.end)
            .collect();
        requests.sort_unstable_by_key(|(_, range)| range.start);
        let download_ranges =
            plan::coalesce_ranges(requests.iter().map(|(_, range)| range.clone()));
        (requests, download_ranges)
    })
    .await
    .map_err(io::Error::other)?;
    let mut downloads = stream::iter(download_ranges)
        .map(|range| {
            let first = requests.partition_point(|(_, requested)| requested.start < range.start);
            let end = requests.partition_point(|(_, requested)| requested.start < range.end);
            read_download_range(storage, path, range, &requests[first..end])
        })
        .buffer_unordered(5);
    while let Some(downloaded) = downloads.try_next().await? {
        for (index, bytes) in downloaded {
            buffers[index] = bytes;
        }
    }
    Ok(buffers)
}

// Requests are sorted, nonempty, and contained in the coalesced download range.
async fn read_download_range(
    storage: &dyn Storage,
    path: &Path,
    range: Range<usize>,
    mut requests: &[(usize, Range<usize>)],
) -> StorageResult<Vec<(usize, OwnedBytes)>> {
    let invalid_length = || {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "download length differs from requested range",
        )
    };
    if let [(index, _)] = requests {
        let bytes = storage.get_slice(path, range.clone()).await?;
        if bytes.len() != range.len() {
            return Err(invalid_length().into());
        }
        return Ok(vec![(*index, bytes)]);
    }
    let reader = storage.get_slice_stream(path, range.clone()).await?;
    let mut reader = tokio::io::BufReader::new(reader);
    let mut buffers = Vec::with_capacity(requests.len());
    let mut position = range.start;
    while !requests.is_empty() {
        // Read overlapping requests together so the stream never needs to seek backwards.
        let mut useful = requests[0].1.clone();
        let mut count = 1;
        while count < requests.len() && requests[count].1.start < useful.end {
            useful.end = useful.end.max(requests[count].1.end);
            count += 1;
        }
        let gap = (useful.start - position) as u64;
        if gap > 0
            && tokio::io::copy_buf(&mut (&mut reader).take(gap), &mut tokio::io::sink()).await?
                != gap
        {
            return Err(invalid_length().into());
        }
        let mut bytes = vec![0; useful.len()];
        reader.read_exact(&mut bytes).await.map_err(|error| {
            if error.kind() == io::ErrorKind::UnexpectedEof {
                invalid_length()
            } else {
                error
            }
        })?;
        let bytes = OwnedBytes::new(bytes);
        for (index, requested) in &requests[..count] {
            let result = if *requested == useful {
                bytes.clone()
            } else {
                // Copy proper subranges so cached results don't retain the entire
                // overlapping-request buffer.
                OwnedBytes::new(
                    bytes[requested.start - useful.start..requested.end - useful.start].to_vec(),
                )
            };
            buffers.push((*index, result));
        }
        position = useful.end;
        requests = &requests[count..];
    }
    debug_assert_eq!(position, range.end);
    if reader.read(&mut [0]).await? != 0 {
        return Err(invalid_length().into());
    }
    Ok(buffers)
}

#[cfg(test)]
mod stream_tests;
#[cfg(test)]
mod tests;
