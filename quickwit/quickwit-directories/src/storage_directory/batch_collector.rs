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
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use tantivy::directory::OwnedBytes;

use super::StorageDirectoryFileHandle;

// Collection ends after this duration, measured from the first caller.
const COLLECTION_DURATION: Duration = Duration::from_millis(1);

type BatchReadFuture = Shared<BoxFuture<'static, Result<Arc<Vec<OwnedBytes>>, Arc<io::Error>>>>;

struct CollectedBatch {
    // Taking these ranges ends collection. Later callers start another batch because
    // the stream may have discarded bytes they need.
    ranges_to_read: Mutex<Option<Vec<Range<usize>>>>,
    read_future: BatchReadFuture,
}

impl CollectedBatch {
    fn new(
        file_handle: StorageDirectoryFileHandle,
        requested_ranges: Vec<Range<usize>>,
    ) -> Arc<Self> {
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
                file_handle
                    .read_collected_ranges(collected_ranges)
                    .await
                    .map(Arc::new)
                    .map_err(Arc::new)
            }
            .boxed()
            .shared();
            Self {
                ranges_to_read: Mutex::new(Some(requested_ranges)),
                read_future,
            }
        })
    }
}

/// Collects callers' batches by file.
/// Callers own the batches. The file map holds weak references.
#[derive(Default)]
pub(super) struct BatchCollector {
    batches_by_file: Mutex<HashMap<PathBuf, Weak<CollectedBatch>>>,
}

impl BatchCollector {
    pub(super) async fn read_batch(
        &self,
        file_handle: StorageDirectoryFileHandle,
        requested_ranges: Vec<Range<usize>>,
    ) -> io::Result<Vec<OwnedBytes>> {
        let (batch, buffer_indices) = self.collect_batch(file_handle, requested_ranges);
        let buffers = batch
            .read_future
            .clone()
            .await
            .map_err(|error| io::Error::new(error.kind(), error))?;
        Ok(buffers[buffer_indices].to_vec())
    }

    fn collect_batch(
        &self,
        file_handle: StorageDirectoryFileHandle,
        requested_ranges: Vec<Range<usize>>,
    ) -> (Arc<CollectedBatch>, Range<usize>) {
        let mut batches_by_file = self.batches_by_file.lock().unwrap();
        if let Some(batch) = batches_by_file
            .get(&file_handle.path)
            .and_then(Weak::upgrade)
        {
            let mut ranges_to_read = batch.ranges_to_read.lock().unwrap();
            if let Some(ranges_to_read) = ranges_to_read.as_mut() {
                let num_collected_ranges = ranges_to_read.len();
                ranges_to_read.extend(requested_ranges);
                return (batch.clone(), num_collected_ranges..ranges_to_read.len());
            }
        }
        let buffer_indices = 0..requested_ranges.len();
        let path = file_handle.path.clone();
        let batch = CollectedBatch::new(file_handle, requested_ranges);
        batches_by_file.insert(path, Arc::downgrade(&batch));
        (batch, buffer_indices)
    }
}

#[cfg(test)]
#[allow(clippy::single_range_in_vec_init)] // A batch can contain a single byte range.
mod tests {
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};

    use futures::poll;
    use quickwit_storage::MockStorage;
    use tantivy::Directory;
    use tokio::io::AsyncWriteExt;

    use super::*;
    use crate::{CachingDirectory, StorageDirectory};

    #[tokio::test(start_paused = true)]
    async fn test_collects_batches_by_file_across_handles() {
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice_stream()
            .times(2)
            .returning(|path, range| {
                let byte_value_offset = if path == Path::new("file") {
                    assert_eq!(range, 0..64);
                    0
                } else {
                    assert_eq!(path, Path::new("other"));
                    assert_eq!(range, 0..12);
                    128
                };
                Ok(Box::new(std::io::Cursor::new(
                    range
                        .map(|position| position as u8 + byte_value_offset)
                        .collect::<Vec<_>>(),
                )))
            });
        let directory = StorageDirectory::new(Arc::new(storage));
        let directory = CachingDirectory::new_unbounded(Arc::new(directory));
        let batches = [
            vec![0..4, 60..64],
            vec![0..4, 20..24, 60..64],
            vec![40..44, 0..4, 4..4],
            vec![20..24, 0..4, 20..24],
            vec![0..4, 4..8],
            vec![0..4, 8..12],
        ];
        let file_handles: Vec<_> = (0..batches.len())
            .map(|batch_index| {
                let path = if batch_index == 5 { "other" } else { "file" };
                directory.clone().get_file_handle(Path::new(path)).unwrap()
            })
            .collect();
        let read_start = tokio::time::Instant::now();
        let buffers_by_batch =
            futures::future::try_join_all(file_handles.iter().zip(&batches).map(
                |(file_handle, requested_ranges)| {
                    file_handle.read_bytes_batch_async(requested_ranges.clone())
                },
            ))
            .await
            .unwrap();
        assert!(read_start.elapsed() >= COLLECTION_DURATION);
        assert_eq!(
            buffers_by_batch[0][0].as_ptr(),
            buffers_by_batch[1][0].as_ptr()
        );
        for (batch_index, ((file_handle, requested_ranges), buffers)) in file_handles
            .iter()
            .zip(batches)
            .zip(buffers_by_batch)
            .enumerate()
        {
            let cached_buffers = file_handle
                .read_bytes_batch_async(requested_ranges.clone())
                .await
                .unwrap();
            assert_eq!(buffers.len(), requested_ranges.len());
            let byte_value_offset = if batch_index == 5 { 128 } else { 0 };
            for ((range, bytes), cached_bytes) in requested_ranges
                .into_iter()
                .zip(buffers)
                .zip(cached_buffers)
            {
                assert_eq!(
                    bytes.as_slice(),
                    range
                        .map(|position| position as u8 + byte_value_offset)
                        .collect::<Vec<_>>()
                );
                assert_eq!(bytes.as_slice(), cached_bytes.as_slice());
            }
        }
    }

    #[tokio::test]
    async fn test_later_batch_reads_separately_after_caller_cancellation() {
        let download_writer = Arc::new(Mutex::new(None));
        let writer_for_storage = download_writer.clone();
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice_stream()
            .times(1)
            .return_once(move |_, range| {
                assert_eq!(range, 0..10);
                let (download_reader, download_writer) = tokio::io::duplex(64);
                *writer_for_storage.lock().unwrap() = Some(download_writer);
                Ok(Box::new(download_reader))
            });
        storage.expect_get_slice().times(1).returning(|_, range| {
            // These bytes were a gap in the first batch's plan.
            assert_eq!(range, 4..6);
            Ok(OwnedBytes::new(vec![4, 5]))
        });
        let directory = StorageDirectory::new(Arc::new(storage));
        let file_handle = directory.get_file_handle(Path::new("file")).unwrap();
        let second_handle = directory.get_file_handle(Path::new("file")).unwrap();
        let mut first_read = Box::pin(file_handle.read_bytes_batch_async(vec![0..2, 8..10]));
        let mut second_read = Box::pin(second_handle.read_bytes_batch_async(vec![0..2, 8..10]));
        assert!(poll!(first_read.as_mut()).is_pending());
        assert!(poll!(second_read.as_mut()).is_pending());
        tokio::time::timeout(Duration::from_secs(5), async {
            while download_writer.lock().unwrap().is_none() {
                assert!(poll!(first_read.as_mut()).is_pending());
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(first_read);
        let later_buffers = file_handle
            .read_bytes_batch_async(vec![4..6])
            .await
            .unwrap();
        assert_eq!(later_buffers[0].as_slice(), &[4, 5]);
        let mut download_writer = download_writer.lock().unwrap().take().unwrap();
        download_writer
            .write_all(&(0..10).collect::<Vec<u8>>())
            .await
            .unwrap();
        download_writer.shutdown().await.unwrap();
        let buffers = second_read.await.unwrap();
        assert_eq!(buffers[0].as_slice(), &[0, 1]);
        assert_eq!(buffers[1].as_slice(), &[8, 9]);
    }

    #[tokio::test(start_paused = true)]
    async fn test_collection_deadline_and_cancelling_all_callers_during_download() {
        let (download_reader, mut download_writer) = tokio::io::duplex(16);
        let download_started = Arc::new(AtomicBool::new(false));
        let download_started_for_storage = download_started.clone();
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice_stream()
            .times(1)
            .return_once(move |_, range| {
                assert_eq!(range, 0..4);
                download_started_for_storage.store(true, Ordering::Relaxed);
                Ok(Box::new(download_reader))
            });
        let directory = StorageDirectory::new(Arc::new(storage));
        let file_handle = directory.get_file_handle(Path::new("file")).unwrap();
        let mut first_read = Box::pin(file_handle.read_bytes_batch_async(vec![0..1]));
        assert!(poll!(first_read.as_mut()).is_pending());
        tokio::time::advance(COLLECTION_DURATION / 2).await;
        let mut second_read = Box::pin(file_handle.read_bytes_batch_async(vec![3..4]));
        assert!(poll!(second_read.as_mut()).is_pending());
        tokio::time::advance(COLLECTION_DURATION / 2).await;
        assert!(poll!(first_read.as_mut()).is_pending());
        // Collection ends at the first caller's deadline.
        assert!(
            directory.batch_collector.batches_by_file.lock().unwrap()[Path::new("file")]
                .upgrade()
                .unwrap()
                .ranges_to_read
                .lock()
                .unwrap()
                .is_none()
        );
        while !download_started.load(Ordering::Relaxed) {
            assert!(poll!(first_read.as_mut()).is_pending());
            tokio::task::yield_now().await;
        }
        drop(first_read);
        assert!(
            directory.batch_collector.batches_by_file.lock().unwrap()[Path::new("file")]
                .upgrade()
                .is_some()
        );
        drop(second_read);
        assert!(
            directory.batch_collector.batches_by_file.lock().unwrap()[Path::new("file")]
                .upgrade()
                .is_none()
        );
        assert_eq!(
            download_writer.write_all(&[0]).await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_collects_batches_per_directory() {
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice_stream()
            .times(2)
            .returning(|_, range| {
                assert_eq!(range, 0..3);
                Ok(Box::new(std::io::Cursor::new(vec![0, 1, 2])))
            });
        let storage = Arc::new(storage);
        let first_handle = StorageDirectory::new(storage.clone())
            .get_file_handle(Path::new("file"))
            .unwrap();
        let second_handle = StorageDirectory::new(storage)
            .get_file_handle(Path::new("file"))
            .unwrap();
        let (first_buffers, second_buffers) = tokio::try_join!(
            first_handle.read_bytes_batch_async(vec![0..1, 2..3]),
            second_handle.read_bytes_batch_async(vec![0..1, 2..3]),
        )
        .unwrap();
        for buffers in [first_buffers, second_buffers] {
            assert_eq!(buffers[0].as_slice(), &[0]);
            assert_eq!(buffers[1].as_slice(), &[2]);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_cancellation_during_collection_releases_batch() {
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice_stream()
            .times(1)
            .returning(|_, range| {
                assert_eq!(range, 4..10);
                Ok(Box::new(std::io::Cursor::new(vec![4, 5, 6, 7, 8, 9])))
            });
        let directory = StorageDirectory::new(Arc::new(storage));
        let file_handle = directory.get_file_handle(Path::new("file")).unwrap();
        let mut read_to_cancel = Box::pin(file_handle.read_bytes_batch_async(vec![0..100]));
        assert!(poll!(read_to_cancel.as_mut()).is_pending());
        drop(read_to_cancel);
        assert!(
            directory.batch_collector.batches_by_file.lock().unwrap()[Path::new("file")]
                .upgrade()
                .is_none()
        );
        let mut read_to_cancel = Box::pin(file_handle.read_bytes_batch_async(vec![4..6]));
        let mut remaining_read = Box::pin(file_handle.read_bytes_batch_async(vec![8..10]));
        assert!(poll!(read_to_cancel.as_mut()).is_pending());
        assert!(poll!(remaining_read.as_mut()).is_pending());
        drop(read_to_cancel);
        let buffers = remaining_read.await.unwrap();
        assert_eq!(buffers.len(), 1);
        assert_eq!(buffers[0].as_slice(), &[8, 9]);
    }
}
