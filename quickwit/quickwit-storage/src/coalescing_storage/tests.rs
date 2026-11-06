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

use std::sync::atomic::{AtomicBool, Ordering};

use futures::poll;
use tokio::io::AsyncWriteExt;

use super::*;
use crate::{MockStorage, StorageErrorKind};

#[tokio::test(start_paused = true)]
async fn test_collects_by_object_and_collector() {
    let mut storage = MockStorage::default();
    storage
        .expect_get_slice_stream()
        .times(3)
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
    let storage = Arc::new(storage);
    let collectors = [
        CoalescingStorage::new(storage.clone()),
        CoalescingStorage::new(storage),
    ];
    let requests = [
        (0, "file", 0..4),
        (0, "file", 60..64),
        (0, "file", 0..4),
        (0, "file", 20..24),
        (0, "file", 4..4),
        (0, "other", 0..4),
        (0, "other", 8..12),
        (1, "file", 0..4),
        (1, "file", 60..64),
    ];
    let (buffers, batch) = tokio::try_join!(
        futures::future::try_join_all(requests.iter().map(|(collector, path, range)| {
            collectors[*collector].get_slice(Path::new(path), range.clone())
        })),
        collectors[0].get_slices(Path::new("file"), vec![8..10, 4..4, 0..4, 8..10]),
    )
    .unwrap();
    assert_eq!(
        batch.iter().map(OwnedBytes::as_slice).collect::<Vec<_>>(),
        vec![&[8, 9][..], &[], &[0, 1, 2, 3], &[8, 9]]
    );
    assert_eq!(batch[0].as_ptr(), batch[3].as_ptr());
    assert_eq!(batch[2].as_ptr(), buffers[0].as_ptr());
    assert_eq!(buffers[0].as_ptr(), buffers[2].as_ptr());
    assert_ne!(buffers[0].as_ptr(), buffers[7].as_ptr());
    for ((_, path, range), buffer) in requests.into_iter().zip(buffers) {
        let byte_value_offset = if path == "file" { 0 } else { 128 };
        assert_eq!(
            buffer.as_slice(),
            range
                .map(|position| position as u8 + byte_value_offset)
                .collect::<Vec<_>>()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn test_singleton_waits_and_preserves_allocation() {
    let bytes = OwnedBytes::new(vec![5, 6, 7, 8]);
    let address = bytes.as_ptr();
    let mut storage = MockStorage::default();
    storage
        .expect_get_slice()
        .times(1)
        .return_once(move |_, range| {
            assert_eq!(range, 5..9);
            Ok(bytes)
        });
    let storage = CoalescingStorage::new(Arc::new(storage));
    let read_start = tokio::time::Instant::now();
    for ranges in [Vec::new(), vec![2..2, usize::MAX..usize::MAX]] {
        let expected_len = ranges.len();
        let buffers = storage.get_slices(Path::new("file"), ranges).await.unwrap();
        assert_eq!(buffers.len(), expected_len);
        assert!(buffers.iter().all(|bytes| bytes.is_empty()));
    }
    assert_eq!(read_start.elapsed(), Duration::ZERO);
    let reversed_range = Range { start: 3, end: 2 };
    let error = storage
        .get_slices(Path::new("file"), vec![0..1, reversed_range])
        .await
        .unwrap_err();
    assert_eq!(io::Error::from(error).kind(), io::ErrorKind::InvalidInput);
    assert!(storage.batches_by_object.lock().unwrap().is_empty());
    let bytes = storage.get_slice(Path::new("file"), 5..9).await.unwrap();
    assert!(read_start.elapsed() >= COLLECTION_DURATION);
    assert_eq!(bytes.as_ptr(), address);
    assert_eq!(bytes.as_slice(), &[5, 6, 7, 8]);
}

#[tokio::test]
async fn test_later_read_starts_another_batch_after_caller_cancellation() {
    let download_writer = Arc::new(Mutex::new(None));
    let writer_for_storage = download_writer.clone();
    let mut storage = MockStorage::default();
    storage
        .expect_get_slice_stream()
        .times(1)
        .return_once(move |_, range| {
            assert_eq!(range, 0..10);
            let (reader, writer) = tokio::io::duplex(64);
            *writer_for_storage.lock().unwrap() = Some(writer);
            Ok(Box::new(reader))
        });
    storage.expect_get_slice().times(1).returning(|_, range| {
        // These bytes were a gap in the first batch's plan.
        assert_eq!(range, 4..6);
        Ok(OwnedBytes::new(vec![4, 5]))
    });
    let storage = CoalescingStorage::new(Arc::new(storage));
    let mut first_read = Box::pin(storage.get_slice(Path::new("file"), 0..2));
    let mut second_read = Box::pin(storage.get_slice(Path::new("file"), 8..10));
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
    let later_bytes = storage.get_slice(Path::new("file"), 4..6).await.unwrap();
    assert_eq!(later_bytes.as_slice(), &[4, 5]);
    let mut writer = download_writer.lock().unwrap().take().unwrap();
    writer
        .write_all(&(0..10).collect::<Vec<u8>>())
        .await
        .unwrap();
    writer.shutdown().await.unwrap();
    assert_eq!(second_read.await.unwrap().as_slice(), &[8, 9]);
}

#[tokio::test(start_paused = true)]
async fn test_collection_deadline_and_cancelling_all_callers_during_download() {
    let (reader, mut writer) = tokio::io::duplex(16);
    let download_started = Arc::new(AtomicBool::new(false));
    let download_started_for_storage = download_started.clone();
    let mut storage = MockStorage::default();
    storage
        .expect_get_slice_stream()
        .times(1)
        .return_once(move |_, range| {
            assert_eq!(range, 0..4);
            download_started_for_storage.store(true, Ordering::Relaxed);
            Ok(Box::new(reader))
        });
    let storage = CoalescingStorage::new(Arc::new(storage));
    let mut first_read = Box::pin(storage.get_slices(Path::new("file"), vec![0..1, 1..2]));
    assert!(poll!(first_read.as_mut()).is_pending());
    tokio::time::advance(COLLECTION_DURATION / 2).await;
    let mut second_read = Box::pin(storage.get_slice(Path::new("file"), 3..4));
    assert!(poll!(second_read.as_mut()).is_pending());
    tokio::time::advance(COLLECTION_DURATION / 2).await;
    assert!(poll!(first_read.as_mut()).is_pending());
    // Collection ends at the first caller's deadline.
    assert!(
        storage.batches_by_object.lock().unwrap()[Path::new("file")]
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
        storage.batches_by_object.lock().unwrap()[Path::new("file")]
            .upgrade()
            .is_some()
    );
    drop(second_read);
    assert!(
        storage.batches_by_object.lock().unwrap()[Path::new("file")]
            .upgrade()
            .is_none()
    );
    assert_eq!(
        writer.write_all(&[0]).await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
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
    let storage = CoalescingStorage::new(Arc::new(storage));
    let mut read_to_cancel = Box::pin(storage.get_slices(Path::new("file"), vec![0..1, 99..100]));
    assert!(poll!(read_to_cancel.as_mut()).is_pending());
    drop(read_to_cancel);
    assert!(
        storage.batches_by_object.lock().unwrap()[Path::new("file")]
            .upgrade()
            .is_none()
    );
    let mut read_to_cancel = Box::pin(storage.get_slices(Path::new("file"), vec![4..5, 5..6]));
    let mut remaining_read = Box::pin(storage.get_slice(Path::new("file"), 8..10));
    assert!(poll!(read_to_cancel.as_mut()).is_pending());
    assert!(poll!(remaining_read.as_mut()).is_pending());
    drop(read_to_cancel);
    assert_eq!(remaining_read.await.unwrap().as_slice(), &[8, 9]);
}

#[tokio::test]
async fn test_open_error_metadata_is_shared() {
    let mut storage = MockStorage::default();
    storage
        .expect_get_slice_stream()
        .times(1)
        .returning(|_, _| {
            Err(StorageErrorKind::Unauthorized
                .with_error(anyhow::anyhow!("credentials expired"))
                .with_retry_after(Some(Duration::from_secs(2))))
        });
    let storage = CoalescingStorage::new(Arc::new(storage));
    let (first, second) = tokio::join!(
        storage.get_slice(Path::new("file"), 0..1),
        storage.get_slice(Path::new("file"), 3..4)
    );
    for error in [first.unwrap_err(), second.unwrap_err()] {
        assert_eq!(error.kind(), StorageErrorKind::Unauthorized);
        assert_eq!(error.retry_after, Some(Duration::from_secs(2)));
        assert!(error.to_string().contains("credentials expired"));
    }
}
