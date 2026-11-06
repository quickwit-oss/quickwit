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

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use quickwit_config::StorageTimeoutPolicy;
use tokio::io::{AsyncWriteExt, ReadBuf};
use tokio_util::io::StreamReader;

use super::*;
use crate::{
    MockStorage, MockStorageCache, StorageErrorKind, TimeoutAndRetryStorage,
    wrap_storage_with_cache,
};

#[tokio::test]
async fn test_plans_collected_ranges_and_preserves_order() {
    const MIB: usize = 1024 * 1024;
    let mut storage = MockStorage::default();
    let unmerged_bytes = OwnedBytes::new(vec![0, 1]);
    let unmerged_address = unmerged_bytes.as_ptr();
    storage
        .expect_get_slice()
        .times(1)
        .return_once(move |_, range| {
            assert_eq!(range, 10 * MIB..10 * MIB + 2);
            Ok(unmerged_bytes)
        });
    let largest_gap_read = Arc::new(AtomicUsize::new(0));
    let gap_read_for_storage = largest_gap_read.clone();
    storage
        .expect_get_slice_stream()
        .times(2)
        .returning(move |path, range| {
            assert_eq!(path, Path::new("file"));
            assert!([0..300_011, 5 * MIB..5 * MIB + 2].contains(&range));
            Ok(Box::new(GeneratedReader {
                position: range.start,
                end: range.end,
                largest_gap_read: gap_read_for_storage.clone(),
            }))
        });
    assert!(
        read_collected_ranges(&storage, Path::new("file"), Vec::new())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        read_collected_ranges(&storage, Path::new("file"), vec![0..0, 9..9])
            .await
            .unwrap()
            .iter()
            .all(|bytes| bytes.is_empty())
    );
    let mut ranges: Vec<_> = [5 * MIB..5 * MIB + 2, 0..2, 1..3, 4..4, 5 * MIB..5 * MIB + 2]
        .into_iter()
        .cycle()
        .take(10_000)
        .collect();
    ranges.extend([
        1000..8000,
        4..16_393,
        4..16_393,
        300_000..300_011,
        10 * MIB..10 * MIB + 2,
    ]);
    let buffers = read_collected_ranges(&storage, Path::new("file"), ranges.clone())
        .await
        .unwrap();
    assert_eq!(buffers.last().unwrap().as_ptr(), unmerged_address);
    let overlapping = &buffers[10_000..10_003];
    assert_eq!(overlapping[1].as_ptr(), overlapping[2].as_ptr());
    // Proper subranges own their bytes rather than retaining the overlapping buffer.
    assert_ne!(
        overlapping[0].as_ptr(),
        overlapping[1].as_ptr().wrapping_add(1000 - 4)
    );
    assert!((1..=64 * 1024).contains(&largest_gap_read.load(Ordering::Relaxed)));
    assert_eq!(buffers.len(), ranges.len());
    for (range, bytes) in ranges.into_iter().zip(buffers) {
        assert_eq!(
            bytes.as_slice(),
            range.map(|offset| offset as u8).collect::<Vec<_>>()
        );
    }
}

// Generate chunks without allocating the full response, including the gap.
struct GeneratedReader {
    position: usize,
    end: usize,
    largest_gap_read: Arc<AtomicUsize>,
}

impl AsyncRead for GeneratedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if (16_393..300_000).contains(&self.position) {
            self.largest_gap_read
                .fetch_max(buffer.remaining(), Ordering::Relaxed);
        }
        let count = buffer.remaining().min(self.end - self.position).min(4093);
        for _ in 0..count {
            buffer.put_slice(&[self.position as u8]);
            self.position += 1;
        }
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn test_collected_batch_limits_streams_and_drops_them_on_cancellation() {
    let writers = Arc::new(Mutex::new(Vec::new()));
    let writers_for_storage = writers.clone();
    let mut storage = MockStorage::default();
    storage
        .expect_get_slice_stream()
        .times(5)
        .returning(move |_, _| {
            let (reader, writer) = tokio::io::duplex(1);
            writers_for_storage.lock().unwrap().push(writer);
            Ok(Box::new(reader))
        });
    let batch = tokio::spawn(async move {
        let ranges = (0..6)
            .flat_map(|index| {
                let start = index * 5 * 1024 * 1024;
                [start..start + 1, start + 2..start + 3]
            })
            .collect();
        read_collected_ranges(&storage, Path::new("file"), ranges).await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while writers.lock().unwrap().len() < 5 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    batch.abort();
    assert!(batch.await.unwrap_err().is_cancelled());
    let writers = std::mem::take(&mut *writers.lock().unwrap());
    assert_eq!(writers.len(), 5);
    for mut writer in writers {
        assert_eq!(
            writer.write_all(&[0]).await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }
}

#[tokio::test]
async fn test_one_download_failure_fails_all_callers_and_cancels_peers() {
    const MIB: usize = 1024 * 1024;
    let (fail_download, wait_for_failure) = tokio::sync::oneshot::channel();
    let mut wait_for_failure = Some(wait_for_failure);
    let (pending_reader, mut pending_writer) = tokio::io::duplex(1);
    let mut pending_reader = Some(pending_reader);
    let opened_downloads = Arc::new(AtomicUsize::new(0));
    let opened_for_storage = opened_downloads.clone();
    let mut storage = MockStorage::default();
    storage
        .expect_get_slice_stream()
        .times(3)
        .returning(move |_, range| {
            opened_for_storage.fetch_add(1, Ordering::Relaxed);
            assert_eq!(range.len(), 3);
            match range.start {
                0 => Ok(Box::new(&b"abc"[..])),
                start if start == 5 * MIB => {
                    let wait_for_failure = wait_for_failure.take().unwrap();
                    let failure = stream::once(async move {
                        wait_for_failure.await.unwrap();
                        Err::<Bytes, _>(io::Error::new(
                            io::ErrorKind::ConnectionReset,
                            "download failed",
                        ))
                    });
                    Ok(Box::new(StreamReader::new(Box::pin(failure))))
                }
                start if start == 10 * MIB => Ok(Box::new(pending_reader.take().unwrap())),
                _ => panic!("unexpected download range: {range:?}"),
            }
        });
    let mut cache = MockStorageCache::default();
    cache.expect_get().times(6).return_const(None);
    cache.expect_put().times(0);
    let storage = wrap_storage_with_cache(
        Arc::new(cache),
        Arc::new(CoalescingStorage::new(Arc::new(storage))),
    );
    let ranges = (0..3).flat_map(|index| {
        let start = index * 5 * MIB;
        [start..start + 1, start + 2..start + 3]
    });
    let mut reads = Box::pin(futures::future::join_all(
        ranges.map(|range| storage.get_slice(Path::new("file"), range)),
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        while opened_downloads.load(Ordering::Relaxed) < 3 {
            assert!(futures::poll!(reads.as_mut()).is_pending());
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fail_download.send(()).unwrap();
    for result in reads.await {
        let error = result.unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::Io);
        assert!(error.to_string().contains("download failed"));
        assert_eq!(
            io::Error::from(error).kind(),
            io::ErrorKind::ConnectionReset
        );
    }
    assert_eq!(
        pending_writer.write_all(&[0]).await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[tokio::test(start_paused = true)]
async fn test_merged_download_retries_body_from_emitted_offset() {
    let mut storage = MockStorage::default();
    let mut sequence = mockall::Sequence::new();
    storage
        .expect_get_slice_stream()
        .withf(|path, range| path == Path::new("file") && *range == (10..18))
        .times(1)
        .in_sequence(&mut sequence)
        .returning(|_, _| {
            let chunks = stream::iter([&b"abcd"[..], &b"wrong"[..]])
                .enumerate()
                .then(|(index, bytes)| async move {
                    if index > 0 {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                    }
                    Ok::<_, io::Error>(Bytes::from_static(bytes))
                });
            Ok(Box::new(StreamReader::new(Box::pin(chunks))))
        });
    storage
        .expect_get_slice_stream()
        .withf(|_, range| *range == (14..18))
        .times(1)
        .in_sequence(&mut sequence)
        .returning(|_, _| Ok(Box::new(&b"efgh"[..])));
    let retrying_storage = TimeoutAndRetryStorage::new(
        Arc::new(storage),
        StorageTimeoutPolicy {
            min_throughtput_bytes_per_secs: 0,
            timeout_millis: 2_000,
            max_num_retries: 1,
        },
    );
    let storage = CoalescingStorage::new(Arc::new(retrying_storage));
    let (first, second) = tokio::try_join!(
        storage.get_slice(Path::new("file"), 10..12),
        storage.get_slice(Path::new("file"), 16..18)
    )
    .unwrap();
    assert_eq!(first.as_slice(), b"ab");
    assert_eq!(second.as_slice(), b"gh");
}

#[tokio::test]
async fn test_singleton_length_mismatch() {
    for length in [1, 3] {
        let mut storage = MockStorage::default();
        storage
            .expect_get_slice()
            .times(1)
            .return_once(move |_, _| Ok(OwnedBytes::new(vec![0; length])));
        let storage = CoalescingStorage::new(Arc::new(storage));
        let error = storage
            .get_slice(Path::new("file"), 0..2)
            .await
            .unwrap_err();
        assert_eq!(io::Error::from(error).kind(), io::ErrorKind::InvalidData);
    }
}

#[tokio::test(start_paused = true)]
async fn test_stream_calls_bypass_collection() {
    let mut storage = MockStorage::default();
    storage
        .expect_get_slice_stream()
        .times(1)
        .returning(|_, range| {
            assert_eq!(range, 0..2);
            Ok(Box::new(&b"ab"[..]))
        });
    let storage = CoalescingStorage::new(Arc::new(storage));
    let read_start = tokio::time::Instant::now();
    let mut reader = storage
        .get_slice_stream(Path::new("file"), 0..2)
        .await
        .unwrap();
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"ab");
    assert_eq!(read_start.elapsed(), Duration::ZERO);
    assert!(storage.batches_by_object.lock().unwrap().is_empty());
}

#[test]
fn test_io_error_kind_survives_storage_boundary() {
    for kind in [
        io::ErrorKind::InvalidData,
        io::ErrorKind::ConnectionReset,
        io::ErrorKind::TimedOut,
    ] {
        let error = crate::StorageErrorKind::Io.with_error(
            anyhow::Error::new(io::Error::new(kind, "body failed")).context("coalesced download"),
        );
        let error = io::Error::from(error);
        assert_eq!(error.kind(), kind);
        assert!(error.to_string().contains("coalesced download"));
    }
}
