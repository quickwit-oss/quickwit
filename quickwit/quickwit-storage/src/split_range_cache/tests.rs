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

use std::path::Path;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use feuer::Download;

use super::test_helpers::{Fixture, SPLIT_BYTES, SPLIT_PATH, wait_until};
use super::{FeuerSplitRangeCache, config_for_test};
use crate::StorageErrorKind;

#[tokio::test]
async fn test_split_range_cache_uses_configured_capacities() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = config_for_test(temp_dir.path());
    // Decimal megabytes are not MiB-aligned; Feuer handles alignment internally.
    config.disk_capacity = bytesize::ByteSize::mb(512);
    let cache = FeuerSplitRangeCache::open(&config).await.unwrap();
    assert_eq!(
        cache.cache.config().directory(),
        config.directory().unwrap()
    );
    assert_eq!(cache.cache.config().file_name(), "split_cache");
    assert_eq!(
        cache.cache.config().disk_capacity(),
        config.disk_capacity.as_u64()
    );
    assert_eq!(
        cache.cache.config().memory_capacity(),
        config.memory_capacity.as_u64()
    );
}

#[tokio::test]
async fn test_empty_and_exact_ranges_without_covering_reuse() {
    let fixture = Fixture::new().await;
    let path = Path::new(SPLIT_PATH);
    assert!(
        fixture
            .storage
            .get_slice(path, 4..4)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(fixture.lower_reads(), 0);
    for _ in 0..2 {
        assert_eq!(
            fixture
                .storage
                .get_slice(path, 1..4)
                .await
                .unwrap()
                .as_slice(),
            b"bcd"
        );
    }
    assert_eq!(fixture.lower_reads(), 1);
    assert_eq!(
        fixture
            .storage
            .get_slice(path, 2..3)
            .await
            .unwrap()
            .as_slice(),
        b"c"
    );
    assert_eq!(
        fixture.lower_reads(),
        2,
        "a covering slice does not serve a different exact range"
    );
    assert_eq!(
        fixture
            .storage
            .get_slice(path, 0..5)
            .await
            .unwrap()
            .as_slice(),
        SPLIT_BYTES
    );
    assert_eq!(
        fixture.lower_reads(),
        3,
        "a larger range still needs a download"
    );
    assert_eq!(
        fixture
            .storage
            .get_slice(path, 0..2)
            .await
            .unwrap()
            .as_slice(),
        b"ab"
    );
    assert_eq!(fixture.lower_reads(), 4);
    assert_eq!(
        fixture
            .storage
            .get_slice(path, 1..4)
            .await
            .unwrap()
            .as_slice(),
        b"bcd"
    );
    assert_eq!(
        fixture.lower_reads(),
        4,
        "exact slices remain independent hits"
    );
}

#[tokio::test]
async fn test_download_and_exact_cache_hit_share_the_destination_buffer() {
    let fixture = Fixture::new().await;
    let path = Path::new(SPLIT_PATH);
    let bytes = fixture.storage.get_slice(path, 1..4).await.unwrap();
    let address = fixture.lower.download_address.load(Ordering::Relaxed);
    assert_eq!(address % 4096, 0);
    assert_eq!(bytes.as_slice().as_ptr() as usize, address);
    let hit = fixture.storage.get_slice(path, 1..4).await.unwrap();
    assert_eq!(hit.as_slice(), b"bcd");
    assert_eq!(hit.as_slice().as_ptr() as usize, address);
    assert_eq!(fixture.lower_reads(), 1);
}

#[tokio::test]
async fn test_identical_concurrent_misses_fetch_once() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = ::metrics::set_default_local_recorder(&recorder);
    let fixture = Fixture::new_with_blocked_lower_read().await;
    let path = Path::new(SPLIT_PATH);
    let first = fixture.storage.get_slice(path, 0..4);
    let second = fixture.storage.get_slice(path, 0..4);
    let release = async {
        // Both requests must miss Feuer's disk tier before releasing the source.
        // Debouncing the whole cache lookup would leave this counter at one.
        // metrics-util 0.20 snapshots drain counters. The misses may arrive in
        // separate snapshots, so accumulate them before checking both callers.
        let disk_misses = std::cell::Cell::new(0);
        wait_until(
            || {
                for (key, _, _, value) in snapshotter.snapshot().into_vec() {
                    if key.key().name() == "feuer_disk_lookup_total"
                        && key
                            .key()
                            .labels()
                            .any(|label| label.key() == "outcome" && label.value() == "absent")
                        && let DebugValue::Counter(count) = value
                    {
                        disk_misses.set(disk_misses.get() + count);
                    }
                }
                assert!(disk_misses.get() <= 2, "unexpected extra disk misses");
                disk_misses.get() == 2
            },
            "both Feuer disk misses",
        )
        .await;
        fixture.release_lower_read();
    };
    let (first_result, second_result, _) = tokio::join!(first, second, release);
    assert_eq!(first_result.unwrap(), second_result.unwrap());
    assert_eq!(fixture.lower_reads(), 1);
}

#[tokio::test]
async fn test_remote_error_is_not_cached_or_rewritten() {
    let fixture = Fixture::new().await;
    for _ in 0..2 {
        let error = fixture
            .storage
            .get_slice(Path::new("missing.split"), 0..4)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::NotFound);
    }
    assert_eq!(fixture.lower_reads(), 2);
}

#[tokio::test]
async fn test_writes_are_unsupported() {
    let fixture = Fixture::new().await;
    let path = Path::new(SPLIT_PATH);
    let put_error = fixture
        .storage
        .put(path, Box::new(b"x".to_vec()))
        .await
        .unwrap_err();
    assert_eq!(put_error.kind(), StorageErrorKind::Io);
    assert!(
        put_error
            .to_string()
            .contains("Unsupported operation. FeuerSplitRangeStorage only supports async reads")
    );
    assert_eq!(
        fixture.storage.delete(path).await.unwrap_err().kind(),
        StorageErrorKind::Io
    );
    let bulk_error = fixture.storage.bulk_delete(&[path]).await.unwrap_err();
    assert_eq!(
        bulk_error.error.as_ref().unwrap().kind(),
        StorageErrorKind::Io
    );
}

#[tokio::test]
async fn test_get_all_is_not_cached() {
    let fixture = Fixture::new().await;
    let path = Path::new(SPLIT_PATH);
    assert_eq!(
        fixture.storage.get_all(path).await.unwrap().as_slice(),
        SPLIT_BYTES
    );
    assert_eq!(fixture.lower_reads(), 0);
    fixture.storage.get_slice(path, 0..5).await.unwrap();
    assert_eq!(fixture.lower_reads(), 1);
}

#[tokio::test]
async fn test_initiating_caller_drop_surviving_waiter_succeeds() {
    let fixture = Fixture::new_with_blocked_lower_read().await;
    let path = Path::new(SPLIT_PATH);
    let mut initiating = Box::pin(fixture.storage.get_slice(path, 0..4));
    tokio::select! {
        biased;
        result = &mut initiating => panic!("fetch completed before release: {result:?}"),
        () = fixture.wait_until_lower_read_started() => {}
    }
    // Join the shared download before dropping the initiating caller.
    let mut waiter = Box::pin(fixture.storage.get_slice(path, 0..4));
    assert!(futures::poll!(waiter.as_mut()).is_pending());
    drop(initiating);
    fixture.release_lower_read();
    assert_eq!(waiter.await.unwrap().as_slice(), b"abcd");
    assert_eq!(fixture.lower_reads(), 1);
}

#[tokio::test]
async fn test_waiter_drop_does_not_cancel_fetch() {
    let fixture = Fixture::new_with_blocked_lower_read().await;
    let path = Path::new(SPLIT_PATH);
    let mut initiating = Box::pin(fixture.storage.get_slice(path, 0..4));
    tokio::select! {
        biased;
        result = &mut initiating => panic!("fetch completed before release: {result:?}"),
        () = fixture.wait_until_lower_read_started() => {}
    }
    let mut waiter = Box::pin(fixture.storage.get_slice(path, 0..4));
    for _ in 0..16 {
        tokio::select! {
            biased;
            result = &mut waiter => panic!("waiter completed before release: {result:?}"),
            () = tokio::task::yield_now() => {}
        }
    }
    drop(waiter);
    fixture.release_lower_read();
    assert_eq!(initiating.await.unwrap().as_slice(), b"abcd");
    assert_eq!(fixture.lower_reads(), 1);
}

#[tokio::test]
async fn test_last_caller_dropped_cancels_download_and_next_request_retries() {
    let fixture = Fixture::new_with_blocked_lower_read().await;
    let path = Path::new(SPLIT_PATH);
    let mut initiating = Box::pin(fixture.storage.get_slice(path, 0..4));
    tokio::select! {
        biased;
        result = &mut initiating => panic!("fetch completed before release: {result:?}"),
        () = fixture.wait_until_lower_read_started() => {}
    }
    let mut waiter = Box::pin(fixture.storage.get_slice(path, 0..4));
    assert!(futures::poll!(waiter.as_mut()).is_pending());
    assert_eq!(fixture.lower_reads(), 1);
    // One receiver belongs to LowerProbe; the other to the shared source future.
    assert_eq!(fixture.gate_tx.receiver_count(), 2);
    drop(initiating);
    assert_eq!(fixture.gate_tx.receiver_count(), 2);
    drop(waiter);
    assert_eq!(
        fixture.gate_tx.receiver_count(),
        1,
        "source future must be dropped"
    );
    assert_eq!(fixture.lower.get_slice_completed.load(Ordering::Relaxed), 0);

    fixture.release_lower_read();
    assert_eq!(
        fixture
            .storage
            .get_slice(path, 0..4)
            .await
            .unwrap()
            .as_slice(),
        b"abcd"
    );
    assert_eq!(
        fixture.lower_reads(),
        2,
        "canceled download must not populate the cache"
    );
    assert_eq!(fixture.lower.get_slice_completed.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn test_oversized_value_is_returned_and_cached() {
    let payload = vec![7u8; 3 * 1024 * 1024];
    let fixture = Fixture::with_payload(&payload, true).await;
    let path = Path::new(SPLIT_PATH);
    for _ in 0..2 {
        assert_eq!(
            fixture
                .storage
                .get_slice(path, 0..payload.len())
                .await
                .unwrap()
                .as_slice(),
            payload.as_slice()
        );
    }
    assert_eq!(fixture.lower_reads(), 1);
}

#[tokio::test]
async fn test_invalid_download_is_an_error_and_is_not_cached() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache = FeuerSplitRangeCache::open(&config_for_test(temp_dir.path()))
        .await
        .unwrap();
    for bytes in [Bytes::new(), Bytes::from_static(b"ab")] {
        let error = cache
            .get_or_fetch("object".to_owned(), 0..4, |_| async move {
                Ok(Download::new(bytes))
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::Internal);
    }
    let bytes = cache
        .get_or_fetch("object".to_owned(), 0..4, |_| async {
            Ok(Download::new(Bytes::from_static(b"abcd")))
        })
        .await
        .unwrap();
    assert_eq!(bytes.as_ref(), b"abcd");
}

#[tokio::test]
async fn test_object_keys_do_not_alias() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache = FeuerSplitRangeCache::open(&config_for_test(temp_dir.path()))
        .await
        .unwrap();
    for (uri, expected) in [
        ("s3://first/a.split", b"abcd"),
        ("s3://second/a.split", b"efgh"),
    ] {
        let bytes = cache
            .get_or_fetch(uri.to_owned(), 0..4, move |_| async move {
                Ok(Download::new(Bytes::from_static(expected)))
            })
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), expected);
    }
}
