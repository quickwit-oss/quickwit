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

use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use feuer::Download;
use tokio::sync::watch;

use super::coalescer::DownloadCoalescer;
use crate::{StorageErrorKind, StorageResult};

const MIB: usize = 1024 * 1024;

#[derive(Clone, Default)]
struct FetchProbe {
    reads: Arc<Mutex<Vec<Range<usize>>>>,
}

impl FetchProbe {
    async fn fetch(self, range: Range<usize>) -> StorageResult<Download> {
        self.reads.lock().unwrap().push(range.clone());
        Ok(Download::new(Bytes::from(vec![7; range.len()])))
    }

    fn ranges(&self) -> Vec<Range<usize>> {
        self.reads.lock().unwrap().clone()
    }
}

#[tokio::test(start_paused = true)]
async fn test_nearby_ranges_coalesce_in_either_order() {
    for reverse in [false, true] {
        let coalescer = DownloadCoalescer::default();
        let probe = FetchProbe::default();
        let mut ranges = [10..20, MIB + 20..MIB + 30];
        if reverse {
            ranges.reverse();
        }
        let first_probe = probe.clone();
        let first = coalescer.get_or_create("object".into(), ranges[0].clone(), move |range| {
            first_probe.fetch(range)
        });
        let second = coalescer.get_or_create("object".into(), ranges[1].clone(), |_| async {
            panic!("the second callback should not run")
        });
        let (first, second) = tokio::join!(first, second);
        for (range, download) in [first.unwrap(), second.unwrap()] {
            assert_eq!(range, 10..MIB + 30);
            assert_eq!(download.bytes().len(), MIB + 20);
        }
        assert_eq!(probe.ranges(), vec![10..MIB + 30]);
    }
}

#[tokio::test(start_paused = true)]
async fn test_distant_ranges_and_distinct_objects_do_not_coalesce() {
    for (second_key, second_range) in [("object", MIB + 11..MIB + 21), ("other", 11..21)] {
        let coalescer = DownloadCoalescer::default();
        let probe = FetchProbe::default();
        let first_probe = probe.clone();
        let second_probe = probe.clone();
        let first = coalescer.get_or_create("object".into(), 0..10, move |range| {
            first_probe.fetch(range)
        });
        let second =
            coalescer.get_or_create(second_key.into(), second_range.clone(), move |range| {
                second_probe.fetch(range)
            });
        let (first, second) = tokio::join!(first, second);
        first.unwrap();
        second.unwrap();
        let mut ranges = probe.ranges();
        ranges.sort_by_key(|range| range.start);
        assert_eq!(ranges, vec![0..10, second_range]);
    }
}

#[tokio::test(start_paused = true)]
async fn test_merged_reads_are_bounded_but_large_individual_reads_work() {
    let coalescer = DownloadCoalescer::default();
    let probe = FetchProbe::default();
    let mut requests = Vec::new();
    // Adjacent reads would form a 9 MiB range without the limit.
    for range in [0..4 * MIB, 4 * MIB..8 * MIB, 8 * MIB..9 * MIB] {
        let probe = probe.clone();
        requests
            .push(coalescer.get_or_create("object".into(), range, move |range| probe.fetch(range)));
    }
    for result in futures::future::join_all(requests).await {
        result.unwrap();
    }
    let mut ranges = probe.ranges();
    ranges.sort_by_key(|range| range.start);
    assert_eq!(ranges, vec![0..8 * MIB, 8 * MIB..9 * MIB]);
    let (range, download) = coalescer
        .get_or_create("large".into(), 0..9 * MIB, move |range| probe.fetch(range))
        .await
        .unwrap();
    assert_eq!(range, 0..9 * MIB);
    assert_eq!(download.bytes().len(), 9 * MIB);
}

#[tokio::test(start_paused = true)]
async fn test_inflight_reads_can_be_shared_but_not_extended() {
    let coalescer = DownloadCoalescer::default();
    let (gate_tx, mut gate_rx) = watch::channel(false);
    let mut first = coalescer.get_or_create("object".into(), 10..20, move |range| async move {
        gate_rx.wait_for(|open| *open).await.unwrap();
        Ok(Download::new(Bytes::from(vec![7; range.len()])))
    });
    assert!(futures::poll!(&mut first).is_pending());
    tokio::time::advance(Duration::from_millis(10)).await;
    assert!(futures::poll!(&mut first).is_pending());
    let covered = coalescer.get_or_create("object".into(), 12..18, |_| async {
        panic!("covered request should share the in-flight download")
    });
    let probe = FetchProbe::default();
    let separate_probe = probe.clone();
    let separate = coalescer.get_or_create("object".into(), 21..30, move |range| {
        separate_probe.fetch(range)
    });
    drop(first);
    gate_tx.send(true).unwrap();
    let (range, download) = covered.await.unwrap();
    assert_eq!(range, 10..20);
    assert_eq!(download.bytes().len(), 10);
    let (range, download) = separate.await.unwrap();
    assert_eq!(range, 21..30);
    assert_eq!(download.bytes().len(), 9);
    assert_eq!(probe.ranges(), vec![21..30]);
}

#[tokio::test(start_paused = true)]
async fn test_coalesced_download_is_cancelled_only_after_last_caller_drops() {
    let coalescer = DownloadCoalescer::default();
    let (gate_tx, mut gate_rx) = watch::channel(false);
    let mut first = coalescer.get_or_create("object".into(), 10..20, move |range| async move {
        gate_rx.wait_for(|open| *open).await.unwrap();
        Ok(Download::new(Bytes::from(vec![7; range.len()])))
    });
    let second = coalescer.get_or_create("object".into(), 30..40, |_| async {
        panic!("nearby request should share the pending download")
    });
    assert!(futures::poll!(&mut first).is_pending());
    tokio::time::advance(Duration::from_millis(10)).await;
    assert!(futures::poll!(&mut first).is_pending());
    drop(first);
    assert_eq!(gate_tx.receiver_count(), 1);
    drop(second);
    assert_eq!(gate_tx.receiver_count(), 0);
    let probe = FetchProbe::default();
    let retry_probe = probe.clone();
    coalescer
        .get_or_create("object".into(), 10..20, move |range| {
            retry_probe.fetch(range)
        })
        .await
        .unwrap();
    assert_eq!(probe.ranges(), vec![10..20]);
}

#[tokio::test(start_paused = true)]
async fn test_pending_batch_survives_initiator_drop_but_not_all_callers() {
    for cancel_all in [false, true] {
        let coalescer = DownloadCoalescer::default();
        let probe = FetchProbe::default();
        let first_probe = probe.clone();
        let mut first = coalescer.get_or_create("object".into(), 10..20, move |range| {
            first_probe.fetch(range)
        });
        let second = coalescer.get_or_create("object".into(), 30..40, |_| async {
            panic!("second callback should not run")
        });
        assert!(futures::poll!(&mut first).is_pending());
        drop(first);
        if cancel_all {
            drop(second);
            tokio::time::advance(Duration::from_millis(10)).await;
            assert!(probe.ranges().is_empty());
        } else {
            let (range, download) = second.await.unwrap();
            assert_eq!(range, 10..40);
            assert_eq!(download.bytes().len(), 30);
            assert_eq!(probe.ranges(), vec![10..40]);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn test_errors_reach_all_waiters_and_can_be_retried() {
    let coalescer = DownloadCoalescer::default();
    let first = coalescer.get_or_create("object".into(), 0..10, |_| async {
        Err(StorageErrorKind::NotFound.with_error(anyhow::anyhow!("missing")))
    });
    let second = coalescer.get_or_create("object".into(), 20..30, |_| async {
        panic!("second callback should not run")
    });
    let (first, second) = tokio::join!(first, second);
    for result in [first, second] {
        let error = result.err().unwrap();
        assert_eq!(error.kind(), StorageErrorKind::NotFound);
        assert!(error.to_string().contains("missing"));
    }
    let probe = FetchProbe::default();
    coalescer
        .get_or_create("object".into(), 0..10, move |range| probe.fetch(range))
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn test_short_merged_download_fails_even_if_it_covers_first_request() {
    let coalescer = DownloadCoalescer::default();
    let first = coalescer.get_or_create("object".into(), 0..10, |_| async {
        Ok(Download::new(Bytes::from(vec![0; 10])))
    });
    let second = coalescer.get_or_create("object".into(), 20..30, |_| async {
        panic!("second callback should not run")
    });
    let (first, second) = tokio::join!(first, second);
    for result in [first, second] {
        assert_eq!(result.err().unwrap().kind(), StorageErrorKind::Internal);
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_exact_slices_charge_shared_allocation_per_entry() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::{FeuerSplitRangeCache, config_for_test};

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = ::metrics::set_default_local_recorder(&recorder);
    let temp_dir = tempfile::tempdir().unwrap();
    let cache = FeuerSplitRangeCache::open(&config_for_test(temp_dir.path()))
        .await
        .unwrap();
    let mut buffer = cache.cache.allocate_buffer(4).unwrap();
    buffer.as_mut_slice().copy_from_slice(b"bcde");
    let download = buffer.into_download();
    let allocation_charge = download.allocation_charge();
    assert!(allocation_charge > download.bytes().len());
    let fetched_download = download.clone();
    let first = cache.get_or_fetch("object".into(), 1..2, move |range| async move {
        assert_eq!(range, 1..5);
        Ok(fetched_download)
    });
    let second = cache.get_or_fetch("object".into(), 4..5, |_| async {
        panic!("second callback must share the merged download")
    });
    let (first, second) = tokio::join!(first, second);
    for (range, bytes) in [(1..2, first.unwrap()), (4..5, second.unwrap())] {
        let expected = download.bytes().slice(range.start - 1..range.end - 1);
        assert_eq!(bytes, expected);
        assert_eq!(bytes.as_ptr(), expected.as_ptr());
        let hit = cache
            .get_or_fetch("object".into(), range, |_| async {
                panic!("exact slice must hit memory")
            })
            .await
            .unwrap();
        assert_eq!(hit.as_ptr(), bytes.as_ptr());
    }
    let charged_bytes = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find(|(key, _, _, _)| key.key().name() == "feuer_memory_used_bytes")
        .unwrap()
        .3;
    assert_eq!(
        charged_bytes,
        DebugValue::Gauge(((2 * allocation_charge) as f64).into())
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_coalesced_download_caches_exact_slices_but_not_gap() {
    use std::path::Path;

    use super::{FeuerSplitRangeCache, config_for_test, wrap_storage_with_split_range_cache};
    use crate::MockStorage;

    let temp_dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(
        FeuerSplitRangeCache::open(&config_for_test(temp_dir.path()))
            .await
            .unwrap(),
    );
    let mut lower = MockStorage::new();
    lower
        .expect_uri()
        .return_const(quickwit_common::uri::Uri::for_test("ram://splits"));
    lower
        .expect_get_slice_into()
        .times(1)
        .returning(|path, range, buffer| {
            assert_eq!(path, Path::new("a.split"));
            assert_eq!(range, 1..5);
            assert_eq!(buffer.len(), 4);
            assert_eq!(buffer.as_ptr() as usize % 4096, 0);
            buffer.copy_from_slice(b"bcde");
            Ok(())
        });
    lower
        .expect_get_slice_into()
        .times(1)
        .returning(|path, range, buffer| {
            assert_eq!(path, Path::new("a.split"));
            assert_eq!(range, 2..4);
            buffer.copy_from_slice(b"cd");
            Ok(())
        });
    let storage = wrap_storage_with_split_range_cache(cache, Arc::new(lower));
    let path = Path::new("a.split");
    let (first, second) =
        tokio::join!(storage.get_slice(path, 1..2), storage.get_slice(path, 4..5));
    assert_eq!(first.unwrap().as_slice(), b"b");
    assert_eq!(second.unwrap().as_slice(), b"e");
    assert_eq!(
        storage.get_slice(path, 1..2).await.unwrap().as_slice(),
        b"b"
    );
    assert_eq!(
        storage.get_slice(path, 4..5).await.unwrap().as_slice(),
        b"e"
    );
    assert_eq!(
        storage.get_slice(path, 2..4).await.unwrap().as_slice(),
        b"cd"
    );
    assert_eq!(
        storage.get_slice(path, 2..4).await.unwrap().as_slice(),
        b"cd"
    );
}
