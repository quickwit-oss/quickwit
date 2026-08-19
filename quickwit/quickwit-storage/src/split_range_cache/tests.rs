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

use std::fmt;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_config::SplitRangeCacheWritePolicy;
use tokio::io::AsyncRead;
use tokio::sync::watch;

use super::metrics::{ADMISSION_MAX_ENTRY_SIZE, REQUESTS_ERROR, REQUESTS_MEMORY, REQUESTS_MISS};
use super::*;
use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, OwnedBytes, PutPayload, RamStorageBuilder, Storage, StorageErrorKind,
    StorageResult, wrap_storage_with_split_range_cache,
};

const SPLIT_PATH: &str = "a.split";
const SPLIT_BYTES: &[u8] = b"abcde";

#[test]
fn test_flush_on_close_pairs_with_write_policy() {
    assert!(foyer_flush_on_close(
        SplitRangeCacheWritePolicy::WriteOnEviction
    ));
    assert!(!foyer_flush_on_close(
        SplitRangeCacheWritePolicy::WriteOnInsertion
    ));
}

#[tokio::test]
async fn test_split_range_cache_builder_uses_configured_policy_and_throttle() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache = FoyerSplitRangeCache::open(&config_for_test(temp_dir.path()))
        .await
        .unwrap();
    assert_eq!(
        cache.cache.policy(),
        foyer::HybridCachePolicy::WriteOnEviction
    );
    cache.close().await.unwrap();
}

#[tokio::test]
async fn test_split_range_cache_builder_write_on_insertion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = config_for_test(temp_dir.path());
    config.write_policy = SplitRangeCacheWritePolicy::WriteOnInsertion;
    let cache = FoyerSplitRangeCache::open(&config).await.unwrap();
    assert_eq!(
        cache.cache.policy(),
        foyer::HybridCachePolicy::WriteOnInsertion
    );
    cache.close().await.unwrap();
}

struct LowerProbe {
    inner: Arc<dyn Storage>,
    get_slice_calls: AtomicUsize,
    get_slice_completed: AtomicUsize,
    gate: watch::Receiver<bool>,
}

impl fmt::Debug for LowerProbe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LowerProbe")
            .field("uri", self.inner.uri())
            .finish()
    }
}

#[async_trait]
impl Storage for LowerProbe {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.inner.check_connectivity().await
    }

    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()> {
        self.inner.put(path, payload).await
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        self.inner.copy_to(path, output).await
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        self.get_slice_calls.fetch_add(1, Ordering::Relaxed);
        let mut gate = self.gate.clone();
        let _ = gate.wait_for(|open| *open).await;
        let result = self.inner.get_slice(path, range).await;
        self.get_slice_completed.fetch_add(1, Ordering::Relaxed);
        result
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        self.inner.get_slice_stream(path, range).await
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        self.inner.get_all(path).await
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.inner.delete(path).await
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        self.inner.bulk_delete(paths).await
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.inner.file_num_bytes(path).await
    }

    fn uri(&self) -> &Uri {
        self.inner.uri()
    }
}

struct Fixture {
    storage: Arc<dyn Storage>,
    cache: Arc<FoyerSplitRangeCache>,
    lower: Arc<LowerProbe>,
    gate_tx: watch::Sender<bool>,
    _temp_dir: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_payload(SPLIT_BYTES, true).await
    }

    async fn new_with_blocked_lower_read() -> Self {
        Self::with_payload(SPLIT_BYTES, false).await
    }

    async fn with_payload(payload: &[u8], gate_open: bool) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let cache = Arc::new(
            FoyerSplitRangeCache::open(&config_for_test(temp_dir.path()))
                .await
                .unwrap(),
        );
        let ram: Arc<dyn Storage> = Arc::new(
            RamStorageBuilder::default()
                .put(SPLIT_PATH, payload)
                .build(),
        );
        let (gate_tx, gate_rx) = watch::channel(gate_open);
        let lower = Arc::new(LowerProbe {
            inner: ram,
            get_slice_calls: AtomicUsize::new(0),
            get_slice_completed: AtomicUsize::new(0),
            gate: gate_rx,
        });
        let storage = wrap_storage_with_split_range_cache(cache.clone(), lower.clone());
        Self {
            storage,
            cache,
            lower,
            gate_tx,
            _temp_dir: temp_dir,
        }
    }

    fn release_lower_read(&self) {
        self.gate_tx.send(true).unwrap();
    }

    fn lower_reads(&self) -> usize {
        self.lower.get_slice_calls.load(Ordering::Relaxed)
    }

    fn lower_completed(&self) -> usize {
        self.lower.get_slice_completed.load(Ordering::Relaxed)
    }

    async fn wait_until_lower_read_started(&self) {
        wait_until(|| self.lower_reads() > 0, "lower read start").await;
    }

    async fn wait_until_lower_read_completed(&self) {
        wait_until(|| self.lower_completed() > 0, "lower read completion").await;
    }

    async fn close(&self) {
        self.cache.close().await.unwrap();
    }
}

async fn wait_until(predicate: impl Fn() -> bool, what: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !predicate() {
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for {what}");
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test]
async fn test_empty_range_and_exact_hit_behavior() {
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
    let misses_before = REQUESTS_MISS.get();
    let memory_hits_before = REQUESTS_MEMORY.get();
    assert_eq!(
        fixture
            .storage
            .get_slice(path, 1..4)
            .await
            .unwrap()
            .as_slice(),
        b"bcd"
    );
    assert!(REQUESTS_MISS.get() > misses_before);
    assert_eq!(
        fixture
            .storage
            .get_slice(path, 1..4)
            .await
            .unwrap()
            .as_slice(),
        b"bcd"
    );
    assert!(REQUESTS_MEMORY.get() > memory_hits_before);
    assert_eq!(fixture.lower_reads(), 1);
    fixture.storage.get_slice(path, 0..5).await.unwrap();
    assert_eq!(
        fixture.lower_reads(),
        2,
        "covering ranges are distinct keys"
    );
    fixture.close().await;
}

#[tokio::test]
async fn test_identical_concurrent_misses_fetch_once() {
    let fixture = Fixture::new_with_blocked_lower_read().await;
    let path = Path::new(SPLIT_PATH);
    let first = fixture.storage.get_slice(path, 0..4);
    let second = fixture.storage.get_slice(path, 0..4);
    let release = async {
        fixture.wait_until_lower_read_started().await;
        fixture.release_lower_read();
    };
    let (first_result, second_result, _) = tokio::join!(first, second, release);
    assert_eq!(first_result.unwrap(), second_result.unwrap());
    assert_eq!(fixture.lower_reads(), 1);
    fixture.close().await;
}

#[tokio::test]
async fn test_remote_error_is_not_cached_or_rewritten() {
    let fixture = Fixture::new().await;
    let errors_before = REQUESTS_ERROR.get();
    for _ in 0..2 {
        let error = fixture
            .storage
            .get_slice(Path::new("missing.split"), 0..4)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::NotFound);
    }
    assert!(REQUESTS_ERROR.get() >= errors_before + 2);
    assert_eq!(fixture.lower_reads(), 2);
    fixture.close().await;
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
            .contains("Unsupported operation. FoyerSplitRangeStorage only supports async reads")
    );
    let delete_error = fixture.storage.delete(path).await.unwrap_err();
    assert_eq!(delete_error.kind(), StorageErrorKind::Io);
    let bulk_error = fixture.storage.bulk_delete(&[path]).await.unwrap_err();
    assert_eq!(
        bulk_error.error.as_ref().unwrap().kind(),
        StorageErrorKind::Io
    );
    fixture.close().await;
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
    fixture.close().await;
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
    drop(initiating);
    let waiter = fixture.storage.get_slice(path, 0..4);
    fixture.release_lower_read();
    assert_eq!(waiter.await.unwrap().as_slice(), b"abcd");
    assert_eq!(fixture.lower_reads(), 1);
    fixture.close().await;
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
    fixture.close().await;
}

#[tokio::test]
async fn test_all_callers_dropped_detached_completion() {
    let fixture = Fixture::new_with_blocked_lower_read().await;
    let path = Path::new(SPLIT_PATH);
    let mut initiating = Box::pin(fixture.storage.get_slice(path, 0..4));
    tokio::select! {
        biased;
        result = &mut initiating => panic!("fetch completed before release: {result:?}"),
        () = fixture.wait_until_lower_read_started() => {}
    }
    drop(initiating);
    fixture.release_lower_read();
    fixture.wait_until_lower_read_completed().await;
    assert_eq!(
        fixture
            .storage
            .get_slice(path, 0..4)
            .await
            .unwrap()
            .as_slice(),
        b"abcd"
    );
    assert_eq!(fixture.lower_reads(), 1);
    fixture.close().await;
}

#[tokio::test]
async fn test_oversized_value_is_memory_only_and_returned() {
    let payload = vec![7u8; 3 * 1024 * 1024];
    let fixture = Fixture::with_payload(&payload, true).await;
    let path = Path::new(SPLIT_PATH);
    let range = 0..payload.len();
    let bypasses_before = ADMISSION_MAX_ENTRY_SIZE.get();
    assert_eq!(
        fixture
            .storage
            .get_slice(path, range.clone())
            .await
            .unwrap()
            .as_slice(),
        payload.as_slice()
    );
    assert_eq!(
        fixture
            .storage
            .get_slice(path, range)
            .await
            .unwrap()
            .as_slice(),
        payload.as_slice()
    );
    assert_eq!(fixture.lower_reads(), 1);
    assert!(ADMISSION_MAX_ENTRY_SIZE.get() > bypasses_before);
    fixture.close().await;
}
