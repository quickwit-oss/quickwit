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
use tokio::io::AsyncRead;
use tokio::sync::watch;

use super::{FeuerSplitRangeCache, config_for_test};
use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, OwnedBytes, PutPayload, RamStorageBuilder, Storage, StorageResult,
    wrap_storage_with_split_range_cache,
};

pub(super) const SPLIT_PATH: &str = "a.split";
pub(super) const SPLIT_BYTES: &[u8] = b"abcde";

pub(super) struct LowerProbe {
    inner: Arc<dyn Storage>,
    get_slice_calls: AtomicUsize,
    pub(super) get_slice_completed: AtomicUsize,
    pub(super) download_address: AtomicUsize,
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

    async fn get_slice_into(
        &self,
        path: &Path,
        range: Range<usize>,
        buffer: &mut [u8],
    ) -> StorageResult<()> {
        self.get_slice_calls.fetch_add(1, Ordering::Relaxed);
        self.download_address
            .store(buffer.as_ptr() as usize, Ordering::Relaxed);
        let mut gate = self.gate.clone();
        let _ = gate.wait_for(|open| *open).await;
        let result = self.inner.get_slice_into(path, range, buffer).await;
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

pub(super) struct Fixture {
    pub(super) storage: Arc<dyn Storage>,
    pub(super) lower: Arc<LowerProbe>,
    pub(super) gate_tx: watch::Sender<bool>,
    _temp_dir: tempfile::TempDir,
}

impl Fixture {
    pub(super) async fn new() -> Self {
        Self::with_payload(SPLIT_BYTES, true).await
    }

    pub(super) async fn new_with_blocked_lower_read() -> Self {
        Self::with_payload(SPLIT_BYTES, false).await
    }

    pub(super) async fn with_payload(payload: &[u8], gate_open: bool) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let cache = Arc::new(
            FeuerSplitRangeCache::open(&config_for_test(temp_dir.path()))
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
            download_address: AtomicUsize::new(0),
            gate: gate_rx,
        });
        let storage = wrap_storage_with_split_range_cache(cache, lower.clone());
        Self {
            storage,
            lower,
            gate_tx,
            _temp_dir: temp_dir,
        }
    }

    pub(super) fn release_lower_read(&self) {
        self.gate_tx.send(true).unwrap();
    }

    pub(super) fn lower_reads(&self) -> usize {
        self.lower.get_slice_calls.load(Ordering::Relaxed)
    }

    pub(super) async fn wait_until_lower_read_started(&self) {
        wait_until(|| self.lower_reads() > 0, "lower read start").await;
    }
}

pub(super) async fn wait_until(predicate: impl Fn() -> bool, what: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}
