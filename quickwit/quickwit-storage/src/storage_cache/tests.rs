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
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_config::StorageCacheConfig;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::watch;

use super::open_storage_cache;
use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, MockStorage, OwnedBytes, PutPayload, RamStorageBuilder, Storage,
    StorageErrorKind, StorageResult, wrap_storage_with_storage_cache,
};

async fn memory_cache() -> feuer::Cache {
    open_storage_cache(
        &StorageCacheConfig {
            directory: None,
            disk_capacity: bytesize::ByteSize::b(0),
            memory_capacity: bytesize::ByteSize::mib(8),
        },
        &crate::FeuerIoQueues::default(),
    )
    .unwrap()
}

#[tokio::test]
async fn test_empty_and_exact_ranges_without_covering_reuse() {
    let fixture = Fixture::new(true).await;
    let path = Path::new(SPLIT_PATH);
    for (range, reads) in [
        (4..4, 0),
        (1..4, 1),
        (1..4, 1),
        (2..3, 2),
        (0..5, 3),
        (0..2, 4),
        (1..4, 4),
    ] {
        let bytes = fixture
            .storage
            .get_slice(path, range.clone())
            .await
            .unwrap();
        assert_eq!(bytes.as_slice(), &SPLIT_BYTES[range.clone()]);
        assert_eq!(fixture.lower_reads(), reads, "range {range:?}");
    }
}

#[tokio::test]
async fn test_download_and_exact_cache_hit_share_the_destination_buffer() {
    let fixture = Fixture::new(true).await;
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
    let fixture = Fixture::new(false).await;
    let path = Path::new(SPLIT_PATH);
    let mut first = Box::pin(fixture.storage.get_slice(path, 0..4));
    assert!(futures::poll!(first.as_mut()).is_pending());
    assert_eq!(fixture.lower_reads(), 1);
    let mut second = Box::pin(fixture.storage.get_slice(path, 0..4));
    assert!(futures::poll!(second.as_mut()).is_pending());
    fixture.gate_tx.send(true).unwrap();
    let (first_result, second_result) = tokio::join!(first, second);
    assert_eq!(first_result.unwrap().as_slice(), b"abcd");
    assert_eq!(second_result.unwrap().as_slice(), b"abcd");
    assert_eq!(fixture.lower_reads(), 1);
}

#[tokio::test]
async fn test_download_errors_are_forwarded_and_retried() {
    let mut lower = MockStorage::new();
    lower
        .expect_uri()
        .return_const(Uri::for_test("ram://splits"));
    let mut attempts = 0;
    lower
        .expect_get_slice_into()
        .times(2)
        .returning(move |path, range, buffer| {
            assert_eq!(path, Path::new(SPLIT_PATH));
            assert_eq!(range, 1..4);
            assert_eq!(buffer.len(), 3);
            attempts += 1;
            if attempts == 1 {
                return Err(StorageErrorKind::NotFound.with_error(anyhow::anyhow!("missing split")));
            }
            buffer.copy_from_slice(b"bcd");
            Ok(())
        });
    let storage = wrap_storage_with_storage_cache(memory_cache().await, Arc::new(lower));
    let error = storage
        .get_slice(Path::new(SPLIT_PATH), 1..4)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::NotFound);
    assert!(error.to_string().contains("missing split"));
    for _ in 0..2 {
        assert_eq!(
            storage
                .get_slice(Path::new(SPLIT_PATH), 1..4)
                .await
                .unwrap()
                .as_slice(),
            b"bcd"
        );
    }
}

#[tokio::test]
async fn test_writes_are_unsupported() {
    let fixture = Fixture::new(true).await;
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
            .contains("Unsupported operation. FeuerStorage only supports async reads")
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
async fn test_other_reads_are_forwarded() {
    let fixture = Fixture::new(true).await;
    let path = Path::new(SPLIT_PATH);
    fixture.storage.check_connectivity().await.unwrap();
    assert_eq!(fixture.storage.uri(), fixture.lower.uri());
    assert_eq!(
        fixture.storage.file_num_bytes(path).await.unwrap(),
        SPLIT_BYTES.len() as u64
    );
    assert_eq!(
        fixture.storage.get_all(path).await.unwrap().as_slice(),
        SPLIT_BYTES
    );
    let mut copied = Vec::new();
    fixture.storage.copy_to(path, &mut copied).await.unwrap();
    assert_eq!(copied, SPLIT_BYTES);
    let mut stream = fixture.storage.get_slice_stream(path, 1..4).await.unwrap();
    let mut streamed = Vec::new();
    stream.read_to_end(&mut streamed).await.unwrap();
    assert_eq!(streamed, b"bcd");
    assert_eq!(fixture.lower_reads(), 0);
    fixture.storage.get_slice(path, 0..5).await.unwrap();
    assert_eq!(fixture.lower_reads(), 1);
}

#[tokio::test]
async fn test_object_keys_do_not_alias() {
    let cache = memory_cache().await;
    for (uri, expected) in [("ram://first", b"abcd"), ("ram://second", b"efgh")] {
        let mut lower = MockStorage::new();
        lower.expect_uri().return_const(Uri::for_test(uri));
        for (path, payload) in [
            ("a.split", expected.as_slice()),
            ("b.split", b"ijkl".as_slice()),
        ] {
            lower
                .expect_get_slice_into()
                .withf(move |actual_path, range, _| {
                    actual_path == Path::new(path) && range == &(0..4)
                })
                .times(1)
                .returning(move |_, _, buffer| {
                    buffer.copy_from_slice(payload);
                    Ok(())
                });
        }
        let storage = wrap_storage_with_storage_cache(cache.clone(), Arc::new(lower));
        for _ in 0..2 {
            assert_eq!(
                storage
                    .get_slice(Path::new("a.split"), 0..4)
                    .await
                    .unwrap()
                    .as_slice(),
                expected
            );
            assert_eq!(
                storage
                    .get_slice(Path::new("b.split"), 0..4)
                    .await
                    .unwrap()
                    .as_slice(),
                b"ijkl"
            );
        }
    }
}

const SPLIT_PATH: &str = "a.split";
const SPLIT_BYTES: &[u8] = b"abcde";

#[derive(Debug)]
struct LowerProbe {
    inner: Arc<dyn Storage>,
    get_slice_calls: AtomicUsize,
    download_address: AtomicUsize,
    gate: watch::Receiver<bool>,
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

    async fn get_slice(&self, _path: &Path, _range: Range<usize>) -> StorageResult<OwnedBytes> {
        panic!("the wrapper must download into its allocated buffer")
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
        assert_eq!(buffer.len(), range.len());
        let mut gate = self.gate.clone();
        gate.wait_for(|open| *open).await.unwrap();
        self.inner.get_slice_into(path, range, buffer).await
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
    lower: Arc<LowerProbe>,
    gate_tx: watch::Sender<bool>,
}

impl Fixture {
    async fn new(gate_open: bool) -> Self {
        let ram = Arc::new(
            RamStorageBuilder::default()
                .put(SPLIT_PATH, SPLIT_BYTES)
                .build(),
        );
        let (gate_tx, gate_rx) = watch::channel(gate_open);
        let lower = Arc::new(LowerProbe {
            inner: ram,
            get_slice_calls: AtomicUsize::new(0),
            download_address: AtomicUsize::new(0),
            gate: gate_rx,
        });
        let storage = wrap_storage_with_storage_cache(memory_cache().await, lower.clone());
        Self {
            storage,
            lower,
            gate_tx,
        }
    }

    fn lower_reads(&self) -> usize {
        self.lower.get_slice_calls.load(Ordering::Relaxed)
    }
}
