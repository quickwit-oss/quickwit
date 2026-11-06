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

use std::sync::Mutex;

use quickwit_storage::{
    BundleStorage, MockStorage, MockStorageCache, PutPayload, SplitPayloadBuilder,
    wrap_storage_with_cache,
};

use super::*;
use crate::StorageDirectory;

const CONTENT: &[u8] = b"0123456789abcdef";
const TEST_PATH: &str = "test.file";

fn hotcache() -> OwnedBytes {
    let mut builder = StaticDirectoryCacheBuilder::default();
    builder
        .add_file(Path::new(TEST_PATH), CONTENT.len() as u64)
        .add_bytes(b"45", 4);
    let mut bytes = Vec::new();
    builder.write(&mut bytes).unwrap();
    OwnedBytes::new(bytes)
}

#[tokio::test]
#[allow(clippy::single_range_in_vec_init)] // A batch containing one byte range.
async fn test_batch_through_directory_caches_and_bundle() {
    let mut builder = SplitPayloadBuilder::default();
    builder.add_payload("prefix".into(), Box::new(b"pad".to_vec()));
    builder.add_payload(TEST_PATH.into(), Box::new(CONTENT.to_vec()));
    let payload = builder.finalize(&[]).unwrap();
    let bytes = payload.read_all().await.unwrap();
    let footer =
        bytes.slice(payload.footer_range.start as usize..payload.footer_range.end as usize);
    let reads = Arc::new(Mutex::new(Vec::new()));
    let recorded_reads = reads.clone();
    let mut storage = MockStorage::default();
    let streamed_bytes = bytes.clone();
    storage
        .expect_get_slice_stream()
        .times(1)
        .returning(move |path, range| {
            assert_eq!(path, Path::new("split.split"));
            recorded_reads.lock().unwrap().push(range.clone());
            Ok(Box::new(std::io::Cursor::new(streamed_bytes.slice(range))))
        });
    let recorded_reads = reads.clone();
    storage
        .expect_get_slice()
        .times(1)
        .returning(move |path, range| {
            assert_eq!(path, Path::new("split.split"));
            recorded_reads.lock().unwrap().push(range.clone());
            Ok(bytes.slice(range))
        });
    let (bundle, _) =
        BundleStorage::open_from_split_bytes(Arc::new(storage), "split.split".into(), footer)
            .unwrap();
    let mut cache = MockStorageCache::default();
    cache.expect_get().times(2).return_const(None);
    cache.expect_put().times(1).return_const(());
    let storage = wrap_storage_with_cache(Arc::new(cache), Arc::new(bundle));
    let storage_directory = StorageDirectory::new(storage);
    let caching_directory = CachingDirectory::new_unbounded(Arc::new(storage_directory.clone()));
    let hot_directory = HotDirectory::open(caching_directory.clone(), hotcache()).unwrap();
    let path = Path::new(TEST_PATH);
    let handle = hot_directory.get_file_handle(path).unwrap();

    // Populate the dynamic cache separately from the static hotcache.
    let initial = handle.read_bytes_async(2..4).await.unwrap();
    assert_eq!(initial.as_slice(), b"23");
    assert_eq!(*reads.lock().unwrap(), vec![5..7]); // Bundled file starts after "pad".
    reads.lock().unwrap().clear();

    // FileSlice translates offsets before the caches remove hits and StorageDirectory plans misses.
    let file = FileSlice::new(handle).slice(2..14);
    let ranges = vec![8..10, 2..4, 0..2, 5..7, 8..10, 4..4];
    let expected: Vec<&[u8]> = vec![b"ab", b"45", b"23", b"78", b"ab", b""];
    let bytes = file.read_bytes_batch_async(ranges.clone()).await.unwrap();
    assert_eq!(
        bytes.iter().map(OwnedBytes::as_slice).collect::<Vec<_>>(),
        expected
    );
    // Misses 10..12, 7..9, 10..12 become one 7..12 download, translated by the bundle to 10..15.
    assert_eq!(*reads.lock().unwrap(), vec![10..15]);

    for (range, expected_bytes) in ranges.iter().zip(&expected) {
        assert_eq!(
            file.read_bytes_slice(range.clone()).unwrap().as_slice(),
            *expected_bytes
        );
    }
    let repeated = file.read_bytes_batch_async(ranges).await.unwrap();
    assert_eq!(
        repeated
            .iter()
            .map(OwnedBytes::as_slice)
            .collect::<Vec<_>>(),
        expected
    );

    // Hotcache-only reads and empty batches bypass storage.
    let hot_handle = hot_directory.get_file_handle(path).unwrap();
    let hot_bytes = hot_handle
        .read_bytes_batch_async(vec![4..6, 6..6])
        .await
        .unwrap();
    assert_eq!(hot_bytes[0].as_slice(), b"45");
    assert!(hot_bytes[1].is_empty());
    for handle in [
        caching_directory.get_file_handle(path).unwrap(),
        hot_handle,
        storage_directory.get_file_handle(path).unwrap(),
    ] {
        assert!(
            handle
                .read_bytes_batch_async(Vec::new())
                .await
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(reads.lock().unwrap().len(), 1);
}
