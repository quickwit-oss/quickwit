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

use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;

use bytesize::ByteSize;
use quickwit_config::{DiskCacheConfig, SearcherConfig, SplitCacheLimits};
use quickwit_proto::search::SplitIdAndFooterOffsets;
use quickwit_storage::{
    CountingStorage, DownloadCounters, FeuerSplitRangeCache, OwnedBytes, PutPayload,
    RamStorageBuilder, SearchSplitCache, SplitPayloadBuilder, Storage, StorageResolver,
    wrap_storage_with_split_range_cache,
};

use crate::SearcherContext;
use crate::leaf::open_split_bundle;

const SPLIT_ID: &str = "range-cache-split";
const BODY_FILE: &str = "segment.fast";
const BODY_BYTES: &[u8] = b"FASTDATA";
const HOTCACHE_BYTES: &[u8] = b"HOT";

fn range_cache_config(path: impl AsRef<Path>) -> DiskCacheConfig {
    DiskCacheConfig {
        path: Some(path.as_ref().to_path_buf()),
        disk_capacity: ByteSize::mib(512),
        memory_capacity: ByteSize::mib(8),
    }
}

fn lower_reads(counters: &DownloadCounters) -> u64 {
    counters.snapshot().1
}

struct SplitBundle {
    split_bytes: OwnedBytes,
    footer_offsets: SplitIdAndFooterOffsets,
}

async fn build_split() -> SplitBundle {
    let temp_dir = tempfile::tempdir().unwrap();
    let body_path = temp_dir.path().join(BODY_FILE);
    std::fs::write(&body_path, BODY_BYTES).unwrap();
    let payload =
        SplitPayloadBuilder::get_split_payload(&[body_path], &[], None, HOTCACHE_BYTES).unwrap();
    let footer_range = payload.footer_range.clone();
    let split_bytes = payload.read_all().await.unwrap();
    SplitBundle {
        split_bytes,
        footer_offsets: SplitIdAndFooterOffsets {
            split_id: SPLIT_ID.to_string(),
            split_footer_start: footer_range.start,
            split_footer_end: footer_range.end,
            ..Default::default()
        },
    }
}

fn split_file_name() -> String {
    format!("{SPLIT_ID}.split")
}

async fn open_range_cache(dir: &Path) -> Arc<FeuerSplitRangeCache> {
    Arc::new(
        FeuerSplitRangeCache::open(&range_cache_config(dir))
            .await
            .unwrap(),
    )
}

fn wrap_counted_ram(split_bytes: &OwnedBytes) -> (Arc<dyn Storage>, Arc<DownloadCounters>) {
    let ram = RamStorageBuilder::default()
        .put(&split_file_name(), split_bytes.as_slice())
        .build();
    CountingStorage::instrument_storage(Arc::new(ram))
}

fn context_with_range_cache(cache: Arc<FeuerSplitRangeCache>) -> SearcherContext {
    SearcherContext::new_without_invoker(SearcherConfig::default(), None, Some(cache))
}

#[tokio::test]
async fn test_open_split_bundle_reuses_footer_from_feuer() {
    let split = build_split().await;
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = open_range_cache(cache_dir.path()).await;
    let context = context_with_range_cache(cache);
    let (counted, counters) = wrap_counted_ram(&split.split_bytes);

    for _ in 0..2 {
        let (hotcache, _bundle) =
            open_split_bundle(&context, counted.clone(), &split.footer_offsets)
                .await
                .unwrap();
        assert_eq!(hotcache.as_slice(), HOTCACHE_BYTES);
        assert_eq!(
            lower_reads(&counters),
            1,
            "only the first footer read misses Feuer"
        );
    }
}

#[tokio::test]
async fn test_open_split_bundle_footer_miss_uses_feuer_then_reuses_storage_for_body() {
    let split = build_split().await;
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = open_range_cache(cache_dir.path()).await;
    let context = context_with_range_cache(cache.clone());
    let (counted, counters) = wrap_counted_ram(&split.split_bytes);

    let (hotcache, bundle) = open_split_bundle(&context, counted.clone(), &split.footer_offsets)
        .await
        .unwrap();
    assert_eq!(hotcache.as_slice(), HOTCACHE_BYTES);
    assert_eq!(lower_reads(&counters), 1, "cold footer is one lower read");

    bundle.get_slice(Path::new(BODY_FILE), 0..4).await.unwrap();
    bundle.get_slice(Path::new(BODY_FILE), 0..4).await.unwrap();
    assert_eq!(
        lower_reads(&counters),
        2,
        "second body range must hit Feuer"
    );

    // A fresh searcher context still finds the footer in the process-wide range cache.
    let fresh_context = context_with_range_cache(cache);
    let (hotcache, _bundle) = open_split_bundle(&fresh_context, counted, &split.footer_offsets)
        .await
        .unwrap();
    assert_eq!(hotcache.as_slice(), HOTCACHE_BYTES);
    assert_eq!(lower_reads(&counters), 2);
}

#[tokio::test]
async fn test_open_split_bundle_footer_skips_whole_split_cache() {
    let split = build_split().await;
    let split_cache_dir = tempfile::tempdir().unwrap();
    std::fs::write(
        split_cache_dir.path().join(split_file_name()),
        split.split_bytes.as_slice(),
    )
    .unwrap();
    let split_cache = SearchSplitCache::with_root_path(
        split_cache_dir.path().to_path_buf(),
        StorageResolver::unconfigured(),
        SplitCacheLimits {
            max_num_bytes: ByteSize::mb(64),
            max_num_splits: NonZeroU32::new(8).unwrap(),
            num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
            max_file_descriptors: NonZeroU32::new(8).unwrap(),
        },
    )
    .unwrap();

    let range_cache_dir = tempfile::tempdir().unwrap();
    let range_cache = open_range_cache(range_cache_dir.path()).await;
    let context = SearcherContext::new_without_invoker(
        SearcherConfig::default(),
        Some(split_cache),
        Some(range_cache),
    );
    let (counted, counters) = wrap_counted_ram(&split.split_bytes);
    let (_hotcache, bundle) = open_split_bundle(&context, counted, &split.footer_offsets)
        .await
        .unwrap();
    assert_eq!(
        lower_reads(&counters),
        1,
        "footer skips SplitCache and reads through Feuer"
    );

    bundle.get_slice(Path::new(BODY_FILE), 0..4).await.unwrap();
    assert_eq!(
        lower_reads(&counters),
        1,
        "body must hit the on-disk whole-split cache"
    );
}

#[tokio::test]
async fn test_open_split_bundle_does_not_reuse_covering_slice_for_footer_and_body() {
    let split = build_split().await;
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = open_range_cache(cache_dir.path()).await;
    let (counted, counters) = wrap_counted_ram(&split.split_bytes);
    let cached_storage = wrap_storage_with_split_range_cache(cache.clone(), counted.clone());
    cached_storage
        .get_slice(Path::new(&split_file_name()), 0..split.split_bytes.len())
        .await
        .unwrap();
    assert_eq!(lower_reads(&counters), 1);

    let context = context_with_range_cache(cache);
    let (_, bundle) = open_split_bundle(&context, counted, &split.footer_offsets)
        .await
        .unwrap();
    assert_eq!(
        bundle
            .get_slice(Path::new(BODY_FILE), 0..4)
            .await
            .unwrap()
            .as_slice(),
        b"FAST"
    );
    assert_eq!(
        lower_reads(&counters),
        3,
        "footer and body require their own exact slices despite a covering cached slice"
    );
}
