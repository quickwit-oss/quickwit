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
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use bytesize::ByteSize;
use quickwit_config::{
    CachePolicy, DiskCompression, RecoverMode, SplitCacheLimits, SplitRangeCacheWritePolicy,
    SplitRangeDiskCacheConfig,
};
use quickwit_proto::search::SplitIdAndFooterOffsets;
use quickwit_storage::{
    CountingStorage, FoyerSplitRangeCache, PutPayload, RamStorageBuilder, SearchSplitCache,
    Storage, StorageResolver,
};

use super::open_split_bundle;
use crate::service::SearcherContext;

const SPLIT_ID: &str = "split-a";
const FAST_BYTES: &[u8] = b"FASTDATA";

struct SplitBundle {
    split_bytes: tantivy::directory::OwnedBytes,
    footer_offsets: SplitIdAndFooterOffsets,
    footer_range: Range<usize>,
}

async fn build_split() -> SplitBundle {
    let temp_dir = tempfile::tempdir().unwrap();
    let fast_path = temp_dir.path().join("segment.fast");
    std::fs::write(&fast_path, FAST_BYTES).unwrap();
    let payload =
        quickwit_storage::SplitPayloadBuilder::get_split_payload(&[fast_path], &[], b"HOTC")
            .unwrap();
    let footer_range = payload.footer_range.start as usize..payload.footer_range.end as usize;
    let split_bytes = payload.read_all().await.unwrap();
    SplitBundle {
        split_bytes,
        footer_offsets: SplitIdAndFooterOffsets {
            split_id: SPLIT_ID.to_string(),
            split_footer_start: footer_range.start as u64,
            split_footer_end: footer_range.end as u64,
            timestamp_start: None,
            timestamp_end: None,
            num_docs: 1,
        },
        footer_range,
    }
}

fn ram_with_split(split_bytes: &[u8]) -> Arc<dyn Storage> {
    Arc::new(
        RamStorageBuilder::default()
            .put(&format!("{SPLIT_ID}.split"), split_bytes)
            .build(),
    )
}

fn range_cache_config(path: &Path) -> SplitRangeDiskCacheConfig {
    SplitRangeDiskCacheConfig {
        path: path.to_path_buf(),
        disk_capacity: ByteSize::mb(64),
        memory_capacity: ByteSize::mb(8),
        buffer_pool_size: ByteSize::mb(4),
        submit_queue_size_threshold: ByteSize::mb(8),
        memory_eviction_policy: CachePolicy::S3Fifo,
        write_policy: SplitRangeCacheWritePolicy::WriteOnEviction,
        compression: DiskCompression::Lz4,
        recover_mode: RecoverMode::Quiet,
        block_size: ByteSize::mb(4),
        max_entry_size: ByteSize::mb(2),
        flushers: 1,
        reclaimers: 1,
    }
}

fn context_with_range_cache(cache: Arc<FoyerSplitRangeCache>) -> SearcherContext {
    let mut context = SearcherContext::for_test();
    context.split_range_disk_cache_opt = Some(cache);
    context
}

fn lower_reads(counters: &quickwit_storage::DownloadCounters) -> u64 {
    counters.snapshot().1
}

#[tokio::test]
async fn test_open_split_bundle_footer_ram_hit_bypasses_lower_tiers() {
    let split = build_split().await;
    let (storage, counters) =
        CountingStorage::instrument_storage(ram_with_split(split.split_bytes.as_slice()));
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(
        FoyerSplitRangeCache::open(&range_cache_config(cache_dir.path()))
            .await
            .unwrap(),
    );
    let context = context_with_range_cache(cache.clone());
    context.split_footer_cache.put(
        SPLIT_ID.to_string(),
        split.split_bytes.slice(split.footer_range.clone()),
    );
    open_split_bundle(&context, storage, &split.footer_offsets)
        .await
        .unwrap();
    assert_eq!(lower_reads(&counters), 0);
    cache.close().await.unwrap();
}

#[tokio::test]
async fn test_open_split_bundle_footer_miss_uses_foyer_then_reuses_storage_for_body() {
    let split = build_split().await;
    let (storage, counters) =
        CountingStorage::instrument_storage(ram_with_split(split.split_bytes.as_slice()));
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(
        FoyerSplitRangeCache::open(&range_cache_config(cache_dir.path()))
            .await
            .unwrap(),
    );
    let context = context_with_range_cache(cache.clone());
    let (_hotcache, bundle) = open_split_bundle(&context, storage, &split.footer_offsets)
        .await
        .unwrap();
    assert_eq!(lower_reads(&counters), 1);
    let fast = bundle
        .get_slice(Path::new("segment.fast"), 0..4)
        .await
        .unwrap();
    assert_eq!(fast.as_slice(), b"FAST");
    bundle
        .get_slice(Path::new("segment.fast"), 0..4)
        .await
        .unwrap();
    assert_eq!(
        lower_reads(&counters),
        2,
        "second body read must be served from Foyer"
    );
    cache.close().await.unwrap();
}

#[tokio::test]
async fn test_open_split_bundle_whole_split_footer_hit_bypasses_foyer() {
    let split = build_split().await;
    let (storage, counters) =
        CountingStorage::instrument_storage(ram_with_split(split.split_bytes.as_slice()));
    let cache_dir = tempfile::tempdir().unwrap();
    std::fs::write(
        cache_dir.path().join(format!("{SPLIT_ID}.split")),
        split.split_bytes.as_slice(),
    )
    .unwrap();
    let split_cache = SearchSplitCache::with_root_path(
        cache_dir.path().to_path_buf(),
        StorageResolver::unconfigured(),
        SplitCacheLimits {
            max_num_bytes: ByteSize::mb(64),
            max_num_splits: NonZeroU32::new(8).unwrap(),
            num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
            max_file_descriptors: NonZeroU32::new(8).unwrap(),
        },
    )
    .unwrap();
    let range_dir = tempfile::tempdir().unwrap();
    let range_cache = Arc::new(
        FoyerSplitRangeCache::open(&range_cache_config(range_dir.path()))
            .await
            .unwrap(),
    );
    let mut context = SearcherContext::for_test();
    context.split_cache_opt = Some(split_cache);
    context.split_range_disk_cache_opt = Some(range_cache.clone());
    open_split_bundle(&context, storage, &split.footer_offsets)
        .await
        .unwrap();
    assert_eq!(
        lower_reads(&counters),
        0,
        "whole-split cache must bypass Foyer and lower storage"
    );
    assert!(
        context
            .split_footer_cache
            .get(&SPLIT_ID.to_string())
            .is_some()
    );
    range_cache.close().await.unwrap();
}

#[tokio::test]
async fn test_open_split_bundle_recovers_footer_from_disk() {
    let split = build_split().await;
    let ram = ram_with_split(split.split_bytes.as_slice());
    let cache_dir = tempfile::tempdir().unwrap();
    let config = range_cache_config(cache_dir.path());
    {
        let (storage, counters) = CountingStorage::instrument_storage(ram.clone());
        let cache = Arc::new(FoyerSplitRangeCache::open(&config).await.unwrap());
        let context = context_with_range_cache(cache.clone());
        open_split_bundle(&context, storage, &split.footer_offsets)
            .await
            .unwrap();
        assert_eq!(lower_reads(&counters), 1);
        cache.close().await.unwrap();
    }
    let (storage, counters) = CountingStorage::instrument_storage(ram);
    let recovered = Arc::new(FoyerSplitRangeCache::open(&config).await.unwrap());
    let context = context_with_range_cache(recovered.clone());
    open_split_bundle(&context, storage, &split.footer_offsets)
        .await
        .unwrap();
    assert_eq!(
        lower_reads(&counters),
        0,
        "recovered footer range must suppress lower storage"
    );
    recovered.close().await.unwrap();
}
