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
use quickwit_config::{
    CachePolicy, DiskCompression, RecoverMode, SearcherConfig, SplitCacheLimits,
    SplitRangeCacheWritePolicy, SplitRangeDiskCacheConfig,
};
use quickwit_proto::search::SplitIdAndFooterOffsets;
use quickwit_storage::{
    CountingStorage, DownloadCounters, FoyerSplitRangeCache, OwnedBytes, PutPayload,
    RamStorageBuilder, SearchSplitCache, SplitPayloadBuilder, Storage, StorageResolver,
};

use crate::SearcherContext;
use crate::leaf::open_split_bundle;

const SPLIT_ID: &str = "range-cache-split";
const BODY_FILE: &str = "segment.fast";
const BODY_BYTES: &[u8] = b"FASTDATA";
const HOTCACHE_BYTES: &[u8] = b"HOT";

fn range_cache_config(path: impl AsRef<Path>) -> SplitRangeDiskCacheConfig {
    SplitRangeDiskCacheConfig {
        path: path.as_ref().to_path_buf(),
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
        clean_block_threshold: 16,
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
        SplitPayloadBuilder::get_split_payload(&[body_path], &[], HOTCACHE_BYTES).unwrap();
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

async fn open_range_cache(dir: &Path) -> Arc<FoyerSplitRangeCache> {
    Arc::new(
        FoyerSplitRangeCache::open(&range_cache_config(dir))
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

fn context_with_range_cache(cache: Arc<FoyerSplitRangeCache>) -> SearcherContext {
    SearcherContext::new_without_invoker(SearcherConfig::default(), None, Some(cache))
}

#[tokio::test]
async fn test_open_split_bundle_footer_ram_hit_bypasses_lower_tiers() {
    let split = build_split().await;
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = open_range_cache(cache_dir.path()).await;
    let context = context_with_range_cache(cache.clone());
    let footer = split.split_bytes.slice(
        split.footer_offsets.split_footer_start as usize
            ..split.footer_offsets.split_footer_end as usize,
    );
    context.split_footer_cache.put(SPLIT_ID.to_string(), footer);

    let (counted, counters) = wrap_counted_ram(&split.split_bytes);
    let (_hotcache, _bundle) = open_split_bundle(&context, counted, &split.footer_offsets)
        .await
        .unwrap();
    assert_eq!(lower_reads(&counters), 0);
    cache.close().await.unwrap();
}

#[tokio::test]
async fn test_open_split_bundle_footer_miss_uses_foyer_then_reuses_storage_for_body() {
    let split = build_split().await;
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = open_range_cache(cache_dir.path()).await;
    let context = context_with_range_cache(cache.clone());
    let (counted, counters) = wrap_counted_ram(&split.split_bytes);

    let (_hotcache, bundle) = open_split_bundle(&context, counted, &split.footer_offsets)
        .await
        .unwrap();
    assert_eq!(lower_reads(&counters), 1, "cold footer is one lower read");

    bundle.get_slice(Path::new(BODY_FILE), 0..4).await.unwrap();
    bundle.get_slice(Path::new(BODY_FILE), 0..4).await.unwrap();
    assert_eq!(
        lower_reads(&counters),
        2,
        "second exact body range must hit Foyer"
    );
    cache.close().await.unwrap();
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
        Some(range_cache.clone()),
    );
    let (counted, counters) = wrap_counted_ram(&split.split_bytes);
    let (_hotcache, bundle) = open_split_bundle(&context, counted, &split.footer_offsets)
        .await
        .unwrap();
    assert_eq!(
        lower_reads(&counters),
        1,
        "footer fetch skips SplitCache and reads through Foyer"
    );

    bundle.get_slice(Path::new(BODY_FILE), 0..4).await.unwrap();
    assert_eq!(
        lower_reads(&counters),
        1,
        "body read must hit the on-disk whole-split cache"
    );
    range_cache.close().await.unwrap();
}

#[tokio::test]
async fn test_open_split_bundle_recovers_footer_from_foyer() {
    let split = build_split().await;
    let cache_dir = tempfile::tempdir().unwrap();
    let config = range_cache_config(cache_dir.path());
    {
        let cache = Arc::new(FoyerSplitRangeCache::open(&config).await.unwrap());
        let context = context_with_range_cache(cache.clone());
        let (counted, counters) = wrap_counted_ram(&split.split_bytes);
        open_split_bundle(&context, counted, &split.footer_offsets)
            .await
            .unwrap();
        assert_eq!(lower_reads(&counters), 1);
        cache.close().await.unwrap();
    }

    let recovered = Arc::new(FoyerSplitRangeCache::open(&config).await.unwrap());
    let context = context_with_range_cache(recovered.clone());
    let (counted, counters) = wrap_counted_ram(&split.split_bytes);
    open_split_bundle(&context, counted, &split.footer_offsets)
        .await
        .unwrap();
    assert_eq!(
        lower_reads(&counters),
        0,
        "recovered footer range must not read lower storage"
    );
    recovered.close().await.unwrap();
}
