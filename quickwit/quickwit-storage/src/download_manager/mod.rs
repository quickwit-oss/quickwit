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

//! Download manager for the search read path.
//!
//! Historically, fetching a byte range of a split for search traversed a deep
//! stack of `#[async_trait] impl Storage` decorators and tantivy `Directory`
//! wrappers (timeout/retry, fast-field cache, on-disk split cache, request
//! debouncing, bundle-offset translation, the ephemeral byte-range cache and the
//! static hotcache). Every layer allocated a boxed future and added an `.await`,
//! even though only the actual network/disk read performs I/O.
//!
//! The [`SplitDownloadView`] collapses that into:
//! - a **synchronous core** ([`SplitDownloadView::resolve`]) that performs every cache lookup, the
//!   bundle logical→physical offset translation and the dedup keying without allocating a single
//!   future, and
//! - **one thin async layer** ([`SplitDownloadView::download`]) that performs the actual download
//!   (on-disk split cache fill or object-store GET), bounded by the backend's existing semaphore,
//!   debounced and retried, and that writes the result back into the caches synchronously on
//!   completion.
//!
//! Caching is unified and keyed by the **physical split path joined with the
//! logical file name inside the split**, plus the file-relative byte range. This
//! keeps the long-term `.fast` field cache and the ephemeral byte-range cache
//! cleanly differentiated per file while remaining globally unique (split files
//! are ULID-named).

mod download;

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use quickwit_config::{CacheConfig, StorageTimeoutPolicy};

pub use self::download::fetch_split_footer;
use crate::{
    AsyncDebouncer, BundleStorageFileOffsets, ByteRangeCache, MemorySizedCache, OwnedBytes,
    SplitCache, Storage, StorageResult,
};

/// Identifies a single download. Used both as the request-deduplication key and
/// as the logical identity of a cached byte range.
///
/// The key carries the logical file name inside the split (not just the physical
/// split path + absolute range) so that caching is differentiated per file.
#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub(crate) struct DownloadKey {
    /// Physical `.split` file path, e.g. `01HX….split`.
    split_path: PathBuf,
    /// Logical file inside the split, e.g. a tantivy segment file. Empty for
    /// reads that address the physical split directly (e.g. the footer).
    file_in_split: PathBuf,
    /// Byte range. File-relative for logical reads, absolute for physical reads.
    byte_range: Range<usize>,
}

/// Outcome of the synchronous [`SplitDownloadView::resolve`] step. No I/O has
/// happened yet.
pub enum Resolution {
    /// Served entirely from a cache tier. Zero futures, return immediately.
    Hit(OwnedBytes),
    /// Not cached. Carries the plan for the (single) async download.
    Miss(DownloadPlan),
}

/// A pending download produced by a cache miss. Opaque to callers: it is fed
/// straight back into [`SplitDownloadView::download`].
pub struct DownloadPlan {
    file_in_split: PathBuf,
    /// File-relative byte range.
    byte_range: Range<usize>,
    is_fast: bool,
}

/// Process-wide, cross-request pieces of the download path shared by every split
/// search: the long-term fast-field cache, the optional on-disk split cache, and
/// the request debouncer. Held by the searcher context and cloned into a
/// per-split [`SplitDownloadView`].
#[derive(Clone)]
pub struct SearchDownloadCaches {
    /// Long-term `.fast` field cache (cross request).
    fast_field_cache: Arc<MemorySizedCache>,
    /// On-disk full-split cache. `None` if not configured.
    split_cache_opt: Option<Arc<SplitCache>>,
    /// Coalesces concurrent identical downloads. Global so concurrent searches
    /// over the same split share a single inflight download.
    inflight: Arc<AsyncDebouncer<DownloadKey, StorageResult<OwnedBytes>>>,
}

impl SearchDownloadCaches {
    /// Builds the shared download caches from the fast-field cache config and the
    /// optional on-disk split cache.
    pub fn new(
        fast_field_cache_config: &CacheConfig,
        split_cache_opt: Option<Arc<SplitCache>>,
    ) -> SearchDownloadCaches {
        let fast_field_cache = Arc::new(MemorySizedCache::from_config(
            fast_field_cache_config,
            &crate::metrics::FAST_FIELD_CACHE,
        ));
        SearchDownloadCaches {
            fast_field_cache,
            split_cache_opt,
            inflight: Arc::new(AsyncDebouncer::default()),
        }
    }
}

/// Per-split handle over the download path. Built once per split search; the
/// ephemeral byte-range cache and bundle offsets are specific to that split,
/// while the fast-field cache, split cache and debouncer are shared.
pub struct SplitDownloadView {
    backend: Arc<dyn Storage>,
    fast_field_cache: Arc<MemorySizedCache>,
    split_cache_opt: Option<Arc<SplitCache>>,
    inflight: Arc<AsyncDebouncer<DownloadKey, StorageResult<OwnedBytes>>>,
    timeout_policy: Option<StorageTimeoutPolicy>,
    bundle_offsets: Arc<BundleStorageFileOffsets>,
    /// Physical `.split` path, e.g. `01HX….split`.
    split_path: PathBuf,
    /// Ephemeral, per-request byte-range cache. `None` when the caller does not
    /// provide one (e.g. doc fetching), in which case only the long-term and
    /// static tiers serve reads.
    byte_range_cache: Option<ByteRangeCache>,
}

impl SplitDownloadView {
    /// Builds a per-split view from the shared caches and the per-split pieces:
    /// the raw backend storage, the optional aggressive-timeout policy, the
    /// bundle logical→physical offset map, the physical split path, and an
    /// optional ephemeral byte-range cache.
    pub fn new(
        caches: &SearchDownloadCaches,
        backend: Arc<dyn Storage>,
        timeout_policy: Option<StorageTimeoutPolicy>,
        bundle_offsets: Arc<BundleStorageFileOffsets>,
        split_path: PathBuf,
        byte_range_cache: Option<ByteRangeCache>,
    ) -> SplitDownloadView {
        SplitDownloadView {
            backend,
            fast_field_cache: caches.fast_field_cache.clone(),
            split_cache_opt: caches.split_cache_opt.clone(),
            inflight: caches.inflight.clone(),
            timeout_policy,
            bundle_offsets,
            split_path,
            byte_range_cache,
        }
    }

    /// Resolves a read against the cache tiers, synchronously. Safe to call from
    /// tantivy's synchronous `read_bytes`.
    ///
    /// Lookup order: ephemeral byte-range tier (per request) → long-term
    /// fast-field tier (only for `.fast` files). A miss returns a [`DownloadPlan`]
    /// without performing any network I/O.
    pub fn resolve(&self, file_in_split: &Path, byte_range: Range<usize>) -> Resolution {
        if byte_range.is_empty() {
            return Resolution::Hit(OwnedBytes::empty());
        }
        let cache_tag = self.cache_tag(file_in_split);
        if let Some(byte_range_cache) = &self.byte_range_cache
            && let Some(bytes) = byte_range_cache.get_slice(&cache_tag, byte_range.clone())
        {
            return Resolution::Hit(bytes);
        }
        let is_fast = is_fast_field_file(file_in_split);
        if is_fast
            && let Some(bytes) = self
                .fast_field_cache
                .get_slice(&cache_tag, byte_range.clone())
        {
            return Resolution::Hit(bytes);
        }
        Resolution::Miss(DownloadPlan {
            file_in_split: file_in_split.to_owned(),
            byte_range,
            is_fast,
        })
    }

    /// Resolves the read, and only awaits if it is a miss. This is the entry
    /// point used by warmup (async context).
    pub async fn get(
        &self,
        file_in_split: &Path,
        byte_range: Range<usize>,
    ) -> StorageResult<OwnedBytes> {
        match self.resolve(file_in_split, byte_range) {
            Resolution::Hit(bytes) => Ok(bytes),
            Resolution::Miss(plan) => self.download(plan).await,
        }
    }

    /// The cache identity of a logical file inside this split: the physical split
    /// path joined with the logical file name. Globally unique (split files are
    /// ULID-named) while keeping per-file differentiation.
    fn cache_tag(&self, file_in_split: &Path) -> PathBuf {
        self.split_path.join(file_in_split)
    }
}

/// Returns whether the logical file is a fast-field file, i.e. eligible for the
/// long-term fast-field cache. Mirrors the historical `.fast` suffix routing.
fn is_fast_field_file(file_in_split: &Path) -> bool {
    file_in_split.to_string_lossy().ends_with(".fast")
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use bytesize::ByteSize;

    use super::*;
    use crate::{RamStorage, RamStorageBuilder};

    fn bundle_offsets(files: &[(&str, Range<u64>)]) -> Arc<BundleStorageFileOffsets> {
        let files = files
            .iter()
            .map(|(name, range)| (PathBuf::from(name), range.clone()))
            .collect();
        Arc::new(BundleStorageFileOffsets { files })
    }

    /// A valid ULID, so the physical split path is recognized by the (unused
    /// here) on-disk split cache.
    const SPLIT_PATH: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV.split";

    fn build_view(
        backend: Arc<dyn Storage>,
        byte_range_cache: Option<ByteRangeCache>,
        offsets: Arc<BundleStorageFileOffsets>,
    ) -> SplitDownloadView {
        let caches = SearchDownloadCaches::new(&ByteSize::mb(10).into(), None);
        SplitDownloadView::new(
            &caches,
            backend,
            None,
            offsets,
            PathBuf::from(SPLIT_PATH),
            byte_range_cache,
        )
    }

    #[tokio::test]
    async fn test_resolve_miss_then_download_then_hit() {
        // The split physical file contains "0123456789", file "seg.fast" maps to
        // bytes [2, 8) i.e. "234567".
        let ram_storage = RamStorageBuilder::default()
            .put(SPLIT_PATH, b"0123456789")
            .build();
        let offsets = bundle_offsets(&[("seg.fast", 2..8)]);
        let byte_range_cache =
            ByteRangeCache::with_infinite_capacity(&crate::metrics::SHORTLIVED_CACHE);
        let view = build_view(
            Arc::new(ram_storage),
            Some(byte_range_cache.clone()),
            offsets,
        );

        // First read is a miss (no cache populated yet).
        assert!(matches!(
            view.resolve(Path::new("seg.fast"), 0..4),
            Resolution::Miss(_)
        ));

        // `get` downloads (logical [0,4) -> physical [2,6) -> "2345").
        let bytes = view.get(Path::new("seg.fast"), 0..4).await.unwrap();
        assert_eq!(bytes.as_slice(), b"2345");

        // The download wrote back into both the ephemeral and the fast-field
        // tiers, so a subsequent resolve is a synchronous hit.
        match view.resolve(Path::new("seg.fast"), 0..4) {
            Resolution::Hit(bytes) => assert_eq!(bytes.as_slice(), b"2345"),
            Resolution::Miss(_) => panic!("expected a cache hit after download"),
        }
    }

    #[tokio::test]
    async fn test_non_fast_field_not_in_fast_cache() {
        let ram_storage = RamStorageBuilder::default()
            .put(SPLIT_PATH, b"0123456789")
            .build();
        let offsets = bundle_offsets(&[("seg.idx", 0..10)]);
        // No ephemeral cache: only the fast-field (long-term) tier could serve a
        // synchronous hit, and a non-`.fast` file must never land there.
        let view = build_view(Arc::new(ram_storage), None, offsets);

        let bytes = view.get(Path::new("seg.idx"), 0..3).await.unwrap();
        assert_eq!(bytes.as_slice(), b"012");
        // Still a miss: the read was not cached anywhere we consult synchronously.
        assert!(matches!(
            view.resolve(Path::new("seg.idx"), 0..3),
            Resolution::Miss(_)
        ));
    }

    #[tokio::test]
    async fn test_empty_range_is_hit() {
        let ram_storage = RamStorage::default();
        let offsets = bundle_offsets(&[("seg.fast", 0..0)]);
        let view = build_view(Arc::new(ram_storage), None, offsets);
        match view.resolve(Path::new("seg.fast"), 0..0) {
            Resolution::Hit(bytes) => assert!(bytes.is_empty()),
            Resolution::Miss(_) => panic!("empty range must resolve to an empty hit"),
        }
    }

    #[tokio::test]
    async fn test_unknown_file_is_not_found() {
        let ram_storage = RamStorage::default();
        let offsets = bundle_offsets(&[("seg.fast", 0..0)]);
        let view = build_view(Arc::new(ram_storage), None, offsets);
        let err = view
            .get(Path::new("does_not_exist"), 0..4)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), crate::StorageErrorKind::NotFound);
    }
}
