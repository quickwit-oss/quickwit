use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use quickwit_proto::search::SplitIdAndFooterOffsets;
use quickwit_query::tokenizers::TokenizerManager;
use quickwit_search::SearcherContext;
use quickwit_storage::Storage;
use tantivy::Index;
use tantivy_datafusion::IndexOpener;

/// Registry of opened tantivy indexes, keyed by split ID.
/// Used for integration tests. Production uses [`StorageSplitOpener`].
pub type SplitRegistry = DashMap<String, Index>;

// ── Test opener (DashMap-backed) ────────────────────────────────────

/// An [`IndexOpener`] backed by an in-memory [`SplitRegistry`].
///
/// For integration tests only. Production uses [`StorageSplitOpener`].
#[derive(Clone)]
pub struct SplitIndexOpener {
    split_id: String,
    registry: Arc<SplitRegistry>,
    tantivy_schema: tantivy::schema::Schema,
    segment_sizes: Vec<u32>,
}

impl SplitIndexOpener {
    pub fn new(
        split_id: String,
        registry: Arc<SplitRegistry>,
        tantivy_schema: tantivy::schema::Schema,
        segment_sizes: Vec<u32>,
    ) -> Self {
        Self {
            split_id,
            registry,
            tantivy_schema,
            segment_sizes,
        }
    }

    pub fn from_index(split_id: String, index: Index, registry: Arc<SplitRegistry>) -> Self {
        let tantivy_schema = index.schema();
        let segment_sizes = index
            .reader()
            .map(|r| {
                r.searcher()
                    .segment_readers()
                    .iter()
                    .map(|sr| sr.max_doc())
                    .collect()
            })
            .unwrap_or_default();
        registry.insert(split_id.clone(), index);
        Self {
            split_id,
            registry,
            tantivy_schema,
            segment_sizes,
        }
    }

    pub fn split_id(&self) -> &str {
        &self.split_id
    }
}

impl fmt::Debug for SplitIndexOpener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SplitIndexOpener")
            .field("split_id", &self.split_id)
            .finish()
    }
}

#[async_trait]
impl IndexOpener for SplitIndexOpener {
    async fn open(&self) -> Result<Index> {
        self.registry
            .get(&self.split_id)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "split {} not found in registry",
                    self.split_id
                ))
            })
    }

    fn schema(&self) -> tantivy::schema::Schema {
        self.tantivy_schema.clone()
    }

    fn segment_sizes(&self) -> Vec<u32> {
        self.segment_sizes.clone()
    }

    fn identifier(&self) -> &str {
        &self.split_id
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// ── Production opener (storage-backed) ──────────────────────────────

/// An [`IndexOpener`] that downloads and opens splits from object
/// storage using Quickwit's caching infrastructure.
///
/// At execution time (on the worker), calls `open_index_with_caches()`
/// to download the split bundle from S3/GCS/local storage, warm the
/// footer cache + fast field cache, and return an opened tantivy `Index`.
///
/// Planning-time metadata (schema, segment sizes) is stored inline —
/// no I/O during plan construction.
#[derive(Clone)]
pub struct StorageSplitOpener {
    split_id: String,
    tantivy_schema: tantivy::schema::Schema,
    segment_sizes: Vec<u32>,
    searcher_context: Arc<SearcherContext>,
    storage: Arc<dyn Storage>,
    footer_offsets: SplitIdAndFooterOffsets,
    tokenizer_manager: Option<TokenizerManager>,
}

impl StorageSplitOpener {
    pub fn new(
        split_id: String,
        tantivy_schema: tantivy::schema::Schema,
        segment_sizes: Vec<u32>,
        searcher_context: Arc<SearcherContext>,
        storage: Arc<dyn Storage>,
        split_footer_start: u64,
        split_footer_end: u64,
    ) -> Self {
        let footer_offsets = SplitIdAndFooterOffsets {
            split_id: split_id.clone(),
            split_footer_start,
            split_footer_end,
            ..Default::default()
        };
        Self {
            split_id,
            tantivy_schema,
            segment_sizes,
            searcher_context,
            storage,
            footer_offsets,
            tokenizer_manager: None,
        }
    }

    /// Set the tokenizer manager from the index's doc mapper.
    ///
    /// Required for full-text queries on fields with custom tokenizers.
    /// Without it, tantivy falls back to the default tokenizer.
    pub fn with_tokenizer_manager(mut self, tm: TokenizerManager) -> Self {
        self.tokenizer_manager = Some(tm);
        self
    }
}

impl fmt::Debug for StorageSplitOpener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageSplitOpener")
            .field("split_id", &self.split_id)
            .finish()
    }
}

#[async_trait]
impl IndexOpener for StorageSplitOpener {
    fn footer_range(&self) -> (u64, u64) {
        (self.footer_offsets.split_footer_start, self.footer_offsets.split_footer_end)
    }

    fn segment_sizes(&self) -> Vec<u32> {
        self.segment_sizes.clone()
    }

    async fn open(&self) -> Result<Index> {
        // Use an unbounded byte range cache so that tantivy can do
        // synchronous reads on the storage-backed directory. Without
        // this, StorageDirectory errors on sync reads.
        let byte_range_cache = quickwit_storage::ByteRangeCache::with_infinite_capacity(
            &quickwit_storage::STORAGE_METRICS.shortlived_cache,
        );
        let (index, _hot_directory) = quickwit_search::leaf::open_index_with_caches(
            &self.searcher_context,
            self.storage.clone(),
            &self.footer_offsets,
            self.tokenizer_manager.as_ref(),
            Some(byte_range_cache),
        )
        .await
        .map_err(|e| DataFusionError::Execution(format!("open split {}: {e}", self.split_id)))?;

        Ok(index)
    }

    fn schema(&self) -> tantivy::schema::Schema {
        self.tantivy_schema.clone()
    }

    fn identifier(&self) -> &str {
        &self.split_id
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
