use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use tantivy::Index;
use tantivy_datafusion::IndexOpener;

/// Registry of opened tantivy indexes, keyed by split ID.
///
/// For integration tests this is populated before query execution.
/// In production this would be replaced by `open_index_with_caches()`.
pub type SplitRegistry = DashMap<String, Index>;

/// An [`IndexOpener`] backed by an in-memory [`SplitRegistry`].
///
/// Planning-time metadata (schema, segment sizes) is stored inline so
/// that the opener can answer schema/partition queries without touching
/// the registry. The actual [`open`](IndexOpener::open) call looks up
/// the registry at execution time.
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

    /// Build an opener by extracting schema and segment sizes from an
    /// already-opened index, then inserting it into the registry.
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
            .field("segment_sizes", &self.segment_sizes)
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
