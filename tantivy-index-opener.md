# Tantivy-DataFusion: IndexOpener Implementation Plan

## Goal

Make tantivy-datafusion providers work with lazy index opening so the same plan can be built on a coordinator (without opening indexes) and executed on workers (where indexes get opened at stream poll time).

## Why

Today all three providers (`TantivyTableProvider`, `TantivyInvertedIndexProvider`, `TantivyDocumentProvider`) take an already-opened `tantivy::Index`. This means:
- The plan can't be serialized across the network (Index holds file handles)
- The coordinator must open every split to build the plan
- Workers can't open splits from their local storage/cache

With `IndexOpener`, the coordinator builds the plan using only metadata (schema, segment count). Workers call `opener.open()` at execution time to get the actual `Index`.

## Current Index Usage

The `Index` is used in two phases:

### Planning time (in `scan()`)

| Provider | What it reads from Index | Why |
|----------|------------------------|-----|
| `TantivyTableProvider` | `index.reader().searcher().segment_readers().len()` | Partition count |
| | `searcher.segment_reader(i).max_doc()` | Doc count per segment for chunking |
| | `index.schema()` (via `tantivy_schema_to_arrow_from_index`) | Arrow schema |
| `TantivyInvertedIndexProvider` | `index.schema()` | Field resolution for query parsing |
| | `QueryParser::for_index(&index, fields)` | Parse full_text queries |
| | `index.reader().searcher().segment_readers().len()` | Partition count |
| `TantivyDocumentProvider` | `index.reader().searcher().segment_readers().len()` | Partition count |

### Execution time (in `DataSource::open()`)

All three clone the `Index` into their `DataSource` struct, then in `open()` call `index.reader()?.searcher()` to read data.

## The Change

### New trait: `IndexOpener`

```rust
// In tantivy-datafusion/src/index_opener.rs

/// Provides a tantivy Index on demand. Used to defer index opening
/// from planning time to execution time, enabling distributed execution
/// where the coordinator builds plans without opening indexes and
/// workers open them from local storage.
#[async_trait]
pub trait IndexOpener: Send + Sync + fmt::Debug {
    /// Open (or return cached) the tantivy Index.
    async fn open(&self) -> Result<Index>;

    /// Return the tantivy schema without opening the full index.
    /// Used during planning for Arrow schema derivation and query parsing.
    fn schema(&self) -> tantivy::schema::Schema;

    /// Return the number of segments and max_doc per segment.
    /// Used during planning to determine partition count.
    /// Returns empty vec if unknown (single-partition fallback).
    fn segment_sizes(&self) -> Vec<u32>;
}
```

**Why `schema()` and `segment_sizes()` are separate from `open()`:** Planning needs schema and segment info synchronously (in `TableProvider::scan()`). We don't want to force an async index open just to count segments. For the distributed case, this info comes from metadata (the coordinator knows the schema from the metastore, and segment count can be derived from split metadata or defaulted to 1 segment per split).

### `DirectIndexOpener` — backward compat for tests and local usage

```rust
// In tantivy-datafusion/src/index_opener.rs

/// Opens an already-opened Index. Used for tests and local (non-distributed) usage.
/// This is the zero-cost path — schema() and segment_sizes() read from the Index directly.
#[derive(Debug, Clone)]
pub struct DirectIndexOpener {
    index: Index,
}

impl DirectIndexOpener {
    pub fn new(index: Index) -> Self {
        Self { index }
    }
}

#[async_trait]
impl IndexOpener for DirectIndexOpener {
    async fn open(&self) -> Result<Index> {
        Ok(self.index.clone())
    }

    fn schema(&self) -> tantivy::schema::Schema {
        self.index.schema()
    }

    fn segment_sizes(&self) -> Vec<u32> {
        match self.index.reader() {
            Ok(reader) => {
                let searcher = reader.searcher();
                (0..searcher.segment_readers().len())
                    .map(|i| searcher.segment_reader(i as u32).max_doc())
                    .collect()
            }
            Err(_) => vec![],
        }
    }
}
```

### Convenience constructor on providers

To keep the existing API ergonomic for tests:

```rust
impl TantivyTableProvider {
    /// Create from an already-opened Index (backward compat).
    pub fn new(index: Index) -> Self {
        Self::from_opener(Arc::new(DirectIndexOpener::new(index)))
    }

    /// Create from an IndexOpener (for distributed execution).
    pub fn from_opener(opener: Arc<dyn IndexOpener>) -> Self {
        let tantivy_schema = opener.schema();
        let arrow_schema = tantivy_schema_to_arrow(&tantivy_schema);
        // Note: uses tantivy_schema_to_arrow (schema-only), not
        // tantivy_schema_to_arrow_from_index (needs opened index for cardinality).
        // Multi-valued detection deferred to execution time.
        Self {
            opener,
            arrow_schema,
            query: None,
            aggregations: None,
        }
    }
}
```

Same pattern for `TantivyInvertedIndexProvider::new(index)` / `from_opener(opener)` and `TantivyDocumentProvider::new(index)` / `from_opener(opener)`.

## Files to Change

### 1. New file: `src/index_opener.rs`

Contains:
- `IndexOpener` trait
- `DirectIndexOpener` struct + impl

~60 lines.

### 2. `src/table_provider.rs`

**Struct change:**
```rust
// Before:
pub struct TantivyTableProvider {
    index: Index,
    arrow_schema: SchemaRef,
    query: Option<Arc<dyn Query>>,
    aggregations: Option<Arc<Aggregations>>,
}

// After:
pub struct TantivyTableProvider {
    opener: Arc<dyn IndexOpener>,
    arrow_schema: SchemaRef,
    query: Option<Arc<dyn Query>>,
    aggregations: Option<Arc<Aggregations>>,
}
```

**`scan()` change:** Replace `self.index.reader()?.searcher().segment_readers().len()` with `self.opener.segment_sizes()`:

```rust
// Before:
let reader = self.index.reader()?;
let searcher = reader.searcher();
let num_segments = searcher.segment_readers().len();
// ... uses searcher.segment_reader(i).max_doc() for chunking

// After:
let segment_sizes = self.opener.segment_sizes();
let num_segments = segment_sizes.len().max(1);
// ... uses segment_sizes[i] for chunking
```

**DataSource change:** Replace `index: Index` with `opener: Arc<dyn IndexOpener>`:

```rust
// Before:
struct FastFieldDataSource {
    index: Index,
    ...
}

// After:
struct FastFieldDataSource {
    opener: Arc<dyn IndexOpener>,
    ...
}
```

**`open()` change:**
```rust
// Before:
fn open(&self, partition: usize, _ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
    let index = self.index.clone();
    let stream = stream::once(async move {
        generate_and_filter_batch(&index, ...)
    });
    ...
}

// After:
fn open(&self, partition: usize, _ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
    let opener = self.opener.clone();
    let stream = stream::once(async move {
        let index = opener.open().await?;
        generate_and_filter_batch(&index, ...)
    });
    ...
}
```

### 3. `src/inverted_index_provider.rs`

Same pattern as table_provider:
- Struct: `index: Index` → `opener: Arc<dyn IndexOpener>`
- Constructor: `new(index)` stays (wraps in DirectIndexOpener), add `from_opener(opener)`
- `scan()`: Use `opener.schema()` for field resolution and query parsing, `opener.segment_sizes()` for partition count
- `InvertedIndexDataSource`: `index: Index` → `opener: Arc<dyn IndexOpener>`
- `open()`: `opener.open().await?` inside the `stream::once(async { ... })`

### 4. `src/document_provider.rs`

Same pattern:
- Struct: `index: Index` → `opener: Arc<dyn IndexOpener>`
- Constructor: same
- `scan()`: `opener.segment_sizes()` for partition count
- `DocumentDataSource`: `index: Index` → `opener: Arc<dyn IndexOpener>`
- `open()`: `opener.open().await?` inside `stream::once(async { ... })`

### 5. `src/agg_exec.rs`

```rust
// Before:
pub(crate) struct TantivyAggregateExec {
    index: Index,
    ...
}

// After:
pub(crate) struct TantivyAggregateExec {
    opener: Arc<dyn IndexOpener>,
    ...
}
```

`execute()`: `opener.open().await?` inside the stream.

### 6. `src/agg_pushdown.rs`

The `AggPushdown` optimizer rule creates `TantivyAggregateExec`. It currently extracts the `Index` from `FastFieldDataSource`. Change to extract and pass the `opener` instead.

### 7. `src/filter_pushdown.rs`

The `FastFieldFilterPushdown` rule reads `index.schema()` from `InvertedIndexDataSource` to convert physical expressions to tantivy queries. Change to read `opener.schema()` instead.

### 8. `src/catalog.rs`

`TantivySchema::table()` currently opens the index and calls `TantivyTableProvider::new(index)`. Change to create a `DirectIndexOpener` and call `from_opener()`. (Behavior unchanged, just flows through the new path.)

### 9. `src/schema_mapping.rs`

`tantivy_schema_to_arrow_from_index()` needs an opened `Index` to detect multi-valued fields. This function is called in `TantivyTableProvider::new()`. With `from_opener()`, we only have the schema (not an opened index), so we fall back to `tantivy_schema_to_arrow()` (all scalar types). Multi-valued detection happens at execution time when the index is actually opened.

This is acceptable because:
- Multi-valued fields are uncommon in the quickwit use case
- The schema mismatch (scalar vs List) would cause a runtime error, not silent wrong results
- A future improvement can add `field_cardinalities()` to `IndexOpener` if needed

### 10. `src/lib.rs`

Add: `pub mod index_opener;` and `pub use index_opener::{IndexOpener, DirectIndexOpener};`

## What Does NOT Change

- `src/fast_field_reader.rs` — reads from an opened `Index`, called from `open()`. No change.
- `src/full_text_udf.rs` — pure UDF, no Index reference.
- `src/topk_pushdown.rs` — operates on plan structure, doesn't touch Index.
- `src/ordinal_group_by.rs` — operates on plan structure, doesn't touch Index.
- All existing tests — `TantivyTableProvider::new(index)` still works (wraps in DirectIndexOpener).

## Test Plan

### Existing tests pass unchanged

All tests use `TantivyTableProvider::new(index)` which now wraps in `DirectIndexOpener`. Behavior identical. Run full test suite, expect zero failures.

### New tests for IndexOpener

```rust
#[tokio::test]
async fn test_direct_opener_schema() {
    let index = create_test_index();
    let opener = DirectIndexOpener::new(index.clone());
    assert_eq!(opener.schema(), index.schema());
}

#[tokio::test]
async fn test_direct_opener_segment_sizes() {
    let index = create_test_index();  // single segment, 5 docs
    let opener = DirectIndexOpener::new(index);
    let sizes = opener.segment_sizes();
    assert_eq!(sizes.len(), 1);
    assert_eq!(sizes[0], 5);
}

#[tokio::test]
async fn test_direct_opener_multi_segment() {
    let index = create_multi_segment_test_index();  // 2 segments: 3 + 2 docs
    let opener = DirectIndexOpener::new(index);
    let sizes = opener.segment_sizes();
    assert_eq!(sizes.len(), 2);
    assert_eq!(sizes[0], 3);
    assert_eq!(sizes[1], 2);
}

#[tokio::test]
async fn test_from_opener_matches_new() {
    let index = create_test_index();
    let provider_old = TantivyTableProvider::new(index.clone());
    let provider_new = TantivyTableProvider::from_opener(Arc::new(DirectIndexOpener::new(index)));

    // Same schema
    assert_eq!(provider_old.schema(), provider_new.schema());

    // Same query results
    let ctx_old = SessionContext::new();
    ctx_old.register_table("t", Arc::new(provider_old)).unwrap();
    let old_result = ctx_old.sql("SELECT id, price FROM t ORDER BY id")
        .await.unwrap().collect().await.unwrap();

    let ctx_new = SessionContext::new();
    ctx_new.register_table("t", Arc::new(provider_new)).unwrap();
    let new_result = ctx_new.sql("SELECT id, price FROM t ORDER BY id")
        .await.unwrap().collect().await.unwrap();

    assert_eq!(old_result, new_result);
}

#[tokio::test]
async fn test_lazy_opener_called_at_execute_time() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let index = create_test_index();
    let opened = Arc::new(AtomicBool::new(false));
    let opened_clone = opened.clone();

    // Custom opener that tracks when open() is called
    struct TrackingOpener {
        inner: DirectIndexOpener,
        opened: Arc<AtomicBool>,
    }
    impl fmt::Debug for TrackingOpener {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "TrackingOpener")
        }
    }
    #[async_trait]
    impl IndexOpener for TrackingOpener {
        async fn open(&self) -> Result<Index> {
            self.opened.store(true, Ordering::SeqCst);
            self.inner.open().await
        }
        fn schema(&self) -> tantivy::schema::Schema { self.inner.schema() }
        fn segment_sizes(&self) -> Vec<u32> { self.inner.segment_sizes() }
    }

    let opener = Arc::new(TrackingOpener {
        inner: DirectIndexOpener::new(index),
        opened: opened_clone,
    });
    let provider = TantivyTableProvider::from_opener(opener);

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider)).unwrap();

    // After registration and planning, open() should NOT have been called
    let df = ctx.sql("SELECT id FROM t").await.unwrap();
    assert!(!opened.load(Ordering::SeqCst), "Index should not be opened during planning");

    // After collect(), open() SHOULD have been called
    let _batches = df.collect().await.unwrap();
    assert!(opened.load(Ordering::SeqCst), "Index should be opened during execution");
}
```

## Order of Implementation

1. Create `src/index_opener.rs` with `IndexOpener` trait + `DirectIndexOpener`
2. Update `src/lib.rs` exports
3. Change `src/table_provider.rs` (TantivyTableProvider + FastFieldDataSource)
4. Run existing tests — should pass
5. Change `src/inverted_index_provider.rs`
6. Change `src/document_provider.rs`
7. Run existing tests — should pass
8. Change `src/agg_exec.rs`
9. Change `src/agg_pushdown.rs` (pass opener instead of index)
10. Change `src/filter_pushdown.rs` (use opener.schema())
11. Change `src/catalog.rs`
12. Run full test suite
13. Add new IndexOpener-specific tests

Each step is independently compilable and testable. The `new(index)` constructor stays throughout, so existing tests never break.
