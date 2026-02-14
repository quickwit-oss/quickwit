# Distributed DataFusion Execution with Quickwit Metastore

## Scope and Constraints

- **No affinity-aware scheduling** (coming later, we own df-distributed)
- **Full-query retry** on failure, not per-split
- **Output is RecordBatches**, not ES-compat JSON (that's the goal)
- **Dynamic fields**: start with string/list-of-string, handle later
- **Thinking distributed from the ground up**
- **We own tantivy-df** — we can change it

---

## The Key Insight: Lazy Index Opening

TantivyDF providers currently take an opened `tantivy::Index`. But we own tantivy-df. Change them to take a lazy opener:

```rust
/// In tantivy-datafusion crate (open source, generic)
#[async_trait]
pub trait IndexOpener: Send + Sync + fmt::Debug {
    async fn open(&self) -> Result<Index>;

    /// Serializable description for distributed execution
    fn to_proto(&self) -> Vec<u8>;
    fn from_proto(bytes: &[u8]) -> Result<Arc<dyn IndexOpener>>;
}
```

**For tests / local usage (unchanged ergonomics):**
```rust
let index = Index::open_in_dir("my_index")?;
let provider = TantivyTableProvider::new(DirectIndexOpener::new(index));
```

**For distributed quickwit:**
```rust
let opener = SplitIndexOpener {
    split_id: "abc".into(),
    footer_offsets: 700..800,
    index_uri: "s3://bucket/index".into(),
    searcher_context: searcher_ctx.clone(),
    storage_resolver: storage_resolver.clone(),
    doc_mapper_str: doc_mapper_json.clone(),
};
let provider = TantivyTableProvider::new(Arc::new(opener));
```

### What This Gives Us

- **One plan, everywhere.** The coordinator builds the plan, optimizer rules fire, the same plan is serialized to workers and executed. No wrapper nodes, no re-planning, no codec magic.
- **Optimizer rules fire once, on the coordinator**, against native tantivy-df nodes — TopKPushdown, FastFieldFilterPushdown, AggPushdown, OrdinalGroupByOptimization all work as-is.
- **Workers just deserialize and execute.** `try_decode` reconstructs `SplitIndexOpener` from proto bytes (no I/O). `DataSource::open()` calls `opener.open().await` at stream poll time.

### Why It Works

The four tantivy-df optimizer rules (`FastFieldFilterPushdown`, `TopKPushdown`, `AggPushdown`, `OrdinalGroupByOptimization`) operate on **plan structure and metadata**, not on opened indexes. They check "is this a HashJoinExec with InvertedIndexDataSource?" and rearrange nodes. The index is only needed when `execute()` is called.

So: coordinator builds the plan → optimizer rules fire → plan is serialized (the `IndexOpener` serializes to its proto bytes) → worker deserializes → `execute()` calls `opener.open()` → tantivy-df reads fast fields/inverted index/stored fields.

---

## What Exists Today

### TantivyDF Three-Provider Architecture

```sql
SELECT f.ts, f.level, d._document, inv._score
FROM inv
JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord
JOIN d ON d._doc_id = f._doc_id AND d._segment_ord = f._segment_ord
WHERE full_text(inv.body, 'error') AND f.status >= 500
ORDER BY inv._score DESC LIMIT 10
```

| Provider | Reads | Key columns |
|----------|-------|-------------|
| `TantivyTableProvider` | Fast fields → Arrow | `_doc_id`, `_segment_ord`, + fast field cols |
| `TantivyInvertedIndexProvider` | Inverted index → doc IDs + BM25 | `_doc_id`, `_segment_ord`, `_score` |
| `TantivyDocumentProvider` | Stored fields → JSON | `_doc_id`, `_segment_ord`, `_document` |

Co-partitioned via `Hash([_doc_id, _segment_ord], num_segments)` → shuffle-free joins.

Optimizer rules:
- **FastFieldFilterPushdown** — merges fast field predicates into inverted index query
- **TopKPushdown** — pushes `ORDER BY _score DESC LIMIT K` into Block-WAND
- **AggPushdown** — replaces DF AggregateExec with native tantivy aggregation
- **OrdinalGroupByOptimization** — dense ordinal array for dictionary GROUP BY

Dynamic filtering: hash join build side pushes `DynamicFilterPhysicalExpr` into probe side. `DocumentDataSource` only reads stored fields for doc IDs surviving filter + TopK.

### Schema Chain

```
Metastore.list_indexes_metadata(index_id)
  → IndexMetadata.index_config.doc_mapping
    → build_doc_mapper(&doc_mapping, &search_settings)
      → DocMapper.schema()           ← tantivy::Schema (full type info)
        → tantivy_schema_to_arrow()  ← Arrow SchemaRef (fast fields, 8 types)
```

Complete. All field types known from the metastore without opening any splits.

---

## Why Two Metastore Calls

Both on the coordinator. Both before the physical plan is built.

**Call 1: `list_indexes_metadata(index_id_patterns)`** — in `SchemaProvider::table()` [async]

Returns the index schema. Needed to:
- Build the `DocMapper` → tantivy `Schema` → Arrow schema
- Know the timestamp field (for extracting time-range filters from the query)
- Know the tag fields (for extracting tag filters)
- Get the index URI (where splits live in storage)

**Call 2: `list_splits(index_uid, time_range, tags)`** — in `TableProvider::scan()` [async]

Returns which splits match the query. Depends on call 1:
- Needs `IndexUid` to scope the query
- Needs `DocMapper` to extract tag filters from the query AST
- Needs timestamp field name to extract time-range bounds

**Structural, not accidental.** DataFusion has exactly two async hooks before the physical plan: `SchemaProvider::table()` and `TableProvider::scan()`. The dependency chain (schema → parse query → extract filters → prune splits) maps to (call 1 → call 2).

---

## Full Architecture

```
                          ┌─────────────────────┐
                          │   Query              │
                          │   (SQL or SearchReq) │
                          └──────────┬──────────┘
                                     │
                                     ▼
┌────────────────────────────────────────────────────────────────────┐
│                     COORDINATOR                                     │
│                                                                     │
│  SchemaProvider::table("logs")  [async]                             │
│    │                                                                │
│    ├── Metastore: list_indexes_metadata("logs")                     │
│    │   → IndexMetadata → DocMapper → Arrow schema                   │
│    │                                                                │
│    └── Returns QuickwitTableProvider                                │
│                                                                     │
│  QuickwitTableProvider::scan(projection, filters, limit)  [async]  │
│    │                                                                │
│    ├── Extract time/tag filters from DF Exprs                       │
│    ├── Metastore: list_splits(index_uid, time, tags)                │
│    │   → [split_abc, split_def, split_ghi]                          │
│    │                                                                │
│    ├── For each split, create a SplitIndexOpener                    │
│    │   (carries: split_id, footer_offsets, index_uri, doc_mapper)   │
│    │                                                                │
│    └── Build tantivy-df plan with openers:                          │
│         Register per-split providers (with lazy openers):           │
│           TantivyTableProvider::new(opener_abc)                     │
│           TantivyInvertedIndexProvider::new(opener_abc)             │
│           TantivyDocumentProvider::new(opener_abc)                  │
│         Build SQL/DataFrame with joins                              │
│         → Returns native tantivy-df exec nodes                      │
│                                                                     │
│  DF physical optimizer (coordinator-side):                          │
│    ALL tantivy-df rules fire here on native nodes:                  │
│    ✓ FastFieldFilterPushdown                                        │
│    ✓ TopKPushdown (Block-WAND)                                      │
│    ✓ AggPushdown                                                    │
│    ✓ OrdinalGroupByOptimization                                     │
│                                                                     │
│  df-distributed optimizer:                                          │
│    Sees N partitions → N tasks on workers                           │
│    Inserts NetworkCoalesceExec                                      │
│                                                                     │
│  Final coordinator plan:                                            │
│                                                                     │
│  SortPreservingMergeExec(ts DESC, fetch=10)                         │
│    └── NetworkCoalesceExec                                          │
│          ├── task 0 ──────────────────────────────────────────       │
│          │   HashJoinExec(INNER)                                    │
│          │     build: InvertedIndexDataSource(opener=abc)            │
│          │            query: "error" AND status>=500                 │
│          │            topk: 10, Block-WAND                          │
│          │     probe: FastFieldDataSource(opener=abc)                │
│          │            pushed_filters: [DynamicFilter]                │
│          │                                                          │
│          ├── task 1: (same, opener=def) ───────────────────          │
│          └── task 2: (same, opener=ghi) ───────────────────          │
│                                                                     │
│  Serialize via PhysicalExtensionCodec:                              │
│    tantivy-df nodes serialize cleanly because IndexOpener           │
│    is just metadata (split_id, footer_offsets, index_uri)           │
│                                                                     │
└──────────────────┬─────────────────┬─────────────────┬──────────────┘
                   │                 │                 │
                   ▼                 ▼                 ▼
┌──────────────────────┐ ┌──────────────────────┐ ┌──────────────────────┐
│      WORKER A        │ │      WORKER B        │ │      WORKER C        │
│                      │ │                      │ │                      │
│ try_decode:          │ │ try_decode:          │ │ try_decode:          │
│   pure deser,        │ │   pure deser,        │ │   pure deser,        │
│   reconstructs       │ │   reconstructs       │ │   reconstructs       │
│   SplitIndexOpener   │ │   SplitIndexOpener   │ │   SplitIndexOpener   │
│   from proto bytes   │ │   from proto bytes   │ │   from proto bytes   │
│                      │ │                      │ │                      │
│ execute():           │ │ execute():           │ │ execute():           │
│                      │ │                      │ │                      │
│ InvertedIndex        │ │ InvertedIndex        │ │ InvertedIndex        │
│ DataSource           │ │ DataSource           │ │ DataSource           │
│   .open()            │ │   .open()            │ │   .open()            │
│     → open_index_    │ │     → open_index_    │ │     → open_index_    │
│       with_caches()  │ │       with_caches()  │ │       with_caches()  │
│     → warmup()       │ │     → warmup()       │ │     → warmup()       │
│     → read inverted  │ │     → read inverted  │ │     → read inverted  │
│       index          │ │       index          │ │       index          │
│                      │ │                      │ │                      │
│ FastField            │ │ FastField            │ │ FastField            │
│ DataSource           │ │ DataSource           │ │ DataSource           │
│   .open()            │ │   .open()            │ │   .open()            │
│     → (same index,   │ │     → (same index,   │ │     → (same index,   │
│        already open)  │ │        already open)  │ │        already open)  │
│     → read fast      │ │     → read fast      │ │     → read fast      │
│       fields → Arrow │ │       fields → Arrow │ │       fields → Arrow │
│                      │ │                      │ │                      │
│ HashJoinExec         │ │ HashJoinExec         │ │ HashJoinExec         │
│   DynamicFilter ──►  │ │   DynamicFilter ──►  │ │   DynamicFilter ──►  │
│   probe only reads   │ │   probe only reads   │ │   probe only reads   │
│   matching docs      │ │   matching docs      │ │   matching docs      │
│                      │ │                      │ │                      │
│       ▼              │ │       ▼              │ │       ▼              │
│ RecordBatches        │ │ RecordBatches        │ │ RecordBatches        │
│ → Flight stream      │ │ → Flight stream      │ │ → Flight stream      │
└──────────┬───────────┘ └──────────┬───────────┘ └──────────┬───────────┘
           │                        │                        │
           └────────────┬───────────┘                        │
                        └──────────┬─────────────────────────┘
                                   ▼
                         ┌──────────────────┐
                         │   COORDINATOR    │
                         │                  │
                         │ SortPreserving   │
                         │ MergeExec        │
                         │ (ts DESC, k=10)  │
                         │                  │
                         │       ▼          │
                         │ RecordBatches    │
                         └──────────────────┘
```

### What Changes in tantivy-df

```rust
// Before:
pub struct TantivyTableProvider {
    index: Index,  // ← opened, not serializable
    ...
}

// After:
pub struct TantivyTableProvider {
    opener: Arc<dyn IndexOpener>,  // ← lazy, serializable
    ...
}

// IndexOpener trait (in tantivy-datafusion crate):
#[async_trait]
pub trait IndexOpener: Send + Sync + fmt::Debug {
    /// Open the tantivy index. Called lazily on first execute().
    /// May be called multiple times (providers share an opener that caches).
    async fn open(&self) -> Result<Index>;
}

// For tests (backward compat):
pub struct DirectIndexOpener { index: Index }
impl IndexOpener for DirectIndexOpener {
    async fn open(&self) -> Result<Index> { Ok(self.index.clone()) }
}

// For quickwit distributed (in quickwit-datafusion crate):
pub struct SplitIndexOpener {
    split_id: String,
    footer_offsets: Range<u64>,
    index_uri: String,
    doc_mapper_str: String,
    searcher_context: Arc<SearcherContext>,
    storage_resolver: StorageResolver,
    opened: OnceCell<Index>,  // cache the opened index
}
impl IndexOpener for SplitIndexOpener {
    async fn open(&self) -> Result<Index> {
        self.opened.get_or_try_init(|| async {
            let storage = self.storage_resolver.resolve(&self.index_uri).await?;
            let offsets = SplitIdAndFooterOffsets { split_id: self.split_id.clone(), ... };
            let (index, _) = open_index_with_caches(&self.searcher_context, storage, &offsets, ...).await?;
            // warmup happens here too
            Ok(index)
        }).await.cloned()
    }
}
```

### Where open() Gets Called: DataSource::open() at Stream Poll Time

The tantivy-df `DataSource::open()` method is **sync** but wraps its work in `stream::once(async move { ... })`. The actual I/O happens lazily when DataFusion polls the stream. This is where `opener.open().await` goes:

```rust
// FastFieldDataSource today:
fn open(&self, partition: usize, _ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
    let index = self.index.clone();  // ← already opened
    let stream = stream::once(async move {
        generate_and_filter_batch(&index, ...)  // ← reads fast fields
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

// FastFieldDataSource with IndexOpener:
fn open(&self, partition: usize, _ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
    let opener = self.opener.clone();  // ← not yet opened
    let stream = stream::once(async move {
        let index = opener.open().await?;       // ← opens here, async, at poll time
        generate_and_filter_batch(&index, ...)  // ← reads fast fields
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}
```

Same pattern for `InvertedIndexDataSource` and `DocumentDataSource` — all three already use `stream::once(async move { ... })`, so the opener call slots in naturally.

### Full Execution Chain on the Worker

```
Arrow Flight receives serialized plan bytes
  │
  ▼
PhysicalExtensionCodec::try_decode()
  │  Pure deserialization. Reconstructs SplitIndexOpener from proto bytes.
  │  No I/O. No index opening.
  │
  ▼
DataSourceExec { data_source: FastFieldDataSource { opener: SplitIndexOpener { split_id: "abc", ... } } }
DataSourceExec { data_source: InvertedIndexDataSource { opener: SplitIndexOpener { split_id: "abc", ... } } }
  │  (same Arc<SplitIndexOpener> shared by all three providers for same split)
  │
  ▼
DataFusion calls DataSourceExec::execute(partition=0)
  │
  ▼
FastFieldDataSource::open(partition=0)           [sync — returns a lazy stream]
  │
  ▼
stream::once(async move {                        [async — runs when DF polls the stream]
  │
  ├── opener.open().await                        ← SplitIndexOpener::open()
  │     ├── storage_resolver.resolve(index_uri)       async: resolve S3/RAM storage
  │     ├── open_index_with_caches(ctx, storage, split_offsets, tokenizer_mgr)
  │     │     async: fetch footer, open BundleStorage → HotDirectory → tantivy::Index
  │     ├── warmup(searcher, warmup_info)              async: prefetch byte ranges
  │     └── return Index (cached in OnceCell for other providers)
  │
  ├── generate_and_filter_batch(&index, ...)     ← reads fast fields → Arrow RecordBatch
  │
  └── return RecordBatch
})
```

The `OnceCell` in `SplitIndexOpener` ensures the index is opened exactly once even though `FastFieldDataSource`, `InvertedIndexDataSource`, and `DocumentDataSource` all call `opener.open().await`. Whichever provider polls first triggers the actual opening; the others get the cached result.
```

Multiple providers sharing the same `Arc<SplitIndexOpener>` open the index once (via `OnceCell`). The index is opened on first `execute()` call, not during planning.

### PhysicalExtensionCodec

```rust
// In tantivy-datafusion: provide a trait for serializing IndexOpener
pub trait IndexOpenerCodec: Send + Sync {
    fn encode(&self, opener: &dyn IndexOpener) -> Result<Vec<u8>>;
    fn decode(&self, bytes: &[u8]) -> Result<Arc<dyn IndexOpener>>;
}

// In quickwit-datafusion: implement it
impl IndexOpenerCodec for QuickwitOpenerCodec {
    fn encode(&self, opener: &dyn IndexOpener) -> Result<Vec<u8>> {
        let split_opener = opener.downcast_ref::<SplitIndexOpener>()?;
        // serialize split_id, footer_offsets, index_uri, doc_mapper_str
    }
    fn decode(&self, bytes: &[u8]) -> Result<Arc<dyn IndexOpener>> {
        // deserialize → SplitIndexOpener (no I/O, just struct construction)
    }
}
```

### Properties of This Design

| Concern | How it's handled |
|---------|-----------------|
| **Planning** | One phase, on coordinator. No worker re-planning. |
| **Optimizer rules** | Fire once on coordinator against native tantivy-df nodes. |
| **Custom exec nodes** | None. Plan contains only stock DF + tantivy-df `DataSourceExec` nodes. |
| **Serialization** | `IndexOpener` serializes to proto bytes (split_id, footer_offsets, index_uri). Pure data. |
| **try_decode** | Reconstructs `SplitIndexOpener` struct from proto. No I/O. |
| **Index opening** | In `DataSource::open()` → `stream::once(async { opener.open().await })`. On worker, at poll time. |
| **Plan identity** | Same plan on coordinator, on the wire, and on worker. |

---

## Concrete Code: What Gets Built

### Change to tantivy-df (small)

1. Add `IndexOpener` trait
2. Change providers to take `Arc<dyn IndexOpener>` instead of `Index`
3. Add `DirectIndexOpener` for backward compat
4. Add `IndexOpenerCodec` trait for serialization
5. In `DataSource::open()` / `execute()`: call `opener.open().await` before reading

### New in quickwit-datafusion crate

1. `SplitIndexOpener` — implements `IndexOpener`, calls `open_index_with_caches()`
2. `QuickwitOpenerCodec` — implements `IndexOpenerCodec`, serializes split metadata
3. `QuickwitSchemaProvider` — implements `SchemaProvider::table()`, metastore call 1
4. `QuickwitTableProvider` — implements `TableProvider::scan()`, metastore call 2, creates providers with `SplitIndexOpener`
5. `QuickwitPhysicalExtensionCodec` — wraps tantivy-df codec + `QuickwitOpenerCodec`

### Test (same for single-node and distributed — same plan)

```rust
#[tokio::test]
async fn test_df_distributed_plan() {
    let sandbox = TestSandbox::create("test-df", DOC_MAPPING, "{}", &["body"]).await.unwrap();
    sandbox.add_documents(vec![
        json!({"body": "hello world", "ts": "2024-01-01T00:00:00Z", "level": "INFO"}),
        json!({"body": "error occurred", "ts": "2024-01-02T00:00:00Z", "level": "ERROR"}),
    ]).await.unwrap();

    // Build session with quickwit catalog (metastore-backed)
    let ctx = SessionContext::new();
    ctx.register_udf(full_text_udf());
    // ... register tantivy-df optimizer rules

    let schema_provider = QuickwitSchemaProvider::new(
        sandbox.metastore(),
        sandbox.storage_resolver(),
        searcher_context.clone(),
    );
    ctx.register_catalog("quickwit", Arc::new(QuickwitCatalog::new(schema_provider)));

    // This query triggers:
    // 1. SchemaProvider::table() → metastore call 1 → schema
    // 2. TableProvider::scan() → metastore call 2 → splits → SplitIndexOpeners
    // 3. Optimizer rules fire on native tantivy-df nodes
    // 4. execute() → opener.open() → open_index_with_caches → read
    let batches = ctx.sql("
        SELECT f.ts, f.level
        FROM quickwit.default.logs_inv inv
        JOIN quickwit.default.logs f
          ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord
        WHERE full_text(inv.body, 'error')
    ").await.unwrap().collect().await.unwrap();

    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}
```

For distributed: add `df-distributed` extensions to the SessionContext. The plan is the same — just gets serialized and sent to workers.

---

## What We Don't Build

- ES-compat aggregation JSON format (output is RecordBatches)
- Scroll/cursor pagination
- Snippet UDF
- Affinity-aware worker scheduling (later, in df-distributed)
- Per-split retry (later, in worker provider)
- Dynamic field type inference (use string for now)
- SearchRequest → DF plan translation (separate step)
