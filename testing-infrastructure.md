# Testing Infrastructure for Distributed DataFusion + Quickwit

## Goals

1. **Plan verification tests** — confirm the full physical plan structure (optimizer rules fired, correct node composition)
2. **Multi-worker execution tests** — simulate multiple workers with their own tantivy splits, execute queries across pseudo-workers locally
3. **Correctness tests** — same query returns same results in single-node vs distributed execution

---

## Building Blocks from Each Codebase

### df-distributed: In-Process Workers via `start_localhost_context`

df-distributed tests spin up real Arrow Flight gRPC servers on random localhost ports:

```rust
// From df-distributed/src/test_utils/localhost.rs
let (ctx, _guard, workers) = start_localhost_context(3, DefaultSessionBuilder).await;
```

This creates:
- A `SessionContext` configured with `DistributedPhysicalOptimizerRule` and a `WorkerResolver` pointing at the 3 workers
- 3 `Worker` instances, each running a Tonic Flight server on a random port
- A `_guard` that shuts down workers when dropped

Custom worker state (for custom codecs) via `WorkerSessionBuilder`:

```rust
async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
    Ok(ctx.builder
        .with_distributed_user_codec(MyCodec)
        .build())
}
let (ctx, _guard, workers) = start_localhost_context(3, build_state).await;
ctx.set_distributed_user_codec(MyCodec);
```

### TantivyDF: Plan Assertions via EXPLAIN

```rust
fn plan_to_string(batches: &[RecordBatch]) -> String {
    let batch = collect_batches(batches);
    let plan_col = batch.column(1).as_string::<i32>();
    (0..batch.num_rows())
        .map(|i| plan_col.value(i))
        .collect::<Vec<_>>()
        .join("\n")
}

// Usage:
let df = ctx.sql("EXPLAIN SELECT ...").await?;
let plan = plan_to_string(&df.collect().await?);
assert!(plan.contains("InvertedIndexDataSource(segments=1, query=true, topk=Some(1))"));
```

TantivyDF also uses exact string matching on the physical plan portion of EXPLAIN output.

### TantivyDF: Test Index Creation

```rust
// Single segment (5 docs, 8 field types)
let index = create_test_index();

// Two segments (3 + 2 docs, different ordinal dictionaries)
let index = create_multi_segment_test_index();

// With deleted documents
let index = create_test_index_with_deletes();
```

### Quickwit: TestSandbox (Real Splits in RAM)

```rust
let sandbox = TestSandbox::create("my-index", doc_mapping_yaml, "{}", &["body"]).await?;
sandbox.add_documents(vec![json!({...}), json!({...})]).await?;  // creates 1 split
sandbox.add_documents(vec![json!({...})]).await?;                 // creates another split

// Access:
sandbox.metastore()          // MetastoreServiceClient
sandbox.storage()            // Arc<dyn Storage> (RAM)
sandbox.doc_mapper()         // Arc<DocMapper>
sandbox.storage_resolver()   // StorageResolver
sandbox.index_uid()          // IndexUid
```

Each `add_documents()` call creates one real tantivy split in RamStorage, published in the metastore.

### Quickwit: Opening a Split Directly

```rust
// From tests.rs:1012-1052 — bypasses root_search, calls leaf directly
let splits = sandbox.metastore()
    .list_splits(ListSplitsRequest::try_from_index_uid(sandbox.index_uid()).unwrap())
    .await?.collect_splits().await?;
let split_offsets: Vec<_> = splits.iter()
    .map(|s| extract_split_and_footer_offsets(&s.split_metadata))
    .collect();

let searcher_ctx = Arc::new(SearcherContext::new(SearcherConfig::default(), None));

// Open the tantivy::Index from the split
let (index, _) = open_index_with_caches(
    &searcher_ctx, sandbox.storage(), &split_offsets[0],
    Some(sandbox.doc_mapper().tokenizer_manager()), None,
).await?;
// `index` is now a standard tantivy::Index — hand it to TantivyDF
```

---

## Test Layer 1: Plan Verification

Verify the physical plan structure to confirm optimizer rules fire correctly and nodes compose as expected.

### 1a. Single-Split Plan Structure

```rust
#[tokio::test]
async fn test_plan_single_split_fast_field_only() {
    let sandbox = TestSandbox::create("plan-test", DOC_MAPPING, "{}", &["body"]).await.unwrap();
    sandbox.add_documents(test_docs()).await.unwrap();

    // Open split, create tantivy-df providers
    let (index, _) = open_split(&sandbox, 0).await;
    let ctx = create_tantivy_df_session(&index);

    // Verify plan structure
    let df = ctx.sql("EXPLAIN SELECT f.ts, f.level FROM f WHERE f.level = 'ERROR'").await.unwrap();
    let plan = plan_to_string(&df.collect().await.unwrap());

    // Should show FastFieldDataSource with pushed filter
    assert!(plan.contains("FastFieldDataSource"));
    assert!(!plan.contains("InvertedIndexDataSource"));  // no text search = no inverted index
}

#[tokio::test]
async fn test_plan_full_text_with_topk() {
    let (index, _) = open_split(&sandbox, 0).await;
    let ctx = create_tantivy_df_session_with_rules(&index);  // all 4 optimizer rules

    let df = ctx.sql("EXPLAIN
        SELECT f.ts, inv._score
        FROM inv
        JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord
        WHERE full_text(inv.body, 'error')
        ORDER BY inv._score DESC LIMIT 10
    ").await.unwrap();
    let plan = plan_to_string(&df.collect().await.unwrap());

    // TopKPushdown should have fired
    assert!(plan.contains("topk=Some(10)"));
    // InvertedIndexDataSource should have the query
    assert!(plan.contains("InvertedIndexDataSource(segments=1, query=true"));
}

#[tokio::test]
async fn test_plan_filter_pushdown_merges_into_inverted() {
    let (index, _) = open_split(&sandbox, 0).await;
    let ctx = create_tantivy_df_session_with_rules(&index);

    let df = ctx.sql("EXPLAIN
        SELECT f.ts
        FROM inv
        JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord
        WHERE full_text(inv.body, 'error') AND f.status >= 500
    ").await.unwrap();
    let plan = plan_to_string(&df.collect().await.unwrap());

    // FastFieldFilterPushdown should have moved status>=500 into inverted index
    // FastFieldDataSource should only have DynamicFilter (no user predicates)
    assert!(plan.contains("pushed_filters=[DynamicFilter"));
    assert!(!plan.contains("status"));  // status filter no longer on fast field side
}
```

### 1b. Multi-Split Plan Structure

```rust
#[tokio::test]
async fn test_plan_multi_split_partitioning() {
    let sandbox = TestSandbox::create("multi-plan", DOC_MAPPING, "{}", &["body"]).await.unwrap();
    sandbox.add_documents(docs_batch_1()).await.unwrap();  // split 1
    sandbox.add_documents(docs_batch_2()).await.unwrap();  // split 2
    sandbox.add_documents(docs_batch_3()).await.unwrap();  // split 3

    // Open all 3 splits
    let indexes = open_all_splits(&sandbox).await;  // Vec<(Index, SplitMetadata)>

    // Create session with union of all splits
    let ctx = SessionContext::new();
    // Register per-split providers, or a QuickwitTableProvider that unions them
    for (i, (index, _)) in indexes.iter().enumerate() {
        ctx.register_table(&format!("f_{i}"), Arc::new(TantivyTableProvider::new(index.clone()))).unwrap();
    }

    // Verify the plan has N partitions
    let df = ctx.sql("EXPLAIN SELECT * FROM f_0 UNION ALL SELECT * FROM f_1 UNION ALL SELECT * FROM f_2").await.unwrap();
    let plan = plan_to_string(&df.collect().await.unwrap());

    // Should show 3 DataSourceExec nodes
    let datasource_count = plan.matches("DataSourceExec").count();
    assert_eq!(datasource_count, 3);
}
```

### 1c. df-distributed Plan Structure (Snapshot Tests)

Using df-distributed's `display_plan_ascii` for full distributed plan verification:

```rust
#[tokio::test]
async fn test_distributed_plan_structure() {
    // Start workers with tantivy-df support
    let (ctx, _guard, _workers) = start_localhost_context(3, build_tantivy_worker_state).await;
    ctx.set_distributed_user_codec(TantivyIndexOpenerCodec::new(storage_resolver, searcher_ctx));

    // Register QuickwitTableProvider (metastore-backed)
    register_quickwit_table(&ctx, &sandbox).await;

    let df = ctx.sql("SELECT ts, level FROM logs WHERE level = 'ERROR' ORDER BY ts DESC LIMIT 10").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let plan_str = display_plan_ascii(plan.as_ref(), false);

    // Snapshot test: verify stage decomposition
    assert_snapshot!(plan_str, @r"
    ┌───── DistributedExec ── Tasks: t0:[p0]
    │ SortPreservingMergeExec: [ts DESC], fetch=10
    │   [Stage 1] => NetworkCoalesceExec: ...
    └──────────────────────────────────────
      ┌───── Stage 1 ── Tasks: t0:[p0] t1:[p0] t2:[p0]
      │ SortExec: TopK(fetch=10), expr=[ts DESC]
      │   DataSourceExec: FastFieldDataSource(...)
      └──────────────────────────────────────
    ");
}
```

---

## Test Layer 2: Multi-Worker Execution with Real Tantivy Splits

### The Pattern

1. Use `TestSandbox` to create N splits with known, distinct documents
2. Start N in-process df-distributed workers
3. Each worker opens its assigned split via `IndexOpener`
4. Execute a query through the coordinator
5. Verify results match expected output

### Core Infrastructure

```rust
/// Create a TestSandbox with N splits, each containing distinct documents.
async fn create_multi_split_sandbox(
    num_splits: usize,
) -> (TestSandbox, Vec<SplitMetadata>) {
    let sandbox = TestSandbox::create("distributed-test", DOC_MAPPING, "{}", &["body"]).await.unwrap();

    for i in 0..num_splits {
        sandbox.add_documents(vec![
            json!({"body": format!("document {} in split {}", 1, i), "ts": format!("2024-01-0{}T00:00:00Z", i+1), "level": "INFO", "status": 200}),
            json!({"body": format!("error {} in split {}", 2, i), "ts": format!("2024-01-0{}T01:00:00Z", i+1), "level": "ERROR", "status": 500}),
        ]).await.unwrap();
    }

    let splits = sandbox.metastore()
        .list_splits(ListSplitsRequest::try_from_index_uid(sandbox.index_uid()).unwrap())
        .await.unwrap()
        .collect_splits().await.unwrap()
        .into_iter().map(|s| s.split_metadata).collect();

    (sandbox, splits)
}

/// Worker session builder that knows how to open quickwit splits.
async fn build_tantivy_worker_state(
    ctx: WorkerQueryContext,
) -> Result<SessionState, DataFusionError> {
    Ok(ctx.builder
        .with_distributed_user_codec(QuickwitIndexOpenerCodec::new(
            storage_resolver.clone(),
            searcher_context.clone(),
        ))
        .build())
}

/// Helper to open a split from a TestSandbox and create tantivy-df providers.
async fn open_split(sandbox: &TestSandbox, split_idx: usize) -> (Index, SplitMetadata) {
    let splits = sandbox.metastore()
        .list_splits(ListSplitsRequest::try_from_index_uid(sandbox.index_uid()).unwrap())
        .await.unwrap().collect_splits().await.unwrap();
    let split = &splits[split_idx].split_metadata;
    let offsets = extract_split_and_footer_offsets(split);
    let searcher_ctx = Arc::new(SearcherContext::new(SearcherConfig::default(), None));
    let (index, _) = open_index_with_caches(
        &searcher_ctx, sandbox.storage(), &offsets,
        Some(sandbox.doc_mapper().tokenizer_manager()), None,
    ).await.unwrap();
    (index, split.clone())
}

/// Helper to set up a session with all tantivy-df providers + optimizer rules for a single index.
fn create_tantivy_df_session(index: &Index) -> SessionContext {
    let num_segments = index.searchable_segments().unwrap().len();
    let config = SessionConfig::new().with_target_partitions(num_segments.max(1));
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(FastFieldFilterPushdown::new()))
        .with_physical_optimizer_rule(Arc::new(TopKPushdown::new()))
        .with_physical_optimizer_rule(Arc::new(AggPushdown::new()))
        .with_physical_optimizer_rule(Arc::new(OrdinalGroupByOptimization::new()))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_udf(full_text_udf());
    ctx.register_table("f", Arc::new(TantivyTableProvider::new(index.clone()))).unwrap();
    ctx.register_table("inv", Arc::new(TantivyInvertedIndexProvider::new(index.clone()))).unwrap();
    ctx.register_table("d", Arc::new(TantivyDocumentProvider::new(index.clone()))).unwrap();
    ctx
}
```

### 2a. Single-Node Correctness (Baseline)

```rust
#[tokio::test]
async fn test_single_node_fast_field_query() {
    let (sandbox, splits) = create_multi_split_sandbox(3).await;
    let (index, _) = open_split(&sandbox, 0).await;
    let ctx = create_tantivy_df_session(&index);

    let batches = ctx.sql("SELECT ts, level FROM f WHERE level = 'ERROR'")
        .await.unwrap().collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 1);  // one ERROR doc per split
    sandbox.assert_quit().await;
}

#[tokio::test]
async fn test_single_node_full_text_search() {
    let (sandbox, _) = create_multi_split_sandbox(1).await;
    let (index, _) = open_split(&sandbox, 0).await;
    let ctx = create_tantivy_df_session(&index);

    let batches = ctx.sql("
        SELECT f.ts, f.level
        FROM inv
        JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord
        WHERE full_text(inv.body, 'error')
    ").await.unwrap().collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 1);
    sandbox.assert_quit().await;
}

#[tokio::test]
async fn test_single_node_aggregation() {
    let (sandbox, _) = create_multi_split_sandbox(1).await;
    let (index, _) = open_split(&sandbox, 0).await;
    let ctx = create_tantivy_df_session(&index);

    let batches = ctx.sql("SELECT level, COUNT(*) as cnt FROM f GROUP BY level ORDER BY cnt DESC")
        .await.unwrap().collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 2);  // INFO and ERROR
    sandbox.assert_quit().await;
}
```

### 2b. Multi-Worker Distributed Execution

```rust
#[tokio::test]
async fn test_distributed_fast_field_query_across_splits() {
    let (sandbox, splits) = create_multi_split_sandbox(3).await;

    // Start 3 workers, each will handle one split
    let (ctx, _guard, _workers) = start_localhost_context(3, |wctx| async move {
        Ok(wctx.builder
            .with_distributed_user_codec(QuickwitIndexOpenerCodec::new(
                sandbox.storage_resolver(),
                Arc::new(SearcherContext::new(SearcherConfig::default(), None)),
            ))
            .build())
    }).await;
    ctx.set_distributed_user_codec(QuickwitIndexOpenerCodec::new(...));

    // Register QuickwitTableProvider — calls metastore, returns SplitIndexOpeners
    let provider = QuickwitTableProvider::new(
        sandbox.metastore(), sandbox.storage_resolver(),
        Arc::new(SearcherContext::new(SearcherConfig::default(), None)),
        sandbox.index_uid(),
    );
    ctx.register_table("logs", Arc::new(provider)).unwrap();

    // Query across all 3 splits
    let batches = ctx.sql("SELECT ts, level FROM logs WHERE level = 'ERROR' ORDER BY ts")
        .await.unwrap().collect().await.unwrap();
    let batch = collect_batches(&batches);

    // 3 splits × 1 ERROR doc each = 3 results
    assert_eq!(batch.num_rows(), 3);
}

#[tokio::test]
async fn test_distributed_aggregation_across_splits() {
    let (sandbox, _) = create_multi_split_sandbox(3).await;
    let (ctx, _guard, _) = start_distributed_context(&sandbox, 3).await;

    let batches = ctx.sql("
        SELECT level, COUNT(*) as cnt FROM logs GROUP BY level ORDER BY level
    ").await.unwrap().collect().await.unwrap();
    let batch = collect_batches(&batches);

    // 3 splits × 2 docs each: 3 ERROR + 3 INFO
    assert_eq!(batch.num_rows(), 2);
    // Verify counts
    let counts: Vec<i64> = batch.column_by_name("cnt").unwrap()
        .as_primitive::<Int64Type>().iter().map(|v| v.unwrap()).collect();
    assert_eq!(counts, vec![3, 3]);  // ERROR=3, INFO=3
}

#[tokio::test]
async fn test_distributed_topk_across_splits() {
    let (sandbox, _) = create_multi_split_sandbox(3).await;
    let (ctx, _guard, _) = start_distributed_context(&sandbox, 3).await;

    let batches = ctx.sql("
        SELECT ts, level FROM logs ORDER BY ts DESC LIMIT 2
    ").await.unwrap().collect().await.unwrap();
    let batch = collect_batches(&batches);

    // Global top-2 by timestamp across 3 splits
    assert_eq!(batch.num_rows(), 2);
}

#[tokio::test]
async fn test_distributed_full_text_with_topk() {
    let (sandbox, _) = create_multi_split_sandbox(3).await;
    let (ctx, _guard, _) = start_distributed_context(&sandbox, 3).await;

    let batches = ctx.sql("
        SELECT f.ts, inv._score
        FROM inv
        JOIN f ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord
        WHERE full_text(inv.body, 'error')
        ORDER BY inv._score DESC LIMIT 2
    ").await.unwrap().collect().await.unwrap();
    let batch = collect_batches(&batches);

    assert_eq!(batch.num_rows(), 2);
}
```

### 2c. Correctness: Local vs Distributed Same Results

```rust
#[tokio::test]
async fn test_local_vs_distributed_same_results() {
    let (sandbox, splits) = create_multi_split_sandbox(3).await;

    // Local: open all splits, query in one session
    let mut local_batches = Vec::new();
    for i in 0..splits.len() {
        let (index, _) = open_split(&sandbox, i).await;
        let ctx = create_tantivy_df_session(&index);
        let batches = ctx.sql("SELECT level, COUNT(*) as cnt FROM f GROUP BY level")
            .await.unwrap().collect().await.unwrap();
        local_batches.extend(batches);
    }
    // Manually merge local results (sum counts per level)

    // Distributed: query across all splits via workers
    let (ctx, _guard, _) = start_distributed_context(&sandbox, 3).await;
    let distributed_batches = ctx.sql("SELECT level, COUNT(*) as cnt FROM logs GROUP BY level ORDER BY level")
        .await.unwrap().collect().await.unwrap();

    // Compare
    assert_eq!(local_merged_result, distributed_result);
}
```

---

## Test Layer 3: Codec Serialization Round-Trip

Verify that `IndexOpener` serializes and deserializes correctly across the Flight boundary.

```rust
#[tokio::test]
async fn test_index_opener_codec_roundtrip() {
    let sandbox = TestSandbox::create("codec-test", DOC_MAPPING, "{}", &["body"]).await.unwrap();
    sandbox.add_documents(test_docs()).await.unwrap();

    let splits = get_splits(&sandbox).await;
    let opener = SplitIndexOpener::new(
        splits[0].split_id.clone(),
        splits[0].footer_offsets.clone(),
        sandbox.index_uid().index_id.to_string(),
        serde_json::to_string(&sandbox.doc_mapper()).unwrap(),
        Arc::new(SearcherContext::new(SearcherConfig::default(), None)),
        sandbox.storage_resolver(),
    );

    let codec = QuickwitIndexOpenerCodec::new(...);

    // Encode
    let mut buf = Vec::new();
    codec.encode(&opener, &mut buf).unwrap();

    // Decode
    let decoded_opener = codec.decode(&buf).unwrap();

    // Verify the decoded opener can actually open the index
    let index = decoded_opener.open().await.unwrap();
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    assert!(searcher.num_docs() > 0);

    sandbox.assert_quit().await;
}
```

---

## Test Helpers to Build

```rust
// Helper: batch concatenation
fn collect_batches(batches: &[RecordBatch]) -> RecordBatch {
    arrow::compute::concat_batches(&batches[0].schema(), batches).unwrap()
}

// Helper: extract plan string from EXPLAIN
fn plan_to_string(batches: &[RecordBatch]) -> String {
    let batch = collect_batches(batches);
    let plan_col = batch.column(1).as_string::<i32>();
    (0..batch.num_rows())
        .map(|i| plan_col.value(i))
        .collect::<Vec<_>>()
        .join("\n")
}

// Helper: open all splits from a sandbox
async fn open_all_splits(sandbox: &TestSandbox) -> Vec<(Index, SplitMetadata)> {
    let splits = sandbox.metastore()
        .list_splits(ListSplitsRequest::try_from_index_uid(sandbox.index_uid()).unwrap())
        .await.unwrap().collect_splits().await.unwrap();
    let searcher_ctx = Arc::new(SearcherContext::new(SearcherConfig::default(), None));
    let mut result = Vec::new();
    for split in splits {
        let offsets = extract_split_and_footer_offsets(&split.split_metadata);
        let (index, _) = open_index_with_caches(
            &searcher_ctx, sandbox.storage(), &offsets,
            Some(sandbox.doc_mapper().tokenizer_manager()), None,
        ).await.unwrap();
        result.push((index, split.split_metadata));
    }
    result
}

// Helper: start distributed context with quickwit split support
async fn start_distributed_context(
    sandbox: &TestSandbox,
    num_workers: usize,
) -> (SessionContext, impl Drop, Vec<Worker>) {
    let storage_resolver = sandbox.storage_resolver();
    let searcher_ctx = Arc::new(SearcherContext::new(SearcherConfig::default(), None));

    let (ctx, guard, workers) = start_localhost_context(num_workers, move |wctx| {
        let sr = storage_resolver.clone();
        let sc = searcher_ctx.clone();
        async move {
            Ok(wctx.builder
                .with_distributed_user_codec(QuickwitIndexOpenerCodec::new(sr, sc))
                .build())
        }
    }).await;

    ctx.set_distributed_user_codec(QuickwitIndexOpenerCodec::new(
        sandbox.storage_resolver(),
        Arc::new(SearcherContext::new(SearcherConfig::default(), None)),
    ));

    // Register table backed by metastore
    let provider = QuickwitTableProvider::from_sandbox(sandbox);
    ctx.register_table("logs", Arc::new(provider)).unwrap();

    (ctx, guard, workers)
}
```

---

## Test Matrix

| Test | Layer | Splits | Workers | What it verifies |
|------|-------|--------|---------|-----------------|
| `test_plan_single_split_fast_field_only` | Plan | 1 | 0 | FastFieldDataSource in plan, no inverted index |
| `test_plan_full_text_with_topk` | Plan | 1 | 0 | TopKPushdown fires, `topk=Some(K)` in plan |
| `test_plan_filter_pushdown_merges_into_inverted` | Plan | 1 | 0 | FastFieldFilterPushdown fires, predicates move |
| `test_plan_multi_split_partitioning` | Plan | 3 | 0 | 3 DataSourceExec nodes in union |
| `test_distributed_plan_structure` | Plan | 3 | 3 | Stage decomposition, NetworkCoalesceExec |
| `test_single_node_fast_field_query` | Exec | 1 | 0 | Basic fast field read → Arrow |
| `test_single_node_full_text_search` | Exec | 1 | 0 | Inverted index join, full_text UDF |
| `test_single_node_aggregation` | Exec | 1 | 0 | GROUP BY + COUNT over fast fields |
| `test_distributed_fast_field_query_across_splits` | Exec | 3 | 3 | Cross-split query, results merged |
| `test_distributed_aggregation_across_splits` | Exec | 3 | 3 | Partial/final agg across workers |
| `test_distributed_topk_across_splits` | Exec | 3 | 3 | Global top-K from local top-K per worker |
| `test_distributed_full_text_with_topk` | Exec | 3 | 3 | Full pipeline: text search + TopK + distributed |
| `test_local_vs_distributed_same_results` | Correctness | 3 | 3 | Same results from both paths |
| `test_index_opener_codec_roundtrip` | Codec | 1 | 0 | SplitIndexOpener survives encode/decode |
