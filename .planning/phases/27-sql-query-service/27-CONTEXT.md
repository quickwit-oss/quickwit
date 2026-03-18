# Phase 27: MetricsSqlLeafService Core - Context

**Gathered:** 2026-01-22
**Status:** Ready for planning

<vision>
## How This Should Work

The MetricsSqlLeafService is a **worker node** service that executes SQL queries on splits that are provided to it. It does NOT fetch splits from the metastore — that is the root service's job (Phase 28).

This mirrors the existing Quickwit architecture:
- `root_search` → fetches splits, distributes to leaves
- `leaf_search` → receives splits, executes queries

The SQL API follows the same pattern:
- `MetricsSqlRootService` → fetches splits, distributes to leaves (Phase 28)
- `MetricsSqlLeafService` → receives splits + SQL, executes DataFusion query (Phase 27)

The flow: Leaf receives SQL + splits from root → register splits with DataFusion → execute query → serialize as Arrow IPC → return response.

</vision>

<essential>
## What Must Be Nailed

- **No metastore access** — Leaf receives pre-fetched splits from root; never calls metastore
- **Arrow IPC response format** — Self-describing, schema included, compatible with pyarrow/polars/DuckDB/ClickHouse
- **Protobuf interface** — MetricsSqlLeafRequest includes SQL + splits list; MetricsSqlLeafResponse returns Arrow IPC

The leaf service is intentionally simple: receive splits + query, execute, return results.

</essential>

<specifics>
## Specific Ideas

### Protobuf Definitions

New `metrics_sql.proto` with leaf messages:
- `MetricsSqlLeafRequest`: sql string, splits list, index_uid, max_rows, timeout_ms
- `MetricsSqlLeafResponse`: arrow_ipc bytes, num_rows, elapsed_micros, failed_splits

### Service Implementation

```rust
pub struct MetricsSqlLeafService {
    storage_resolver: StorageResolver,
    searcher_context: Arc<SearcherContext>,
}

impl MetricsSqlLeafService {
    pub async fn execute(
        &self,
        request: MetricsSqlLeafRequest,
    ) -> Result<MetricsSqlLeafResponse, SearchError> {
        // 1. Get storage for index
        // 2. Create DataFusion session
        // 3. Register provided splits (NOT fetched from metastore)
        // 4. Execute SQL
        // 5. Serialize as Arrow IPC
        // 6. Return response
    }
}
```

### Arrow IPC Format

Use streaming format: schema message → RecordBatch messages → end-of-stream marker.
Compatible with standard Arrow IPC readers.

</specifics>

<notes>
## Additional Context

**Scope:** Leaf service only. Root service that fetches splits is Phase 28.

**Key existing files to leverage:**
- `quickwit/quickwit-search/src/metrics_leaf.rs` — `leaf_search_metrics_split` pattern
- `quickwit/quickwit-metrics-engine/src/query/context.rs` — MetricsSessionContext
- `quickwit/quickwit-metrics-engine/src/query/provider.rs` — MetricsTableProvider

**New files to create:**
- `quickwit/quickwit-proto/protos/quickwit/metrics_sql.proto`
- `quickwit/quickwit-metrics-engine/src/sql/mod.rs`
- `quickwit/quickwit-metrics-engine/src/sql/leaf_service.rs`
- `quickwit/quickwit-metrics-engine/src/sql/arrow_ipc.rs`

This phase prepares the foundation for Phase 28 (Root service + REST/gRPC endpoints) and Phase 29 (Arrow Flight).

</notes>

---

*Phase: 27-sql-query-service*
*Context gathered: 2026-01-22*
