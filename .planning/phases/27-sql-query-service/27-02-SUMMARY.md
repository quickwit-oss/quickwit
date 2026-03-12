---
phase: 27
plan: 02
status: complete
duration: ~30 min
---

# Summary 27-02: MetricsSqlLeafService Implementation

## What Was Done

Implemented the MetricsSqlLeafService that executes SQL queries on provided splits using DataFusion and returns results as Arrow IPC.

## Files Changed

| File | Change |
|------|--------|
| `quickwit/quickwit-metrics-engine/src/sql/mod.rs` | Created - module exports |
| `quickwit/quickwit-metrics-engine/src/sql/leaf_service.rs` | Created - service implementation |
| `quickwit/quickwit-metrics-engine/src/lib.rs` | Modified - added sql module |

## Key Implementation Details

### MetricsSqlLeafService

```rust
pub struct MetricsSqlLeafService {
    config: MetricsQueryConfig,
}

impl MetricsSqlLeafService {
    pub fn new() -> Self;
    pub fn with_config(config: MetricsQueryConfig) -> Self;
    
    pub async fn execute_on_splits(
        &self,
        sql: &str,
        splits: &[MetricsSplit],
        base_path: &Path,
        table_name: &str,
    ) -> Result<ExecuteResult, MetricsSqlError>;
    
    pub async fn load_splits<S: SplitStorage>(
        &self,
        storage: &S,
        split_ids: &[String],
    ) -> SplitLoadResult;
}
```

### ExecuteResult

Contains:
- `arrow_ipc` - Arrow IPC bytes
- `num_rows` - Total rows
- `elapsed_micros` - Execution time
- `num_successful_splits` - Splits processed

### Error Types

- `MetricsSqlError::Storage` - Storage access errors
- `MetricsSqlError::DataFusion` - Query execution errors
- `MetricsSqlError::ArrowIpc` - Serialization errors
- `MetricsSqlError::Context` - Session creation errors
- `MetricsSqlError::SplitLoad` - Split loading errors

### SplitStorage Trait

Abstraction for loading splits from storage, enabling mock testing:

```rust
#[async_trait::async_trait]
pub trait SplitStorage: Send + Sync {
    async fn load_split(&self, split_id: &str) -> Result<MetricsSplit, SplitStorageError>;
}
```

## Verification

- [x] Service creates DataFusion session
- [x] Service registers splits with DataFusion
- [x] Service executes SQL queries
- [x] Results serialized as Arrow IPC
- [x] Empty splits return schema-only IPC
- [x] Tests pass (3 tests)

## Commits

- `01e587da` feat(27-02): add MetricsSqlLeafService implementation

## Notes

The service follows the Quickwit root/leaf pattern:
- Root service (Phase 28) will fetch splits and distribute to leaves
- Leaf service executes on provided splits only
- No metastore access in leaf service

The `load_splits` method provides split loading with partial failure handling, to be used when integrating with the root service.
