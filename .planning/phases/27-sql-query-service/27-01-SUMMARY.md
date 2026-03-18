---
phase: 27
plan: 01
status: complete
duration: ~15 min
---

# Summary 27-01: Protobuf Definitions for MetricsSqlLeaf

## What Was Done

Created protobuf definitions for the MetricsSqlLeafService that accepts SQL queries with pre-assigned splits and returns Arrow IPC responses.

## Files Changed

| File | Change |
|------|--------|
| `quickwit/quickwit-proto/protos/quickwit/metrics_sql.proto` | Created - proto definitions |
| `quickwit/quickwit-proto/build.rs` | Modified - added metrics_sql compilation |
| `quickwit/quickwit-proto/src/quickwit/mod.rs` | Modified - added metrics_sql module |
| `quickwit/quickwit-proto/src/quickwit/metrics_sql/mod.rs` | Created - Rust module wrapper |
| `quickwit/quickwit-proto/src/lib.rs` | Modified - re-export metrics_sql |
| `quickwit/quickwit-proto/src/codegen/quickwit/quickwit.metrics_sql.rs` | Generated - prost output |

## Key Implementation Details

### Protobuf Messages

**MetricsSqlLeafRequest:**
- `sql` - SQL query (DataFusion dialect)
- `splits` - Vec<SplitIdAndFooterOffsets> assigned by root
- `index_uid` - Index UID for storage resolution
- `index_uri` - Index URI for storage access
- `max_rows` - Maximum rows (0 = unlimited)
- `timeout_ms` - Query timeout (0 = no timeout)

**MetricsSqlLeafResponse:**
- `arrow_ipc` - Arrow IPC streaming format bytes
- `num_rows` - Total rows in result
- `elapsed_micros` - Execution time
- `failed_splits` - Partial failure info
- `num_successful_splits` - Success count

**MetricsSqlLeafError:**
- `split_id` - Failed split ID
- `message` - Error description
- `retryable` - Whether error is transient

### Module API

Added convenience constructors and helpers:
- `MetricsSqlLeafRequest::new(sql, splits, index_uid, index_uri)`
- `MetricsSqlLeafRequest::with_max_rows()`
- `MetricsSqlLeafRequest::with_timeout_ms()`
- `MetricsSqlLeafResponse::success()`
- `MetricsSqlLeafResponse::empty()`
- `MetricsSqlLeafError::permanent()`
- `MetricsSqlLeafError::transient()`

## Verification

- [x] Proto compiles without errors
- [x] Generated Rust types include serde derive
- [x] Types accessible via `quickwit_proto::metrics_sql`
- [x] Reuses existing `SplitIdAndFooterOffsets` from search.proto

## Commits

- `4ecd4e6e` feat(27-01): add MetricsSqlLeaf protobuf definitions

## Notes

The protobuf messages follow the root/leaf separation pattern:
- Root assigns splits to leaf (no metastore access in leaf)
- Arrow IPC returned as opaque bytes (serialization in Phase 27-03)
- Error reporting per-split for partial failure handling
