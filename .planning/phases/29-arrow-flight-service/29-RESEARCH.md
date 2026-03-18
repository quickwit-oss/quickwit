# Phase 29: gRPC Arrow IPC Extension - Research

**Researched:** 2026-01-26
**Domain:** gRPC protobuf extension, Arrow IPC serialization
**Confidence:** HIGH

## Summary

Phase 29 extends the existing `MetricsSqlLeafService` (Phase 27) to optionally return Arrow IPC format in responses. This is a straightforward protobuf extension with conditional serialization logic.

The research confirms this phase involves:
1. Adding two optional fields to existing protobuf messages (backward compatible)
2. Adding conditional Arrow IPC serialization in the leaf service implementation
3. Following established Quickwit patterns for protobuf regeneration
4. Creating integration tests that deserialize and validate Arrow IPC responses

**Primary recommendation:** This is a minimal extension to existing infrastructure. The `batches_to_ipc()` function already exists, protobuf tooling is in place, and test patterns are established. Focus planning on backward compatibility verification and end-to-end Arrow IPC validation.

## Standard Stack

The phase extends existing infrastructure with no new dependencies.

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| arrow | 57 | Arrow IPC serialization | Industry standard for columnar data interchange |
| prost | 0.13 | Protobuf code generation | Standard Rust protobuf library, used throughout Quickwit |
| tonic | 0.13 | gRPC server/client | De facto standard for gRPC in Rust, used by all Quickwit services |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| datafusion | 51 | SQL query execution | Already used in Phase 27 for metrics queries |
| prost-build | 0.13 | Build-time proto compilation | Required for regenerating Rust code from .proto files |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Arrow IPC (existing) | Arrow Flight | Flight is overkill; already decided against in CONTEXT.md |
| Optional protobuf field | Separate RPC method | Optional field is more backward compatible |

**Installation:**
No new dependencies required. All libraries already in workspace Cargo.toml.

## Architecture Patterns

### Recommended Project Structure
```
quickwit/quickwit-proto/
├── protos/quickwit/metrics_sql.proto    # Extend with optional fields
└── build.rs                              # Already handles metrics_sql.proto

quickwit/quickwit-metrics-engine/
└── src/sql/
    ├── leaf_service.rs                   # Add return_arrow flag handling
    └── arrow_ipc.rs                      # Already has batches_to_ipc()
```

### Pattern 1: Protobuf Backward-Compatible Extension
**What:** Add optional fields to existing messages without breaking clients
**When to use:** Extending existing gRPC services without version bumps

**Current protobuf (from Phase 27):**
```protobuf
message MetricsSqlLeafRequest {
  string sql = 1;
  repeated SplitIdAndFooterOffsets splits = 2;
  string index_uid = 3;
  string index_uri = 4;
  uint64 max_rows = 5;
  uint64 timeout_ms = 6;
  // NEW: Add field 7
}

message MetricsSqlLeafResponse {
  bytes arrow_ipc = 1;     // Already present! Phase 27 always populates
  uint64 num_rows = 2;
  uint64 elapsed_micros = 3;
  repeated MetricsSqlLeafError failed_splits = 4;
  uint32 num_successful_splits = 5;
  // arrow_ipc already exists, no new field needed
}
```

**Key insight:** Phase 27 ALREADY includes `arrow_ipc` in the response (field 1). Phase 29 just adds the REQUEST flag to control whether it's populated. No response field changes needed.

**Extension pattern:**
```protobuf
// Add to MetricsSqlLeafRequest:
bool return_arrow = 7;  // Default false for backward compat
```

### Pattern 2: Conditional Response Population
**What:** Populate expensive response fields only when requested
**When to use:** Response field is already present but should be conditionally filled

**Implementation in leaf_service.rs:**
```rust
pub async fn execute_on_splits(
    &self,
    sql: &str,
    splits: &[MetricsSplit],
    base_path: &std::path::Path,
    table_name: &str,
    return_arrow: bool,  // NEW parameter
) -> Result<ExecuteResult, MetricsSqlError> {
    // ... existing execution logic ...
    let batches: Vec<RecordBatch> = df.collect().await?;

    // Conditional Arrow IPC serialization
    let arrow_ipc = if return_arrow {
        if batches.is_empty() {
            let schema = MetricsSchema::new();
            schema_to_ipc(&schema.arrow_schema())?
        } else {
            batches_to_ipc(&batches)?
        }
    } else {
        Vec::new()  // Empty for backward compat
    };

    Ok(ExecuteResult {
        arrow_ipc,
        num_rows: count_rows(&batches),
        elapsed_micros: start.elapsed().as_micros() as u64,
        num_successful_splits: splits.len() as u32,
    })
}
```

### Pattern 3: Protobuf Regeneration Workflow
**What:** Rebuild protobuf definitions after .proto file changes
**When to use:** Any time a .proto file is modified

**Quickwit's workflow (from build.rs analysis):**
```bash
# Edit .proto file
vim quickwit/quickwit-proto/protos/quickwit/metrics_sql.proto

# Regenerate Rust code (build.rs runs prost-build automatically)
cd quickwit/quickwit-proto
cargo build

# Generated files appear in:
# - src/codegen/quickwit/quickwit.metrics_sql.rs
# - src/codegen/quickwit/metrics_sql_descriptor.bin
```

**Source:** `/Users/george.talbot/dd/quickhouse-pomsky/quickhouse-pomsky/quickwit/quickwit-proto/build.rs` lines 259-271

### Pattern 4: Arrow IPC Validation in Tests
**What:** Deserialize Arrow IPC bytes and verify RecordBatch contents
**When to use:** Integration tests that verify end-to-end Arrow IPC correctness

**Existing pattern (from arrow_ipc.rs tests):**
```rust
use arrow::ipc::reader::StreamReader;
use std::io::Cursor;

// Test helper function
fn verify_arrow_ipc(ipc_bytes: &[u8]) -> (Arc<Schema>, Vec<RecordBatch>) {
    let cursor = Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None).expect("valid IPC");
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()
        .expect("deserialize batches");
    (schema, batches)
}

#[tokio::test]
async fn test_leaf_service_with_arrow_ipc() {
    let service = MetricsSqlLeafService::new();

    let result = service.execute_on_splits(
        "SELECT metric_name, AVG(value) FROM metrics GROUP BY metric_name",
        &test_splits,
        test_path,
        "metrics",
        true,  // return_arrow = true
    ).await.unwrap();

    // Verify Arrow IPC is present
    assert!(!result.arrow_ipc.is_empty());

    // Deserialize and validate
    let (schema, batches) = verify_arrow_ipc(&result.arrow_ipc);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), result.num_rows as usize);
}
```

**Source:** Pattern from `/Users/george.talbot/dd/quickhouse-pomsky/quickhouse-pomsky/quickwit/quickwit-metrics-engine/src/sql/arrow_ipc.rs` lines 209-317

### Anti-Patterns to Avoid

- **Breaking backward compatibility:** DO NOT make `return_arrow` a required field. Default must be false.
- **Changing existing field semantics:** DO NOT change what `arrow_ipc` field means. It should remain empty when flag is false.
- **Conditional field presence:** DO NOT use protobuf `optional` for `arrow_ipc`. Keep it as `bytes` (defaults to empty).

## Don't Hand-Roll

Problems that look simple but have existing solutions:

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Arrow IPC serialization | Custom binary format | `batches_to_ipc()` in arrow_ipc.rs | Already implemented, tested, handles schema-only case |
| Arrow IPC deserialization | Custom reader | `ipc_to_batches()` or `StreamReader` | Standard Arrow library, handles all edge cases |
| Protobuf regeneration | Manual code generation | `cargo build` in quickwit-proto | build.rs already configured correctly |
| gRPC service implementation | Custom RPC layer | Extend existing service patterns | Quickwit has established patterns via tonic |

**Key insight:** Phase 27 already implemented all the hard parts (Arrow IPC serialization, DataFusion integration, error handling). Phase 29 is just adding a request flag and conditional logic.

## Common Pitfalls

### Pitfall 1: Protobuf Field Numbering Conflicts
**What goes wrong:** Adding a field with a number already in use causes build failures or silent data corruption
**Why it happens:** Proto files from different phases may have been extended independently
**How to avoid:**
- Check existing field numbers before adding new fields
- Always use the next sequential number
- Current last field in `MetricsSqlLeafRequest` is `timeout_ms = 6`, so use `7` for `return_arrow`
**Warning signs:**
- Build errors: "field number X is already used"
- Proto compiler warnings about reserved ranges

### Pitfall 2: Arrow IPC Empty vs Schema-Only Confusion
**What goes wrong:** Tests fail when query returns no rows because response has no data
**Why it happens:** Arrow IPC has two valid "empty" states: truly empty (no bytes) vs schema-only (bytes with schema but no batches)
**How to avoid:**
- When `return_arrow=false`: Use `Vec::new()` (truly empty)
- When `return_arrow=true` with no results: Use `schema_to_ipc()` (schema-only)
- Phase 27 already handles this correctly in `execute_on_splits()` lines 170-177
**Warning signs:**
- Arrow deserialization errors on empty results
- Tests expecting empty bytes getting schema bytes

### Pitfall 3: Backward Compatibility Testing Insufficient
**What goes wrong:** Old clients break when they receive new responses
**Why it happens:** Not testing the default case where `return_arrow` is false/unset
**How to avoid:**
- Test three cases: flag true, flag false, flag unset (default)
- Verify `arrow_ipc` field is empty when flag is false
- Verify existing response fields (num_rows, elapsed_micros) work in all cases
**Warning signs:**
- Integration tests only test `return_arrow=true`
- No tests for default protobuf field behavior

### Pitfall 4: Forgetting Protobuf Regeneration
**What goes wrong:** Rust code doesn't match .proto file, causing compilation errors
**Why it happens:** Editing .proto but not running `cargo build` in quickwit-proto
**How to avoid:**
- Always rebuild quickwit-proto after editing any .proto file
- Check that generated files in `src/codegen/` are updated
- Run `cargo check` in dependent crates (quickwit-metrics-engine) to verify
**Warning signs:**
- Undefined type errors in Rust code
- Field access errors despite .proto having the field
- Git diff shows .proto changed but no codegen/ changes

## Code Examples

Verified patterns from existing codebase:

### Example 1: Protobuf Extension (add to metrics_sql.proto)
```protobuf
// Source: Pattern from search.proto and existing metrics_sql.proto
// File: quickwit/quickwit-proto/protos/quickwit/metrics_sql.proto

message MetricsSqlLeafRequest {
  // ... existing fields 1-6 ...

  // Request Arrow IPC format in response.
  // When true, arrow_ipc field in response will be populated.
  // When false or unset, arrow_ipc will be empty (backward compatible).
  bool return_arrow = 7;
}

// MetricsSqlLeafResponse requires NO changes - arrow_ipc already exists!
```

### Example 2: Service Implementation Change (leaf_service.rs)
```rust
// Source: Extending existing execute_on_splits in leaf_service.rs
// File: quickwit/quickwit-metrics-engine/src/sql/leaf_service.rs

// Modify signature to accept flag (line 126-131)
pub async fn execute_on_splits(
    &self,
    sql: &str,
    splits: &[MetricsSplit],
    base_path: &std::path::Path,
    table_name: &str,
    return_arrow: bool,  // NEW parameter
) -> Result<ExecuteResult, MetricsSqlError> {
    // ... existing execution logic lines 133-167 ...

    // Replace lines 169-177 with conditional logic:
    let arrow_ipc = if return_arrow {
        if batches.is_empty() {
            let schema = MetricsSchema::new();
            let arrow_schema = schema.arrow_schema();
            schema_to_ipc(&arrow_schema)?
        } else {
            batches_to_ipc(&batches)?
        }
    } else {
        Vec::new()  // Empty bytes when not requested
    };

    // ... rest remains the same ...
}
```

### Example 3: Integration Test with Arrow IPC Validation
```rust
// Source: Pattern from arrow_ipc.rs tests (lines 239-268)
// and ingestion_profile_bench.rs (lines 350-355)
// New test file or add to existing leaf_service.rs tests

use arrow::ipc::reader::StreamReader;
use std::io::Cursor;

#[tokio::test]
async fn test_execute_with_return_arrow_flag() {
    let service = MetricsSqlLeafService::new();
    let test_splits = vec![create_test_split("split-1")];
    let temp_dir = TempDir::new().unwrap();

    // Test with return_arrow = true
    let result = service.execute_on_splits(
        "SELECT * FROM metrics",
        &test_splits,
        temp_dir.path(),
        "metrics",
        true,  // Request Arrow IPC
    ).await.unwrap();

    assert!(!result.arrow_ipc.is_empty(), "Arrow IPC should be populated");

    // Deserialize Arrow IPC and verify
    let cursor = Cursor::new(&result.arrow_ipc[..]);
    let reader = StreamReader::try_new(cursor, None)
        .expect("valid Arrow IPC");
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()
        .expect("deserialize batches");

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), result.num_rows as usize);

    // Verify schema has expected fields
    assert!(schema.field_with_name("metric_name").is_ok());
    assert!(schema.field_with_name("value").is_ok());
}

#[tokio::test]
async fn test_execute_without_return_arrow_flag() {
    let service = MetricsSqlLeafService::new();
    let test_splits = vec![create_test_split("split-1")];
    let temp_dir = TempDir::new().unwrap();

    // Test with return_arrow = false (backward compatibility)
    let result = service.execute_on_splits(
        "SELECT * FROM metrics",
        &test_splits,
        temp_dir.path(),
        "metrics",
        false,  // Do NOT request Arrow IPC
    ).await.unwrap();

    assert_eq!(result.arrow_ipc.len(), 0, "Arrow IPC should be empty");
    assert!(result.num_rows > 0, "Still returns metadata");
    assert!(result.elapsed_micros > 0, "Still returns timing");
}
```

### Example 4: Helper Function for Arrow IPC Validation
```rust
// Source: Pattern from arrow_ipc.rs ipc_to_batches (line 170)
// Useful for tests

/// Deserialize Arrow IPC bytes to batches for test validation.
fn deserialize_arrow_ipc(bytes: &[u8]) -> Result<(Arc<Schema>, Vec<RecordBatch>), ArrowIpcError> {
    let cursor = Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None)?;
    let schema = reader.schema();
    let batches: Result<Vec<_>, _> = reader.collect();
    Ok((schema, batches?))
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| N/A - new feature | Optional Arrow IPC in gRPC response | Phase 29 (2026-01) | Enables ClickHouse integration without Arrow Flight |
| Always serialize Arrow IPC | Conditional serialization via flag | Phase 29 (2026-01) | Backward compatible, saves bandwidth when not needed |

**Current best practices (2026-01):**
- Arrow 57.x with IPC streaming format (standard since Arrow 1.0)
- Prost 0.13 / Tonic 0.13 for protobuf/gRPC (latest stable)
- Conditional response fields via boolean flags (protobuf best practice)
- Schema-only IPC for empty results (Arrow standard pattern)

**Deprecated/outdated:**
- N/A - this is a new feature, no deprecated patterns to replace

## Open Questions

Things that couldn't be fully resolved:

1. **gRPC Service Definition**
   - What we know: Phase 27 created protobuf messages but NO gRPC service definition in metrics_sql.proto
   - What's unclear: Does Phase 29 need to add the gRPC service definition, or does that come from Phase 28's root service?
   - Recommendation: Check Phase 28 implementation. If Phase 28 only added SearchService::plan_metrics_splits (split planning), then Phase 29 needs to add the actual MetricsSqlLeafService gRPC service definition and tonic server implementation. Review Phase 28 deliverables to confirm scope.

2. **Call Site Updates**
   - What we know: execute_on_splits() signature will change (add return_arrow parameter)
   - What's unclear: Are there existing call sites in Phase 28's root service that need updating?
   - Recommendation: Audit all calls to execute_on_splits() and update them to pass the flag. Default to `false` for existing call sites to maintain backward compatibility.

3. **Performance Impact**
   - What we know: Arrow IPC serialization adds CPU cost and memory allocation
   - What's unclear: Is there a maximum result size where serialization becomes problematic?
   - Recommendation: Use existing max_rows configuration to limit result set size. Phase 27 already has this field (field 5 in request). No additional throttling needed in Phase 29.

## Sources

### Primary (HIGH confidence)
- `/Users/george.talbot/dd/quickhouse-pomsky/quickhouse-pomsky/quickwit/quickwit-proto/protos/quickwit/metrics_sql.proto` - Current protobuf definition (Phase 27)
- `/Users/george.talbot/dd/quickhouse-pomsky/quickhouse-pomsky/quickwit/quickwit-metrics-engine/src/sql/leaf_service.rs` - MetricsSqlLeafService implementation (Phase 27)
- `/Users/george.talbot/dd/quickhouse-pomsky/quickhouse-pomsky/quickwit/quickwit-metrics-engine/src/sql/arrow_ipc.rs` - Arrow IPC utilities (Phase 27)
- `/Users/george.talbot/dd/quickhouse-pomsky/quickhouse-pomsky/quickwit/quickwit-proto/build.rs` - Protobuf build configuration
- `/Users/george.talbot/dd/quickhouse-pomsky/quickhouse-pomsky/quickwit/Cargo.toml` - Dependency versions
- `.planning/phases/29-arrow-flight-service/29-CONTEXT.md` - User decisions and scope

### Secondary (MEDIUM confidence)
- `/Users/george.talbot/dd/quickhouse-pomsky/quickhouse-pomsky/quickwit/quickwit-serve/src/search_api/grpc_adapter.rs` - gRPC service adapter pattern
- `.planning/phases/27-sql-query-service/27-CONTEXT.md` - Phase 27 architecture decisions
- `.planning/phases/28-rest-grpc-endpoints/28-CONTEXT.md` - Phase 28 architecture decisions

### Tertiary (LOW confidence)
- Arrow 57 documentation (not fetched, assumed from dependency version)
- Prost/Tonic 0.13 documentation (not fetched, assumed from dependency version)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - All libraries directly inspected in Cargo.toml
- Architecture: HIGH - All patterns verified in existing codebase
- Pitfalls: HIGH - Derived from protobuf best practices and Arrow IPC edge cases observed in existing code
- Implementation details: HIGH - All code examples reference actual file locations and line numbers

**Research limitations:**
- Did not fetch external documentation for Arrow/Prost/Tonic (used versions from Cargo.toml)
- Did not verify if Phase 28 created a gRPC service definition (marked as Open Question #1)
- Did not audit all call sites that would need updating (marked as Open Question #2)

**Research date:** 2026-01-26
**Valid until:** 2026-03-01 (30 days - protobuf and Arrow IPC patterns are stable)
