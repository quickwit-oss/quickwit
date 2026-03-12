# Phase 31: Metadata Foundation - Research

**Researched:** 2026-02-23
**Domain:** Proto codegen, sort schema parsing, PostgreSQL schema migration, metastore RPCs, time-window arithmetic
**Confidence:** HIGH

## Summary

Phase 31 establishes all metadata types, database schema, and core functions that downstream compaction phases (32-36) depend on. The scope is pure infrastructure: proto types, string parsers, PostgreSQL columns, metastore RPCs, and a canonical window_start() function. No actor changes, no ingestion changes, no merge logic.

The research reveals that the existing codebase already has the skeletal infrastructure in place: MetricsSplitMetadata struct, PostgreSQL `metrics_splits` table, metastore RPCs for stage/publish/list metrics splits, and a prost-based proto build pipeline. Phase 31 extends all of these rather than creating from scratch. The Go source code from Husky (`schemautils.go`, `validation.go`, `zonemap/`) provides exact specifications for the sort schema parser and its test cases, reducing ambiguity to near zero.

**Primary recommendation:** Implement as four plans (proto+parsing, metadata extensions+migration, metastore RPCs, window_start), strictly translating Go logic where applicable and extending existing Rust structures where infrastructure exists.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

#### Proto sync strategy
- Vendored copy of `event_store_sortschema.proto` from dd-source into `quickwit/quickwit-proto/protos/`
- Keep the proto identical to dd-source -- byte-for-byte copy with a comment noting origin
- Do NOT strip unused fields (e.g., `expired` bool on RowKeys stays)
- Code generation via `build.rs` with `prost-build` -- standard Rust approach, proto compiled at cargo build time
- Generated code lives in OUT_DIR (not checked in)

#### Sort schema string parsing
- **V2 only** -- reject V1 (INCORRECT_TRIM) strings. We're greenfield for metrics, no legacy data exists
- **Strict parsing** -- any malformed string returns an error. Fail loud at config time, no guessing
- **Named schema versions (the `version` field) skipped for now** -- always use full schema description. Named versions add complexity we don't need yet
- **LSM comparison cutoff (`&` separator) included** -- parse and respect the `&` cutoff. Needed for correct compaction scope decisions
- Direct Go-to-Rust translation of `schemautils.go` -- all test cases from `schemautils_test.go` must pass in Rust

#### RowKeys storage
- **PostgreSQL:** Serialized proto bytes stored as BYTEA column. Compact, fast to read/write. Opaque to SQL queries (acceptable -- we don't query RowKeys via SQL in Phase 1)
- **Parquet key_value_metadata:** Both proto bytes (canonical) AND human-readable JSON key for debugging. Belt and suspenders -- operators can inspect files with parquet-tools
- **Zonemap regex:** Separate column in PostgreSQL (not embedded in RowKeys). Independent of RowKeys structure
- **sort_schema field:** Stored as the Husky-style string representation (e.g., `metric_name|host|env|timestamp/V2`). Human-readable, compact, easy to compare and index in PostgreSQL

#### Window start semantics
- **UTC epoch aligned** -- `window_start = timestamp - (timestamp % duration)`. Simple, deterministic, no timezone dependency. Use `div_euclid`/`rem_euclid` for correct negative timestamp handling
- **window_start stored as DateTime\<Utc\>** -- type-safe, prevents mixing seconds/millis/nanos
- **window_duration must divide 3600** -- enforced strictly at parse time. ADR-003 TW-2 invariant. Reject durations that don't divide evenly into one hour
- **window_duration is per-index** -- different indexes can have different window durations. Matches sort_schema being per-index. Stored as part of index configuration

### Claude's Discretion
- Exact PostgreSQL column types and index strategy for new metadata fields
- How to organize the proto build.rs integration with existing quickwit-proto build
- Specific error types and messages for sort schema parsing failures
- How DateTime\<Utc\> interacts with the existing MetricsSplitMetadata i64 timestamp fields

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| META-01 | Sort schema configurable per metrics index using Husky-style string format, reuse SortSchema/SortColumn/SortColumnDirection protos | Proto vendoring strategy fully researched; Go StringToSchema/SchemaToString code analyzed line-by-line; type system mapping (TypeID postfix suffixes) fully documented |
| META-02 | timeseries_id hash tiebreaker column computed from all tag key/value pairs | Sort schema parser handles `timeseries_id` as a regular column name. Computation of the hash itself is Phase 32 (ingestion), but the schema type definition is Phase 31 |
| META-03 | MetricsSplitMetadata extended with window_start, window_duration_secs, sort_schema, num_merge_ops | Existing MetricsSplitMetadata struct analyzed; extension points identified; DateTime\<Utc\> interaction with existing i64 timestamps documented |
| META-04 | PostgreSQL migration adding compaction columns to metrics_splits with composite index | Existing migration 25 analyzed; new ALTER TABLE migration designed; column types and index strategy recommended |
| META-05 | list_metrics_splits_for_compaction RPC scoped by (index_id, window_start, sort_schema) | Existing ListMetricsSplitsRequest proto analyzed; new RPC message design documented |
| META-06 | Atomic replace semantics in publish_metrics_splits (staged + replaced in single transaction) | Existing PublishMetricsSplitsRequest already has replaced_split_ids field; implementation requires PostgreSQL transaction logic |
| META-07 | Self-describing Parquet files with sort_schema, window_start, min/max in key_value_metadata | Existing WriterProperties builder analyzed; key_value_metadata extension point identified |
| META-08 | Per-column statistics using RowKeys proto + zonemap regex | RowKeys proto fully analyzed (214 lines); zonemap Go implementation analyzed (automaton + regex builder); storage strategy (BYTEA + separate regex column) documented |
| PIPE-10 | Canonical window_start() function using div_euclid/rem_euclid with proptest | div_euclid/rem_euclid semantics verified; proptest 1.x available in workspace; edge cases catalogued |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `prost` | 0.13 | Proto code generation for SortSchema/RowKeys types | Already used throughout quickwit-proto for all gRPC services |
| `prost-build` | 0.13 | Build-time proto compilation | Already in quickwit-proto build.rs |
| `chrono` | 0.4 (clock, std features) | DateTime\<Utc\> for window_start type safety | Already in workspace Cargo.toml |
| `proptest` | 1.x | Property-based testing for window_start | Already in workspace Cargo.toml |
| `sea-query` | (workspace version) | PostgreSQL query building | Already used by MetricsSplits Iden derive |
| `serde` / `serde_json` | (workspace version) | Serialization for MetricsSplitMetadata | Already used for split_metadata_json |
| `sqlx` | (workspace version) | PostgreSQL migration runner | Already used by quickwit-metastore |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `tonic-build` | 0.13 | gRPC service code generation | Only if new RPCs are added as gRPC services (not needed if using existing MetastoreService) |
| `quickwit-codegen` | local | Custom Quickwit codegen extensions for service traits | Only if new RPCs need the Quickwit service trait pattern |
| `regex` | (workspace version) | Zonemap regex validation at parse time | When validating regex strings from zonemap builder output |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| DateTime\<Utc\> for window_start | i64 seconds | i64 is simpler but loses type safety; mixing seconds/millis/nanos is a real risk. DateTime\<Utc\> is the locked decision |
| BYTEA for RowKeys | JSONB | JSONB is queryable but slower to read/write for opaque proto bytes. BYTEA is the locked decision |
| Separate zonemap_regex TEXT column | Embed in RowKeys proto | Separate column allows independent evolution and future SQL queries. Locked decision |

## Architecture Patterns

### Proto Build Integration

The existing `quickwit-proto/build.rs` compiles ~12 proto files across 4 directories (quickwit, cloudprem, jaeger, opentelemetry). The sort schema proto should be added as a new standalone compilation step.

**Recommended approach:** Add a new `prost_build::Config` block in build.rs that compiles `protos/event_store_sortschema/event_store_sortschema.proto` into `src/codegen/sortschema/`. This follows the exact pattern of the existing jaeger and opentelemetry proto compilations.

```rust
// In quickwit-proto/build.rs, add after existing compilations:
let mut sortschema_config = prost_build::Config::default();
sortschema_config
    .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
    .out_dir("src/codegen/sortschema");

sortschema_config.compile_protos(
    &["protos/event_store_sortschema/event_store_sortschema.proto"],
    &["protos"],
)?;
```

**Key detail:** The proto file uses `package sortschema;` and has `option go_package = "..."` -- prost ignores the go_package option and generates Rust types in a module named after the package (i.e., `sortschema`). The generated types will be `sortschema::SortSchema`, `sortschema::SortColumn`, `sortschema::SortColumnDirection`, `sortschema::ColumnValue`, `sortschema::ColumnValues`, `sortschema::RowKeys`.

**File placement:** Create directory `quickwit-proto/protos/event_store_sortschema/` and place the vendored proto there. This mirrors the `protos/third-party/` pattern used for jaeger and opentelemetry.

### Sort Schema Parser Module Structure

The sort schema parsing logic should live in `quickwit-parquet-engine` (not quickwit-proto), since it contains domain logic beyond what the proto generates. The proto types live in quickwit-proto; the parser and validation are consumers.

```
quickwit-parquet-engine/src/
  sort_schema/
    mod.rs           # Public API: parse(), to_string(), to_short_string(), validate()
    parser.rs        # StringToSchema equivalent (~200 lines)
    display.rs       # SchemaToString, SchemaToStringShort equivalents (~100 lines)
    column_type.rs   # Husky TypeID mapping and postfix parsing (~80 lines)
    validation.rs    # ValidateSchema equivalent (~60 lines)
    equivalence.rs   # EquivalentSchemas, EquivalentSchemasForCompaction (~40 lines)
    tests.rs         # All test cases from schemautils_test.go (~250 lines)
```

### Column Type Mapping (Husky to Rust)

The Go code uses a `TypeID` integer enum with postfix suffixes (`__s` = string, `__i` = int64, `__nf` = float64). For the Rust translation, we need a mapping from column name suffixes to the proto's `column_type` u64 field.

**Critical mapping from Go source:**
| Postfix | Go TypeID | Value (iota) | Proto column_type |
|---------|-----------|------|-------------------|
| `__s` | TypeIDString | 14 | 14 |
| `__i` | TypeIDInt64 | 2 | 2 |
| `__nf` | TypeIDFloat64 | 10 | 10 |
| `timestamp` (special) | TypeIDInt64 | 2 | 2 |
| `__sk` | TypeIDSketch | 17 | 17 |

The proto's `column_type` field is `uint64` and maps 1:1 with Go's TypeID iota values. The Rust code needs the same postfix-to-type-id mapping for parsing and the same type-id-to-string mapping for serialization.

**V2 enforcement:** The parser must reject `SortVersion::INCORRECT_TRIM` (value 0). Only accept `SortVersion::TRIMMED_WITH_BUDGET` (value 1) or higher. In the Go code, INCORRECT_TRIM is the default zero-value, so a schema string without `/V#` suffix defaults to V0 (INCORRECT_TRIM). The Rust parser should reject strings without an explicit `/V2` (or `/V1` which maps to TRIMMED_WITH_BUDGET).

**Decision point:** The CONTEXT says "V2 only -- reject V1 (INCORRECT_TRIM)". Note that in the proto, INCORRECT_TRIM = 0 and TRIMMED_WITH_BUDGET = 1. The string `/V1` maps to TRIMMED_WITH_BUDGET (not INCORRECT_TRIM). So "V2 only" means: require the version suffix, and the version number must be >= 1. Unversioned strings (which default to INCORRECT_TRIM = 0) are rejected.

### MetricsSplitMetadata Extension Pattern

The existing `MetricsSplitMetadata` struct needs new fields. Since it derives `Serialize/Deserialize`, new fields must be backward-compatible (use `Option<T>` or `#[serde(default)]`).

```rust
pub struct MetricsSplitMetadata {
    // Existing fields...
    pub split_id: SplitId,
    pub index_id: String,
    pub time_range: TimeRange,
    pub num_rows: u64,
    pub size_bytes: u64,
    pub metric_names: HashSet<String>,
    pub low_cardinality_tags: HashMap<String, HashSet<String>>,
    pub high_cardinality_tag_keys: HashSet<String>,
    pub created_at: SystemTime,
    pub parquet_files: Vec<String>,

    // New Phase 31 fields:
    /// Window start as DateTime<Utc>. None for pre-Phase-31 splits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_start: Option<DateTime<Utc>>,

    /// Window duration in seconds. Paired with window_start.
    #[serde(default)]
    pub window_duration_secs: u32,

    /// Sort schema as Husky-style string (e.g., "metric_name|host|timestamp/V2").
    /// Empty string for pre-Phase-31 splits.
    #[serde(default)]
    pub sort_schema: String,

    /// Number of merge operations this split has been through.
    #[serde(default)]
    pub num_merge_ops: u32,

    /// RowKeys (sort-key min/max boundaries) as proto bytes.
    /// None for pre-Phase-31 splits or splits without sort schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_keys_proto: Option<Vec<u8>>,

    /// Per-column zonemap regex strings, keyed by column name.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub zonemap_regexes: HashMap<String, String>,
}
```

**DateTime\<Utc\> and i64 interaction:** The existing `time_range` uses `TimeRange { start_secs: u64, end_secs: u64 }`. The new `window_start` uses `DateTime<Utc>` for type safety. Conversion is straightforward: `DateTime::from_timestamp(secs, 0)` creates from i64 seconds, and `.timestamp()` extracts i64 seconds. In PostgreSQL, `window_start` will be stored as `BIGINT` (i64 seconds since epoch), same as `time_range_start`. The `DateTime<Utc>` wrapper is Rust-side only.

### PostgreSQL Migration Design

**Recommended:** Migration 26 as `ALTER TABLE` adding columns to existing `metrics_splits`.

```sql
-- 26_add-compaction-metadata.up.sql
ALTER TABLE metrics_splits
    ADD COLUMN window_start BIGINT,
    ADD COLUMN window_duration_secs INTEGER,
    ADD COLUMN sort_schema TEXT NOT NULL DEFAULT '',
    ADD COLUMN num_merge_ops INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN row_keys BYTEA,
    ADD COLUMN zonemap_regexes JSONB NOT NULL DEFAULT '{}';

-- Composite index for compaction scope queries:
-- "Give me all Published splits for (index_id, window_start, sort_schema)"
CREATE INDEX idx_metrics_splits_compaction_scope
    ON metrics_splits (index_id, sort_schema, window_start)
    WHERE split_state = 'Published';

-- Index for window-based queries (retention, query planning)
CREATE INDEX idx_metrics_splits_window
    ON metrics_splits (window_start, window_duration_secs)
    WHERE split_state = 'Published';
```

**Column type rationale:**
- `window_start BIGINT`: Nullable (NULL for pre-Phase-31 splits). Seconds since epoch matching time_range_start convention.
- `window_duration_secs INTEGER`: Nullable. 32-bit sufficient (max 3600 seconds = 1 hour).
- `sort_schema TEXT NOT NULL DEFAULT ''`: Empty string for pre-Phase-31 splits. Indexable for equality comparisons in compaction scope.
- `num_merge_ops INTEGER NOT NULL DEFAULT 0`: Always 0 for newly ingested splits.
- `row_keys BYTEA`: Nullable. Opaque proto bytes. Not queryable via SQL.
- `zonemap_regexes JSONB NOT NULL DEFAULT '{}'`: JSONB instead of TEXT to support future SQL queries on regex values (e.g., checking if a column has a zonemap). Default empty object.

### Metastore RPC for Compaction

The existing `ListMetricsSplitsRequest` uses a JSON query predicate. For compaction, we need a specialized RPC that scopes by (index_id, window_start, sort_schema).

**Option A (Recommended): Extend existing ListMetricsSplitsRequest query format.**

The existing `query_json` field can support a new query type that includes compaction scope fields. This avoids adding a new RPC and keeps the metastore service interface stable.

```json
{
  "scope": "compaction",
  "index_id": "metrics-prod",
  "window_start": 1700000000,
  "sort_schema": "metric_name|host|env|timestamp/V2",
  "split_state": "Published"
}
```

**Option B: Add new dedicated RPC.**

```protobuf
message ListMetricsSplitsForCompactionRequest {
  string index_id = 1;
  int64 window_start = 2;
  string sort_schema = 3;
}
```

Option A is simpler to implement (no proto changes, no service trait changes) but less type-safe. Option B is more explicit. Given that the metastore proto already has the pattern of JSON query predicates (see `ListMetricsSplitsRequest.query_json`), Option A is consistent with the existing design.

**Recommendation:** Option B (new dedicated RPC). It is more explicit, avoids JSON parsing ambiguity, and the cost of adding a new RPC is low in this codebase (the pattern is well-established).

### Atomic Publish with Replace

The existing `PublishMetricsSplitsRequest` already has `repeated string replaced_split_ids = 3` (annotated "For merge operations (future)"). Phase 31 implements this field.

The PostgreSQL implementation must execute in a single transaction:
1. UPDATE staged_split_ids SET split_state = 'Published'
2. UPDATE replaced_split_ids SET split_state = 'MarkedForDeletion'

Both must succeed or both must roll back. This is standard PostgreSQL transaction semantics.

### Window Start Function

```rust
/// Compute the start of the time window containing the given timestamp.
///
/// Uses div_euclid/rem_euclid for correct handling of negative timestamps
/// (timestamps before Unix epoch, which can occur in test data or historical imports).
///
/// # Panics
/// Panics if duration_secs is 0.
pub fn window_start(timestamp_secs: i64, duration_secs: i64) -> DateTime<Utc> {
    debug_assert!(duration_secs > 0, "window duration must be positive");
    let remainder = timestamp_secs.rem_euclid(duration_secs);
    let start_secs = timestamp_secs - remainder;
    DateTime::from_timestamp(start_secs, 0)
        .expect("window_start timestamp out of range")
}
```

**Why div_euclid/rem_euclid:**
- Standard `%` in Rust truncates toward zero: `-1 % 900 = -1` (wrong for windowing)
- `rem_euclid` always returns non-negative: `(-1i64).rem_euclid(900) = 899`
- This means `window_start(-1, 900) = -1 - 899 = -900`, correctly placing timestamp -1 in the window [-900, 0).

**Window duration validation:**

```rust
const VALID_WINDOW_DURATIONS_SECS: [u32; 12] = [
    60, 120, 180, 240, 300, 360, 600, 720, 900, 1200, 1800, 3600,
];

pub fn validate_window_duration(duration_secs: u32) -> Result<(), Error> {
    if 3600 % duration_secs != 0 {
        return Err(Error::InvalidWindowDuration {
            duration_secs,
            reason: "must evenly divide 3600 (one hour)",
        });
    }
    if duration_secs == 0 {
        return Err(Error::InvalidWindowDuration {
            duration_secs,
            reason: "must be positive",
        });
    }
    Ok(())
}
```

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Proto code generation | Custom Rust struct definitions for SortSchema | prost-build from vendored .proto | Proto is the canonical schema definition; hand-rolling risks drift from dd-source |
| Sort schema string parsing | Ad-hoc regex-based parser | Direct port of Go `StringToSchema` | Go implementation is battle-tested with comprehensive test coverage; same format, same edge cases |
| Column type postfix resolution | Custom suffix matching | Port of Go `TypeIDFromPostFix` | The suffix convention (__s, __i, __nf) is a dd-source standard; must match exactly |
| Zonemap regex generation | Custom regex builder | Port of Go `zonemap/automaton.go` + `regex.go` | The prefix-preserving regex algorithm is non-trivial (DFA with pruning); incorrect implementation produces regexes that don't cover all values |
| PostgreSQL migration versioning | Manual SQL scripts | sqlx migration framework | Already used by quickwit-metastore; auto-applies on startup |
| DateTime arithmetic for window_start | Manual epoch math | chrono::DateTime\<Utc\> | chrono handles leap seconds, timezone correctness, and provides type safety |

**Key insight:** The Go implementations in dd-source are the specification, not just a reference. Diverging from them (even in edge cases) creates incompatibility between Husky and Quickhouse-Pomsky sort schemas.

## Common Pitfalls

### Pitfall 1: TypeID Integer Values Drift
**What goes wrong:** Rust code uses different integer values for column types than Go code, causing sort schemas to be incompatible between Husky and Quickhouse.
**Why it happens:** The Go TypeID enum uses iota (auto-incrementing), and there are deprecated values that leave gaps (e.g., TypeIDFloat32 at position 11 is deprecated). Hard-coding the wrong integer breaks the proto column_type field.
**How to avoid:** Use the explicit integer values from the Go `init()` assertions: TypeIDInt64=2, TypeIDFloat64=10, TypeIDString=14. Write a test that verifies the Rust enum values match Go.
**Warning signs:** Sort schema round-trip tests fail; schemas parsed from Go-produced strings have wrong types.

### Pitfall 2: Timestamp Column Special Casing
**What goes wrong:** Parser fails to handle the `timestamp` column name, which has no type suffix but is implicitly TypeIDInt64 with default descending direction.
**Why it happens:** In Go, `columns.Timestamp = "timestamp"` is special-cased in `TypeIDFromPostFix` to return TypeIDInt64, and in `StringToSchema` to default to descending. Miss either of these and parsing breaks.
**How to avoid:** The Rust column type resolver must special-case "timestamp" (no suffix) as TypeIDInt64. The parser must default "timestamp" direction to descending.
**Warning signs:** `StringToSchema("timestamp")` returns wrong type or direction.

### Pitfall 3: Serde Backward Compatibility
**What goes wrong:** Adding new fields to MetricsSplitMetadata breaks deserialization of existing JSON in the `split_metadata_json` column.
**Why it happens:** Existing rows in PostgreSQL contain JSON without the new fields. If new fields are not `Option<T>` or `#[serde(default)]`, deserialization fails.
**How to avoid:** All new fields must use `#[serde(default)]` or be `Option<T>` with `#[serde(default)]`. Write a test that deserializes a JSON string from the current schema format.
**Warning signs:** Deserialization errors when reading existing splits from PostgreSQL.

### Pitfall 4: Window Start Negative Timestamp
**What goes wrong:** `window_start(-1, 900)` returns 0 instead of -900 because standard `%` operator was used instead of `rem_euclid`.
**Why it happens:** Rust's `%` operator returns the sign of the dividend: `-1 % 900 = -1`. For windowing, we need `rem_euclid` which always returns non-negative.
**How to avoid:** Use `i64::rem_euclid` exclusively. Proptest with negative timestamps.
**Warning signs:** Splits near epoch have incorrect window assignments.

### Pitfall 5: LSM Cutoff on First Column
**What goes wrong:** Parser accepts `&service__s|env__s|timestamp` (cutoff marker on first column), which makes no sense (ignores all columns for LSM comparison).
**Why it happens:** Not validating that the `&` marker cannot be on position 0.
**How to avoid:** Direct port of Go validation: `if i == 0 { return error }`. Port all error test cases from `TestStringToSchemaWithLSMCutoff`.
**Warning signs:** LSM cutoff marker tests from Go don't have Rust equivalents.

### Pitfall 6: Proto Package Path in build.rs
**What goes wrong:** prost generates types in the wrong module path, causing compilation errors or type mismatches.
**Why it happens:** The proto `package sortschema;` creates a module named `sortschema`. If the `out_dir` or module structure doesn't match, the types won't be accessible.
**How to avoid:** Create `src/codegen/sortschema/` directory and configure build.rs with `.out_dir("src/codegen/sortschema")`. Add a `mod sortschema` in the appropriate parent module. Test that `sortschema::SortSchema`, `sortschema::RowKeys`, etc. are accessible.
**Warning signs:** Compilation errors like "module sortschema not found" or "type SortSchema not found".

## Code Examples

### Sort Schema String Parsing (Rust Translation)

```rust
// Source: Direct translation of dd-go/.../fragment/schemautils.go:StringToSchema
// Confidence: HIGH -- line-by-line port with all Go test cases

/// Parse a Husky-style sort schema string into a SortSchema proto.
///
/// Format: `[name=]column[+/-]|...[&column[+/-]|...]/V#`
///
/// Examples:
///   "metric_name|host|env|timestamp/V1"
///   "testSchema=columnA__s|columnB__i:-|timestamp/V1"
///   "service__s|&env__s|timestamp/V1"  (& marks LSM cutoff)
pub fn parse_sort_schema(input: &str) -> Result<sortschema::SortSchema, SortSchemaError> {
    let mut schema = sortschema::SortSchema::default();
    let mut s = input;

    // 1. Extract optional schema name: "name=rest"
    let parts: Vec<&str> = s.splitn(3, '=').collect();
    match parts.len() {
        1 => { /* no name */ }
        2 => {
            schema.name = parts[0].to_string();
            s = parts[1];
        }
        _ => return Err(SortSchemaError::MalformedSchema(input.to_string())),
    }

    // 2. Extract version suffix: "schema/V#"
    let version_parts: Vec<&str> = s.splitn(3, '/').collect();
    match version_parts.len() {
        1 => { /* no version -- will be rejected for V2-only enforcement */ }
        2 => {
            let version_str = version_parts[1];
            if !version_str.starts_with('V') {
                return Err(SortSchemaError::BadSortVersion(version_str.to_string()));
            }
            let version_num: i32 = version_str[1..].parse()
                .map_err(|_| SortSchemaError::BadSortVersion(version_str.to_string()))?;
            schema.sort_version = version_num;
            s = version_parts[0];
        }
        _ => return Err(SortSchemaError::MalformedSchema(input.to_string())),
    }

    // V2-only enforcement: reject INCORRECT_TRIM (0)
    if schema.sort_version < 1 {
        return Err(SortSchemaError::UnsupportedVersion {
            version: schema.sort_version,
            minimum: 1,
        });
    }

    // 3. Parse pipe-delimited columns
    let columns: Vec<&str> = s.split('|').collect();
    let mut cutoff_count = 0u32;

    for (i, col_str) in columns.iter().enumerate() {
        // Check for LSM cutoff marker
        let col_str = if col_str.starts_with('&') {
            cutoff_count += 1;
            if cutoff_count > 1 {
                return Err(SortSchemaError::MultipleCutoffMarkers);
            }
            if i == 0 {
                return Err(SortSchemaError::CutoffOnFirstColumn);
            }
            let stripped = &col_str[1..];
            if stripped.is_empty() {
                return Err(SortSchemaError::EmptyColumnAfterCutoff);
            }
            schema.lsm_comparison_cutoff = i as i32;
            stripped
        } else if col_str.contains('&') {
            return Err(SortSchemaError::CutoffInMiddleOfName(col_str.to_string()));
        } else {
            col_str
        };

        // Parse column: "name:type:+/-" or "name:+/-" or "name"
        let parts: Vec<&str> = col_str.split(':').collect();
        let (name, column_type, direction) = match parts.len() {
            1 => parse_column_implicit(parts[0])?,
            2 => parse_column_with_direction(parts[0], parts[1])?,
            3 => parse_column_explicit(parts[0], parts[1], parts[2])?,
            _ => return Err(SortSchemaError::InvalidColumnFormat(col_str.to_string())),
        };

        schema.column.push(sortschema::SortColumn {
            name: name.to_string(),
            column_type: column_type as u64,
            sort_direction: direction as i32,
        });
    }

    validate_schema(&schema)?;
    Ok(schema)
}
```

### Window Start Function with Proptest

```rust
// Source: ADR-003 specification + CONTEXT.md decisions
// Confidence: HIGH -- pure arithmetic with well-defined semantics

use chrono::{DateTime, Utc};
use proptest::prelude::*;

/// Compute the start of the time window containing the given timestamp.
pub fn window_start(timestamp_secs: i64, duration_secs: i64) -> DateTime<Utc> {
    debug_assert!(duration_secs > 0, "window duration must be positive");
    let remainder = timestamp_secs.rem_euclid(duration_secs);
    let start_secs = timestamp_secs - remainder;
    DateTime::from_timestamp(start_secs, 0)
        .expect("window_start timestamp out of range")
}

#[cfg(test)]
mod tests {
    use super::*;

    proptest! {
        #[test]
        fn window_start_is_aligned(
            ts in -1_000_000_000i64..2_000_000_000i64,
            dur in prop::sample::select(vec![60i64, 120, 180, 240, 300, 360,
                                             600, 720, 900, 1200, 1800, 3600])
        ) {
            let ws = window_start(ts, dur);
            let ws_secs = ws.timestamp();
            // window_start is aligned to duration
            prop_assert_eq!(ws_secs.rem_euclid(dur), 0);
            // timestamp is within [window_start, window_start + duration)
            prop_assert!(ws_secs <= ts);
            prop_assert!(ts < ws_secs + dur);
        }

        #[test]
        fn window_start_is_deterministic(
            ts in -1_000_000_000i64..2_000_000_000i64,
            dur in prop::sample::select(vec![60i64, 300, 900, 3600])
        ) {
            let ws1 = window_start(ts, dur);
            let ws2 = window_start(ts, dur);
            prop_assert_eq!(ws1, ws2);
        }
    }

    #[test]
    fn test_negative_timestamp_crossing() {
        // Timestamp -1 with 900s window should be in [-900, 0)
        let ws = window_start(-1, 900);
        assert_eq!(ws.timestamp(), -900);
    }

    #[test]
    fn test_zero_crossing() {
        // Timestamp 0 should be in [0, 900)
        let ws = window_start(0, 900);
        assert_eq!(ws.timestamp(), 0);
    }

    #[test]
    fn test_aligned_timestamp() {
        // Timestamp exactly on boundary
        let ws = window_start(900, 900);
        assert_eq!(ws.timestamp(), 900);
    }
}
```

### PostgreSQL Migration

```sql
-- Source: Analysis of existing migration 25 + ADR-003 requirements
-- Confidence: HIGH

-- 26_add-compaction-metadata.up.sql
ALTER TABLE metrics_splits
    ADD COLUMN IF NOT EXISTS window_start BIGINT,
    ADD COLUMN IF NOT EXISTS window_duration_secs INTEGER,
    ADD COLUMN IF NOT EXISTS sort_schema TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS num_merge_ops INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS row_keys BYTEA,
    ADD COLUMN IF NOT EXISTS zonemap_regexes JSONB NOT NULL DEFAULT '{}';

-- Compaction scope: (index_id, sort_schema, window_start) for Published splits
CREATE INDEX IF NOT EXISTS idx_metrics_splits_compaction_scope
    ON metrics_splits (index_id, sort_schema, window_start)
    WHERE split_state = 'Published';
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Fixed hardcoded sort in ParquetField::sort_order() | Configurable per-index sort schema (ADR-002) | Phase 31 | Enables custom sort per workload |
| No window partitioning (PartitionGranularity enum) | Fine-grained epoch-aligned windows (1-60 min) | Phase 31 | Enables time-windowed compaction |
| No compaction metadata columns | 6 new columns for compaction scope | Phase 31 | Enables compaction planning queries |
| split_metadata_json as sole metadata | SQL-queryable columns + JSON backup | Phase 31 | Enables efficient metastore queries |

**Deprecated/outdated:**
- `PartitionGranularity` enum (Hour/Day/Week): Will coexist with but be superseded by the new epoch-aligned windowing. Not removed in Phase 31.
- Fixed sort on `(MetricName, TagService, TagEnv, ...)`: Remains as the default until sort schema configuration is wired to the writer (Phase 32).

## Open Questions

1. **Zonemap regex complexity budget**
   - What we know: Husky uses `maxNumTransitions = 64` and `pruneEvery = 1000` as defaults. The automaton algorithm is ~300 lines of Go.
   - What's unclear: Whether to port the full automaton/DFA/pruning algorithm in Phase 31 or defer to Phase 32 (when zonemaps are actually computed at write time). Phase 31 only needs the storage types and columns, not the computation.
   - Recommendation: Define the storage types and columns in Phase 31. Port the regex builder in Phase 32 when it's needed at write time. This keeps Phase 31 focused on metadata types.

2. **RowKeys proto bytes serialization format in Parquet key_value_metadata**
   - What we know: The decision says "proto bytes (canonical) AND human-readable JSON key for debugging."
   - What's unclear: The exact key names in the Parquet key_value_metadata map.
   - Recommendation: Use `qh.row_keys` for proto bytes (base64 encoded since key_value_metadata values are strings) and `qh.row_keys_json` for human-readable JSON. The `qh.` prefix avoids collision with Parquet/Arrow standard keys.

3. **Should the new compaction RPC use the existing MetastoreService or a new service?**
   - What we know: The existing MetastoreService already has StageMetricsSplits, PublishMetricsSplits, ListMetricsSplits. Adding ListMetricsSplitsForCompaction follows the pattern.
   - What's unclear: Whether quickwit-codegen's service generation handles incremental RPC additions gracefully.
   - Recommendation: Add to existing MetastoreService. The Codegen builder pattern (used for all other RPCs) supports incremental additions.

## Sources

### Primary (HIGH confidence)
- Proto source: `/Users/george.talbot/dd/dd-source/domains/event-platform/shared/libs/event-store-proto/protos/event_store_sortschema/event_store_sortschema.proto` -- 214 lines, complete type definitions
- Go schemautils: `/Users/george.talbot/go/src/github.com/DataDog/dd-go/logs/apps/logs-event-store/storage/fragment/schemautils.go` -- parser, serializer, equivalence checks
- Go schemautils tests: `/Users/george.talbot/go/src/github.com/DataDog/dd-go/logs/apps/logs-event-store/storage/fragment/schemautils_test.go` -- 343 lines, comprehensive test cases
- Go validation: `/Users/george.talbot/go/src/github.com/DataDog/dd-go/logs/apps/logs-event-store/storage/fragment/validation.go` -- ValidateSchema function
- Go column types: `/Users/george.talbot/go/src/github.com/DataDog/dd-go/logs/apps/logs-event-store/types/types.go` -- TypeID definitions with explicit iota assertions
- Go column postfixes: `/Users/george.talbot/go/src/github.com/DataDog/dd-go/logs/apps/logs-event-store/columns/types.go` -- __s, __i, __nf suffix conventions
- Go zonemap builder: `/Users/george.talbot/go/src/github.com/DataDog/dd-go/logs/apps/logs-event-store/zonemap/builder.go` -- Fragment zone map builder
- Go zonemap automaton: `/Users/george.talbot/go/src/github.com/DataDog/dd-go/logs/apps/logs-event-store/zonemap/automaton.go` -- DFA-based regex generation
- Go zonemap minmax: `/Users/george.talbot/go/src/github.com/DataDog/dd-go/logs/apps/logs-event-store/zonemap/minmax.go` -- Hash-based min/max tracking
- Existing Rust metadata: `/Users/george.talbot/go/src/github.com/DataDog/quickhouse-pomsky/quickhouse-pomsky/quickwit/quickwit-parquet-engine/src/split/metadata.rs`
- Existing Rust postgres model: `/Users/george.talbot/go/src/github.com/DataDog/quickhouse-pomsky/quickhouse-pomsky/quickwit/quickwit-parquet-engine/src/split/postgres.rs`
- Existing build.rs: `/Users/george.talbot/go/src/github.com/DataDog/quickhouse-pomsky/quickhouse-pomsky/quickwit/quickwit-proto/build.rs`
- Existing SQL migration: `/Users/george.talbot/go/src/github.com/DataDog/quickhouse-pomsky/quickhouse-pomsky/quickwit/quickwit-metastore/migrations/postgresql/25_create-metrics-splits.up.sql`
- Existing metastore proto: `/Users/george.talbot/go/src/github.com/DataDog/quickhouse-pomsky/quickhouse-pomsky/quickwit/quickwit-proto/protos/quickwit/metastore.proto`
- ADR-002: `/Users/george.talbot/go/src/github.com/DataDog/quickhouse-pomsky/quickhouse-pomsky/docs/internals/adr/002-sort-schema-parquet-splits.md`
- ADR-003: `/Users/george.talbot/go/src/github.com/DataDog/quickhouse-pomsky/quickhouse-pomsky/docs/internals/adr/003-time-windowed-sorted-compaction.md`
- GAP-004: `/Users/george.talbot/go/src/github.com/DataDog/quickhouse-pomsky/quickhouse-pomsky/docs/internals/adr/gaps/004-incomplete-split-metadata.md`

### Secondary (MEDIUM confidence)
- Workspace Cargo.toml versions: prost 0.13, chrono 0.4, proptest 1.x, tonic 0.13 -- verified from `/Users/george.talbot/go/src/github.com/DataDog/quickhouse-pomsky/quickhouse-pomsky/quickwit/Cargo.toml`

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all libraries already in workspace, versions verified from Cargo.toml
- Architecture: HIGH -- all extension points verified from source code, Go reference implementations fully analyzed
- Pitfalls: HIGH -- derived from actual Go test cases and Rust codebase analysis, not hypothetical
- Proto integration: HIGH -- existing build.rs pattern analyzed, ~12 existing proto compilations as reference

**Research date:** 2026-02-23
**Valid until:** 2026-04-23 (stable domain, no external dependency changes expected)
