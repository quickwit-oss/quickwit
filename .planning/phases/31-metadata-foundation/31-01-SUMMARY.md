---
phase: 31-metadata-foundation
plan: 01
subsystem: compaction
tags: [protobuf, prost, sort-schema, parser, husky-interop]

# Dependency graph
requires: []
provides:
  - "Generated Rust types from event_store_sortschema.proto (SortSchema, SortColumn, SortColumnDirection, ColumnValue, ColumnValues, RowKeys)"
  - "Sort schema parser (parse_sort_schema) matching Go StringToSchema with strict V2-only enforcement"
  - "Display functions (schema_to_string, schema_to_string_short) matching Go SchemaToString/SchemaToStringShort"
  - "Validation (validate_schema) matching Go ValidateSchema"
  - "Equivalence functions (equivalent_schemas, equivalent_schemas_for_compaction) matching Go EquivalentSchemas"
  - "ColumnTypeId enum with Go-compatible discriminant values"
affects: [31-02, 31-03, 31-04, 32-ingestion-pipeline, 33-merge-policy, 35-compaction-planning]

# Tech tracking
tech-stack:
  added: [prost-build (sortschema proto compilation)]
  patterns: [Go-to-Rust port with strict V2-only enforcement, proto type derivation with Serialize/Deserialize]

key-files:
  created:
    - quickwit/quickwit-proto/protos/event_store_sortschema/event_store_sortschema.proto
    - quickwit/quickwit-proto/src/codegen/sortschema/mod.rs
    - quickwit/quickwit-proto/src/codegen/sortschema/sortschema.rs
    - quickwit/quickwit-parquet-engine/src/sort_schema/parser.rs
    - quickwit/quickwit-parquet-engine/src/sort_schema/display.rs
    - quickwit/quickwit-parquet-engine/src/sort_schema/column_type.rs
    - quickwit/quickwit-parquet-engine/src/sort_schema/validation.rs
    - quickwit/quickwit-parquet-engine/src/sort_schema/equivalence.rs
    - quickwit/quickwit-parquet-engine/src/sort_schema/tests.rs
  modified:
    - quickwit/quickwit-proto/build.rs
    - quickwit/quickwit-proto/src/lib.rs
    - quickwit/quickwit-parquet-engine/Cargo.toml
    - quickwit/quickwit-parquet-engine/src/sort_schema/mod.rs

key-decisions:
  - "Strict V2-only enforcement: reject sort_version < 2 (INCORRECT_TRIM=0, TRIMMED_WITH_BUDGET=1)"
  - "Use standalone prost_build::Config (not Codegen builder) since proto has no gRPC service"
  - "ColumnTypeId enum with only sort-schema-relevant types (Int64=2, Float64=10, String=14, Sketch=17, CpcSketch=20, ItemSketch=22)"
  - "type_id_from_string accepts only dense-* canonical names to ensure 3-part type verification works correctly"

patterns-established:
  - "Proto vendoring: copy from dd-source, add origin comment, compile with prost_build"
  - "Go-to-Rust port: match Go function signatures and error semantics, port all test cases"
  - "Sort schema module structure: column_type, parser, display, validation, equivalence as separate focused files"

requirements-completed: [META-01, META-02]

# Metrics
duration: 11min
completed: 2026-02-23
---

# Phase 31 Plan 01: Sort Schema Proto and Parser Summary

**Vendored Husky event_store_sortschema.proto with prost-build compilation and complete sort schema parser ported from Go schemautils.go with strict V2-only enforcement**

## Performance

- **Duration:** 11 min
- **Started:** 2026-02-23T20:30:35Z
- **Completed:** 2026-02-23T20:42:28Z
- **Tasks:** 2
- **Files modified:** 14

## Accomplishments
- Vendored event_store_sortschema.proto from dd-source with all 6 message/enum types generated via prost-build (SortSchema, SortColumn, SortColumnDirection, ColumnValue, ColumnValues, RowKeys)
- Complete sort schema parser matching Go StringToSchema with all 3 column formats (1-part, 2-part, 3-part), LSM cutoff marker parsing, and named schema support
- Strict V2-only enforcement: unversioned, V0, and V1 strings rejected; only sort_version >= 2 accepted
- 43 tests ported from Go schemautils_test.go covering parsing, display, validation, equivalence, LSM cutoff, round-trips, and error paths (65 total including existing window tests)

## Task Commits

Each task was committed atomically:

1. **Task 1: Vendor proto and add prost-build compilation** - `449b03c76` (feat)
2. **Task 2: Implement sort schema parser, display, validation, equivalence with tests** - `c21cebf6e` (feat)

## Files Created/Modified
- `quickwit/quickwit-proto/protos/event_store_sortschema/event_store_sortschema.proto` - Vendored proto with all Husky sort schema types
- `quickwit/quickwit-proto/build.rs` - Added prost_build compilation block for sortschema
- `quickwit/quickwit-proto/src/lib.rs` - Added `pub mod sortschema` export
- `quickwit/quickwit-proto/src/codegen/sortschema/sortschema.rs` - Generated Rust types from proto
- `quickwit/quickwit-proto/src/codegen/sortschema/mod.rs` - Module declaration with include
- `quickwit/quickwit-parquet-engine/Cargo.toml` - Added quickwit-proto dependency
- `quickwit/quickwit-parquet-engine/src/sort_schema/mod.rs` - Extended with new modules and SortSchemaError variants
- `quickwit/quickwit-parquet-engine/src/sort_schema/column_type.rs` - ColumnTypeId enum and type_id_from_postfix/string functions
- `quickwit/quickwit-parquet-engine/src/sort_schema/parser.rs` - parse_sort_schema function (Go StringToSchema port)
- `quickwit/quickwit-parquet-engine/src/sort_schema/display.rs` - schema_to_string and schema_to_string_short functions
- `quickwit/quickwit-parquet-engine/src/sort_schema/validation.rs` - validate_schema function (Go ValidateSchema port)
- `quickwit/quickwit-parquet-engine/src/sort_schema/equivalence.rs` - equivalent_schemas and equivalent_schemas_for_compaction functions
- `quickwit/quickwit-parquet-engine/src/sort_schema/tests.rs` - 43 tests ported from Go schemautils_test.go

## Decisions Made
- **Strict V2-only enforcement**: Reject sort_version < 2 per user-confirmed decision. INCORRECT_TRIM (V0) and TRIMMED_WITH_BUDGET (V1) are not compatible with the LSM algorithm and must be excluded.
- **Standalone prost_build::Config**: The proto has no gRPC service, so we used prost_build directly rather than the Quickwit Codegen builder which generates service stubs.
- **ColumnTypeId with Go-compatible discriminants**: Int64=2, Float64=10, String=14, Sketch=17 exactly match Go iota values for cross-system interop.
- **type_id_from_string limited to dense canonical names**: Only accepts "dense-int64", "dense-string", "dense-float64", "dense-sketch" etc. to ensure the 3-part type verification correctly rejects mismatches.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- Pre-existing `Path::exists()` usage in `split_writer.rs` causes clippy deny error when running with `--tests` flag. This is out of scope for this plan and does not affect the sort schema code (zero clippy warnings from sort_schema modules).

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Proto types accessible from `quickwit_proto::sortschema::*` for all downstream phases
- Sort schema parser ready for use in metadata deserialization (31-02, 31-03)
- Equivalence functions ready for compaction planning (35)
- All Go test cases passing, ensuring cross-system interoperability

## Self-Check: PASSED

All 9 created files verified present. Both task commits (449b03c76, c21cebf6e) verified in git history.

---
*Phase: 31-metadata-foundation*
*Completed: 2026-02-23*
