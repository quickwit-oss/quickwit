# Deviation 001: Dual Parquet merge engines during streaming-engine rollout

## Summary

Two Parquet merge engines coexist in production behind a runtime YAML flag.
The streaming engine (`execute_merge_operation`) matches the intent of
[ADR-003 §4](../003-time-windowed-sorted-compaction.md) (page-granular
streaming, bounded memory). The in-memory engine
(`merge_sorted_parquet_files`) is retained as the runtime fallback so an
operator can flip back via configuration if the streaming engine hits a
production bug. The dual-engine state is intentional and time-bounded —
it ends when the streaming engine has soaked at the new default in
production.

## Related ADR

- **ADR**: [ADR-003 Time-Windowed Sorted Compaction](../003-time-windowed-sorted-compaction.md)
- **Section**: §4 Sorted Merge, Phase 2 (column streaming)

## ADR States

> Phase 2: Stream columns through the merge.
>
> Once the global sort order is determined, each column is read from the
> input splits and written to the output in sorted order. Columns are
> processed one at a time (or in small groups) for memory efficiency.
>
> For large columns, it may be advantageous to operate at **page
> granularity** rather than loading an entire column from each input:
> read individual Parquet pages from inputs as needed and write
> individual pages to the output. This bounds memory usage for columns
> with large values (e.g., high-cardinality string tags, large attribute
> maps) and avoids materializing an entire column across all inputs
> simultaneously.

ADR-003 §4 describes the merge as a streaming operation that bounds
memory by reading and writing pages incrementally. The in-memory
`merge_sorted_parquet_files` engine pre-materializes whole columns from
all inputs simultaneously — directly contrary to the ADR's stated
memory model.

## Current Implementation

Both engines live in `quickwit-parquet-engine/src/merge/`:

- **Streaming engine** (`execute_merge_operation`, in `merge/mod.rs`,
  backed by `merge/streaming.rs`). Column-major, page-bounded body cache,
  reads inputs through `RemoteByteSource`. This is the
  ADR-003-compliant implementation. It is the unconditional path for
  promotion merges (the in-memory path can't handle mixed
  `rg_partition_prefix_len`) and the opt-in path for regular merges.
- **In-memory engine** (`merge_sorted_parquet_files`, in `merge/mod.rs`).
  Buffers all inputs through arrow-rs into memory, runs the merge under
  `run_cpu_intensive`. This is the original bootstrap implementation
  retained as the runtime fallback.

`ParquetMergeExecutor::handle` routes between them:

```rust
let is_promotion = scratch.merge_operation.target_prefix_len_override.is_some();
if is_promotion || self.use_streaming_engine {
    execute_merge_operation(&op, sources, &output_dir, &config).await
} else {
    run_cpu_intensive(move || {
        merge_sorted_parquet_files(&input_paths, &output_dir_clone, &config)
    }).await
}
```

The `use_streaming_engine` boolean is sourced from the node-level
`IndexerConfig::parquet_merge_use_streaming_engine` YAML field, default
`false`.

Row-content equivalence between the two engines is enforced by parity
tests in `quickwit-parquet-engine/src/merge/tests.rs::parity`. These
must keep passing as long as both engines coexist.

## Signal Impact

Applies to **metrics** (the only signal currently using the Parquet
pipeline). Will apply to **traces** and **logs** when those signals
adopt Parquet splits. The deviation does not affect Tantivy-backed
pipelines.

## Impact

| Aspect | ADR Target | Current Reality |
|--------|------------|-----------------|
| Engines in production | One streaming engine | Two (streaming + in-memory) |
| Memory model | Page-bounded; ~constant per column | In-memory engine: O(total input column size) per merge |
| Configuration surface | None — engine choice is internal | One YAML flag (`parquet_merge_use_streaming_engine`) |
| Code surface to maintain | One engine | Two engines + parity tests + routing branch |
| Operator rollback | Not applicable — only one path | Flip flag to `false`, no redeploy needed |

## Why This Exists

The streaming engine is new code. ADR-003 describes the target memory
model but does not guarantee bug-free first-deployment behavior. Three
forces produced the dual-engine state:

1. **Production safety.** The in-memory engine has been the live merge
   path during the metrics pipeline's bring-up. Replacing it wholesale
   on a single PR, without an in-place fallback, would mean any bug in
   the streaming engine requires a redeploy to recover. With the flag,
   recovery is `config edit + restart`.
2. **Staged rollout.** Production confidence is built by enabling the
   streaming engine on a soak fleet, observing for some time, then
   flipping the default. The dual-engine state is the necessary
   infrastructure for that rollout.
3. **Parity verifiable, not certain.** The parity tests in
   `merge::tests::parity` cover representative synthetic fixtures.
   Production data has shapes those fixtures don't cover. The fallback
   exists because parity is a strong-but-not-total guarantee.

## Priority Assessment

- **PoC / MVP**: acceptable — dual-engine is in fact the deliberate
  state during MVP rollout.
- **Production (current)**: acceptable — flag defaults to `false`,
  rollout has not begun. The streaming engine is exercised only by
  promotion merges (whose execution will start once GAP-011 is closed).
- **Production (post-soak)**: not acceptable. Once the streaming engine
  has soaked at default-`true` in production, the in-memory engine
  becomes dead code that complicates the merge-executor and obscures
  the ADR-003 memory contract. Resolve before merging additional
  significant work into `parquet_merge_executor.rs`.

## Exit Criteria

The deviation resolves when **all** of the following hold:

1. `IndexerConfig::default_parquet_merge_use_streaming_engine` defaults
   to `true` in `quickwit-config`.
2. At least one production fleet has run with the flag set to `true` for
   a soak window of ≥ 2 weeks with no merge-correctness incidents (no
   data loss, no schema mismatch, no merge-output-rows-≠-input-rows
   alerts).
3. No deviation-resolving rollback has been issued during the soak.

When those are met, the follow-up PR:

- Deletes `merge_sorted_parquet_files` from `quickwit-parquet-engine`.
- Deletes the in-memory branch in `ParquetMergeExecutor::handle`.
- Deletes the `use_streaming_engine` field on `ParquetMergeExecutor` and
  `ParquetMergePipelineParams`.
- Deletes `IndexerConfig::parquet_merge_use_streaming_engine`.
- Deletes `merge::tests::parity` (both engines no longer exist to
  compare).
- Closes this deviation.

## Work Required to Match ADR

| Change | Difficulty | Description |
|--------|------------|-------------|
| Flip default to `true` | Trivial | One-line change in `IndexerConfig::default_parquet_merge_use_streaming_engine`. Lands after soak. |
| Production soak | Operational | Run with `true` on at least one fleet for ≥ 2 weeks, monitor merge-correctness signals. |
| Delete in-memory engine | Moderate | Remove `merge_sorted_parquet_files`, the fallback branch, the flag, and the parity tests. Mechanically straightforward but touches several call sites. |

## Recommendation

**Accept for now.** The dual-engine state is the deliberate output of a
flag-with-fallback rollout pattern (see commit history of #6441 and
related PRs). Resolution is a known follow-up, not technical debt that
needs to be paid down ahead of schedule.

Track the exit criteria in this doc. When all three conditions hold,
open the deletion PR and close this deviation.

## References

- [ADR-003 Time-Windowed Sorted Compaction](../003-time-windowed-sorted-compaction.md) §4
- [GAP-011 No legacy promotion planner](../gaps/011-no-legacy-promotion-planner.md)
- [GAP-012 Merge downloads instead of streaming](../gaps/012-merge-downloads-instead-of-streaming.md)
- PR #6441 (wire-in of the YAML flag)

## Date

2026-05-18
