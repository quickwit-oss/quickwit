# GAP-011: No Planner-Level Legacy Promotion

**Status**: Open
**Discovered**: 2026-05-18
**Context**: Codex review on PR #6423 (`feat(merge): legacy promotion path + body-col schema evolution`) flagged that the promotion path is wired end-to-end at the library + executor layer but has no production trigger at the planner / policy level.

## Problem

The streaming Parquet merge stack now contains a complete *legacy promotion* pipeline:

- `ParquetMergeOperation::promote_legacy(splits, target_prefix_len)` constructs an operation with
  `target_prefix_len_override = Some(target)`.
- `merge::execute_merge_operation` routes each input through `LegacyInputAdapter` when its
  declared `rg_partition_prefix_len < target` and through `StreamingParquetReader` otherwise. The
  streaming engine then sees a homogeneous stream advertising `prefix_len = target` on every
  input.
- `ParquetMergeExecutor` (in `quickwit-indexing`) detects `target_prefix_len_override.is_some()`
  and routes those merges through `execute_merge_operation` (with `LocalFileByteSource`) instead
  of the in-memory `merge_sorted_parquet_files` path.
- `merge_parquet_split_metadata` accepts a `mixed_prefix_ok: bool` flag so the post-merge
  aggregator skips the input-side equality check.

What's missing: **nothing in the planner ever creates a `promote_legacy` operation in
production**. `MergePolicyState::record_split` buckets each split by
`CompactionScope::from_split`, and that scope key includes `rg_partition_prefix_len`. Legacy
splits (`prefix_len = 0`) and aligned splits (`prefix_len > 0`) therefore land in *different*
buckets before `ParquetMergePolicy::operations` ever runs. The production policy then iterates
each bucket independently and emits `ParquetMergeOperation::new` (regular merge). A repo-wide
search finds `promote_legacy` only in tests.

In a mixed deployment (legacy + aligned splits coexisting), legacy splits therefore stay in
their `prefix_len = 0` bucket forever — never gaining the prefix alignment that downstream
locality compaction depends on. The promotion plumbing is reachable only from tests.

## Evidence

- `quickwit-parquet-engine/src/merge/policy/mod.rs`: `ParquetMergePolicy::operations` calls
  `ParquetMergeOperation::new(...)` only. `promote_legacy` is constructed only by tests in the
  same file.
- `MergePolicyState::record_split` keys its `BTreeMap` by `CompactionScope::from_split`. The
  scope derivation includes `rg_partition_prefix_len`, so a legacy split and a prefix-aligned
  split with otherwise identical sort fields / window / merge level are never compared by the
  policy.
- The executor branch added in PR #6423 (`scratch.merge_operation.target_prefix_len_override
  .is_some()`) routes promotion through `execute_merge_operation`. Library coverage at
  `test_promote_legacy_executor_end_to_end` exercises a `prefix_len = 0` + `prefix_len = 1` pair
  successfully. But that operation is only ever constructed inside the test.

## State of the Art

- **Iceberg**: Compaction policies inspect file-level metadata (partitioning, sort order) and
  can rewrite files to align with the latest table partitioning even when individual files
  pre-date the change. The compaction service treats schema-evolution-style rewrites as
  first-class operations.
- **Husky**: Background re-organization passes that promote files into newer storage layouts.
  Tracked separately from the size-tiered compaction policy so cost trade-offs can be tuned.

In both cases, the design separates the *trigger* (decision to promote) from the *mechanism*
(how the promotion is performed). Quickwit currently has the mechanism but not the trigger.

## Potential Solutions

### Option A: Merge legacy + aligned buckets in `CompactionScope::from_split`

Drop `rg_partition_prefix_len` from the scope key (or normalize it to a target value before
bucketing). The policy then sees legacy and aligned splits as candidates for the same
compaction operation and `ParquetMergePolicy::operations` decides whether to emit a regular
merge or a `promote_legacy` operation based on whether the bucket contains mixed prefix
lengths.

Simplest change, but requires the policy to detect mixed-prefix buckets and choose between
`new` and `promote_legacy` per operation.

### Option B: Dedicated promotion pass

Run a separate pass before the regular compaction policy that scans for legacy splits and emits
`promote_legacy` operations for them. The regular policy then sees only aligned splits.

Cleaner separation of concerns, but means legacy splits are migrated *before* any opportunity
to coalesce them with aligned neighbors in a single multi-input merge — possibly more work
overall.

### Option C: Hybrid — bucket together, prefer single-pass promotion

Keep scope bucketing as in option A. Inside the policy, when a bucket contains mixed prefix
lengths AND has enough splits to merit a multi-input merge, emit `promote_legacy`. When only
legacy splits exist (no aligned neighbor), emit `promote_legacy` with the same target — single-
input promotion is still valuable because it converts the file to the new format for future
locality compaction.

Most flexible; gives the policy the freedom to amortize promotion cost when there are aligned
neighbors AND to still promote isolated legacy splits in the background.

## Signal Impact

Primarily affects **metrics** in the near term: the legacy split format pre-dates the
prefix-aligned RG layout, and only metrics has both formats in flight today. Traces and logs
on the Parquet path will eventually reach the same state if a layout change ever happens; the
same planner machinery would cover them.

## Cost Considerations

Promotion is strictly more expensive than a regular merge: the legacy adapter buffers the full
input file in memory and re-encodes it as a single-RG stream before the merge engine sees it.
For 50 MB metrics splits this is acceptable; for larger inputs the in-memory buffer is the
gating cost.

The planner should account for this when scheduling — promotion is best amortized into a
multi-input merge rather than performed as a standalone file rewrite. Option C's "prefer
multi-input promotion, fall back to single-input" structure captures this.

## Impact

- **Severity**: Medium. Legacy splits accumulate cost (every query against them pays the
  prefix-less scan cost) but correctness is preserved — the locality compaction stack still
  works on aligned splits.
- **Frequency**: Persistent. Legacy splits never migrate without an explicit trigger.
- **Affected Areas**: `quickwit-parquet-engine/src/merge/policy/`, `quickwit-parquet-engine/src/merge/mod.rs` (`MergePolicyState::record_split` + `CompactionScope`).

## Next Steps

- [ ] Decide between options A / B / C based on operational priorities and benchmark data.
- [ ] Design the policy-level "should promote?" heuristic: how many legacy splits before
      triggering, whether to wait for aligned neighbors, how to deprioritize promotion vs
      regular compaction.
- [ ] Add metrics for `legacy_splits_pending_promotion` and `promotion_operations_emitted` so we
      can observe the policy in production.
- [ ] Wire whichever option is chosen, with an integration test that exercises the full path
      (legacy split → planner → executor → published prefix-aligned split).

## References

- PR #6423 (legacy promotion path + body-col schema evolution).
- Codex review comment id `4311184497` (raised the gap).
- `test_promote_legacy_executor_end_to_end` in `quickwit-parquet-engine::merge::streaming` —
  library-level coverage of the mechanism.
