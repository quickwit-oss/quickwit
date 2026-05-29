# GAP-012: Parquet Merge Executor Downloads Inputs Instead of Streaming Them

**Status**: Open
**Discovered**: 2026-05-18
**Context**: Code review of the Parquet streaming merge stack (PRs #6407–#6428) — specifically the executor wiring on #6423 — surfaced the question of why the merge actor downloads every input to local disk before merging when the streaming engine is designed around `RemoteByteSource`.

## Problem

The Parquet streaming merge engine in `quickwit-parquet-engine` consumes inputs through a
minimal `RemoteByteSource` trait (`file_size`, `get_slice`, `get_slice_stream`). The trait was
deliberately defined so the engine can pull pages column-major directly from object storage —
two GETs per input (footer + body stream) and the merge progresses as bytes arrive, holding
only the page-bounded engine state in memory.

The actor pipeline in `quickwit-indexing` doesn't use that design. The
`ParquetMergeSplitDownloader` actor pulls each input via `storage.copy_to_file(remote_path,
local_path)` into a scratch directory, then hands `Vec<PathBuf>` to the
`ParquetMergeExecutor`. The executor then either:

- Calls the in-memory `merge_sorted_parquet_files(input_paths, ...)` (regular merges), which
  reads each file fully into Arrow RecordBatches before merging, OR
- Wraps each local path in a `LocalFileByteSource` and calls `execute_merge_operation` (added in
  PR #6423 for promotion merges only).

Either way, the streaming engine's central design benefit — overlapping the fetch with the
merge and skipping the scratch disk entirely — is unused in production. Every merge reads each
input twice: once over the network into scratch, once off scratch through the merger.

## Evidence

- `quickwit-indexing/src/actors/parquet_pipeline/parquet_merge_split_downloader.rs`: per-split
  loop calling `self.storage.copy_to_file(...)` to materialize every input on local disk before
  forwarding `ParquetMergeScratch` to the executor.
- `quickwit-indexing/src/actors/parquet_pipeline/parquet_merge_executor.rs`: receives
  `downloaded_parquet_files: Vec<PathBuf>` and chooses between the in-memory path or
  `execute_merge_operation` with `LocalFileByteSource` wrappers — never a `RemoteByteSource`
  that actually streams from object storage.
- `quickwit-parquet-engine/src/storage/streaming_reader.rs:62-67`: the `RemoteByteSource` trait
  doc explicitly notes that callers in `quickwit-indexing` "provide a thin adapter that
  delegates to `quickwit_storage::Storage`." The adapter exists in principle but isn't wired up
  for the merge executor.

## State of the Art

- **ClickHouse `MergeTree`**: parts are accessed via the same storage abstraction whether the
  merge runs locally or against tiered/object storage. There's no separate "download then
  merge" actor pair — the merger reads parts where they live.
- **Iceberg compaction**: data files are read directly from object storage by the compaction
  job. Local scratch is used only for the output file before commit.
- **Husky**: column-major streaming merge reads directly from blob storage. Designed around the
  "two GETs per input" model the Quickwit streaming engine inherits.

Across these systems, downloading inputs before merging is treated as a fallback for
operational reasons (unreliable network, kernel page-cache effects), not the default.

## Trade-offs

### Why download-first is the current default
- **Retry locality**: the downloader actor centralizes retry/backoff/timeout for one file at a
  time. A mid-fetch S3 hiccup retries the download alone; the merger sees only successful
  downloads.
- **Pure-compute executor**: once files are on disk the executor has no network dependency.
  Mid-merge failures are restricted to disk I/O and compute errors.
- **Predictable disk budget**: scratch usage is bounded by `Σ input_sizes` per concurrent
  merge. Easy to reason about; easy to cap.
- **Legacy in-memory path**: `merge_sorted_parquet_files` predates the streaming engine and
  requires local file paths. The download-first pattern existed before there was a streaming
  alternative.

### What download-first costs
- **2× I/O per merge**: each input is transferred over the network into scratch AND read off
  scratch into the merger. The kernel page cache mitigates the disk-read pass to some extent but
  doesn't fully erase it.
- **Serialized phases**: the merge can't start until *all* inputs are downloaded. First-byte
  latency on the merger is `max(input download time)` instead of `min(input first-byte time)`.
- **Scratch disk usage**: a typical compaction merging 8× 50 MB splits holds 400 MB of scratch
  per merge, multiplied by the concurrent merge count. On lightweight indexer pods this caps
  parallelism.
- **Underused design**: the streaming engine's single-body-GET model + page-bounded memory was
  built specifically for the no-scratch-disk case. Wiring through `LocalFileByteSource` works
  but bypasses the property the design was built around.

### What streaming-directly would cost
- **Mid-merge retry surface**: a connection failure mid-body-GET kills the merge attempt
  entirely. Single-body-GET is forward-only — no partial recovery. The retry surface becomes
  "the merge failed after 30 % of work," not "the download failed, retry the file."
- **Per-merge S3 connection count**: an N-way merge holds N concurrent body streams plus N
  footer connections. On dense merger nodes this multiplies.
- **Tail latency**: the merge progresses at the speed of the slowest input. With downloads,
  parallel fetches average out; with streaming a slow input throttles the whole merge.

## Potential Solutions

### Option A: Streaming-directly when the input is reachable, download as fallback

The executor receives a hint from the storage layer (or detects mid-merge failure rates) and
chooses per merge. Splits stored on reliable, low-latency backends go through `RemoteByteSource`
adapters that talk directly to `quickwit_storage::Storage`; on flaky or high-latency backends
the downloader actor still materializes files first.

Largest design lift but matches what mature compaction systems do.

### Option B: Stream-directly by default, fall back to download on persistent failures

Default to streaming; a circuit-breaker on per-merge failure rate routes the next attempt
through download-first. Operationally simpler than Option A; tail latency is bounded by the
circuit's reaction time.

### Option C: Keep download-first but eliminate the in-memory merge path

Make every merge go through `execute_merge_operation` with `LocalFileByteSource`. This doesn't
recover the streaming engine's "no scratch disk" benefit but does remove the legacy in-memory
codepath, simplifying the executor to a single path.

Smallest change, smallest gain. Worth doing regardless of A/B as a stepping stone.

### Option D: Streaming-directly only for promotion merges

Promotion already routes through `execute_merge_operation`; extend it to skip the download
phase entirely for those operations and let the regular path stay as-is. Gains: legacy-adapter-
backed promotion merges (the in-memory-buffering-heaviest case in the pipeline) avoid double
I/O. Costs: split executor logic into "promotion = stream" vs "regular = download."

## Signal Impact

All Parquet-backed signals. Metrics is the first product to ship, so the impact lands on
metrics first; traces and logs (when they migrate to Parquet storage) will pay the same cost
unless this is addressed by then.

## Cost Considerations

The streaming engine's body-col page cache is already designed for backpressure: pages stream
in column-major order as bytes arrive, and the engine processes them as quickly as it can. The
bottleneck for streaming-directly becomes the slowest input's transfer rate rather than the
total input size — usually a smaller number, but a longer tail.

## Impact

- **Severity**: Medium. Correctness is unaffected; the streaming engine works equivalently
  whether the source is local or remote. The cost is bandwidth, disk, and wall-clock latency.
- **Frequency**: Every merge in production today pays the download cost.
- **Affected Areas**: `quickwit-indexing/src/actors/parquet_pipeline/parquet_merge_split_downloader.rs`,
  `quickwit-indexing/src/actors/parquet_pipeline/parquet_merge_executor.rs`,
  `quickwit-parquet-engine::merge::execute_merge_operation` callers.

## Next Steps

- [ ] Measure the current download-vs-merge phase breakdown on a representative production
      merge load (wall-clock + bytes-read + disk-write).
- [ ] Build a `RemoteByteSource` adapter over `quickwit_storage::Storage` and prototype
      streaming-directly for promotion merges (Option D) to validate the engine's behavior
      against the existing storage backends.
- [ ] Decide between options A / B based on observed mid-merge failure rates under real S3
      conditions.
- [ ] Even if the default stays download-first, consider Option C as a simplification — the
      in-memory merge path is dead weight once the streaming engine handles every case.

## References

- PR #6407–#6428 (Parquet streaming merge stack).
- [PR #6423 discussion](https://github.com/quickwit-oss/quickwit/pull/6423) — surfaced the
  question while wiring promotion through `execute_merge_operation`.
- `quickwit-parquet-engine/src/storage/streaming_reader.rs` (`RemoteByteSource` trait).
- `quickwit-indexing/src/actors/parquet_pipeline/parquet_merge_executor.rs::LocalFileByteSource`.
