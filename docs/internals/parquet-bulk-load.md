# Parquet bulk load in `quickwit tool local-ingest`

## Goal

For single-machine benchmarks: load a Parquet file into an index as fast as the
machine allows. The load:

- bypasses the ingest API, the ingesters and the WAL;
- runs N indexing pipelines in parallel in one process, all fed from the same
  file;
- goes through the real indexing pipeline
  (`DocProcessor → Indexer → IndexSerializer → Packager → Uploader → Sequencer → Publisher`).

```
quickwit tool local-ingest --index idx --input-path file:///data/x.parquet \
    --num-pipelines N
```

## Constraints

- No new `SourceType`, no new `SourceInputFormat` and no proto change. The CLI
  detects Parquet from the `.parquet` extension of `--input-path` and builds a
  regular `file` source config with the `json` input format: the Parquet source
  decodes the rows and emits them as JSON documents.
- The server path is unchanged: the Parquet source exists only in the CLI's
  indexing service (see [Work distribution](#work-distribution)).
- `--num-pipelines > 1` requires a `.parquet` input file. The NDJSON path is
  unchanged.
- Parquet support is behind the `parquet` Cargo feature of `quickwit-indexing`
  (forwarded by `quickwit-cli`), off by default. #6809 removed Parquet from the
  default build on purpose.
- Local `file://` inputs only.
- No new merge logic (see [Merges](#merges)).

## All-or-nothing load

There is no checkpoint and no resume. A load requires an index without
published splits (`--overwrite` clears it). If anything fails once the
pipelines are spawned, the CLI stops all the actors, clears the index and exits
with an error. Failures include:

- a pipeline restart, or a pipeline spawn retry: `IndexingPipeline` respawns
  failed pipelines forever, so the CLI fails as soon as a pipeline reports a
  `generation` or a `num_spawn_attempts` above 1;
- a merge pipeline failure;
- invalid documents, or a number of indexed documents different from the number
  of rows of the file.

The sources emit empty checkpoint deltas, which the metastore skips.

## Work distribution

`ParquetLoadPlan` (`quickwit-indexing/src/source/parquet_file/plan.rs`) holds
the parsed footer and an atomic next-row-group counter. The CLI builds it, then
hands it to the pipelines' sources:

```
CLI
 ├─ plan = ParquetLoadPlan::try_new(file)
 ├─ source_loader: File → ParquetSourceFactory { plan }
 ├─ IndexingService::new(...).with_source_loader(source_loader)
 └─ SpawnPipeline × N
      └─ IndexingPipeline: source_loader.load_source(...) → ParquetSource { plan }
```

`IndexingService` and `IndexingPipelineParams` carry a `SourceLoader`, which
defaults to `quickwit_supported_sources()`. Only the CLI replaces it, so a
server never runs a Parquet source. `ParquetSourceFactory` fails if the source
config reads another file than the plan, or if its input format is not `json`.

Each source leases one row group at a time, streams it as record batches, and
exits with success once no row group is left. A dynamic queue balances row
groups of different sizes.

All the pipelines use `CLI_SOURCE_ID` with distinct `PipelineUid`s, so they
share one merge pipeline. The CLI detaches it after all the spawns, otherwise
the indexing service would spawn a new one.

## Reading and converting Parquet

- `ParquetRecordBatchReaderBuilder` over a local file, built from the footer
  parsed once by the plan, with `.with_row_groups(vec![idx])` and
  `.with_batch_size(batch_num_rows)` (default 8192).
- Decoding and conversion run on the `Blocking` runtime: the source actor runs
  on the small `NonBlocking` runtime.
- Record batches are converted to NDJSON with `arrow_json` and sent as regular
  `RawDocBatch`es of at most 5 MiB. The `DocProcessor` receives JSON documents
  and does not know about Parquet, so VRL, the doc mapping and the error
  counters are unchanged. The cost
  is a JSON round trip: going straight from Arrow to documents is a possible
  follow-up.
- The plan trial-decodes one row, so an unsupported schema fails before any
  pipeline is spawned.

| Arrow | JSON |
|---|---|
| `Timestamp` / `Date*` | RFC 3339 / date string. Timestamps without a time zone get a `Z` suffix |
| `Binary` | hex string |
| `Struct` / `List` / `Map` | object / array |
| `Decimal` | number |
| null | key omitted |

## Merges

For benchmarks, use the `no_merge` merge policy and size the splits with the
indexing settings. A split is cut when the first of these limits is reached:

- `split_num_docs_target` documents;
- `resources.heap_size` of indexer memory;
- `commit_timeout_secs`;
- the end of the pipeline's input.

For example, 1M-document splits:

```yaml
indexing_settings:
  split_num_docs_target: 1000000
  commit_timeout_secs: 3600
  resources:
    heap_size: 2GB
  merge_policy:
    type: no_merge
```

This gives about `num_rows / split_num_docs_target` splits, plus up to one
partial split per pipeline. With another merge policy, the CLI shuts the merge
pipeline down with `FinishPendingMergesAndShutdownPipeline`, like the NDJSON
path: ongoing and pending merges complete, but the result is not the layout
that continuous merging would reach.

## CLI flow (`quickwit-cli/src/tool/parquet_ingest.rs`)

1. Run the index checklist, clear the index with `--overwrite`, and fail if the
   index has published splits.
2. Build the plan and a source loader that creates Parquet sources from it.
3. Spawn the `IndexingService` with that loader and N pipelines, then display their progress
   until they all exit.
4. Send `FinishPendingMergesAndShutdownPipeline` and wait for the merge
   pipeline.
5. Print a report (rows and bytes, throughput, split count, effective settings)
   and check it.
6. On failure in steps 3 to 5, quit the universe and clear the index.

Sizing: each pipeline uses about 1.5 to 2 cores, so `--num-pipelines` defaults
to half the number of CPUs. Each pipeline uses up to the index's `heap_size`.
For large runs, use a PostgreSQL metastore: the file-backed metastore rewrites
the whole index metadata on every publish.

## Testing

- `parquet_file/tests.rs`: the source emits every row in order. The factory
  rejects a source config for another file or with a non-`json` input format,
  and the plan rejects remote files.
- `quickwit-cli/tests/parquet.rs` (`--features parquet`): 20 row groups loaded
  by 4 pipelines with the `no_merge` policy. A second load fails
  without `--overwrite`, and a load with an invalid document clears the index.
