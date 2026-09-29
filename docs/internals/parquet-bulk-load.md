# Parquet bulk load

Benchmark-only: `quickwit tool local-ingest` loads a local `.parquet` file with
N indexing pipelines in one process, bypassing the ingest API and the WAL.
Requires the `parquet` Cargo feature (off by default).

```
quickwit tool local-ingest --index idx --input-path file:///data/x.parquet --num-pipelines N
```

## How it works

```
CLI
 ├─ plan = ParquetLoadPlan::try_new(file)       parses the footer once
 ├─ source_loader: File → ParquetSourceFactory { plan }
 ├─ IndexingService::new(...).with_source_loader(source_loader)
 └─ SpawnPipeline + DetachIndexingPipeline × N → ParquetSource { plan }
```

- Sources pull row groups from the plan's atomic counter, decode them on the
  blocking runtime, and emit JSON documents (`arrow_json`). The `DocProcessor`
  doesn't know about Parquet.
- Only the CLI replaces the source loader, so servers can't run a Parquet
  source.

Native binary values become base64 strings, matching the default `bytes` mapping.
Non-finite floats (NaN and infinities) in emitted values, including nested values,
fail the load rather than silently becoming JSON nulls; masked null children and
unused dictionary entries are ignored.

## Failure handling

No checkpoint: the index must be empty (`--overwrite` clears it), with no other
writers during the load. On failure, the CLI stops actors, attempts to clear the
index, and reports cleanup errors alongside the load error.
Rollback is best effort: detached uploads can outlive actor shutdown after an
early pipeline failure. Process termination is not covered.

## Benchmark settings

Parquet loads never spawn a merge pipeline, regardless of the configured merge
policy. Size splits with `split_num_docs_target`; use `no_merge` to keep them
unchanged when running a server afterwards.
`--num-pipelines` defaults to half the CPUs: 16 pipelines was the fastest on an
m8gd.8xlarge (32 vCPUs) with 5M-document splits.
