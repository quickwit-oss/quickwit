# Parquet bulk load (cli only)

Index a parquet file with several indexing pipelines.

```
quickwit tool local-ingest --index idx --input-path file:///data/x.parquet --num-pipelines N
```

## How it works

A `.parquet` input uses the regular `local-ingest` flow with a different source
loader:

```
CLI
 ├─ shared_reader = ParquetSharedReader::try_new(file) // parses the footer once
 ├─ source_loader: File → ParquetSourceFactory { shared_reader }
 ├─ IndexingService::new(...).with_source_loader(source_loader)
 ├─ SpawnPipeline × N → ParquetSource { shared_reader }, sharing one merge pipeline
 └─ wait for the pipelines, finish pending merges, shut down
```

- Sources pull row groups from the shared reader, decode them on the
blocking runtime, and emit JSON documents (`arrow_json`).
- Only the CLI replaces the source loader, so servers can't run a Parquet
source.
- Supported Parquet compression codecs: uncompressed, Snappy, Zstd and LZ4.
- `--exclude-columns` drops top-level columns at read time, so they are never
decoded. Unknown column names are rejected.

## Checkpoint and failure handling

Like an NDJSON file, a Parquet file is recorded in the checkpoint of the CLI
source: one partition per row group, positioned at its number of rows read.
Sources skip the rows already in the checkpoint, so loading the same file again
only loads the missing rows, and the metastore rejects duplicate publishes.

The input is validated before `--overwrite` clears the index and resets the
checkpoint. A pipeline restart aborts the load, since the row groups claimed by
the failed pipeline are not claimed again. Rerun a failed load to resume it.

## Benchmark settings

Splits are merged according to the index merge policy. Size splits with
`split_num_docs_target`; use `no_merge` to keep them unchanged.
`--num-pipelines` defaults to half the CPUs: 16 pipelines was the fastest on an
m8gd.8xlarge (32 vCPUs) with 5M-document splits.
