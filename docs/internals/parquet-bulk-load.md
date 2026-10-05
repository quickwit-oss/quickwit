# Parquet bulk load (cli only)

Index a parquet file without merges.

 

```
quickwit tool local-ingest --index idx --input-path file:///data/x.parquet --num-pipelines N
```

## How it works

```
CLI
 ├─ plan = ParquetLoadPlan::try_new(file) // parses the footer once
 ├─ source_loader: File → ParquetSourceFactory { plan }
 ├─ IndexingService::new(...).with_source_loader(source_loader)
 └─ SpawnPipeline + DetachIndexingPipeline × N → ParquetSource { plan }
```

- Sources pull row groups from the plan, decode them on the
blocking runtime, and emit JSON documents (`arrow_json`).
- Only the CLI replaces the source loader, so servers can't run a Parquet
source.

## Failure handling

No checkpoint. On failure, the CLI stops actors, attempts to clear the
index, and reports cleanup errors alongside the load error.
Rollback is best effort: detached uploads can outlive actor shutdown after an
early pipeline failure. Process termination is not covered.

## Benchmark settings

Size splits with `split_num_docs_target`; use `no_merge` to keep them
unchanged when running a server afterwards.
`--num-pipelines` defaults to half the CPUs: 16 pipelines was the fastest on an
m8gd.8xlarge (32 vCPUs) with 5M-document splits.