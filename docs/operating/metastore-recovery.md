---
title: Metastore recovery
sidebar_position: 5
---

In case of disaster recovery you can rebuild the metastore state from
split recovery metadata.
The `quickwit tool recover` command reads that embedded metadata from each split and republishes
the split metadata into an existing index.

## Requirements

- **The index storage must support object listing.** Recovery discovers splits by listing the
  index URI, which only S3-compatible storages implement. A `file://` index cannot be recovered
  this way.
- **The splits must embed the recovery metadata.** It is written on every upload, but splits
  produced by an older version of Quickwit do not have it and are skipped.
- **The index must exist, with the doc mapping the splits were built with.** Recovery restores
  splits, not the index configuration. A mismatched doc mapping yields an index that is searchable
  but returns wrong or missing fields for the recovered splits.

## Recovering an index

### 1. Recreate the index

Create the index with the same doc mapping as before the loss, and make sure its `index_uri`
points at the directory holding the splits:

```bash
curl -X POST http://localhost:7280/api/v1/indexes \
  -H "Content-Type: application/yaml" \
  --data-binary @index-config.yaml
```

### 2. Preview the recovery

```bash
quickwit tool recover --index otel-logs-v0_9 --config ./config/quickwit.yaml --dry-run
```

```
Recovering index otel-logs-v0_9 from s3://my-bucket/indexes/otel-logs-v0_9

+-------------------------------------+-------+
|         Recovery summary (dry run)          |
+-------------------------------------+-------+
| Splits found in storage             | 12    |
| Publishable                         | 5     |
| Documents                           | 1,099 |
| Skipped (merged into another split) | 0     |
| Skipped (already in metastore)      | 6     |
| Unreadable                          | 1     |
+-------------------------------------+-------+
```

The command then lists the splits it would publish, the IDs it skipped, and the split files it
could not read along with the reason.

### 3. Publish the splits

Run the same command without `--dry-run`. The splits are staged and published in batches.

### 4. Verify

Compare the hit count over the time range covered by the recovered splits with the document count
reported by the command:

```bash
curl "http://localhost:7280/api/v1/otel-logs-v0_9/search?query=*&max_hits=0\
&start_timestamp=1792257120&end_timestamp=1792258260"
```

## What the command does

1. Lists the `.split` objects at the root of the index URI.
2. Fetches the recovery metadata of each split, ten splits at a time. The object size comes from
   the listing, and the footer and the metadata file usually land in a single `GET` per split.
3. Rebuilds the split metadata, replacing the index UID recorded in the split with the UID of the
   target index.
4. Skips the splits that were superseded by a merge. A merged split and its parents coexist in the
   storage until garbage collection removes the parents.
5. Leaves out the splits the metastore already knows about, which makes a second run a no-op.
   Splits left staged by an interrupted run are not considered known: they are staged and
   published again.
6. Stages and publishes the remaining splits, 1000 per request.

Splits whose metadata cannot be read are reported individually and do not interrupt the run: a
single truncated upload would otherwise make every other split unrecoverable.

## What is not recovered

- **Source checkpoints.** The index checkpoint is left empty. A source restarted against a
  recovered index resumes from the position its own configuration dictates, which may re-ingest or
  skip data depending on the source.
- **Delete tasks.** The delete opstamp recorded in each split is restored, but the delete task
  queue itself is not. Pending deletes have to be submitted again.
- **Splits without embedded recovery metadata.** They are listed as unreadable and left in the
  storage, untouched.
