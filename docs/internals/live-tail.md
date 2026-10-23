# Live tail

Live tail streams the documents of one or several indexes that match a query, as they are indexed,
through the `GET /api/v1/{index_id}/tail` REST endpoint (Server-Sent Events). See the REST API
reference for the user-facing contract.

## Why not search?

A document is only searchable once its split is published, i.e. after `commit_timeout_secs`
(60s by default). Live tail instead intercepts documents in the indexing pipelines, right after
they are processed, which gives a sub-second latency.

## Architecture

```
Client ──SSE──▶ REST node (coordinator)
                 │ 1. resolves the index patterns, builds the doc mappers, parses and validates the
                 │    query (HTTP 400 if invalid)
                 │ 2. finds the indexers running pipelines for the indexes (`indexer.task:` keys in
                 │    the cluster state), refreshed every 5s
                 │ 3. opens an `OpenLiveTail` gRPC stream toward each of them
                 │ 4. forwards the streams to the client as SSE events
                 ▼
Indexer: DocProcessor ──LiveTailTap──▶ LiveTailHub (one broadcast channel per index)
                                          └──▶ session: micro-batch ─▶ in-RAM Tantivy index ─▶ query
```

### Tap (`quickwit-indexing`, `DocProcessor`)

Every source (ingest API, OTLP, Kafka, Kinesis, Pulsar, SQS, ...) goes through the `DocProcessor`,
which parses the documents, applies the VRL transform and maps them with the doc mapper. When at
least one live tail session watches the index, the `DocProcessor` clones the JSON object of each
successfully mapped document and publishes the batch, along with the pipeline's doc mapper, to the
`LiveTailHub` of the node.

Contracts:
- The tap costs nothing when nobody watches the index: `LiveTailTap::is_active` is checked once
  per batch.
- Publishing never blocks indexing: the hub uses `tokio::sync::broadcast` channels. A session that
  falls behind loses batches and reports them in `dropped` events with the `lagged` reason.
- Documents are published after the VRL transform, exactly as they are indexed.
- Documents are published before they are committed: a pipeline that restarts from its checkpoint
  publishes the replayed documents again (at-least-once).

The merge pipelines are not tapped.

### Filtering (`quickwit-live-tail`, `TailFilter`)

Filtering happens on the indexer, where the documents are, so that only matching documents travel
over the network. Each session accumulates documents for up to 250ms, indexes them into a
single-segment in-RAM Tantivy index built with the schema and tokenizers of the doc mapper of the
pipeline that processed them, and runs the query built by `DocMapper::query`. Using the pipeline's
doc mapper keeps live tail consistent with indexing when the index is updated during a session. This guarantees that the query has exactly the same semantics as
a search (tokenizers, JSON fields, ranges, phrase queries, etc.). `MatchAll` queries skip the
indexing. Only the matching documents allowed by the rate limiter are serialized.

The coordinator resolves the user query with the index default search fields and validates it
against the index doc mapping before sending it: the indexers do not know the index search
settings.

The number of concurrent sessions per indexer is limited by `live_tail.max_sessions` (default: 4).
Each session is rate limited to the `max_docs_per_sec` requested by the client, capped by
`live_tail.max_docs_per_sec` (default: 10,000). Both settings can be overridden with the
`QW_LIVE_TAIL_MAX_SESSIONS` and `QW_LIVE_TAIL_MAX_DOCS_PER_SEC` environment variables.

The rate limit applies after filtering: the CPU cost of a session is proportional to the indexing
throughput of the index, not to the number of matching documents. Each session indexes the
documents of the tailed index once more. This work runs on a dedicated thread pool of 2 threads,
not on the `run_cpu_intensive` pool shared with ingest (document validation, decompression), so
that live tail cannot starve ingestion. When the pool does not keep up, sessions lag and report
the documents they miss as `lagged`. Cloning the documents in the `DocProcessor`, done once per
batch for all sessions and only while at least one session is open, is the only cost on the
indexing path.

### Coordinator (`quickwit-serve`, `live_tail_api`)

Any node serving the REST API can coordinate a live tail. The coordinator:
- validates the request and builds one plan per index before opening the stream, so that invalid
  requests get an HTTP error status;
- every 5s, lists the ready indexer nodes whose indexing tasks include the index, opens streams
  toward new ones, closes the streams toward indexers that stopped running pipelines for the index,
  and reopens failed streams. Only this loop spawns and aborts the forwarder tasks, which send their
  events directly to the client;
- ends the session when the client disconnects, which drops the gRPC streams and, in turn, the
  indexer sessions.

There is no global ordering: documents are sent in processing order within a pipeline.

## Limits and future work

- `max_docs_per_sec` is enforced by each indexer: with N indexers, up to N × `max_docs_per_sec`
  documents per second are sent to the client.
- Reconnections lose the documents processed while disconnected (no `Last-Event-ID` support).
- Each session builds its own in-RAM index: sessions on the same index could share one index per
  batch.
