# Live tail

Live tail streams the documents of one or several indexes that match a query, as they are
ingested, through the `GET /api/v1/{index_id}/tail` REST endpoint (Server-Sent Events). See the
REST API reference for the user-facing contract.

## Why not search?

A document is only searchable once its split is published, i.e. after `commit_timeout_secs`
(60s by default). Live tail instead intercepts documents in the ingesters, right after they are
written to the write-ahead log, which gives a sub-second latency.

## Architecture

```
Client ──SSE──▶ REST node (coordinator)
                 │ 1. resolves the index patterns, builds the doc mappers, parses and validates the
                 │    query (HTTP 400 if invalid)
                 │ 2. finds the ingesters hosting open shards of the indexes
                 │    (`ingester.primary_shards:` keys in the cluster state), refreshed every 5s
                 │ 3. opens an `OpenLiveTail` gRPC stream toward each of them
                 │ 4. forwards the streams to the client as SSE events
                 ▼
Ingester: persist ──▶ LiveTailHub (one broadcast channel per index)
                        └──▶ session: micro-batch ─▶ in-RAM Tantivy index ─▶ query
```

### Tap (`quickwit-ingest`, `Ingester::persist`)

The ingest API, the Elasticsearch bulk API and the OTLP endpoints route the documents to the
ingesters (ingest V2). When at least one live tail session watches the index, the ingester
publishes the batch of valid documents of each persist subrequest, along with the doc mapper of
the shard, to the `LiveTailHub` of the node, once the batch is written to the write-ahead log.

Contracts:
- The tap costs nothing when nobody watches the index: `LiveTailHub::is_active` is checked once
  per persist subrequest.
- Publishing never blocks ingestion and does not copy the documents: the hub uses
  `tokio::sync::broadcast` channels, and the documents are parsed lazily by the sessions, once for
  all the sessions of the node. A session that falls behind loses batches and reports them in
  `dropped` events with the `lagged` reason.
- Only the documents accepted by the write-ahead log are published, each one exactly once: there
  is no replay, unlike in the indexing pipelines that restart from their checkpoint.
- Batches are published while the ingester state lock is held: the documents of a shard are
  published in the order of the write-ahead log.

Documents are published before the VRL transform, which the indexing pipeline applies. Filtering
them with the doc mapping would not match what is indexed, so the coordinator rejects the indexes
whose `_ingest-source` source has a transform. The sources read by the indexing pipelines (Kafka,
Kinesis, Pulsar, SQS, etc.) do not go through the ingesters and are not tailed.

### Filtering (`quickwit-live-tail`, `filter_batches`)

Filtering happens on the ingester, where the documents are, so that only matching documents
travel over the network. Each session accumulates documents for up to 250ms, indexes them into a
single-segment in-RAM Tantivy index built with the schema and tokenizers of the doc mapper of the
shard that persisted them, and runs the query built by `DocMapper::query`. Using the shard's doc
mapper keeps live tail consistent with indexing when the index is updated during a session. This
guarantees that the query has exactly the same semantics as a search (tokenizers, JSON fields,
ranges, phrase queries, etc.). Documents rejected by the doc mapper, which the ingester only lets
through when document validation is disabled, are skipped like the indexing pipeline does.
`MatchAll` queries skip the indexing. Only the matching documents allowed by the rate limiter are
serialized.

The coordinator resolves the user query with the index default search fields and validates it
against the index doc mapping before sending it: the ingesters do not know the index search
settings.

The number of concurrent sessions per ingester is limited by `live_tail.max_sessions` (default: 4).
Each session is rate limited to the `max_docs_per_sec` requested by the client, capped by
`live_tail.max_docs_per_sec` (default: 10,000). Both settings can be overridden with the
`QW_LIVE_TAIL_MAX_SESSIONS` and `QW_LIVE_TAIL_MAX_DOCS_PER_SEC` environment variables.

The rate limit applies after filtering: the CPU cost of a session is proportional to the ingestion
throughput of the index, not to the number of matching documents. Each session indexes the
documents of the tailed index once more. This work runs on a dedicated thread pool of 2 threads,
not on the `run_cpu_intensive` pool shared with ingest (document validation, decompression), so
that live tail cannot starve ingestion. When the pool does not keep up, sessions lag and report
the documents they miss as `lagged`. Cloning the batch of documents in the ingester, which does
not copy them, is the only cost on the ingest path.

### Coordinator (`quickwit-serve`, `live_tail_api`)

Any node serving the REST API can coordinate a live tail. The coordinator:
- validates the request and builds one plan per index before opening the stream, so that invalid
  requests get an HTTP error status;
- every 5s, lists the ready ingesters that broadcast at least one open shard of the index, opens
  streams toward new ones, closes the streams toward ingesters that no longer host open shards of
  the index, and reopens failed streams. Only this loop spawns and aborts the forwarder tasks,
  which send their events directly to the client;
- ends the session when the client disconnects, which drops the gRPC streams and, in turn, the
  ingester sessions.

There is no global ordering: documents are sent in the order of the write-ahead log within a
shard.

## Limits and future work

- `max_docs_per_sec` is enforced by each ingester: with N ingesters, up to N × `max_docs_per_sec`
  documents per second are sent to the client.
- Shards are discovered through the cluster state, which is broadcast every few seconds: the
  documents persisted in a shard during the first seconds after it is opened on a new ingester
  can be missed.
- Reconnections lose the documents persisted while disconnected (no `Last-Event-ID` support).
- Indexes with a VRL transform and the sources read by the indexing pipelines are not supported.
- Each session builds its own in-RAM index: sessions on the same index could share one index per
  batch.
