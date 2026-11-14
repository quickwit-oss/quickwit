# Feuer access traces

Set `QW_FEUER_ACCESS_TRACE_URI` on searchers to trace accesses to the enabled storage cache:

```text
QW_FEUER_ACCESS_TRACE_URI=s3://my-bucket/access-traces/my-benchmark
```

Tracing is disabled by default. Uploads use the node's storage configuration and
credentials; `file:///...` destinations also work. Each run creates a unique
directory with `manifest.json` and numbered `.feuer` packages. Invalid destinations
or failed manifest uploads abort startup.

Packages upload at 8 MiB. There is no timed flush, and shutdown can lose the tail.
Failed uploads are logged and their batches dropped. Events are dropped when the
trace queue is full, so slow uploads never delay cache lookups. Traces can contain
unmatched start and completion events. Keep exports private: object hashes are not
anonymized.

See the [Feuer repository](https://github.com/PSeitz/feuer) for the decoder and
access-trace format. Each exact split byte range is a separate cache key;
traces cannot reconstruct the original split bounds or count remote GETs.
