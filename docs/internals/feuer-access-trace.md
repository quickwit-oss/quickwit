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
Failed uploads retry; slow uploads can delay cache lookups. Keep exports private:
object hashes are not anonymized.

Use the decoder for the pinned Feuer revision. See its
[access-trace format](https://github.com/PSeitz/feuer/blob/b5af89073def5948ff6e4b2963e7b61a832808e4/access-trace.md)
for record definitions. Each exact split byte range is a separate cache key;
traces cannot reconstruct the original split bounds or count remote GETs.
