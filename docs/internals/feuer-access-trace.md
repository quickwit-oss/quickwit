# Feuer access traces

Set `QW_FEUER_ACCESS_TRACE_URI` on searchers with `searcher.disk_cache` enabled:

```text
QW_FEUER_ACCESS_TRACE_URI=s3://my-bucket/access-traces/my-benchmark
```

Unset means disabled. Invalid destinations or failed manifest uploads fail startup.
Uploads use the node's storage configuration and credentials. `file:///...` also
works. Each cache instance creates a unique ULID directory containing
`manifest.json` and numbered `000000000000.feuer` packages. The manifest records
cache settings, available node/pod/host/image and policy environment variables,
and an approximate wall-clock start time captured **before** trace attachment.
Look for `Feuer access trace enabled` and `Feuer access trace batch uploaded` logs.

Packages are uploaded unchanged, without compression or sampling, in Feuer's
[FETR v2 format](https://github.com/PSeitz/feuer/blob/965bfcb000bf4f9ce19f0bd715c8f8b0337e1095/access-trace.md).
Each package has an eight-byte header followed by 41-byte events. Successful
outcomes record the object payload length. Records contain no byte ranges.
Request IDs are local to a run. Keep run directories separate. Only accesses
reaching Feuer are traced. Callback outcomes can share a coalesced download and
are not S3 GET counts. Each exact split slice is a separate whole-object key.
Hashes include the URI and slice bounds, and payload lengths describe the
requested slices. Trace records cannot reconstruct the original split bounds.
FETR v1 captures use a different layout and must be decoded by version.
Object hashes are not anonymized. Keep exports private.

Feuer emits serial packages at 8 MiB, or drains the tail after all traced handles
are dropped. **There is no timed or explicit flush.** Idle partial batches can
remain buffered indefinitely. Upload errors retry the same key and payload every
five seconds, in addition to storage-client retries. Slow uploads fill Feuer's
65,536-event queue and backpressure lookups. Trace buffers are outside cache
capacity.

Dropping handles schedules delivery but does not wait for uploads. Quickwit's
shutdown does not guarantee tail persistence before runtime teardown. There is no
durable spool. An uploaded manifest or contiguous batch numbers do not prove
completeness. Preserve upload/restart logs when choosing benchmark windows.
