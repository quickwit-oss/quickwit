# Read coalescing

Tantivy sends dictionary and postings reads as batches of range descriptors.
`StorageDirectory` uses Tantivy's ordered, five-at-a-time fallback to forward the
original ranges through storage caches. `BundleStorage` translates file offsets
into split-object offsets. `CoalescingStorage` sits below these caches and above
timeout/retry storage, so exact-range cache lookup precedes merging.

The collector groups concurrent `get_slice` calls by object path within one
storage instance. The first caller starts a 1 ms timer. Collection ends when it
expires, including for a singleton read. Valid empty ranges bypass collection.
Cache hits above the collector bypass the wait. Handles and directories using
the same collector share collection. Ranges from different internal files of one
split object can merge. Separate collector instances collect independently.

After collection ends, Quickwit sorts the collected descriptors and merges
overlaps and gaps up to 2 MiB. Callers drive a shared future that runs at most five
downloads per collected batch. Later callers start another batch, which can run
alongside earlier batches. This limits each batch's downloads rather than total
storage concurrency. Upstream forwarding exposes at most five reads per Tantivy
batch at once, so the collector plans the misses currently available, rather than
the whole descriptor batch. Large batches may need several collection windows.

Merged downloads stream through timeout/retry storage. The reader retains
requested slices and discards gaps. Unmerged reads use the underlying buffered
API and preserve its allocation. Results preserve each caller's original range,
and the directory batch fallback preserves order, duplicates, and empty ranges.
Proper subranges are copied so cached results do not retain larger backing
allocations. Gap bytes still cross the network. Stream retries resume after bytes
already delivered, including gap bytes.

A cache integration that supplies its own destination buffer must copy the
collector's `get_slice` result into that buffer. This temporarily retains both
the requested slice and the cache buffer, plus extraction buffers for overlapping
requests. The copy preserves the borrowed-buffer API. Forwarding the fill through
`get_slice_stream` instead would bypass collection: stream calls forward directly
to the underlying storage.

A stream may have discarded bytes that a later caller needs, so later callers
start another batch. A download error fails every caller in the collected batch.
Storage errors retain their kind and metadata. Body I/O errors retain their kind
when converted back to directory errors. Caches publish requested slices after
successful reads; an earlier forwarding window can succeed before a later one
fails.

Dropping one caller leaves reads running for the other callers. Its ranges remain
in the plan. Dropping all collector callers cancels the shared future, including
its timer or downloads. An outer cache controls the lifetime of its miss callback,
so dropping a search only cancels the download if those callbacks also drop.
CPU planning already submitted to the thread pool runs to completion. The map
keeps object paths and weak references to batches. Subsequent batches replace
those references.
