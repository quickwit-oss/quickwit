# Read coalescing

Tantivy sends dictionary and postings reads as batches of range descriptors.
`StorageDirectory` collects batches by file. The first caller starts a 1 ms timer,
and collection ends when it expires. Handles and clones of a directory share its
batch collector. Each file and directory instance collects batches separately.
Single reads and batches containing only empty ranges bypass collection.

After collection ends, Quickwit sorts the collected descriptors and merges
overlaps and gaps up to 2 MiB. Callers share a future that reads the collected batch
and returns their requested buffers. They drive collection and downloads by
polling that future, which runs at most five downloads concurrently. Batches
arriving after collection ends start another timer and can run alongside earlier
batches.

Merged downloads stream through storage. The reader retains the slices requested
by all callers and discards gaps. Each caller receives its own results in input
order, including duplicates and empty ranges. Proper subranges are copied so
cached results do not retain larger backing allocations. Gap bytes still cross
the network.

A stream may have discarded bytes that a later caller needs, so later callers
start another batch. Callers within a collected batch share the returned buffers.
A download error fails every caller's batch in that collection, preserving the
I/O error kind. Directory caches insert requested slices only after success.

Dropping one caller leaves the shared reads running for the other callers. Its
ranges remain part of the collected batch. Dropping all callers cancels the shared
future, including its timer or downloads. CPU planning already submitted to the
thread pool runs to completion. The map keeps file paths and weak references to
batches. Subsequent batches replace those references.
