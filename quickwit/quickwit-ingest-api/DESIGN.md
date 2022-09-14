# Quickwit Ingest API Design

Quickwit ingest API is in charge of receiving documents pushed to the ingest API,
durably store them on disk to allow for a crash recovery,
and act as source, for an indexing pipeline.

> :information: For the moment, neither Quickwit's indexing nor its ingest API is distributed. This will change soon.

One of the key constraint for Quickwit's API is to behave properly even
when hosting thousands of indices.

Appending to thousands of files did not seem like the right idea.

A first solution we considered, was to write all documents in a joined write-ahead log (WAL),
and keep separate queues in memory. Consumers would advertise the last published checkpoint.

The truncation of the WAL would then require a bit of juggling to keep track of the oldest
(in other words, the minimum) last published checkpoint amongst all of the indexes.

Quickwit, on a single indexer thread typically at 40MB/s.
With a delay between ingestion and publication of 45s, we would then be looking at 2GB or RAM
spent per indexing thread, above which we would need some mechanism for push back.
While feasible this seemed like a lot of waste.

# RocksDB for the win

Rather than reinventing the wheel, we rely on RocksDB, as it behaves similarly to the system
described above.

Each queue is modelized as a column family. All column family share the same WAL, but have
separate SSTable files and Mem-table.
RocksDB also offers both a per-column family and global memory limit to control when memtables
should be flushed.

It also contains some queue specific optimization when deleting ranges.




