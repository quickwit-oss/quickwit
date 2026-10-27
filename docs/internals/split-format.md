# Split format

Quickwit's index are divided into small independent immutable piece of index called split.

For convenience, a split consists in a single file, with the extension `.split`.

In reality, this file hides an internal mini static filesystem,
with:
- the Tantivy index files (`.idx`, `.pos`, `.term`...)
- a Quickwit specific file with the list of fields, including those indexed as part of a JSON type. 
It contains the field name, type and capabilities.
- a versioned protobuf entry named `split_recovery_metadata` containing the immutable split
  metadata and direct lineage needed to reconstruct its metastore record.

The split file data layout looks like this:
- concatenation all of the files in the split
- a footer

The footer follows this format:

- a JSON object called `BundleFileRanges` containing the `[start, end)` byte ranges
of all files.
- the length of this json (`u32`, little endian)
- a hotcache, a small static cache that contains some important file sections.
- the length of this hotcache (`u32`, little endian)
- optionally, a fixed-size footer trailer:
  - the inclusive footer start offset (`u64`, little endian)
  - the trailer format version (`u32`, little endian)
  - the four-byte magic value `QWFT`

This footer plays a key role in Quickwit.
It packs in one read all of the information required to open a split.

When opening a file from remote storage, Quickwit's metastore normally supplies the byte offsets
of this footer. A reader without metastore metadata can instead use the object size and the final
16 bytes to locate a footer trailer.

Legacy splits without a trailer remain self-discoverable by reading the final four-byte hotcache
length, then walking backward to the four-byte bundle-metadata length.

## Footer trailer rollout

Readers accept splits both with and without the trailer. Writers keep producing the legacy format
by default because older Quickwit readers interpret the final four bytes as the hotcache length and
cannot open a split with an appended trailer.

The trailer is enabled for writers with `QW_ENABLE_SPLIT_FOOTER_TRAILER=true`. Roll it out in two
phases:

1. Upgrade every split reader (searchers, indexers, compactors, and split-inspection tooling) while
   leaving the environment variable unset.
2. After every reader is upgraded, set the environment variable on split writers, primarily
   indexers and compactors.
