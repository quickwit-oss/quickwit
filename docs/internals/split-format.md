# Split format

Quickwit's index are divided into small independent immutable piece of index called split.

For convenience, a split consists in a single file, with the extension `.split`.

In reality, this file hides an internal mini static filesystem,
with:
- the Tantivy index files (`.idx`, `.pos`, `.term`...)
- a Quickwit specific file with the list of fields, including those indexed as part of a JSON type. 
It contains the field name, type and capabilities.

The split file data layout looks like this:
- concatenation all of the files in the split
- a footer

The footer follows the following format.

- a json object called `BundleStorageFileOffsets` containing the `[start, end)` byte-offsets
of all files.
- the length of this json (8 bytes little endian)
- a hotcache, a small static cache that contains some important file sections.
- the length of this hotcache (8 bytes little endian)

This footer plays a key role a very important role in quickwit.
It packs in one read all of the information required to open a split.

When opening a file from a distant storage,  Quickwit's metastore stores the byte offsets of this footer to make this read possible.

If this footer offset information is not available, for instance if the split is just a file on the filesystem, it is still possible to open it by reading the last 8 bytes of the split (encoding the length of the hotcache), deducing the position of the meta information and unpacking this in turn.
