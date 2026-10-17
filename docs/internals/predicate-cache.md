# Predicate hitsets

The predicate cache stores matching document IDs per split and serialized predicate
AST. Its values contain a segment ID and a compressed `HitSet`. This is an
in-process memory cache, not a persistent split format; no on-disk migration is
needed when its representation changes.

## Hitset layout

All integers use native byte order. A hitset contains:

- A four-byte document count.
- One eight-byte directory entry per 128-document block: its first document ID
  and its byte offset relative to the payload section.
- Block payloads, each beginning with a one-byte encoding marker.

Full blocks use `BitPacker4x` strictly-sorted delta compression with the indexed
first ID minus one as the base (`None` when the first ID is zero). This makes the
first encoded gap zero. The decoder reconstructs absolute IDs directly, without
an additional pass. Blocks are independently decodable, and a large gap before a
block cannot inflate its delta bit width. Large gaps within a block can still do
so.

A partial final block stores raw four-byte IDs. Marker 255 denotes raw IDs;
other markers are the compressed delta bit width. Iterators pad the decoded tail
with `TERMINATED`.

A seek binary-searches the first IDs to find the block starting at or before the
target. If that block ends before the target, it loads the next block. Thus it
decodes at most two blocks, without decoding intervening blocks. Within a block,
`seek` locates the next document;
`seek_danger` checks membership and, on a miss, returns the next document as a
lower bound without repositioning the in-block cursor.

The directory costs 1/16 byte per document for full blocks. Larger blocks amortize
this overhead while keeping each seek's decoding bounded to 128 documents. A
separate bitset encoding is deliberately omitted to keep the format small.

Run the scan/seek benchmarks with:

```sh
cd quickwit
cargo bench -p quickwit-query --bench hit_set
```
