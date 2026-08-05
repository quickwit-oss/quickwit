# IPFS storage backend

The `ipfs` feature of `quickwit-storage` adds an `ipfs://` storage backend that
stores splits on an IPFS node, making every split content-addressed: each split
file has a CID and can be pinned, replicated, and exported (e.g. as a CAR file)
across the IPFS network.

## Design

### Why MFS

Quickwit's read path is built around `Storage::get_slice(path, byte_range)`:
one tail read fetches the split footer + hotcache, and subsequent small ranged
reads are mostly served from the hotcache. Raw IPLD blocks are chunk-addressed,
not byte-addressed, so something must translate byte ranges into block fetches.

Rather than reimplementing that translation, the backend delegates it to the
node's MFS (Mutable File System, the `/api/v0/files/*` RPC surface of
[Kubo](https://github.com/ipfs/kubo)):

- `files/write` chunks the payload into a UnixFS DAG and links it at an MFS
  path. The DAG is the content-addressed artifact; the MFS path is the mutable
  name Quickwit uses (`<index-uri>/<split-id>.split`).
- `files/read?offset=&count=` resolves a byte range to the covering blocks
  using the UnixFS index — the exact "contiguous byte range over a set of
  blocks" view that `get_slice` needs.
- `files/stat` returns the file size and its CID.

### Mapping

| Quickwit | IPFS |
| --- | --- |
| `ipfs://indexes/my-index` (index URI) | MFS directory `/indexes/my-index` |
| `Storage::put` | `files/write` to `<path>.temp`, then `files/mv` into place (atomic publish: readers never see partial splits) |
| `Storage::get_slice` | `files/read` with `offset`/`count` |
| `Storage::get_all` / `copy_to` | `files/stat` (size) + streamed `files/read`, with a length-enforcing reader that fails on truncation |
| `Storage::delete` | `files/rm --force` (missing entries are not an error) |
| `Storage::file_num_bytes` | `files/stat` |
| split CID | `IpfsStorage::file_cid(path)` (`files/stat`) |

### Chunking and read amplification

The dominant latency cost is the first read per split (footer + hotcache tail
range). The default chunker is `size-1048576` (1 MiB fixed-size chunks) with
raw leaves, so a typical tail read spans a small number of blocks that a local
node serves in one RPC round-trip. Both knobs are exposed in
`IpfsStorageConfig` (`chunker`, `raw_leaves`).

### What stays conventional

- **Metastore.** Metastore state is small, mutable, and rewritten constantly —
  the opposite of content-addressed data. Keep it on PostgreSQL or a file
  backend. `StorageResolver` and `MetastoreResolver` are independent, so
  `index_uri: ipfs://...` + `metastore_uri: postgres://...` composes with no
  coupling.
- **Split cache.** The searcher split cache still materializes hot splits on
  local disk; IPFS replaces the *origin* storage, not the local cache.
- **Split identity.** A split's CID alone is not sufficient to open it: the
  metastore row carries `footer_offsets`. Verifiable replication of an index
  needs the split CIDs *plus* a metastore snapshot.

## Operational notes

- The backend talks to a single node's RPC API (`QW_IPFS_API_ENDPOINT` or
  `storage.ipfs.api_endpoint`, default `http://127.0.0.1:5001`). Run the node
  colocated with indexers/searchers; the IPFS network is the replication
  layer, not the query path.
- The RPC API grants full control of the node. Never expose port 5001 beyond
  localhost / a private network.
- The dev `kubo` service in `docker-compose.yml` runs with the `test` profile
  and `--offline`: no bootstrap peers, no outbound traffic. Remove both to
  join the public IPFS network — an explicit opt-in, since published CIDs
  become discoverable.
- Writes are staged to `<path>.temp` and moved into place; a crash can leave
  `.temp` entries behind. They are overwritten (truncate) on retry and can be
  garbage-collected safely.
- **Deletes free disk space only after IPFS garbage collection.** The backend
  does not use pins: files are GC-protected by being reachable from the MFS
  root. `Storage::delete` (`files/rm`) unlinks the entry, which makes its
  blocks GC-*eligible*, not deleted. Kubo does not GC by default, so on
  merge-heavy indexes the blockstore grows until `ipfs repo gc` runs — run the
  daemon with `--enable-gc` (or GC periodically) to reclaim space from deleted
  and merged-away splits. Conversely, a split whose CID was explicitly pinned
  (e.g. for replication) survives GC after deletion in Quickwit, by design.

## Testing

Unit tests: `cargo test -p quickwit-storage --features ipfs,testsuite ipfs`.

Integration suite (requires a running node, e.g.
`docker compose up -d kubo`):

```bash
cargo test -p quickwit-storage \
  --features ipfs,integration-testsuite,ci-test \
  --test ipfs_storage
```

This runs the generic `storage_test_suite` (put/get/slice/stream/delete/bulk
delete/size/exists) plus single-part and 15 MB multi-part uploads and a CID
round-trip against the node.
