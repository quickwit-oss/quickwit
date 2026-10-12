# Local borrowed-JSON ingestion candidate

This candidate requires an **unreleased upstream `serde_json_borrow` API**. The
manifest and lockfile still name published 0.9.0; that release does not provide
`ObjectAsVec::as_vec_mut`. Validation uses an isolated published-0.9.0-plus-accessor
patch via Cargo configuration, not a shipping override. An actual upstream release
and an approved dependency update remain shipping gates. No production fork or
new release number is assumed.

## Representation and actor boundary

`DocProcessor` selects the borrowed path only for `SourceInputFormat::Json` when
there is no configured VRL transform. This is configuration dispatch, not a
parse/mapping-error fallback. OTLP logs/traces, plain text, and VRL-produced values
retain their existing native owned processing.

The raw JSON path is synchronous within one `process_raw_doc` invocation:

1. The local `Bytes` owns the input storage.
2. `borrowed_json::parse_json_object` parses one native borrowed tree and normalizes
   objects in place to the active `serde_json::Map` semantics.
3. If configured, the fingerprinter reads that same normalized tree.
4. `DocMapper::doc_from_normalized_object` computes routing and consumes it using
   the existing mapping implementation. Source/dynamic fields retain the same
   Tantivy JSON/date behavior as before. Mapped array elements are released after
   their synchronous insertion, not held alongside the growing document.
5. Timestamp extraction and common result handling produce the existing owned
   `ProcessedDoc` / `TantivyDocument` payload. No borrowed tree, input reference,
   self-referential object, or borrowed message escapes to another actor.

There is no owned-DOM conversion, JSON serialization/reparse adaptation, second
mapper, boxed iterator, per-document dispatch allocation, or new public toggle.
The owned `doc_from_json_obj` API remains available and consumes native owned
containers directly.

## Parsing, errors and accounting

Parsing and mapping have an explicit boundary. The object-only parser returns
`serde_json::Error` using `deserialize_map`, retaining the legacy root restrictions
and serde diagnostics. `DocProcessor` converts these into `JsonParsing`, just as
its former `serde_json::Map` parse did. It does **not** call the mapper's combined
`doc_from_json_bytes`, whose public error contract intentionally differs.

Mapping and required-timestamp errors remain `DocMapperParsing`. Both dispatch
paths share timestamp extraction and the existing valid/error accounting policy:
valid records use the processed record's byte count; errors use the original raw
record's byte count. In particular, OTLP's existing per-record accounting is not
replaced by a one-record-per-input assumption. Checkpoint and publish-lock handling
and actor message types do not change.

Normalization retains the last duplicate value and native key order, including
escaped-equivalent keys and the first-key position in insertion-order builds.
Superseded values are discarded before mapping. First-error selection therefore
tracks the actual serde backend selected by Cargo feature unification. Hashes
from different serde feature builds are not canonicalized to match each other.

## Shared fingerprint rules

`json_read::JsonRead` is a sealed, read-only subset factored from the private
`MappingJson` implementation. Only the native owned and borrowed JSON value types
implement it. It exposes scalars, native arrays/objects, exact-size entry iteration
and native key lookup. It cannot construct or mutate a normalized root.

Owned lookup still uses `serde_json::Map::get` (tree or hash-map lookup, depending
on features). Borrowed object lookup is linear in vector length. Entry iterator
lengths are constant time; raw-object hashing does not count by traversing an
object a second time.

The existing fingerprinter uses one rule implementation for both representations:

- Policy and configured-field order, type tags, length encoding, absent/present
  markers, field boundaries and token separators are unchanged.
- Structure policies recurse only through objects. **Arrays are leaves.** Exact
  excluded paths and sorted leaf-path sets keep their existing interpretation.
- Raw numbers use canonical `serde_json::Number` formatting, including `1.0` and
  negative zero. Owned numbers are borrowed; only a borrowed numeric scalar is
  converted, never a tree. Borrowed `Display` is not a compatible substitute.
- Tokenized values remain raw strings, not Tantivy date-aware values. Missing and
  non-string values, configured/default token limits and separators are unchanged.

The existing public owned fingerprint API remains; the native generic rule entry
point is private to the indexing crate. No clustering subsystem was moved.

## Validation and performance boundaries

Regression coverage adds independent scalar/token/structure fingerprint goldens,
native owned/borrowed comparisons, exact legacy parser error comparisons, actor
counters/bytes/timestamps, strict/lenient/dynamic behavior, and actual processor
output consumed by a single-segment writer through finalization, stored-document
reads and a term-count query. The existing VRL/OTLP/plain-text and mapper storage
identity/normalization tests remain. Test source is not evidence that a test ran;
execution status belongs in the isolated validation report.

The prepared-owned performance shipping gate remains **inconclusive**. Six paired
runs had unresolved variability; a bounded fixed-count, scoped-CPU diagnostic
showed no consistent penalty but is not confirmation. Separate allocation runs
found equal request counts and gross requested bytes on the original four-field
fixture, not an RSS or pipeline-memory improvement. The parent explicitly allowed
local integration work without declaring that shipping gate passed.

No prospective Searchbench parse/map result is a delivered DocProcessor claim.
Actual-actor timing, representative fingerprint/VRL controls, complete relevant
feature-matrix validation, upstream release and independent review remain required
before making shipping or end-to-end performance claims. A processor-to-finalized
single segment is not the whole server/WAL/replication/upload pipeline.
