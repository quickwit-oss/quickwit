# JSON document mapping

The indexing `DocProcessor` still parses raw JSON into an owned `serde_json::Map`.
VRL and OTLP produce owned JSON in memory and use the same mapper. Routing and
clustering fingerprints are computed from this original JSON representation.
The actor boundary still carries an owned compact `TantivyDocument`.

## Direct insertion

The mapper inserts text by reference and inserts JSON fields, `_source`, and
`_dynamic` through Tantivy's existing `Value` implementation for
`&serde_json::Value`. Tantivy copies into the compact document synchronously;
no reference to the input survives mapping. This avoids intermediate recursive
`OwnedValue` trees and cloning JSON for source storage or concatenation.
Numeric, date, IP, and bytes fields retain their explicit typed conversions.
Multi-valued fields consume their input elements one at a time, releasing each
element after insertion rather than retaining the full array alongside its copy.

Compatibility constraints:

- JSON insertion must match `OwnedValue::from(serde_json::Value)`: small positive
  integers become `I64`, larger integers become `U64`, floats remain `F64`, and
  RFC3339 strings can become dates. Tests compare against that pinned Tantivy
  conversion, including nested arrays and objects.
- Mapped text and concatenate strings must **not** auto-convert to dates.
  Concatenate visits primitive values in object-key/array order, skips nulls,
  and preserves the configured numeric type for explicitly mapped fields.
- Object traversal follows the active `serde_json::Map` backend: sorted keys by
  default, insertion order when Cargo enables `serde_json/preserve_order` anywhere
  in the dependency graph. Duplicate keys retain the last value (and the first
  key position in insertion-order builds). `_source` and `_dynamic` roots follow
  the same order as nested objects; no extra root-only BTreeMap sort is applied.
  First-error selection and concatenate traversal follow this native order.
  Routing hashes match within a build, not necessarily across differently
  configured builds.
- Null handling, cardinality, errors, field-presence computation, routing, and
  fingerprinting are unchanged. Parsing failures do not trigger fallback.

## Native consuming input

The mapper has one private sealed mapping implementation for native owned
`serde_json::Value` and normalized `serde_json_borrow::Value`. Owned input keeps
its native maps and array buffers; there is no whole-tree owned-to-borrowed
conversion. Object and array iterators are statically dispatched. Numeric scalar
access borrows owned numbers and converts only borrowed numbers to
`serde_json::Number`, preserving the separate mapped versus JSON/date rules.

Raw mapper string/byte entry points parse one borrowed tree and normalize it in
place. A private-constructor root wrapper records that normalization happened.
A `LazyLock` observes the actual serde Map backend once (including Cargo feature
unification); recursive normalization preserves last-value-wins duplicates and
native map order. Legacy unnormalized ingest validation keeps its original
Display-based diagnostic behavior. Normalized mapper errors serialize directly
to diagnostic text, without constructing or reparsing another tree.

Unknown dynamic subtrees move intact. Only partially mapped object boundaries
need replacement output shells, assembled locally once per node, without global
parent scans or per-append duplicate searches on borrowed objects. Input keys
move onto the diagnostic path and then into dynamic output; strings are never
cloned for this traversal. Arrays move out as their native Vec and each element
is dropped after synchronous Tantivy insertion, including on an error. Routing
and source capture precede consumption; only owned Tantivy documents escape.
No DocProcessor, fingerprinter, or actor integration is included here.

**Dependency gate:** published `serde_json_borrow` 0.9 does not expose the needed
`ObjectAsVec::as_vec_mut`. This candidate requires a supported upstream API
before production landing; an experimental local override is validation only,
not a dependency delivery solution. No broader constructors or changed
`Value::get` API are required.

## Measurement

Run the focused benchmark with existing binggan tooling:

```bash
cd quickwit
DOC_MAPPING_BENCH=1 cargo bench -p quickwit-doc-mapper --bench doc_to_json_bench
```

It reports throughput and peak live allocated memory (not allocation counts).
`parse_and_map` includes parsing. `clone_owned_and_map` starts with parsed JSON
but includes cloning to replenish the consumed input on every iteration; that
cost is included equally in baseline and modified runs. Cases include mapped
fields without `_source`, a large mapped text array, flat/escaped source JSON,
and 100 nested JSON keys.
These are mapper microbenchmarks, not end-to-end indexing measurements.

The bounded `consuming_input_bench` example compares native mapping with the
direct-insertion baseline. It prepares owned inputs outside the timed mapping
region and reports that throughput separately from raw parse-and-map. It also
reports process-local instrumented peak allocation above scope entry (not
allocation counts, RSS, or pipeline memory). Saved paired runs and load records
are required before claiming an owned-path no-regression result. In particular,
this change makes no real DocProcessor throughput or memory claim.
