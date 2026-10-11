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
- Null handling, cardinality, errors, field-presence computation, routing, and
  fingerprinting are unchanged. Parsing failures do not trigger fallback.

## Measurement and deferred borrowed parsing

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

A borrowed-parser prototype was deliberately deferred. `serde_json_borrow` 0.9
retains duplicate keys and input order, unlike the current sorted, last-key-wins
objects. Restoring compatibility with a second borrowed structural tree saved
additional peak memory, but reduced median throughput versus direct insertion
by approximately 10% for escaped input and 18% for the nested 100-key case in
local measurements. The final implementation retains owned JSON parsing rather
than adding normalization machinery or changing production semantics.
