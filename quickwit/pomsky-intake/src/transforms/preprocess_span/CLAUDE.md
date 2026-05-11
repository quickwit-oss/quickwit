# SpanMap → preprocess_span field map

`preprocess_span` operates on the same canonical span shape as Java
apm-processing's `SpansProtobufPayloadParser.toSpanMap` — same field names,
same paths. The canonical shape is produced by `preprocess_dd_trace` per-span
before explode; add new span-level processors here and read paths exactly as
the Java pipeline does.

OTLP is not supported today; `preprocess_span` only handles `datadog_agent`.

## Divergences from apm-processing

| apm-processing path | preprocess_span path | Notes |
| --- | --- | --- |
| `meta`, `metrics`, `meta_struct` | `.meta`, `.metrics`, `.meta_struct` | top-level keys are flat with literal dots; Java nests them. **Pomsky convention — do not change.** Consumers read the literal-dotted key directly; splitting on `.` would force every consumer to switch to nested path traversal. `meta_struct` leaves are msgpack-decoded into structured `Value`s, so the values themselves are nested even though the top-level keys are flat. |

## What's gone or out of scope

- `ingest_size` — computed at Java parse time; not on the wire to Pomsky.
- Chunk-level fields (`host`, `env`, sampler config, …) — live on the pre-explode chunk event in `preprocess_dd_trace`. Only `_dd.hostname` and `env` are propagated into per-span `meta`.

## References

- Java parser: `SpansProtobufPayloadParser.toSpanMap` in `~/dd/logs-backend/.../com/fsmatic/workload/processing/`
- Pre-explode (chunk) shape: `../preprocess_dd_trace/CLAUDE.md`
- Schema mapping (canonical → index doc): `./span_to_schema.md`
- Migration tracker: `./README.md`
- Explode boundary: `../explode_trace_spans.rs`
