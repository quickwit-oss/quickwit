# `span_to_schema` — canonical span → `datadog-spans` index doc

Runs at the tail of `preprocess_span`. Consumes the apm-processing-aligned
canonical span (see `./CLAUDE.md`) and emits the document shape the
`datadog-spans` index expects.

## Fields emitted


| index-doc path                       | derived from                                                                      | Notes                                                                           |
| ------------------------------------ | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| `.start_time`                        | `.start`                                                                          | i64 unix ns                                                                     |
| `.timestamp`                         | `floor((start + duration) / 1e6)`                                                 | rfc3339 ms; index `timestamp_field`                                             |
| `.discovery_timestamp`               | ingest now                                                                        | i64 unix ms                                                                     |
| `.span_id`, `.parent_id`             | canonical i64                                                                    | unsigned decimal string (`u64::to_string`)                                      |
| `.trace_id`                          | canonical i64 (lower 64) + optional `meta._dd.p.tid` (upper 64)                  | 32-char hex `{upper}{lower:016x}` when `meta._dd.p.tid` is a valid 16-char lowercase hex string; falls back to unsigned decimal of the lower 64 bits when absent or invalid. Mirrors apm-processing's `getValidatedHigher64BitsTraceId` validation. |
| `.trace_id_low`                      | canonical i64                                                                     | always unsigned decimal of the lower 64                                         |
| `.operation_name`                    | rename of `.name`                                                                 |                                                                                 |
| `.resource_name`                     | rename of `.resource`                                                             |                                                                                 |
| `.resource_hash`                     | `murmur3_x64_128(resource)[lower 64]` as hex                                      | matches `Resources.resourceHash` in logs-backend                                |
| `.status`                            | `.error` flag                                                                     | `"ok"` / `"error"`                                                              |
| `.error`                             | `.meta["error.type"]`                                                             | object carrying only `type`; the rest of `error.*` stays under `custom.error.*` |
| `.host`, `.env`                      | `.meta["_dd.hostname"]`, `.meta["env"]`                                           |                                                                                 |
| `.single_span`, `.analytics_enabled` | hardcoded `false`                                                                 | SaaS-side flags with no wire signal                                             |
| `.tiebreaker`                        | random `[0, u32::MAX]`                                                            | matches SaaS magnitude                                                          |
| `.custom`                            | fold of `meta`, `metrics`, `meta_struct`, `duration`, `span_links`, `span_events` | `meta`/`metrics`/`meta_struct` keys are filtered through `is_valid_tag` (mirroring dd-go's `events.isValidTag`): `_`-prefixed and `ddtags` keys are stripped unless allowlisted by `KEEP_ATTRIBUTES` or under a `KEEP_NAMESPACES` prefix. Schema declares `expand_dots: true`. |


## Fields dropped

- `.start` — already extracted into `.start_time`.
- `.error` (i64 flag) — replaced by `.status`.

