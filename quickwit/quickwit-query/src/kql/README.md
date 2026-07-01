# KQL — Kibana Query Language support

> ⚠️ **Disambiguation.** "KQL" is overloaded in the industry — it refers
> to two unrelated query languages:
>
> - **Kibana Query Language** (this module): a single-expression
>   predicate grammar — `level:error and status:>=500` — used by the
>   Kibana UI for log search. Public grammar reference:
>   <https://www.elastic.co/docs/explore-analyze/query-filter/languages/kql>.
> - **Kusto Query Language** (Microsoft): a pipeline language —
>   `Table | where x > 5 | summarize count() by foo | top 10 by ts` —
>   used by Azure Data Explorer, Log Analytics, Sentinel, Defender.
>   **Not implemented here.** If you want Kusto support, propose it
>   under a different name (e.g. `kusto`, `kustoql`) to avoid collision
>   with this module.
>
> Throughout this codebase, `KQL` / `kql` / `?kql=` always means the
> Kibana variant.

End-user query language for Quickwit, drawn from the public Kibana KQL
grammar referenced above.

This module owns the parser, the AST, the lowering pass to Quickwit's
internal `QueryAst`, and the Prometheus metrics emitted from the parse
path.

## Wire surface

Two ways to send KQL to a running Quickwit cluster.

### 1. Native REST parameter

```bash
curl 'http://<host>:7280/api/v1/<index>/search?kql=level:error+and+status:>=500'
```

The `kql` query parameter is mutually exclusive with the existing `query`
parameter (Tantivy/Lucene-ish grammar). Exactly one must be supplied; both
or neither returns HTTP 400.

POST variant:

```bash
curl -X POST 'http://<host>:7280/api/v1/<index>/search' \
  -H 'Content-Type: application/json' \
  -d '{"kql": "level:error and service:api", "max_hits": 20}'
```

**Note.** KQL is intentionally **not** exposed via the
`/api/v1/_elastic/<index>/_search` endpoint. That namespace mirrors the
Elasticsearch query DSL, which has no `kql` variant — a real
Elasticsearch cluster rejects `{"query": {"kql": ...}}` with
`parsing_exception`. Keeping the `_elastic/` surface honest means KQL
lives only on the two native paths above.

## Supported grammar

Every form documented in the Kibana KQL reference, modulo the divergences
called out below. The conformance corpus
[`kibana_conformance.rs`](kibana_conformance.rs) pins the expected AST for
each idiom and fails CI on drift.

| Form | Example |
|---|---|
| Field-value match | `level:error` |
| Phrase match | `message:"connection refused"` |
| Bare term against default fields | `refused` |
| Bare phrase against default fields | `"connection refused"` |
| Wildcard value | `service:work*` |
| Field-exists check | `level:*` |
| Match-all | `*` (lowers to `QueryAst::MatchAll`, no automaton work) |
| Boolean AND (explicit) | `level:error and service:api` |
| Boolean AND (juxtaposition) | `level:error service:api` |
| Boolean OR | `level:error or level:warn` |
| Boolean NOT | `not level:error` |
| Parens | `(level:error or level:warn) and service:api` |
| Value group OR | `level:(error or warn)` |
| Value group AND | `tags:(prod and critical)` |
| Range `>=` / `>` / `<=` / `<` | `status:>=500`, `latency_ms:<0.1` |
| Compound range | `status:>=200 and status:<500` |
| Quoted ISO timestamp in range | `@timestamp:<"2025-01-01T00:00:00Z"` |
| Escaped colon in field name | `metric\:count:value` |
| Escaped keyword as field name | `\and:value` |

Precedence: `not` binds tightest, then `and`, then `or` (loosest). Parens
override.

## Intentional divergences from Kibana

| Kibana behavior | Quickwit behavior | Reason |
|---|---|---|
| Unquoted ISO timestamps in range values (`@timestamp:>=2025-01-01T00:00:00Z`) | Requires quotes (`@timestamp:>="2025-01-01T00:00:00Z"`) | Our lexer tokenizes on `:`. Documented in error messages. |
| Nested-field object syntax (`nested:{ name:foo }`) | Rejected with a clear error pointing to flat dotted paths | Quickwit has no nested-field type. |
| `field:(other:value)` — nested field qualifier in value group | Rejected | Silent rebinding would be a wrong-data footgun. Kibana also errors. |

## Safety rails

All limits are hard caps — exceeding them returns HTTP 400 with a specific
error message.

| Limit | Value | Where |
|---|---|---|
| Max KQL string length (REST) | 16,384 bytes | [`rest_handler.rs:MAX_KQL_INPUT_LEN`](../../../quickwit-serve/src/search_api/rest_handler.rs) |
| Max parser nesting depth | 64 | [`parser.rs:MAX_KQL_DEPTH`](parser.rs) |
| Max bare-token length | 1,024 bytes | [`lexer.rs:MAX_BARE_TOKEN_LEN`](lexer.rs) |
| Max quoted-phrase length | 4,096 bytes | [`lexer.rs:MAX_PHRASE_LEN`](lexer.rs) |

Together these close the obvious DoS angles: oversized inputs, pathological
nesting, single-token memory bombs.

## Field-type validation

KQL is not schema-aware at parse or lowering time — the KQL AST only
carries field *names*, not types. Type-aware validation runs at the
same seam every Quickwit query language hits:

```
KQL string                          → parser: syntax errors only
   │
   ▼  kql_to_query_ast()
QueryAst { Range, FullText, ... }   → lowering: no schema access
   │
   ▼  serialized over proto to root
root: parse_user_query()             → resolves UserInputQuery vessels
   │
   ▼  proto sent to leaf
leaf: build_tantivy_query(ctx)       → ★ here. `ctx` carries the schema.
   │                                   This is where type validation runs.
   ▼
Tantivy execution
```

Concrete examples — three distinct outcomes depending on the failure mode:

| KQL input | Outcome | Why |
|---|---|---|
| `kql=service:>=5` where `service` is `text` | **HTTP 400** | `RangeQuery::build_tantivy_ast_impl` rejects ranges on non-numeric/datetime fields |
| `kql=status:err*` where `status` is `u64` | **HTTP 400** | `WildcardQuery::build_tantivy_ast_impl` rejects wildcards on non-text fields |
| `kql=status:>=abc` where `status` is `u64` | **HTTP 400** | `RangeQuery::convert_bound::<u64>` cannot coerce the non-numeric literal |
| `kql=no_such_field:value` against a **dynamic-mapped** index (Quickwit's default) | **HTTP 200, 0 hits** | Dynamic mode treats unknown fields as legitimately absent — no error, no match |
| `kql=no_such_field:value` against a **strict-mode** index | **HTTP 400** | Strict mode rejects references to undeclared fields |

Note the third row: missing-field handling depends on the index's
`doc_mapping.mode` (`dynamic` vs `strict`), not on KQL itself. This is
the **same behavior** the Tantivy-grammar `?query=` path and the ES query
DSL `{"query": {"range": {...}}}` path have today — KQL inherits the
shared validation surface rather than duplicating it.

### Known limitations of this approach

1. **Error-message origin.** Errors come from the Tantivy / Quickwit
   query-building layer, not from KQL. A KQL user who writes
   `kql=status:err*` sees something like "wildcard only supported on text
   fields" — the message does not include the originating KQL clause.
2. **Lazy timing.** Validation fires at leaf-search time, not at parse
   time. A syntactically valid KQL string can still 400 after the search
   has already started executing on some leaves.
3. **Cross-index inconsistency.** `GET /api/v1/logs-*/search?kql=...`
   against indexes with different schemas will succeed on some and fail
   on others; the whole request fails with whichever leaf errored first.

Earlier / friendlier validation would require either schema access at
the REST layer or a new pre-flight pass at the search root. Both are
*generic* improvements (they would benefit every query language, not
just KQL) and are intentionally deferred to a separate proposal so this
PR stays focused on the KQL surface.

## Observability

Prometheus metrics emitted from the parse path under the `quickwit_kql_*`
namespace:

| Metric | Type | Meaning |
|---|---|---|
| `quickwit_kql_parse_total` | counter | Every parse attempt that reaches `KqlQuery::parse_user_query` |
| `quickwit_kql_parse_failures_total` | counter | Subset that returned an error |
| `quickwit_kql_parse_duration_seconds` | histogram | Wall-clock from parse-start to AST-or-error |

Structured tracing fields on every search log line: `kql=true/false`,
`tantivy_grammar=true/false` — lets you split KQL vs. Lucene traffic in
Splunk/Elastic without parsing raw query strings.

## Architecture

KQL is translated eagerly at the REST entry point. There is **no new
variant on `QueryAst`** — the output is built from existing variants
(`BoolQuery`, `FullTextQuery`, `RangeQuery`, `FieldPresenceQuery`,
`WildcardQuery`, `MatchAll`, `UserInputQuery`). Bare default-field values
are wrapped in `UserInputQuery` so the existing search root resolves
them against each index's `default_search_fields` — same deferred-
resolution mechanism the Tantivy-grammar `?query=` path already uses.

```
        ┌──────────────────────────────────────────────────────────┐
        │  REST handler                                            │
        │    quickwit-serve/src/search_api/rest_handler.rs         │
        │    • SearchRequestQueryString { query, kql, ... }        │
        │    • build_query_ast(kql_text) → kql_to_query_ast(...)   │
        └─────────────────────────┬────────────────────────────────┘
                                  │
                                  ▼
        ┌──────────────────────────────────────────────────────────┐
        │  kql/  ◀──── you are here                                 │
        │    lexer.rs    → Token stream, size caps                  │
        │    parser.rs   → KqlAst, depth cap                        │
        │    lower.rs    → KqlAst → existing QueryAst variants      │
        │                  (Bool / FullText / Range / FieldPresence │
        │                   / Wildcard / MatchAll / UserInputQuery) │
        │    metrics.rs  → counters + histogram                     │
        └─────────────────────────┬────────────────────────────────┘
                                  │ QueryAst (no new variant)
                                  ▼
        ┌──────────────────────────────────────────────────────────┐
        │  Existing search pipeline — UNCHANGED                    │
        │    quickwit-search/src/root.rs                           │
        │    • UserInputQuery vessels resolve via the existing     │
        │      deferred-default-field path                          │
        │    • All other variants flow through as-is               │
        └──────────────────────────────────────────────────────────┘
```

## Testing layers

| Layer | Where | What it proves |
|---|---|---|
| Unit | this crate's `#[cfg(test)]` blocks | Per-function correctness for lexer / parser / lowering / metrics wire-up |
| Conformance | [`kibana_conformance.rs`](kibana_conformance.rs) | Documented Kibana grammar idioms produce the expected `KqlAst` |
| Proptest fuzz | inside `parser.rs::tests::proptest_*` | Parser never panics for arbitrary ASCII or Unicode input (≈6k cases per run) |
| Integration | [`../../../../rest-api-tests/scenarii/kql_search/`](../../../../rest-api-tests/scenarii/kql_search/) | End-to-end through the HTTP stack against a real index — exact `num_hits` per query |
| Load | [`../../../../rest-api-tests/scenarii/kql_search/load_test.py`](../../../../rest-api-tests/scenarii/kql_search/load_test.py) | Throughput + p50/p95/p99 + safety-rail behavior under concurrency |
| Multi-node | [`../../../../rest-api-tests/scenarii/kql_search/docker-compose.cluster.yml`](../../../../rest-api-tests/scenarii/kql_search/docker-compose.cluster.yml) | Distributed root→leaf, PostgreSQL metastore, LocalStack S3 |

Run the integration scenarios:

```bash
cd quickwit/rest-api-tests
python3 run_tests.py --engine quickwit \
  --binary <path>/target/debug/quickwit \
  --test scenarii/kql_search
```

## Isolation audit — what this feature touches in the rest of the codebase

KQL is implemented as a **thin translation layer at the REST entry
point**, not as a new query AST node. The `QueryAst` enum, the visitor
traits, tag pruning, and root-search are all unchanged.

### New files (pure isolation)

| Path | Purpose |
|---|---|
| `quickwit-query/src/kql/` (this directory) | Lexer, parser, AST, lowering, metrics, conformance corpus |
| `rest-api-tests/scenarii/kql_search/` | YAML scenarios, load test, multi-node compose |

### Existing files modified

| File | Change | Risk to non-KQL traffic |
|---|---|---|
| `quickwit-query/src/lib.rs` | `mod kql;` + `pub use kql::kql_to_query_ast` | None — adds a module and one public function |
| `quickwit-query/Cargo.toml` | Added `quickwit-metrics` dep | None — already a workspace member |
| `quickwit-serve/src/search_api/rest_handler.rs` | Added `kql` field to `SearchRequestQueryString`; new `build_query_ast` helper that calls `kql_to_query_ast`; structured log fields | **One wire-contract change**: `query` was required, now `#[serde(default)]`. Requests with `{}` previously failed at JSON deserialization; now fail at validation with HTTP 400 "either `query` or `kql` must be supplied". OpenAPI schema correctly reports both as optional/nullable. |
| `quickwit-cli/src/tool.rs` | Added `kql: None` to one struct literal that didn't use `..Default::default()` | None |

### Files I did NOT touch

- `quickwit-query/src/query_ast/mod.rs` — **the core `QueryAst` enum is unchanged.** No new variant, no new match arms.
- `quickwit-query/src/query_ast/visitor.rs` — **`QueryAstVisitor` and `QueryAstTransformer` traits are unchanged.** External visitors keep working without recompilation.
- `quickwit-query/src/elastic_query_dsl/mod.rs` — **the ES query DSL enum is unchanged.** KQL is deliberately not exposed under `_elastic/` because real Elasticsearch has no `kql` variant.
- `quickwit-doc-mapper/src/tag_pruning.rs` — unchanged.
- `quickwit-search/src/root.rs` — unchanged.
- All ES DSL variants (`term`, `match`, `range`, `bool`, ...) — unchanged.
- Indexing pipeline, metastore, storage, control plane, cluster, actors — unchanged.
- The Tantivy-grammar `UserInputQuery` lowering path — unchanged (KQL reuses it as a deferred-resolution vessel; no code change to that path).
- On-disk data formats — zero impact.

### What "no effect on main code" actually means

- **End users of the existing `query=` parameter or other ES DSL variants**: no behavior change.
- **Operators / SREs**: a handful of new metrics under `quickwit_kql_*`, no removed metrics, no changes to existing dashboards.
- **Data at rest**: zero impact. KQL translates to the same `QueryAst` types Quickwit already executes.
- **Rust callers of `QueryAst`, `QueryAstVisitor`, `QueryAstTransformer`**: zero source-level breakage. These types are exactly as they were before this feature.
- **Rust callers of `SearchRequestQueryString`**: one new field (`kql: Option<String>`); callers using `..Default::default()` keep working; the one in-workspace site that listed every field (`quickwit-cli/src/tool.rs`) was updated.

This is the wrapper architecture — KQL is added without growing the core
query system's surface. The full-integration variant (a new
`QueryAst::Kql` variant with deferred parsing at root) is also viable and
would have been more ergonomic for cross-index queries with differing
defaults, but it required ~377 lines across 9 files including visitor
trait extensions. The wrapper variant trades a tiny bit of fidelity (the
multi-index error message stays as the existing generic Tantivy-grammar
one) for a substantially smaller, more reviewable change.

## Performance reference

Numbers from a 30-second load test against a debug build, single node, on
a MacBook (the floor — release-mode + a real load generator will go
substantially higher):

- Sustained throughput: **~1,560 req/s** across 14 happy-path shapes + 5
  adversarial shapes
- p99 latency under load: **< 30 ms** for every happy-path shape
- p99 latency for adversarial rejects: **< 18 ms** (rejection happens
  before any search work)
- Parse-stage cost: **93% of parses < 100 µs**, 100% < 1 ms
- Errors during 47k-request sweep: **0 unexpected statuses**

## Known limitations (not yet implemented)

- Real Kibana frontend has not been pointed at this server. Grammar
  matches the public Kibana docs; the conformance corpus pins each
  idiom. A standing Kibana → Quickwit smoke test is the next layer.
- The KQL parser is hand-rolled; the Tantivy-grammar path uses
  `tantivy::query_grammar`. Two parsers means two maintenance surfaces.
  Consolidating either upstream or behind a single grammar is deferred.
- Authenticated / multi-tenant exercising not covered by these tests.
