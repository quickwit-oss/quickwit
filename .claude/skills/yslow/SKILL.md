---
name: yslow
description: "Analyze a root_resource_stats log line from Pomsky and explain why the query was slow, with a phase breakdown and bottleneck identification."
user-invocable: true
---

## What it does

Takes a `root_resource_stats` log line (JSON or raw message text) pasted by the user, parses all the structured fields, and produces a human-readable performance report modeled after YSlow: what ran, how long each phase took, and where the time went.

## Background: search execution flow

Each query fans out from a **root node** to one or more **leaf nodes**. Each leaf node searches one or more **splits** (index shards). Every split goes through four sequential phases:

1. **Acquire search permit** — waits for a counting semaphore that caps concurrent downloads and memory usage. This prevents OOM on the node. Time: `wait_for_search_permit_microsecs`.
2. **Warmup** — downloads the required Tantivy data (posting lists, fast fields, term dicts, norms) from object storage into caches, making all data resident before the CPU search begins. Time: `warmup_microsecs`.
3. **Acquire CPU pool slot** — waits for a thread in the blocking CPU pool (`run_cpu_intensive`). Tantivy search must run synchronously without async I/O, so it is dispatched to a thread pool. Time: `wait_for_cpu_pool_microsecs`.
4. **CPU search** — runs the actual Tantivy search (predicate matching, collection, per-segment harvest). Time: `cpu_search_microsecs`.

Splits within a leaf run concurrently, but are gated by the permit system. The leaf's wall time is dominated by the slowest pipeline of splits, not the sum.

## Log field reference

### Root-level fields (global)

| Field | Meaning |
|-------|---------|
| `leaf_num_calls` | Number of leaf gRPC calls, excluding retries |
| `leaf_num_calls_including_retries` | Including retries |
| `num_failed_splits` | Splits that returned an error across all leaves |
| `leaf_wall_times_microsecs_avg` | Average wall time across all leaves |
| `leaf_wall_times_microsecs_second_slowest` | Second entry of the descending-sorted wall-time vector — compare to `wleaf_wall_time_microsecs` to see if one leaf was a straggler |

### Prefix scheme

| Prefix | View |
|--------|------|
| `wleaf_` | Worst leaf (highest wall time) |
| `sleaf_` | Sum across all leaves |
| `wleaf_wsplit_` | Worst split within the worst leaf (ranked by warmup+cpu_search, excluding queueing phases) |
| `wleaf_ssplit_` | Sum of splits within the worst leaf |
| `sleaf_wsplit_` | Worst split from the summed-leaf view |
| `sleaf_ssplit_` | Sum of all splits across all leaves |

### Per-leaf fields (both `wleaf_` and `sleaf_` prefixes)

| Suffix | Meaning |
|--------|---------|
| `wall_time_microsecs` | Wall-clock duration of the leaf gRPC call |
| `partial_result_cache_num_splits` | Splits served from partial-result cache (no work done) |
| `partial_result_cache_num_docs` | Docs in cache-hit splits |
| `localexec_num_splits` | Splits searched locally (excludes cache hits and lambda offloads) |
| `localexec_num_docs` | Docs in locally-searched splits |
| `min_wait_for_search_permit_microsecs` | Minimum permit wait across all splits on this leaf. If high, the node was saturated — even the luckiest split had to wait. This is the key saturation signal. |
| `min_wait_for_cpu_pool_microsecs` | Minimum CPU-pool wait across all splits on this leaf |
| `lambda_num_splits` / `lambda_num_docs` | Splits sent to Lambda |
| `lambda_success_num_splits` / `lambda_success_num_docs` | Lambda splits that succeeded |
| `lambda_bottleneck` | 1 if Lambda path finished after local path |

### Per-split fields (both `w` and `s` variants for each leaf view)

| Suffix | Meaning |
|--------|---------|
| `split_num_docs` | Total documents in the split |
| `input_memory_bytes` | Bytes resident in warmup cache after warmup (measure of data processed) |
| `download_num_bytes` | Bytes downloaded from storage |
| `download_num_requests` | Number of storage GET requests |
| `matched_num_docs` | Documents matched by the query |
| `wait_for_search_permit_microsecs` | Time waiting to acquire the search permit |
| `warmup_microsecs` | Time spent downloading and populating Tantivy caches |
| `wait_for_cpu_pool_microsecs` | Time waiting for a CPU thread pool slot |
| `cpu_search_microsecs` | Time executing the Tantivy search itself |

## How to analyze

When the user pastes a log line, do the following steps in order.

### Step 1 — Parse the input

The user may paste:
- A raw JSON object: parse `message` field, then extract all `key=value` pairs (values may be quoted strings or bare numbers).
- A raw log message (no JSON wrapper): extract `key=value` pairs directly.

Build a flat map of field name → numeric value. Ignore `operation`, `level`, `service`, etc.

### Step 2 — Compute derived metrics

For the **worst split in the worst leaf**, compute:

```
split_total = wleaf_wsplit_wait_for_search_permit_microsecs
            + wleaf_wsplit_warmup_microsecs
            + wleaf_wsplit_wait_for_cpu_pool_microsecs
            + wleaf_wsplit_cpu_search_microsecs

permit_pct   = wleaf_wsplit_wait_for_search_permit_microsecs / split_total * 100
warmup_pct   = wleaf_wsplit_warmup_microsecs / split_total * 100
cpu_pool_pct = wleaf_wsplit_wait_for_cpu_pool_microsecs / split_total * 100
cpu_pct      = wleaf_wsplit_cpu_search_microsecs / split_total * 100
```

For the **worst leaf** aggregate:
```
wleaf_avg_per_split_permit = wleaf_ssplit_wait_for_search_permit_microsecs / wleaf_localexec_num_splits
wleaf_avg_per_split_warmup = wleaf_ssplit_warmup_microsecs / wleaf_localexec_num_splits
wleaf_avg_per_split_cpu_pool = wleaf_ssplit_wait_for_cpu_pool_microsecs / wleaf_localexec_num_splits
wleaf_avg_per_split_cpu = wleaf_ssplit_cpu_search_microsecs / wleaf_localexec_num_splits
```

Compute selectivity (fraction of docs matched):
```
selectivity = sleaf_ssplit_matched_num_docs / sleaf_ssplit_split_num_docs * 100
```

Compute download efficiency:
```
avg_bytes_per_request = sleaf_ssplit_download_num_bytes / sleaf_ssplit_download_num_requests
```

Compute cache hit rate:
```
total_splits = sleaf_partial_result_cache_num_splits + sleaf_localexec_num_splits + sleaf_lambda_num_splits
cache_hit_rate = sleaf_partial_result_cache_num_splits / total_splits * 100
```

Compute the straggler ratio (how much slower the worst leaf was vs the second-slowest):
```
straggler_ratio = wleaf_wall_time_microsecs / leaf_wall_times_microsecs_second_slowest
```
If `leaf_wall_times_microsecs_second_slowest` is absent (only 1 leaf), skip this.

### Step 3 — Format the report

Output the following sections. Format all times as `X.XXs` (seconds) or `XXXms` if under 1 second. Show raw µs only in the detailed table.

---

**YSLOW — Pomsky Query Performance Report**

```
Operation : <operation>
Timestamp : <timestamp from log if present>
Leaves    : <leaf_num_calls> calls (<leaf_num_calls_including_retries> incl. retries)
Wall time : <wleaf_wall_time_microsecs>s (worst leaf) | avg <leaf_wall_times_microsecs_avg>s across leaves
Failed    : <num_failed_splits> splits
```

**Scope**
```
Splits searched : <sleaf_localexec_num_splits> local + <sleaf_partial_result_cache_num_splits> cache hits + <sleaf_lambda_num_splits> lambda
Docs scanned    : <sleaf_ssplit_split_num_docs> total, <sleaf_ssplit_matched_num_docs> matched (<selectivity>%)
Cache hit rate  : <cache_hit_rate>% of splits served from cache
Data downloaded : <sleaf_ssplit_download_num_bytes / 1024 / 1024>.X MB in <sleaf_ssplit_download_num_requests> requests (<avg_bytes_per_request / 1024>.X KB avg)
```

**Worst split phase breakdown** (worst split in worst leaf):
```
Phase                  Time        Share
─────────────────────────────────────────
Search permit wait   X.XXs         XX%   ← [SATURATION if >50%]
Warmup (download)    X.XXs         XX%
CPU pool wait        X.XXs         XX%
CPU search           X.XXs         XX%
─────────────────────────────────────────
Total (serial)       X.XXs        100%
```

**Per-split averages** (worst leaf, <wleaf_localexec_num_splits> splits):
```
Avg permit wait   : X.XXs
Avg warmup        : X.XXs
Avg CPU pool wait : X.XXs
Avg CPU search    : X.XXs
Min permit wait   : X.XXs  ← <wleaf_min_wait_for_search_permit_microsecs>s [key saturation signal]
```

If `leaf_num_calls > 1`, add a **Leaf balance** section:
```
Leaf balance:
  Worst  : X.XXs
  2nd    : X.XXs
  Avg    : X.XXs
  Ratio  : X.Xx  ← >2.0 suggests a straggler
```

If lambda was used, add a **Lambda** section.

### Step 4 — Identify bottleneck and explain

After the tables, write a **Bottleneck** section with a plain-English explanation. Use the following rules:

**Search permit saturation** (most common):
- Signal: `permit_pct > 50%` OR `wleaf_min_wait_for_search_permit_microsecs > 500_000` (> 0.5s)
- Explanation: The search permit semaphore serializes concurrent downloads to protect memory. Even the luckiest split waited >0.5s, which means the node was continuously saturated. This points to either too many splits hitting a node at once, or per-split memory budgets too small (forcing tight permit limits).
- Recommendation: Consider increasing `search_permit_num_download_slots`, reducing splits-per-node, or enabling Lambda offload for large queries.

**Warmup / storage bottleneck**:
- Signal: `warmup_pct > 40%` AND `permit_pct < 30%`
- Explanation: Splits spend most time downloading data from storage. This is normal for cache-cold queries but can indicate the data is scattered across too many files (`download_num_requests` per split is high).
- Recommendation: Check compaction status. More compacted splits = fewer storage requests per split = faster warmup.

**CPU pool saturation**:
- Signal: `cpu_pool_pct > 30%`
- Explanation: Splits are warmed up and waiting for a CPU thread. The node's CPU pool is a bottleneck.
- Recommendation: Reduce `num_searcher_threads` or add CPU capacity.

**CPU-bound query**:
- Signal: `cpu_pct > 40%` AND warmup and permit waits are low
- Explanation: The query itself is expensive — high selectivity, complex aggregation, or large matched-doc count driving collection overhead.
- Recommendation: Narrow the time range, add more selective filters, or review aggregation cardinality.

**Straggler leaf**:
- Signal: `straggler_ratio > 2.0`
- Explanation: One leaf took more than 2× as long as the second-slowest, which implies uneven split distribution or a hot node.
- Recommendation: Check split assignment balance across nodes.

**Healthy query** (all phases low):
- Signal: all percentages below 20%, wall time under 1s
- Explanation: No single phase dominates; the query is well-distributed.

Multiple bottlenecks can be active simultaneously — report all that apply, in order of severity (highest percentage first).

### Step 5 — Summary line

End with a single sentence that names the primary bottleneck and its magnitude:

> "The query spent X.Xs / XX% of the worst split's serial time waiting for a search permit, indicating node saturation at the memory/download-slot level."

---

## Output style

- Be direct. Skip preamble.
- Use the tables above verbatim; fill in numbers.
- Render times consistently: prefer `X.XXs`; use `Xms` for sub-second; never show raw µs in the summary tables.
- Bold the dominant phase in the breakdown table.
- Keep the Bottleneck section concise: 2-4 sentences per bottleneck identified.
