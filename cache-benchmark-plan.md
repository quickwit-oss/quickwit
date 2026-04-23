# Cache Improvement Benchmark Plan

**Date:** 2026-04-19
**Goal:** Prove that caching changes reduce search latency and S3 GET requests with reproducible, measurable benchmarks.

## What we already have

The codebase already emits all the metrics we need. No new instrumentation is required for the benchmark:

### Latency metrics (Prometheus)
- `leaf_search_split_duration_secs` — per-split search time (most granular)
- `leaf_search_request_duration_seconds` — per-leaf-request time (multiple splits)
- `root_search_request_duration_seconds` — end-to-end search time

### S3 GET metrics (Prometheus + Datadog)
- `object_storage_gets_total` — number of S3 GET requests (Prometheus)
- `object_storage_get_requests.count` — same, as DD metric
- `object_storage_get_requests_bytes.count` — bytes downloaded from S3

### Cache metrics (per cache: fast_field, splitfooter, partial_request, predicate)
- `cache_hits_num_items` / `cache_hits_num_bytes`
- `cache_misses_num_items`
- `cache_evict_num_items` / `cache_evict_num_bytes`
- `cache_in_cache_num_bytes` (current occupancy)

## Benchmark Design

### Workload: simulate a Dailymotion-like monitor + dashboard pattern

We simulate Dailymotion's actual usage at realistic scale. They currently have 10 dashboards with 103 queries (from our earlier analysis), but they are growing — we should benchmark for their near-term target:

**Monitor workload (continuous, every 60s):**
- 100 log monitors, each with a filter query (e.g., `service:cloudflare @BotScore:<20`, `service:GraphQL @graphql.errors.type:*`, etc.)
- Evaluating on a 15-minute rolling window
- Each monitor hits ~15-30 splits per evaluation
- That's **~100 queries/min sustained**, each fanning out to multiple splits

**Dashboard workload (burst, every 5 min):**
- 100 dashboards, averaging 10 widgets each → ~1000 widget queries per reload
- Mix of query types:
  - Simple count (77% of queries — `count` with filter, no group-by)
  - Avg on numeric field with group-by (15% — `avg(@BotScore) by @ClientASN top 10`)
  - Cardinality with nested group-by (8% — `cardinality(@ClientRequestPath) by @network.client.ip top 1000, @ClientASN top 10`)
- Querying last 4h of data
- Not all dashboards reload at once — simulate staggered loads across the 5-min window

**Ad-hoc workload (concurrent users):**
- 10 concurrent users doing log explorer searches
- Mix of text search (`service:X error`) and aggregation queries
- Varying time ranges (last 15min to last 7d)

**Combined steady-state query load:**
- Monitors: ~100 queries/min (sustained)
- Dashboards: ~1000 queries/5min = ~200 queries/min (bursty)
- Ad-hoc: ~10 queries/min (random)
- **Total: ~300 queries/min peak, ~150 queries/min average**

### Data setup

- Ingest **20 TB/day** for 7 days → ~140 TB stored (Dailymotion scale)
- Use realistic log schemas (Cloudflare logs with `@BotScore`, `@ClientASN`, `@ClientRequestPath`, `@network.client.ip`, `service`, `@http.status_code`)
- Multiple indexes to match Dailymotion's setup

### Test procedure

**Run A — Baseline (current config):**
1. Start searchers with default cache config
2. Run the combined workload for 1 hour
3. Record all metrics

**Run B — Partial request cache bumped to 1GB:**
1. Same setup, `partial_request_cache.capacity = 1gb`
2. Run the same workload for 1 hour
3. Compare

**Run C — Add term dict + posting list caching:**
1. Add `.term` and `.idx` routes to `QuickwitCache` (e.g., 2GB each)
2. Run the same workload for 1 hour
3. Compare

**Run D — Foyer hybrid cache (Phase 2):**
1. Replace memory caches with foyer (memory + NVMe)
2. Run the same workload for 1 hour
3. Compare

### What to compare (the scorecard)

| Metric | How to measure | Target |
|---|---|---|
| **Monitor latency p50** | `leaf_search_request_duration_seconds{status="success"}` percentile | < 500ms |
| **Monitor latency p99** | Same, p99 | < 2s |
| **Dashboard load time p50** | `root_search_request_duration_seconds` for dashboard queries | < 2s |
| **Dashboard load time p99** | Same, p99 | < 10s |
| **S3 GET count / hour** | `delta(object_storage_gets_total[1h])` | -50% vs baseline |
| **S3 bytes downloaded / hour** | `delta(object_storage_get_requests_bytes.count[1h])` | -50% vs baseline |
| **Cache hit rate (fast fields)** | `hits / (hits + misses)` on fast_field_cache | > 80% |
| **Cache hit rate (term dict)** | Same on term_dict_cache (Run C+) | > 70% |
| **Cache hit rate (posting lists)** | Same on posting_list_cache (Run C+) | > 70% |
| **Cache hit rate (partial request)** | Same on partial_request_cache | > 60% |
| **Timeouts (>30s)** | count of queries exceeding 30s | 0 |

### How to build the load generator

We need a tool that replays realistic queries at controlled rates. Two options:

**Option A: Script using the search API**
```bash
# Pseudo-code: a loop that fires queries via the REST/gRPC API
while true:
    # monitors — every 60s
    for monitor in monitors:
        POST /api/v1/{index}/search { query: monitor.query, start/end: last_15m }

    # dashboards — every 5min
    if tick_5m:
        for dashboard in dashboards:
            for widget in dashboard.widgets:
                POST /api/v1/{index}/search { query: widget.query, aggs: widget.aggs }

    # ad-hoc — continuous
    POST /api/v1/{index}/search { query: random_query(), time_range: random_range() }
```

**Option B: Record and replay from Dailymotion**
If we can capture the actual queries hitting Dailymotion's CloudPrem (via searcher access logs), we can replay them at the same rate. This is the most realistic but requires access to their query logs.

### Practical setup

| Resource | Spec |
|---|---|
| **Cluster** | Dogfooding cluster or dedicated test cluster |
| **Searchers** | 4 pods, 8 vCPU / 32GB RAM each (smaller than Dailymotion but enough to show the effect) |
| **NVMe** | 200GB local disk per searcher (for foyer Phase 2) |
| **S3** | Standard, same region |
| **Data** | Synthetic Cloudflare-like logs, 20TB/day × 7 days |
| **Duration per run** | 1 hour steady-state (+ 15 min warmup) |

## Reporting

For each run, produce a single-page report:

```
Run: [A|B|C|D] — [description]
Duration: 1h steady-state

Latency (seconds)
                    p50     p95     p99     max
Monitor queries:    0.120   0.450   1.200   3.500
Dashboard queries:  0.800   2.100   5.400   12.000
Ad-hoc queries:     0.300   1.500   4.200   15.000

S3 Requests
GET count/hour:     125,000
GET bytes/hour:     45 GB

Cache Hit Rates
Fast fields:        72%
Term dictionary:    N/A (not cached in this run)
Posting lists:      N/A (not cached in this run)
Partial request:    45%
Predicate:          38%

Errors
Timeouts (>30s):    3
OOMs:               0
```

Then a side-by-side comparison table across runs to show the improvement.

## Estimated S3 cost impact

For context on why S3 GETs matter:

| | Baseline (est.) | After caching (target) |
|---|---|---|
| S3 GET requests/day | ~3M | ~1.5M |
| S3 GET cost/month ($0.0004 per 1000) | ~$36 | ~$18 |
| S3 download bytes/day | ~1 TB | ~500 GB |
| S3 transfer cost/month ($0.09/GB) | ~$2,700 | ~$1,350 |

The download bytes are the bigger cost driver. Caching posting lists and term dicts avoids re-downloading the same data on every monitor evaluation.
