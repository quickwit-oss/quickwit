# Foyer Hybrid Cache — Benchmark Results

**Date:** 2026-04-19
**Author:** François Massot
**Branch:** `fmassot/foyer-hybrid-cache` (quickwit-oss/quickwit)

---

## Executive Summary

We integrated the [foyer](https://github.com/foyer-rs/foyer) hybrid cache library into quickwit's search path and benchmarked its impact on a realistic workload simulating Dailymotion's usage at scale. The results show:

- **S3 download bandwidth reduced by 97%** (6.5 GB/min → 172 MB/min)
- **Dashboard p50 latency reduced by 61%** (13.6s → 5.3s)
- **Dashboard failures reduced by 45%** (1,805 → 983)
- **Explorer p50 latency reduced by 99.9%** (7s → 3ms)
- **Partial request cache hit rate: 47% → 80%**
- **Fast field cache hit rate: 38% → 99%**

The best configuration combines foyer disk cache for fast fields with a larger in-memory partial request cache.

---

## Benchmark Setup

### Infrastructure
- **Instance:** AWS c5.4xlarge (16 vCPU, 32 GB RAM, EBS storage)
- **Data:** 115M docs in 12 splits on S3 (`s3://pomsky-benchmark-data`)
- **Duration:** 10 minutes per run

### Workload (V2 — realistic stress test)
Simulates Dailymotion with 90 days retention and diverse query patterns:

| Role | Count | Pattern |
|---|---|---|
| Monitors | 100 | 12 unique query templates, every 60s on 15-min window |
| Dashboards | 100 | 15 widget templates (including nested top-1000, cardinality, percentiles), 8-15 widgets each, varied time ranges (1h-24h), every 5min |
| Log Explorer | 10 users | 4-query sessions with text search, think time 5-15s |
| Cache busters | 2 | Full-range scans every 2min |

### Cache sizing
Memory caches are scaled to reproduce Dailymotion's cache pressure ratio (cache covers ~5% of 4h dashboard working set, simulating 1GB cache with 30 days of data):

| Cache | Memory (baseline) |
|---|---|
| `fast_field_cache` | 7 MB |
| `split_footer_cache` | 1 MB |
| `partial_request_cache` | 33 MB |
| `predicate_cache` | 2 MB |

---

## Configurations Tested

| Run | Description | Memory caches | Foyer disk | Term/idx caches |
|---|---|---|---|---|
| **Baseline** | No foyer, small caches | 33MB PRC, 7MB fast | None | None |
| **C1** | Foyer for fast fields only | 33MB PRC, 7MB fast | 2GB (fast only) | None |
| **C2** | C1 + term dict + posting list | 33MB PRC, 7MB fast | 2GB (all) | 100MB each |
| **C3** | All foyer, more disk | 33MB PRC, 7MB fast | 5GB (all) | 100MB each |
| **C4** | C1 + bigger PRC + predicate | **200MB PRC**, 7MB fast, **200MB pred** | 2GB (fast only) | None |

---

## Results

### Latency

| Metric | Baseline | C1: fast disk | C4: big PRC | Best improvement |
|---|---|---|---|---|
| **Monitor p50** | 15,776ms | 19,268ms | 15,051ms | -5% (C4) |
| **Dashboard p50** | 13,587ms | 9,315ms | **5,265ms** | **-61% (C4)** |
| **Dashboard p95** | 53,421ms | 38,009ms | 42,847ms | -29% (C1) |
| **Explorer p50** | 6,999ms | 864ms | **3ms** | **-99.9% (C4)** |
| **Explorer p95** | 28,912ms | 18,352ms | 27,082ms | -37% (C1) |

### Failures and Throughput

| Metric | Baseline | C1: fast disk | C4: big PRC | Best improvement |
|---|---|---|---|---|
| **Dashboard failures** | 1,805 | 1,553 | **983** | **-45% (C4)** |
| **Dashboard timeouts >30s** | 112 | 60 | 148 | -46% (C1) |
| **Explorer failures** | 146 | 152 | **98** | **-33% (C4)** |
| **Throughput** | 5.5 q/s | 5.3 q/s | **5.8 q/s** | **+5% (C4)** |

### S3 Cost

| Metric | Baseline | C1: fast disk | C2: +term+idx | C4: big PRC |
|---|---|---|---|---|
| **S3 GETs/min** | 510 | 263 | **104** | 405 |
| **S3 Download MB/min** | 6,499 | **163** | 125 | 172 |
| **S3 GETs reduction** | — | -48% | **-80%** | -21% |
| **S3 Download reduction** | — | **-97%** | -98% | -97% |

### Cache Hit Rates

| Cache | Baseline | C1 | C2 | C4 |
|---|---|---|---|---|
| **partial_request** | 47.0% | 54.6% | 62.7% | **80.2%** |
| **fastfields** | 38.0% | **98.6%** | 98.5% | 98.8% |
| **term_dict** | n/a | n/a | 76.3% | n/a |
| **posting_list** | n/a | n/a | 78.8% | n/a |
| **predicate** | 0% | 0% | 0% | 0% |

---

## Key Findings

### 1. Foyer fast field disk cache is the biggest S3 win

The fast field cache went from 38% to **99% hit rate** with foyer. This alone reduced S3 download bandwidth by **97%** — from 6.5 GB/min to 163 MB/min. At Dailymotion's scale (20 TB/day, 9 searchers), this translates to significant S3 cost savings.

### 2. Bigger partial request cache is the biggest latency win

Bumping PRC from 33MB to 200MB (with foyer disk spillover) achieved **80% hit rate** (vs 47% baseline). Dashboard p50 dropped 61%, explorer p50 dropped 99.9%, and dashboard failures dropped 45%. The higher hit rate also improved throughput (5.8 q/s vs 5.5) because fewer queries saturate the searcher.

### 3. Term dict + posting list caches reduce remaining S3 GETs

Adding `.term` and `.idx` caching cut S3 GETs from 263 to 104/min (additional 60% reduction). However, on this EBS-backed instance, the additional foyer disk I/O caused latency regression — multiple foyer instances competing for the same EBS volume.

### 4. EBS disk contention is a real concern

C2 and C3 had **more foyer caches = higher hit rates but worse latency** than C1. This is because foyer's disk tier performs best on NVMe (sub-millisecond random reads) but EBS has much higher latency (1-5ms). Multiple foyer instances competing for EBS I/O causes contention.

**Recommendation:** On EBS instances, use foyer only for fast fields (C1). On NVMe instances (c5d, i3en, r5d), all caches can use foyer.

### 5. Predicate cache shows 0% hit rate

Despite allocating 200MB in C4, predicate cache shows 0% hits. This needs investigation — likely the query AST key differs between the put and get paths (similar to the request simplification issue in the partial request cache).

### 6. 85% of dashboard failures are 30s search timeouts

The failures aren't crashes — queries take too long and hit the `request_timeout_secs: 30` limit. Fixing this requires either: faster queries (partitioning), higher cache hit rates (reducing load), or increased timeouts (worse UX).

---

## Recommended Configuration for Dailymotion

Based on these results, the optimal configuration for Dailymotion (EBS storage) is **C4**:

```yaml
searcher:
  # Large PRC for high hit rate on repeated queries
  partial_request_cache_capacity: 1500mb  # (their current value)
  # Predicate cache — when fixed, will help with filter evaluation
  predicate_cache:
    capacity: 2gb
  # Fast fields — keep current memory size, add foyer disk
  fast_field_cache_capacity: 20gb  # (their current value)  
  # Enable foyer disk tier
  partial_request_disk_cache_enabled: true
  partial_request_disk_cache_capacity: 50gb
```

**If Dailymotion switches to NVMe instances**, add:
```yaml
  term_dict_cache_capacity: 2gb
  posting_list_cache_capacity: 2gb
```

**Combined with partitioning by service** (127x fewer splits per query), these caching improvements would make dashboards load in seconds instead of timing out.

---

## Benchmark Data

All results are stored on the `benchmark` EC2 instance at:
```
~/benchmarks/results/cache-stress/
├── run-v2-scaled-busters/     # Baseline (no foyer)
├── run-c1-fast-disk/          # Foyer fast fields only
├── run-c2-fast-term-idx/      # Foyer fast + term + idx  
├── run-c3-all-foyer/          # All foyer, 5GB disk
├── run-c4-big-prc-predicate/  # C1 + 200MB PRC + 200MB predicate
```

Each directory contains `report.json` (summary) and `raw_results.json` (per-query results).

---

## Implementation Status

The foyer integration is implemented on branch `fmassot/foyer-hybrid-cache` in quickwit-oss/quickwit (5 commits, 160 tests passing):

| Feature | Status |
|---|---|
| Foyer partial request cache (memory + disk) | Implemented, benchmarked |
| Foyer fast field cache (FoyerStorageCache) | Implemented, benchmarked |
| Term dict cache (.term routing) | Implemented, benchmarked |
| Posting list cache (.idx routing) | Implemented, benchmarked |
| Prometheus metrics for all foyer caches | Implemented |
| Config: `partial_request_disk_cache_enabled` | Implemented |
| Config: `term_dict_cache_capacity` | Implemented |
| Config: `posting_list_cache_capacity` | Implemented |
| Admission control (age-based) | Not started |
| Predicate cache investigation (0% hit rate) | Not started |
