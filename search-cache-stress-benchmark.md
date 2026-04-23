# Search Stress Benchmark

## Goal

Determine how many users, dashboards and monitors a single BYOC cluster can support before search performance degrades, and measure how caching strategies affect that limit.

## Why

Customers like Dailymotion report dashboard timeouts and slow Log Explorer queries. As adoption grows, clusters need to handle 100+ monitors evaluating every minute, 100+ dashboards reloading every 5 minutes, and concurrent users — all competing for the same searcher resources.

We don't have a way to answer accurately to: 
- **"how many many vCPU (search only) the cluster needs to handle a workload?"**
- **"What is the compute and storage costs (GET requests included) of the cluster?"** 


The goal of this benchmark is provide those answers.

## What it simulates

The benchmark replays realistic concurrent workloads against a CloudPrem cluster backed by S3:

| Role | Count | Behavior |
|---|---|---|
| **Monitors** | 100 | 12 unique query templates (term filters, text search, range queries, aggregations). Each fires every 60s on a rolling 15-min window. |
| **Dashboards** | 100 | 8-15 widgets per dashboard from 15 templates (timeseries, nested group-bys with top 1000, cardinality, percentiles, avg). Varied time ranges (1h to 24h). Reload every 5 min. |
| **Log Explorer users** | 10 | 4-query sessions (histogram, log stream, facet sidebar, count) with random text search terms. Think time between sessions. |
| **Cache busters** | 2 | Full-range scans that evict hot cache entries, simulating ad-hoc queries on old data. |

This produces ~300 queries/min peak, ~150 queries/min average.

## What it measures

| Category | Metrics |
|---|---|
| **Latency** | p50, p95, p99 per role (monitor, dashboard, explorer) |
| **Reliability** | Failure count, timeout count (>30s) |
| **S3 cost** | GET requests/min, download MB/min |
| **Cache efficiency** | Hit rate per cache (partial request, fast fields, term dict, posting list) |
| **Throughput** | Queries/sec sustained |

## How to run

### Prerequisites

1. A CloudPrem instance with the generated-logs index (115M to 1B docs, 12 to 120 splits on S3)
2. Python 3 with `aiohttp` installed
3. Benchmark scripts

### Quick start

```bash
cd benchmarks/tracks/generated-logs-cache-stress

# Run with defaults (100 monitors, 100 dashboards, 10 users, 10 min)
python3 run_cache_stress.py --endpoint http://localhost:7280

# Run with cache busters to simulate ad-hoc historical queries
python3 run_cache_stress.py --endpoint http://localhost:7280 --cache-busters 2

# Shorter run for quick iteration
python3 run_cache_stress.py --endpoint http://localhost:7280 --duration 300 --monitors 50 --dashboards 50
```

### Comparing configurations

Run the same workload with different CloudPrem configs (cache sizes/policies) and compare the output `report.json` files.

