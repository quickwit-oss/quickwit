# KQL test harnesses

Three concentric layers of verification for the `?kql=` REST parameter and
the `{"kql": {...}}` Elastic-DSL extension. Each layer exercises a different
slice of the production surface, from pure correctness to distributed
behavior under load.

## Layer 1 — REST scenarios (`0001_*.yaml` ... `0005_*.yaml`)

YAML-driven correctness tests. Already wired into the existing rest-api-tests
runner; no additional setup beyond starting a Quickwit binary.

```bash
# From this directory:
python3 ../../run_tests.py --engine quickwit \
  --binary <path-to-quickwit-binary> \
  --test scenarii/kql_search
```

Asserts exact `num_hits` for every documented KQL grammar feature against a
deterministic 6-document dataset.

> **Note**: if a stale Quickwit container or unrelated process is holding
> port 7280 on the host (Docker port-forwarding binds `0.0.0.0:7280`,
> intercepting `localhost` traffic), spawn Quickwit on a different port and
> point the scenarios at it by editing `_ctx.yaml`'s `api_root`. The
> default already points at `http://127.0.0.1:8280/api/v1/`.

## Layer 2 — Concurrent load (`load_test.py`)

Drives many workers at the running Quickwit for a fixed wall-clock window,
mixing **happy-path** queries (must return 200) with **adversarial inputs**
(must return 400). Reports per-shape throughput and latency percentiles, plus
verifies the server-side `quickwit_kql_*` counters move in lockstep with the
client-side counts.

```bash
# Quickwit must already be running.
python3 load_test.py \
  --base-url http://127.0.0.1:8280 \
  --duration 30 \
  --workers 16
```

Exit code 0 means every shape held to its expected status (200 / 400) for
every request. The harness fails loudly the first time the depth-limit guard
or the size cap stops rejecting an adversarial input — those are the rails
that prevent a DoS, and silent regression is unacceptable.

Reference numbers (debug build, MacBook, single node, 16 workers, 30s):
- ~1500 req/s sustained
- p99 < 30 ms for every shape
- 0 unexpected-status responses across ~47k requests

A release build with a real load generator (`wrk`, `oha`) lifts these
substantially; these are floor numbers, not ceiling.

## Layer 3 — Distributed multi-node cluster (`docker-compose.cluster.yml`)

Brings up two Quickwit nodes against a PostgreSQL metastore and LocalStack S3
— the production-grade backends — so the root → leaf search path,
distributed metastore, and S3 split storage all participate in the test.
Exercises the same scenarios from Layer 1 but through a real cluster.

```bash
# From repo root: build a Quickwit Docker image.
docker build -t quickwit/quickwit:kql-test .

# From this directory: bring the cluster up.
docker compose -f docker-compose.cluster.yml up -d --wait

# Run the scenarios against the root node (host port 7290).
# Edit _ctx.yaml's api_root to point at http://127.0.0.1:7290 first, or
# pass --api-root if you've added that support to run_tests.py.
python3 ../../run_tests.py --engine quickwit --test scenarii/kql_search

# Tear down.
docker compose -f docker-compose.cluster.yml down -v
```

Use this layer before a release. It catches regressions that single-node
tests miss — proto serialization quirks, metastore-fanout races, split
placement edge cases under KQL queries.

## What still isn't covered

- **Real Kibana frontend.** The KQL grammar matches Kibana's public docs
  (pinned by `kibana_conformance.rs`'s corpus), but no real Kibana instance
  has been pointed at this server. A standing Kibana → Quickwit smoke test
  is the next layer.
- **Production-scale data volume.** The load test indexes ~10k docs. Real
  workloads operate on millions to billions; latency distribution at that
  scale needs separate measurement.
- **Authenticated multi-tenant.** None of these layers test the auth surface
  or per-tenant isolation — that's an orthogonal concern that belongs in a
  dedicated harness.
