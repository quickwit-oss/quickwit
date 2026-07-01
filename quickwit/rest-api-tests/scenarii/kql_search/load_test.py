#!/usr/bin/env python3
# Copyright 2021-Present Datadog, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""KQL concurrent-load harness.

Drives a running Quickwit instance with a mix of KQL queries — simple
field-value, boolean composition, ranges, exists checks, large-but-legal
inputs, and adversarial inputs that exercise the depth + size guards — for
a fixed wall-clock duration with multiple worker threads.

Reports throughput, latency percentiles per query shape, and parser-failure
rate. Reads the `quickwit_kql_*` Prometheus counters before/after to
cross-check internal accounting against client-side counts.

Usage::

    python3 load_test.py --base-url http://127.0.0.1:8280 \\
        --index kql_load --duration 30 --workers 16
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Iterable

import requests

# Each entry is (shape_name, kql_string). The shapes deliberately span the
# parser surface so a regression in any one of them shows as a degraded
# percentile or a spike in errors for that shape alone.
QUERY_SHAPES: list[tuple[str, str]] = [
    ("simple_field", "level:error"),
    ("phrase", '"job started"'),
    ("bare_default", "error"),
    ("and", "level:error and service:api"),
    ("or", "level:error or level:warn"),
    ("not", "not level:error"),
    ("group", "level:(error or warn)"),
    ("range_gte", "status:>=500"),
    ("range_compound", "status:>=200 and status:<500"),
    ("exists", "level:*"),
    ("match_all_star", "*"),
    ("wildcard_value", "service:work*"),
    # Below: deliberately near the parser guards — depth and length —
    # without crossing them.
    (
        "near_depth_limit",
        # 32 nested parens → well below the 64 cap; should still parse.
        "(" * 32 + "level:error" + ")" * 32,
    ),
    (
        "large_legal_input",
        # ~256 ORed clauses → sizeable but legal.
        " or ".join(f"service:s{n}" for n in range(256)),
    ),
]

# Adversarial inputs — each MUST be rejected with HTTP 400. Verified
# alongside the happy-path traffic so the safety rails don't quietly
# regress under load.
ADVERSARIAL_SHAPES: list[tuple[str, str]] = [
    ("over_depth", "(" * 128 + "x" + ")" * 128),
    ("oversize_input", "a" * (16 * 1024 + 1)),
    ("dangling_colon", "level:"),
    ("unbalanced_paren", "(level:error"),
    ("nested_field_qualifier", "level:(severity:high)"),
]


@dataclass
class ShapeStats:
    """Latency and error counters for one query shape."""

    requests: int = 0
    errors: int = 0
    status_unexpected: int = 0
    latencies_ms: list[float] = field(default_factory=list)

    def record(self, latency_ms: float, ok: bool, expected_status: int, actual_status: int) -> None:
        self.requests += 1
        if not ok:
            self.errors += 1
        if actual_status != expected_status:
            self.status_unexpected += 1
        self.latencies_ms.append(latency_ms)


def percentile(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    values_sorted = sorted(values)
    rank = max(0, min(len(values_sorted) - 1, int(round((p / 100.0) * (len(values_sorted) - 1)))))
    return values_sorted[rank]


def setup_index(base_url: str, index_id: str) -> None:
    requests.delete(f"{base_url}/api/v1/indexes/{index_id}")
    create_resp = requests.post(
        f"{base_url}/api/v1/indexes/",
        json={
            "version": "0.7",
            "index_id": index_id,
            "doc_mapping": {
                "index_field_presence": True,
                "field_mappings": [
                    {"name": "level", "type": "text", "tokenizer": "raw", "fast": True},
                    {"name": "status", "type": "u64", "fast": True, "indexed": True},
                    {"name": "service", "type": "text", "tokenizer": "raw", "fast": True},
                    {"name": "message", "type": "text", "tokenizer": "default", "record": "position"},
                ],
            },
            "search_settings": {"default_search_fields": ["message"]},
        },
        timeout=30,
    )
    if create_resp.status_code not in (200, 201):
        raise SystemExit(f"failed to create index {index_id}: {create_resp.status_code} {create_resp.text}")
    # Generate ~10k docs across a handful of services / levels / statuses so
    # the queries return non-trivial hit counts.
    services = ["api", "worker", "ingest", "scheduler", "compactor"]
    levels = ["error", "warn", "info", "debug"]
    statuses = [200, 201, 400, 404, 500, 503]
    rows: list[dict] = []
    for i in range(10_000):
        rows.append(
            {
                "level": levels[i % len(levels)],
                "status": statuses[i % len(statuses)],
                "service": services[i % len(services)],
                "message": f"event {i} completed normally",
            }
        )
    ndjson = "\n".join(json.dumps(r) for r in rows) + "\n"
    ingest_resp = requests.post(
        f"{base_url}/api/v1/{index_id}/ingest?commit=force",
        data=ndjson,
        headers={"Content-Type": "application/json"},
        timeout=120,
    )
    if ingest_resp.status_code != 200:
        raise SystemExit(f"ingest failed: {ingest_resp.status_code} {ingest_resp.text}")


def teardown_index(base_url: str, index_id: str) -> None:
    requests.delete(f"{base_url}/api/v1/indexes/{index_id}")


def fetch_metric(base_url: str, metric_name: str) -> float | None:
    """Return the current value of a `quickwit_kql_*` counter, or None if absent."""
    try:
        resp = requests.get(f"{base_url}/metrics", timeout=5)
        if resp.status_code != 200:
            return None
        for line in resp.text.splitlines():
            if line.startswith("#") or not line.strip():
                continue
            head, _, value = line.rpartition(" ")
            if head.split("{")[0] == metric_name:
                return float(value)
    except requests.RequestException:
        return None
    return None


def worker_loop(
    base_url: str,
    index_id: str,
    shapes: list[tuple[str, str]],
    expected_status: int,
    stop_at: float,
    stats: dict[str, ShapeStats],
    stats_lock: threading.Lock,
) -> None:
    """Spin requests through `shapes` until wall-clock `stop_at` passes."""
    session = requests.Session()
    idx = 0
    while time.monotonic() < stop_at:
        shape_name, kql = shapes[idx % len(shapes)]
        idx += 1
        started = time.monotonic()
        try:
            resp = session.get(
                f"{base_url}/api/v1/{index_id}/search",
                params={"kql": kql, "max_hits": 1},
                timeout=10,
            )
            ok = resp.status_code == expected_status
            actual = resp.status_code
        except requests.RequestException:
            ok = False
            actual = -1
        latency_ms = (time.monotonic() - started) * 1000.0
        with stats_lock:
            stats[shape_name].record(latency_ms, ok, expected_status, actual)


def run_load(args: argparse.Namespace) -> int:
    print(f"setting up index {args.index!r} on {args.base_url}")
    setup_index(args.base_url, args.index)
    try:
        before_total = fetch_metric(args.base_url, "quickwit_kql_parse_total")
        before_failures = fetch_metric(args.base_url, "quickwit_kql_parse_failures_total")

        stats: dict[str, ShapeStats] = {}
        for shape_name, _ in QUERY_SHAPES + ADVERSARIAL_SHAPES:
            stats[shape_name] = ShapeStats()
        stats_lock = threading.Lock()

        stop_at = time.monotonic() + args.duration
        print(
            f"driving {args.workers} workers for {args.duration}s — happy-path + adversarial mix"
        )
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            # Split workers between happy-path and adversarial shapes so we
            # exercise both rails simultaneously. ~10% to adversarial.
            adversarial_workers = max(1, args.workers // 10)
            happy_workers = args.workers - adversarial_workers
            futures = []
            for _ in range(happy_workers):
                futures.append(
                    pool.submit(
                        worker_loop,
                        args.base_url,
                        args.index,
                        QUERY_SHAPES,
                        200,
                        stop_at,
                        stats,
                        stats_lock,
                    )
                )
            for _ in range(adversarial_workers):
                futures.append(
                    pool.submit(
                        worker_loop,
                        args.base_url,
                        args.index,
                        ADVERSARIAL_SHAPES,
                        400,
                        stop_at,
                        stats,
                        stats_lock,
                    )
                )
            for fut in as_completed(futures):
                fut.result()

        after_total = fetch_metric(args.base_url, "quickwit_kql_parse_total")
        after_failures = fetch_metric(args.base_url, "quickwit_kql_parse_failures_total")

        print_report(args, stats, before_total, after_total, before_failures, after_failures)
        return summarize_exit_status(stats)
    finally:
        teardown_index(args.base_url, args.index)


def print_report(
    args: argparse.Namespace,
    stats: dict[str, ShapeStats],
    before_total: float | None,
    after_total: float | None,
    before_failures: float | None,
    after_failures: float | None,
) -> None:
    print()
    print("=" * 92)
    print(f"{'shape':<28} {'req':>8} {'rps':>8} {'err':>6} {'p50ms':>8} {'p95ms':>8} {'p99ms':>8}")
    print("-" * 92)
    total_requests = 0
    total_errors = 0
    total_unexpected = 0
    for shape_name in (s for s, _ in QUERY_SHAPES + ADVERSARIAL_SHAPES):
        s = stats[shape_name]
        total_requests += s.requests
        total_errors += s.errors
        total_unexpected += s.status_unexpected
        rps = s.requests / args.duration if args.duration > 0 else 0.0
        p50 = percentile(s.latencies_ms, 50)
        p95 = percentile(s.latencies_ms, 95)
        p99 = percentile(s.latencies_ms, 99)
        marker = " !" if s.status_unexpected > 0 else ""
        print(
            f"{shape_name:<28} {s.requests:>8} {rps:>8.1f} {s.status_unexpected:>6} "
            f"{p50:>8.2f} {p95:>8.2f} {p99:>8.2f}{marker}"
        )
    print("-" * 92)
    overall_rps = total_requests / args.duration if args.duration > 0 else 0.0
    print(
        f"{'TOTAL':<28} {total_requests:>8} {overall_rps:>8.1f} "
        f"{total_unexpected:>6} (unexpected-status responses)"
    )
    if before_total is not None and after_total is not None:
        parse_delta = after_total - before_total
        print(f"\nserver-side quickwit_kql_parse_total delta:   {parse_delta:.0f}")
    if before_failures is not None and after_failures is not None:
        fail_delta = after_failures - before_failures
        print(f"server-side quickwit_kql_parse_failures_total delta: {fail_delta:.0f}")


def summarize_exit_status(stats: dict[str, ShapeStats]) -> int:
    """Exit 0 only when every shape stayed at its expected status — happy
    paths must succeed, adversarial paths must reject."""
    bad: list[str] = []
    for shape_name, s in stats.items():
        if s.requests == 0:
            continue
        if s.status_unexpected > 0:
            bad.append(f"{shape_name}: {s.status_unexpected}/{s.requests} unexpected")
    if bad:
        print("\nFAIL")
        for line in bad:
            print(f"  - {line}")
        return 1
    print("\nPASS")
    return 0


def main(argv: Iterable[str]) -> int:
    parser = argparse.ArgumentParser(description="KQL load test harness")
    parser.add_argument("--base-url", default="http://127.0.0.1:8280")
    parser.add_argument("--index", default="kql_load")
    parser.add_argument("--duration", type=int, default=30, help="seconds")
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args(list(argv))
    return run_load(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
