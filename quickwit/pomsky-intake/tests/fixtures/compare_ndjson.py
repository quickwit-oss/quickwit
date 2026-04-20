#!/usr/bin/env python3
# Copyright 2021-Present Datadog, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Equivalence comparator for the USM intake dump-equivalence workflow.

Compares two NDJSON streams produced by:

  * dd-source byoc-usm-stats with `EMIT_MODE=jsonall` (Go reference)
  * pomsky-intake `dump_equivalence_write_ndjson` test (Rust under test)

Usage:

    python3 compare_ndjson.py <go.ndjson> <rust.ndjson>

Equivalence relation:

    * Counter values       : exact equality on bit pattern.
    * Sketch keys/counts   : element-by-element exact equality (bins are
                             shipped bit-identical across sides).
    * Sketch count         : exact equality.
    * Sketch sum/min/max/avg : relative tolerance 1e-9
                             (abs(a - b) / max(abs(a), abs(b), 1e-12) < eps).
    * `trace.services_by_operation.duration` counter value: same relative
                             tolerance as sketch scalars — the value is a
                             denormalised copy of sketch.sum and drifts the
                             same way under float-accumulation order.

The tolerance exists because the two runtimes compute scalar sketch
statistics in different orders; both are correct mathematically and differ
only in FP rounding order (see plan Decision Log #6).

Exits 0 on EQUIVALENT, 1 on DIVERGENT.
"""
import json
import sys
from collections import defaultdict

EPS = 1e-9
DURATION_METRIC = "trace.services_by_operation.duration"


def load(path):
    records = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            key = (
                r["metric"],
                r["type"],
                r["timestamp"],
                r["host"],
                tuple(r["tags"]),
            )
            records[key] = r
    return records


def approx_eq(a, b):
    if a == b:
        return True
    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        return False
    denom = max(abs(a), abs(b), 1e-12)
    return abs(a - b) / denom < EPS


def compare_record(go, rust):
    if go["type"] != rust["type"]:
        return f"type-mismatch go={go['type']} rust={rust['type']}"
    if go["type"] == "counter":
        gv, rv = go["value"], rust["value"]
        if gv == rv:
            return None
        if go["metric"] == DURATION_METRIC and approx_eq(gv, rv):
            return None
        return f"counter-value go={gv} rust={rv}"
    gs, rs = go["sketch"], rust["sketch"]
    if gs["keys"] != rs["keys"]:
        return f"sketch-keys go_len={len(gs['keys'])} rust_len={len(rs['keys'])}"
    if gs["counts"] != rs["counts"]:
        return "sketch-counts"
    if gs["count"] != rs["count"]:
        return f"sketch-count go={gs['count']} rust={rs['count']}"
    for scalar in ("sum", "min", "max", "avg"):
        if not approx_eq(gs[scalar], rs[scalar]):
            return f"sketch-{scalar} go={gs[scalar]} rust={rs[scalar]}"
    return None


def main():
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    go_path, rust_path = sys.argv[1], sys.argv[2]
    print(f"loading go   : {go_path}")
    go = load(go_path)
    print(f"loading rust : {rust_path}")
    rust = load(rust_path)
    print(f"go records   : {len(go)}")
    print(f"rust records : {len(rust)}")

    go_keys = set(go)
    rust_keys = set(rust)
    only_go = go_keys - rust_keys
    only_rust = rust_keys - go_keys
    both = go_keys & rust_keys

    divergences = defaultdict(int)
    sample = {}
    for k in both:
        reason = compare_record(go[k], rust[k])
        if reason is not None:
            sig = reason.split(" ")[0]
            divergences[sig] += 1
            if sig not in sample:
                sample[sig] = (k, reason)

    print()
    print("=== summary ===")
    print(f"common keys       : {len(both)}")
    print(f"only in go        : {len(only_go)}")
    print(f"only in rust      : {len(only_rust)}")
    print(f"common divergent  : {sum(divergences.values())} ({len(divergences)} kinds)")
    print(f"common equivalent : {len(both) - sum(divergences.values())}")

    if divergences:
        print()
        print("=== divergence by kind ===")
        for sig, n in sorted(divergences.items(), key=lambda x: -x[1]):
            print(f"  {n:>8}  {sig}")
        print()
        print("=== sample divergence per kind (up to 1 each) ===")
        for sig, (k, reason) in sorted(sample.items(), key=lambda x: x[0]):
            print(f"  {sig}")
            print(f"    key: {k[0]} ts={k[2]} host={k[3]}")
            print(f"    {reason}")

    if only_go:
        print()
        print(f"=== sample of 'only in go' (up to 5 of {len(only_go)}) ===")
        for k in list(only_go)[:5]:
            print(f"  {k}")
    if only_rust:
        print()
        print(f"=== sample of 'only in rust' (up to 5 of {len(only_rust)}) ===")
        for k in list(only_rust)[:5]:
            print(f"  {k}")

    equivalent = not only_go and not only_rust and sum(divergences.values()) == 0
    if equivalent:
        print("\nEQUIVALENT")
        sys.exit(0)
    else:
        print("\nDIVERGENT")
        sys.exit(1)


if __name__ == "__main__":
    main()
