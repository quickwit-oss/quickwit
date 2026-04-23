#!/usr/bin/env python3
"""Upload synthetic Datadog test data to a Pomsky intake pipeline.

Usage:
    python upload-test-data.py <base_url> [--metrics] [--logs] [--traces] [--api-key KEY]

    # Upload all data types:
    python upload-test-data.py http://localhost:8080

    # Upload only metrics:
    python upload-test-data.py http://localhost:8080 --metrics

    # Upload logs and traces with a custom API key:
    python upload-test-data.py http://localhost:8080 --logs --traces --api-key abc123

If no data type flags are given, all types are uploaded.

Metrics are sent as v2 protobuf.
"""

import argparse
import os
import sys
import urllib.error
import urllib.request

DATA_DIR = os.path.join(os.path.dirname(__file__), "sandbox")

ENDPOINTS = {
    "metrics": {
        "file": "metrics.pb",
        "path": "/api/v2/series",
        "content_type": "application/x-protobuf",
    },
    "sketches": {
        "file": "sketches.pb",
        "path": "/api/beta/sketches",
        "content_type": "application/x-protobuf",
    },
    "logs": {
        "file": "logs.json",
        "path": "/api/v2/logs",
        "content_type": "application/json",
    },
    "traces": {
        "file": "traces.pb",
        "path": "/api/v0.2/traces",
        "content_type": "application/x-protobuf",
    },
}


def upload(base_url, data_type, api_key):
    ep = ENDPOINTS[data_type]
    file_path = os.path.join(DATA_DIR, ep["file"])

    if not os.path.exists(file_path):
        print(f"  SKIP {data_type}: {file_path} not found (run the generator first)")
        return False

    with open(file_path, "rb") as f:
        body = f.read()

    url = base_url.rstrip("/") + ep["path"]
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", ep["content_type"])
    req.add_header("dd-api-key", api_key)

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            print(f"  OK   {data_type}: POST {ep['path']} -> {resp.status} ({len(body):,} bytes)")
            return True
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")[:200]
        print(f"  FAIL {data_type}: POST {ep['path']} -> {e.code} {e.reason}: {body_text}")
        return False
    except urllib.error.URLError as e:
        print(f"  FAIL {data_type}: POST {ep['path']} -> {e.reason}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Upload synthetic Datadog test data to a Pomksy intake pipeline.")
    parser.add_argument("base_url", help="Base URL of the Pomsky intake (e.g. http://localhost:8080)")
    parser.add_argument("--metrics", action="store_true", help="Upload metrics (series + sketches)")
    parser.add_argument("--logs", action="store_true", help="Upload logs")
    parser.add_argument("--traces", action="store_true", help="Upload traces")
    parser.add_argument("--api-key", default="test_api_key", help="Datadog API key (default: dummy 32-char key)")
    args = parser.parse_args()

    # If no flags specified, upload all
    types = []
    if args.metrics:
        types.extend(["metrics", "sketches"])
    if args.logs:
        types.append("logs")
    if args.traces:
        types.append("traces")
    if not types:
        types = ["metrics", "sketches", "logs", "traces"]

    print(f"Uploading to {args.base_url}")
    results = {}
    for t in types:
        results[t] = upload(args.base_url, t, args.api_key)

    successes = sum(1 for v in results.values() if v)
    failures = sum(1 for v in results.values() if not v)
    print(f"\nDone: {successes} succeeded, {failures} failed")
    sys.exit(0 if failures == 0 else 1)


if __name__ == "__main__":
    main()
