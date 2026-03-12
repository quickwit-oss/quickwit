---
status: diagnosed
phase: 25-full-pipeline-e2e-tests
source: [25-FIX-SUMMARY.md]
started: 2026-01-21T11:10:00Z
updated: 2026-01-21T11:15:00Z
---

## Current Test

[testing complete - blocker found]

## Tests

### 1. Full Pipeline E2E Test Passes
expected: Run `make docker-metrics-up` then `make test-metrics-e2e`. Both tests (test_full_metrics_pipeline_e2e and test_query_accuracy_e2e) should PASS.
result: issue
reported: "Still timing out: Publisher should have published at least 1 split: Timeout waiting for 1 published splits - FIX did not resolve the issue"
severity: blocker

### 2. Tests Are Idempotent
expected: Run `make test-metrics-e2e` TWICE in a row (without docker-metrics-down). Second run should pass without "index already exists" error.
result: [pending]

### 3. Splits Published to Metastore
expected: After tests pass, query Postgres: `docker exec -it quickwit-postgres psql -U quickwit -d metastore -c "SELECT COUNT(*) FROM splits;"` should show rows (not 0).
result: [pending]

## Summary

total: 3
passed: 0
issues: 1
pending: 2
skipped: 0

## Issues for /gsd:plan-fix

- UAT-005: Uploader spawns background task - process_pending_and_observe returns before work completes (blocker) - Test 1
  root_cause: MetricsUploader.handle() uses spawn_named_task for async staging/upload. A single process_pending_and_observe() call returns immediately after spawning, before the actual work completes. Need to poll uploader counters until staged/uploaded counts increase, THEN wait for publisher.
