---
phase: 22-rate-limiting-backpressure
plan: 01
subsystem: ingest
tags: [rate-limiting, backpressure, token-bucket, metrics-pipeline]

# Dependency graph
requires:
  - phase: 21-retry-error-handling
    provides: MetricsIngestError, recovery module
  - phase: 18-metrics-wal-integration
    provides: MetricsWal, MetricsWalConfig, NotEnoughCapacityError
provides:
  - MetricsShardRateLimiter for per-shard rate limiting
  - BackpressureError enum for clean failure signaling
  - check_can_append() for combined capacity + rate limit checks
affects: [metrics-ingester, durability-completion]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - Token bucket rate limiting per shard
    - Lazy rate limiter creation
    - Combined capacity and rate limit checks

key-files:
  created:
    - quickwit/quickwit-ingest/src/metrics/rate_limiter.rs
  modified:
    - quickwit/quickwit-ingest/src/metrics/state.rs
    - quickwit/quickwit-ingest/src/metrics/mod.rs
    - quickwit/quickwit-ingest/src/metrics/recovery.rs

key-decisions:
  - "Per-shard rate limiting with lazy initialization"
  - "check_can_append() combines capacity and rate checks"
  - "BackpressureError distinguishes rate limit vs capacity errors"

patterns-established:
  - "Token bucket pattern: 10MB burst, 5MB/s rate per shard"
  - "Backpressure via explicit error types, not silent dropping"

# Metrics
duration: 5 min
completed: 2026-01-20
---

# Phase 22 Plan 01: Rate Limiting Module Summary

**MetricsShardRateLimiter with per-shard token bucket rate limiting, BackpressureError enum, and check_can_append() for combined capacity and rate limit checks — completing v0.3 Durability milestone.**

## Performance

- **Duration:** 5 min
- **Started:** 2026-01-20T00:06:12Z
- **Completed:** 2026-01-20T00:10:56Z
- **Tasks:** 3/3
- **Files modified:** 4

## Accomplishments

- Created MetricsShardRateLimiter with per-shard token bucket rate limiting
- Integrated rate limiter into MetricsIngesterState with check_can_append() method
- Added 4 integration tests covering all backpressure scenarios from 22-CONTEXT.md
- BackpressureError provides clear distinction between rate limit and capacity failures

## Task Commits

Each task was committed atomically:

1. **Task 1: Create MetricsRateLimiter module** - `956e0731` (feat)
2. **Task 2: Integrate rate limiter into MetricsIngesterState** - `2c2fbd00` (feat)
3. **Task 3: Add integration tests for backpressure scenarios** - `1bbc63fc` (test)

## Files Created/Modified

- `quickwit/quickwit-ingest/src/metrics/rate_limiter.rs` - New module with MetricsShardRateLimiter, MetricsRateLimiterConfig, BackpressureError
- `quickwit/quickwit-ingest/src/metrics/state.rs` - Added rate_limiter field to MetricsIngesterInner, check_can_append() method
- `quickwit/quickwit-ingest/src/metrics/mod.rs` - Added rate_limiter module and re-exports
- `quickwit/quickwit-ingest/src/metrics/recovery.rs` - Updated tests for new constructor signature

## Decisions Made

- **Per-shard rate limiting**: Each shard gets independent rate limiter to prevent hot shards from starving others
- **Lazy initialization**: Rate limiters created on first access to avoid pre-allocating for unused shards
- **Combined checks**: check_can_append() checks both WAL capacity and rate limit in one call
- **Clear error types**: BackpressureError distinguishes rate limit exceeded from capacity exceeded

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- v0.3 Durability milestone is now complete (all 6 phases done)
- Ready for milestone completion and archival
- All three essential backpressure guarantees validated:
  - OOM protection (capacity checks)
  - Fair shard distribution (per-shard rate limiting)
  - Graceful degradation (clean error types)

---
*Phase: 22-rate-limiting-backpressure*
*Completed: 2026-01-20*
