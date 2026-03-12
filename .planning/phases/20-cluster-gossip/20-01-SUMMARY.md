---
phase: 20-cluster-gossip
plan: 01
subsystem: gossip
tags: [chitchat, gossip, actor, cluster, shard-positions, fnv]

# Dependency graph
requires:
  - phase: 19-checkpointing
    provides: MetricsShardPosition, truncation tracking infrastructure
provides:
  - MetricsShardPositionsService actor for cluster-wide position gossip
  - LocalMetricsShardPositionsUpdate event for local position propagation
  - Chitchat integration with `metrics.shard_positions:` prefix
affects: [21-retry-routing, 22-rate-limiting, coordination, truncation]

# Tech tracking
tech-stack:
  added: [fnv]
  patterns: [actor-based-gossip, prefix-filtered-subscription, monotonic-position-tracking]

key-files:
  created:
    - quickwit/quickwit-ingest/src/metrics/shard_positions_service.rs
  modified:
    - quickwit/quickwit-ingest/src/metrics/mod.rs
    - quickwit/quickwit-ingest/Cargo.toml

key-decisions:
  - "Use `metrics.shard_positions:` prefix to isolate metrics from logs"
  - "Mirror ShardPositionsService pattern exactly for consistency"
  - "Combine tests with implementation in single commit (common practice)"

patterns-established:
  - "Metrics gossip uses separate prefix from logs for isolation"
  - "Actor pattern for cluster-wide state synchronization"

# Metrics
duration: 5 min
completed: 2026-01-19
---

# Phase 20 Plan 01: MetricsShardPositionsService Summary

**MetricsShardPositionsService actor enabling cluster-wide metrics shard position gossip via chitchat with separate prefix for isolation**

## Performance

- **Duration:** 5 min
- **Started:** 2026-01-19T08:05:03Z
- **Completed:** 2026-01-19T08:09:37Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments

- Created MetricsShardPositionsService actor mirroring ShardPositionsService pattern
- Implemented chitchat integration with `metrics.shard_positions:` prefix (avoiding collision with logs)
- Added LocalMetricsShardPositionsUpdate event for local position propagation
- Implemented monotonic position tracking (only keeps max position per shard)
- Added 3 comprehensive tests verifying cluster gossip propagation and monotonicity

## Task Commits

Each task was committed atomically:

1. **Task 1 & 2: Create MetricsShardPositionsService + Unit Tests** - `6a79a81a` (feat)
   - Combined implementation and tests as is common practice

**Plan metadata:** (this commit, docs)

## Files Created/Modified

- `quickwit/quickwit-ingest/src/metrics/shard_positions_service.rs` - MetricsShardPositionsService actor with chitchat integration
- `quickwit/quickwit-ingest/src/metrics/mod.rs` - Added module export and re-exports
- `quickwit/quickwit-ingest/Cargo.toml` - Added fnv dependency
- `quickwit/Cargo.lock` - Updated lockfile

## Decisions Made

- **Use `metrics.shard_positions:` prefix:** Prevents collision with logs which use `indexer.shard_positions:`. Critical for namespace isolation.
- **Mirror ShardPositionsService exactly:** Proven pattern for logs; metrics should behave identically with different prefix.
- **Combined implementation with tests:** Tests are inline in the module, committed together as is standard practice.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- MetricsShardPositionsService ready for integration with metrics ingester
- Cluster gossip infrastructure in place for coordinated WAL truncation
- Event-driven pattern established for local observers to react to position changes
- Ready for Phase 20 Plan 02 (if exists) or next phase

---
*Phase: 20-cluster-gossip*
*Completed: 2026-01-19*
