# Phase 4: Stream Integration - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md -- this log preserves the alternatives considered.

**Date:** 2026-04-20
**Phase:** 04-stream-integration
**Areas discussed:** select! loop architecture, Graceful shutdown behavior, Integration test design

---

## select! Loop Architecture

### Batch Size Trigger

| Option | Description | Selected |
|--------|-------------|----------|
| Inline after each event (Recommended) | After processing each event, check pending.len() >= batch_size. If true, flush immediately before yielding the event. Simple, deterministic. | ✓ |
| Separate select! branch with notify | Use tokio::Notify or channel to signal when batch_size is reached. Flush happens in its own select! branch. More complex. | |
| Check on next timer tick only | Don't check batch_size per-event. Let flush timer handle it. Simpler but burst could pile past batch_size. | |

**User's choice:** Inline after each event (Recommended)
**Notes:** Deterministic flush at exact threshold preferred.

### select! Macro Choice

| Option | Description | Selected |
|--------|-------------|----------|
| tokio::select! biased (Recommended) | Prioritize input events over timers. Standard pattern in Vector codebase. Well-understood cancellation model. | ✓ |
| futures::select! with Fuse | More explicit about which futures are done. Slightly more boilerplate. | |
| You decide | Claude picks most idiomatic approach. | |

**User's choice:** tokio::select! biased (Recommended)
**Notes:** None

### Stream Yield Style

| Option | Description | Selected |
|--------|-------------|----------|
| async_stream::stream! (Recommended) | Wrap entire select! loop. Clean, readable, handles Pin<Box<dyn Stream>> naturally. | ✓ |
| Manual poll-based Stream | Implement Stream trait manually. More control but significant boilerplate. | |
| You decide | Claude picks based on Vector patterns. | |

**User's choice:** async_stream::stream! (Recommended)
**Notes:** None

---

## Graceful Shutdown Behavior

### Shutdown Flush Failure

| Option | Description | Selected |
|--------|-------------|----------|
| Log and proceed (Recommended) | Log at warn!, persist CSV anyway, exit. Matches D-08 drop-on-failure. Metrics re-detected after restart. | ✓ |
| Single retry with short timeout | Retry once with tighter timeout. Log and proceed on second failure. | |
| Block until success or global timeout | Keep trying until Vector's shutdown timeout kills the task. | |

**User's choice:** Log and proceed (Recommended)
**Notes:** Consistent with existing drop-on-failure design.

### Shutdown CSV Persist

| Option | Description | Selected |
|--------|-------------|----------|
| Always persist on shutdown (Recommended) | Write CSV unconditionally. Ensures final flush's succeeded metrics are persisted. Negligible cost. | ✓ |
| Only if dirty | Track dirty flag. Only write if map changed since last persist. More bookkeeping. | |

**User's choice:** Always persist on shutdown (Recommended)
**Notes:** None

---

## Integration Test Design

### Test Scope

| Option | Description | Selected |
|--------|-------------|----------|
| TaskTransform + wiremock (Recommended) | Construct via TransformConfig::build(), call transform() with event stream, wiremock for HTTP. Full lifecycle through real trait. | ✓ |
| Full Vector pipeline test | Start minimal Vector topology. Maximum realism but heavyweight. | |
| You decide | Claude picks best approach for success criteria. | |

**User's choice:** TaskTransform + wiremock (Recommended)
**Notes:** None

### Assertions

| Option | Description | Selected |
|--------|-------------|----------|
| Flush request + known-set update (Recommended) | Assert wiremock received flush POST with correct body/headers. Verify CSV contains succeeded metrics with valid TTLs. | ✓ |
| Minimal: pass-through + CSV only | Just verify events come out and CSV exists. Doesn't verify HTTP integration. | |
| Comprehensive multi-scenario | Multiple integration tests: happy path, failure, shutdown, empty stream. More coverage but more test code. | |

**User's choice:** Flush request + known-set update (Recommended)
**Notes:** Covers HTTP-01, HTTP-02, HTTP-03, XFRM-01 in one test.

---

## Claude's Discretion

- Timer reset strategy details
- Persist-tick operation ordering
- Input stream drain behavior after None
- Dead code cleanup (#[allow(dead_code)] removal)
- async_stream crate version
- Integration test helper design
- Batch-size flush vs event yield ordering

## Deferred Ideas

None -- discussion stayed within phase scope
