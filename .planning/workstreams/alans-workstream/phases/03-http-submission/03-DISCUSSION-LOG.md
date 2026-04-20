# Phase 3: HTTP Submission - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-20
**Phase:** 03-http-submission
**Areas discussed:** Request/response format, Flush trigger wiring, HTTP client setup, Testing approach

---

## Request/Response Format

| Option | Description | Selected |
|--------|-------------|----------|
| I'll provide the format | User has the request/response JSON structure and will share it | |
| Match the Go service | Reverse-engineer from dd-source byoc-metrics-metadata Go code — exact field names, nesting, and types must match | ✓ |
| Claude's discretion | Use a reasonable JSON structure based on requirements | |

**User's choice:** Match the Go service
**Notes:** Go service source at `/Users/alan.gates/go/src/github.com/DataDog/dd-source/domains/quickhouse/apps/byoc-metrics-metadata/`. Researcher agent reads it during plan-phase.

### Follow-up: Source Access

| Option | Description | Selected |
|--------|-------------|----------|
| I'll paste the relevant Go code | User shares the HTTP handler or client code | |
| Research phase reads it | Researcher agent accesses dd-source and extracts wire format | ✓ |
| I have a spec/doc | API documentation or spec file exists | |

**User's choice:** Research phase reads it
**Notes:** dd-source is available locally at `/Users/alan.gates/go/src/github.com/DataDog/dd-source`

---

## Flush Trigger Wiring

| Option | Description | Selected |
|--------|-------------|----------|
| Standalone flush method | Phase 3 builds async flush_pending() on FlushClient. Phase 4 calls from select! loop. Phase 3 tests call directly. | ✓ |
| Full timer wiring now | Phase 3 implements interval + batch-size with tokio timers. Phase 4 just calls into already-wired logic. | |
| Claude's discretion | Let planner decide boundary | |

**User's choice:** Standalone flush method (Recommended)

### Follow-up: Struct Design

| Option | Description | Selected |
|--------|-------------|----------|
| Separate FlushClient struct | Own struct, testable independently. Follows module-per-responsibility pattern. | ✓ |
| Methods on transform | Add flush_pending() directly to MetricMetadataTransform | |

**User's choice:** Separate FlushClient struct (Recommended)

---

## HTTP Client Setup

| Option | Description | Selected |
|--------|-------------|----------|
| Build once in TransformConfig::build() | Create reqwest::Client once with timeout. Store in FlushClient. Connection pooling automatic. | ✓ |
| Create per-request | New reqwest::Client per flush call | |
| Claude's discretion | Let planner decide | |

**User's choice:** Build once in TransformConfig::build() (Recommended)

### Follow-up: Failure Logging

| Option | Description | Selected |
|--------|-------------|----------|
| Warn-level log on failure | Log warn!() with status/error before dropping pending | ✓ |
| Silent drop | No log, no metric | |
| Debug-level log | Log at debug! level only | |

**User's choice:** Warn-level log on failure (Recommended)

---

## Testing Approach

| Option | Description | Selected |
|--------|-------------|----------|
| wiremock | Declarative HTTP mock server, well-established in Rust | ✓ |
| axum test server | Build tiny axum handler, more control but more code | |
| mockito | Similar to wiremock but older API style | |
| Claude's discretion | Let planner pick based on workspace | |

**User's choice:** wiremock (Recommended)

### Follow-up: Key Test Scenarios

| Option | Description | Selected |
|--------|-------------|----------|
| Partial success | succeeded_metrics is a subset — only those get added to known set | ✓ |
| Empty succeeded_metrics | Server returns 200 but empty list | |
| Both of the above | Test both scenarios | |
| Claude's discretion | Let planner design full test matrix | |

**User's choice:** Partial success (Recommended)

---

## Claude's Discretion

- Exact error type design for FlushError
- Whether flush_pending() takes ownership or borrows pending map
- Request serialization details
- Additional edge-case tests beyond partial success

## Deferred Ideas

None — discussion stayed within phase scope
