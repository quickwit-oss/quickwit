---
phase: 28-rest-grpc-endpoints
verified: 2026-01-23T17:15:00Z
status: passed
score: 5/5 must-haves verified
---

# Phase 28: MetricsSqlRootService & Endpoints Verification Report

**Phase Goal:** Root coordinator with metastore access, REST/gRPC endpoints
**Verified:** 2026-01-23T17:15:00Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | External client can call gRPC PlanMetricsSplits endpoint and receive split assignments | ✓ VERIFIED | GrpcSearchAdapter::plan_metrics_splits implemented (grpc_adapter.rs:170-178), calls service, returns PlanMetricsSplitsResponse |
| 2 | External client can call REST POST /api/v1/metrics/plan-splits and receive JSON split assignments | ✓ VERIFIED | REST handler registered (rest.rs:363-365), plan_splits_post implemented (rest_handler.rs:98-147), returns JSON |
| 3 | Root service accesses metastore to retrieve splits based on search criteria | ✓ VERIFIED | SearchServiceImpl::plan_metrics_splits calls plan_splits_for_root_search with metastore (service.rs:340-344) |
| 4 | Returned splits include node assignments for orchestration | ✓ VERIFIED | SplitAssignment populated with node_ids from cluster_client (service.rs:347, 373) |
| 5 | Split planning reuses existing SearchRequest pruning logic | ✓ VERIFIED | Uses plan_splits_for_root_search for time range/index pattern pruning (service.rs:340) |

**Score:** 5/5 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `quickwit-proto/protos/quickwit/search.proto` | PlanMetricsSplits messages | ✓ VERIFIED | Messages at lines 594-623: PlanMetricsSplitsRequest, SplitAssignment, PlanMetricsSplitsResponse |
| `quickwit-proto/protos/quickwit/search.proto` | SearchService RPC | ✓ VERIFIED | RPC defined at line 77 |
| `quickwit-search/src/service.rs` | SearchService trait method | ✓ VERIFIED | Trait method at line 139-142, implementation at 328-383 (56 lines, substantive) |
| `quickwit-serve/src/search_api/grpc_adapter.rs` | gRPC adapter method | ✓ VERIFIED | Implementation at lines 170-178, follows pattern with set_parent_span + convert_to_grpc_result |
| `quickwit-search/src/client.rs` | Client method | ✓ VERIFIED | SearchServiceClient::plan_metrics_splits at lines 249-263, handles both Grpc and Local |
| `quickwit-serve/src/metrics_sql_api/rest_handler.rs` | REST endpoint | ✓ VERIFIED | 147 lines, plan_splits_post handler at 98-147, OpenAPI docs at 86-97 |
| `quickwit-serve/src/metrics_sql_api/mod.rs` | Module exports | ✓ VERIFIED | 17 lines, exports MetricsSqlApi and plan_splits_handler |

**All artifacts:** EXISTS + SUBSTANTIVE + WIRED

### Key Link Verification

| From | To | Via | Status | Details |
|------|-----|-----|--------|---------|
| GrpcSearchAdapter | SearchServiceImpl | self.0.plan_metrics_splits() | ✓ WIRED | grpc_adapter.rs:176 calls service, error conversion at 177 |
| REST handler | SearchServiceImpl | search_service.plan_metrics_splits() | ✓ WIRED | rest_handler.rs:118-120 calls service, converts response at 123-140 |
| SearchServiceImpl | metastore | plan_splits_for_root_search | ✓ WIRED | service.rs:340 passes &mut self.metastore.clone() |
| SearchServiceImpl | cluster client | search_job_placer.all_node_addrs() | ✓ WIRED | service.rs:347 gets node addresses, assigns at 373 |
| REST server | plan_splits_handler | .or(plan_splits_handler(...)) | ✓ WIRED | rest.rs:363-365 registers route in warp router |
| OpenAPI docs | MetricsSqlApi | merge_components_and_paths | ✓ WIRED | openapi.rs:100 includes MetricsSqlApi in docs |

### Requirements Coverage

No explicit requirements mapped to Phase 28 in REQUIREMENTS.md.

### Anti-Patterns Found

None.

**Stub pattern scan:**
- ❌ No TODO/FIXME comments in implementation files
- ❌ No placeholder text or console.log-only implementations
- ❌ No empty returns or hardcoded values
- ✓ All handlers have real implementations with metastore/cluster access
- ✓ Proper error handling throughout

### Human Verification Required

None. All verification completed programmatically:
- gRPC endpoint structure verified through code inspection
- REST endpoint route registration confirmed
- Wiring to metastore and cluster client verified
- Tests exist (6 unit tests + 1 integration test)

For end-to-end functional testing, see Phase 28 SUMMARYs which document successful test execution.

### Test Coverage

**Unit tests** (quickwit-search/src/service.rs):
1. `test_plan_metrics_splits_basic` - Returns correct number of splits
2. `test_plan_metrics_splits_time_range_pruning` - Time range filtering works
3. `test_plan_metrics_splits_index_pattern_matching` - Index pattern matching works
4. `test_plan_metrics_splits_node_assignments` - Node IDs populated
5. `test_plan_metrics_splits_empty_result` - Handles non-existent index
6. `test_plan_metrics_splits_missing_request` - Error handling for missing request

**Integration test** (quickwit-search/src/tests.rs):
- `test_plan_metrics_splits_client` - Client method with TestSandbox (line 2130)

---

_Verified: 2026-01-23T17:15:00Z_
_Verifier: Claude (gsd-verifier)_
