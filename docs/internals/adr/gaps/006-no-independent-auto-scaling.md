# GAP-006: No Independent Auto-Scaling of Query, Ingest, and Compaction

**Status**: Open
**Discovered**: 2026-02-19
**Context**: Cloud-native storage characteristics analysis (independent scaling, burst handling)

## Problem

Quickwit does not independently auto-scale its query, ingest, and compaction workloads. All three share the same node pool and resource allocation. When ingest load spikes, query performance degrades; when query load spikes, compaction falls behind, accumulating small files that further degrade queries.

Cloud-native observability systems require independent scaling because these workloads have fundamentally different resource profiles and burstiness patterns:

- **Ingest** is I/O-bound and can be bursty (traffic spikes, batch uploads, replay).
- **Query** is CPU-bound and driven by user activity and monitor evaluation.
- **Compaction** is I/O-and-CPU-bound and should run as background work that doesn't interfere with ingest or query, but can scale up when there is a backlog of small files.

Without independent scaling, there is no burst handling mechanism (characteristic C17). When ingest traffic exceeds the capacity of the shared node pool, there is no overflow buffer or burst lane to absorb the excess.

## Evidence

Quickwit nodes serve all roles simultaneously. The `quickwit run` command starts a node that handles ingest, indexing, searching, and compaction. While Kubernetes deployments can separate indexer and searcher roles, the compaction (merge) workload always runs co-located with the indexer and cannot be independently scaled.

## State of the Art

- **Husky**: Writers, compactors, and leaf readers are independently auto-scaled services. Compactor fleet scales based on file backlog; writer fleet scales based on ingest QPS; leaf readers scale based on query QPS.
- **Mimir/Cortex**: Ingesters, compactors, and queriers are separate Kubernetes deployments with independent HPA policies.

## Potential Solutions

- **Option A**: Separate Quickwit into distinct indexer, compactor, and searcher Kubernetes deployments with independent auto-scaling policies. This requires the compaction (merge) pipeline to be runnable as a standalone service.
- **Option B**: Implement resource isolation within a shared node (CPU/memory limits per workload type). Less flexible but simpler to deploy.

## Signal Impact

All signals equally affected. Independent scaling is signal-agnostic.

## Impact

- **Severity**: High
- **Frequency**: Constant (under production load)
- **Affected Areas**: Deployment architecture, Kubernetes configuration, merge pipeline decoupling

## Next Steps

- [ ] Evaluate separating the merge pipeline into a standalone compactor service
- [ ] Design auto-scaling policies for each workload type (ingest QPS, query QPS, file backlog)
- [ ] Investigate burst handling for ingest (overflow buffer, backpressure, burst lane)

