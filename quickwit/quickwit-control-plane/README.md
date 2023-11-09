# Quickwit Control Plane

The Control Plane is responsible for scheduling indexing tasks to indexers. Its role is to ensure that the cluster is correctly running all indexing tasks on each indexer.

An indexing task is simply identified by a couple `(IndexId, SourceId, Option<Vec<ShardId>>)`.

## Scheduling algorithm

The control plane keeps an up to date partial view of the metastore.
This is enforced by routing all of the index/shards/sources alternating
command be routed through the control plane.

On startup, or when a metastore event is received, the scheduler computes the list of indexing tasks.
It then applies a placement algorithm to decide which indexer should be running each indexing task. The result of this placement is called the physical indexing plan, and associated each indexer to a list of indexing tasks.

The control plane then emits gRPC to the indexers that are not already following their assigned part of the indexing plan.

```mermaid
flowchart TB
    StartScheduling(Start scheduling)--"(Sources, Nodes)"-->BuildPhysical
    style StartScheduling fill:#ff0026,fill-opacity:0.5,stroke:#ff0026,stroke-width:4px
    BuildPhysical[Build Physical Plan]--PhysicalPlan-->Apply
    Apply[Apply plan to each indexer] --IndexerPlan--> Indexer1
    Apply --IndexerPlan--> Indexer2
    Apply --IndexerPlan--> Indexer...
```

## Control loop

Each indexer reports its currently running plan via chitchat.
A control loop makes sure that this cluster state matches the latest applied plan.
If a divergence is observed (for instance, if a node leaves the cluster), or if a node reports not being running a given pipeline, the control plane will take the necessary actions (respectively recompute the physical plan or reapply the plan).

## Read more in the Rust docs

[Scheduler Rust docs](./src/scheduler.rs#L66)
