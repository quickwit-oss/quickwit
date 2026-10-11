# Indexing scheduler

The control plane turns the list of enabled sources into a physical indexing plan: which indexer runs which pipeline. This page describes how a source is weighed in that plan, and how the weight of a non-sharded source can follow its measured load.

## Weighing sources

The scheduler works on an abstract problem: each source contributes a number of units (shards for the ingest API, pipelines for the other sources), each unit has a load in milli-CPU, and each indexer has a CPU capacity. A full pipeline, merges included, is assumed to take 4 000 milli-CPU.

| Source type | Units | Load per unit |
| --- | --- | --- |
| Ingest API (v2) | shards | from the throughput reported by ingesters, when `QW_ENABLE_VARIABLE_SHARD_LOAD=true`; a constant otherwise |
| Kafka, Kinesis, NATS, Pub/Sub, Pulsar, file notifications | `num_pipelines` pipelines | 4 000 milli-CPU, or the measured load when `QW_ENABLE_VARIABLE_PIPELINE_LOAD=true` |

Capacities are first inflated so that everything fits, then sources are placed largest first on the indexer with the most free capacity, starting from the previous plan. An indexer that ends up above its capacity sheds its smallest sources. Because the inflation keeps 20% of headroom, pipelines only move when an indexer is actually overloaded in the plan.

With a constant load per pipeline, pipelines are balanced by count: an idle tenant and a saturated one weigh the same, and the load of an indexer depends on which tenants happen to share it.

## Measured load of non-sharded pipelines

In cooperative indexing mode (`indexer.enable_cooperative_indexing: true`), each pipeline takes one of the indexer's indexing permits when it creates its workbench and releases it when it commits. At each commit, the pipeline measures the share of the commit period it spent holding the permit and reports it as `cpu_load`, between 0 and 4 000 milli-CPU, along with its throughput:

- a commit on an empty mailbox reports the time actually spent indexing;
- a commit on the commit timeout, the heap size or the number of docs reports the whole period, since the pipeline kept its permit until then and queues for a permit again right away;
- a pipeline that has not held a permit for more than a commit period reports itself as idle.

Indexers gossip the last report of each source under the `pipeline_metrics:<index_uid>:<source_id>` key. When `QW_ENABLE_VARIABLE_PIPELINE_LOAD=true` is set on the control plane, the scheduler uses them as follows:

- the load of a source is the average of the reports of the indexers running one of its pipelines;
- it is rounded up to a multiple of 400 milli-CPU (10% of a pipeline) and never below 400, so that an idle pipeline still counts for the actors and connection it keeps alive, and so that small fluctuations do not change the plan;
- a source whose pipelines stopped reporting, typically because they are restarting on another indexer, keeps its last known load;
- a source that never reported, or whose pipelines do not run in cooperative mode, is weighed as a full pipeline.

On each control loop iteration, the scheduler refreshes the loads from the indexer pool. If the rounded load of at least one source changed, it rebuilds the plan starting from the current one, which moves pipelines only if an indexer is now above its capacity.

Pipelines of sources that are not cooperative keep the constant load, so enabling the flag on a cluster without cooperative indexing changes nothing.
