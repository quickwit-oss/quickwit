# Scheduling logic

Quickwit needs to assign indexing tasks to a set of indexers nodes.
We call the result of this decision the indexing physical plan.

This needs to be done under the constraints of:
- not exceeding the maximum load of each node. (O)

We also want to observe some interesting properties such as:
- (A) we want to avoid moving indexing tasks from one indexer to another one needlessly.
- (B) we want a source to be spread amongst as few nodes as possible
- (C) we prefer to respect some margin on the capacity of all nodes.
- (D) when we are working with the Ingest API source, we prefer to colocate indexers on
  the ingesters holding the data.

# Problem abstraction

To simplify the logic and make it easier to test it, we first abstract this in the following
optimization problem. In Quickwit, we have two types of source:

- The push api source: they have a given (changing) set of shards associated to them.
  A shard is rate-limited to ensure their throughput is lower than `5MB/s` worth of
  uncompressed data. This guarantees that a given shard can be indexed by a
  single indexing pipeline.

- Other sources, like Kafka. It is a very common use case to use quickwit to index large
  amounts of historical data. Right now, the user is therefore expected to supply a desired
  number of pipeline.

Routers send their batch to the different ingesters they know using a round-robin logic.
We assume that routers's list of known shards gets eventually updated after a shard addition, so that
we can assume that shard have roughly the same load.

Indexers inform of observed load of all of their pipelines. 
This load is assumed unidimensional. This is imperfect of course: indexing consumes network, io, etc.
Still for the sake of simplification we pick one metric, measured as the amount of CPU spent
in the indexer. 

The control plane consolidates this figure to create a load_per_shard metric expressed in millicpu.

The hypothesis above allow us to see both kafka and ingest sources through the same lens, and stop 
making a distinction between shards.

In our scheduler, a source simply has:
- an identifier (a `u32`)
- a number of shard.
- a load per shard identified by a u32, expression thousandth of CPU.

And indexer has:
- a maximum total load (that we will need to measure or configure).

The problem is now greatly simplified.
A solution is a sparse matrix of `(num_indexers, num_sources)` that holds a number of shards to be run.
The different constraint and wanted properties can all be re-expressed. For instance:
- We want the dot product of the load per shard vector with each row, to be lower than the maximum load
  of each node. (O)
- We do not want a large distance between the two solution matrixes (A)
- We want that matrix as sparse as possible (B).

The actual cost function we would craft is however not linear. For instance, the benefit of keeping
some free capacity for a given node is clearly not a linear function. In fact, keeping some imbalance
could be a good thing.

# The heuristic

We use the following heuristic.

## Phase 1: Remove extraneous shards

Starting from the existing solution, we first reduce it to make sure we do not have too many shards assigned.
This is done by reducing the number of shard wherever needed, picking in priority nodes with few shards.

We call the resulting solution "reduced solution". The reduced solution is usually not a valid solution as some shard
may have been added. We will add these in Phase 3.

If we compute the distance to the previous solution, we want to use the "reduced solution" and not the actual
previous solution.

## Phase 2: Enforce nodes maximum load

We then remove entire sources, in order to match the constraint (O).
For every given node, we remove in priority sources that have an overall small load on the node.

Matrix-wise, note that phase 1 and phase 2 creates a matrix lower or equal to the previous solution.

## Phase 3: Greedy assignment

At this point we have reach a solution that fits on the cluster, but we possibly have several missing shards.
We therefore use a greedy algorithm to allocate these shard. We assign the shards source by source, in the order of decreasing total load.
We assign the source to the node with largest remaining load capacity.

If this phase fails, it is ok to log an error, and stop assigning sources.

## Phase 4: Optimization

This is not implemented yet. We could craft a proper optimization cost and use a BFS search to explore
better solutions.


# Code organization

All of this scheduling is done in the scheduling directory.
Clients only have to call the `build_physical_indexing_plan` function.

The code converts the list of sources into a "scheduling problem" that abstracts away kafka pipelines and ingest v2 pipelines.
The problem then goes through our optimization code.
The solution at this point only contains the number of shards of each type to be assigned to each indexers.
The function expands this solution into a complete physical plan, with shard ids and pipelines.
