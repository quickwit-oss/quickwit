# Leaf search query scheduling 

Leaf search request target a list of splits which result in as many single split search task to schedule.

The following is meant to explain query scheduling.

Each single split search task can be cut in two phases:
- warmup: this is a very strange named, but this is where we download all of the data required for search.
We populate that in the abstraction tantivy offers for IO: the Directory.
This phase is very IO bound: it takes little CPU and a lot of waiting. We execute in tokio with a large concurrency.
- search: this phase is 100% cpu. We execute it on a CPU pool with very little currency.

We do not know in advance how much memory will be downloaded by a request.

Our system is aiming at the following:
- keep the memory usage bounded (avoid OOM)
- minimize the average search latency
- no deadlock (of course)

It comes with the following side effect:
- a scheduled split search request fails if a single split needs more warmup memory than
  `warmup_single_split_max_allocation`. Contention on the global `warmup_memory_budget` waits for
  memory to be freed rather than failing.


## Scheduling order

An actor called the SearchPermitProvider is in charge of scheduling these tasks.
THis is done by delivering SearchPermit.

The permits of the tasks of a search request have to be requested together as a batch.
The SearchPermitProvider then returns a batch of permit futures.
All task will then be scheduled when their permit is granted.

Request split search permit  as a batch is important because it makes it possible for the SearchPermitProvider
to schedule them while being aware of the search request they belong to.
We do not care about minimizing the latency of individual split search: this is not visible to users.
We want to minimize the latency of entire search requests.

Considering two requests A and B with 3 splits each, interleaving the execution of the two would be catastrophical:
| A1 | B1 | A2 | B2 | A3 (A returns)| B3 (B returns) |
vs 
| A1 | A2 | A3 (A returns) | B1 | B2 | B3 (B returns) |

The SearchPermitProvider will therefore prioritze tasks using [Shortest remaining time strategy](https://en.wikipedia.org/wiki/Shortest_remaining_time).

## Gating and limits

We want to run single split search tasks  concurrently but with limits of two kinds:
- count limit: we run things concurrently to maximize our throughput with S3, but this comes with a cost and increase the time it takes for first tasks to reach the CPU pool. There are diminishing returns to running things concurrently, so we have some hard limit to the number of running tasks (in the hundreds).
- memory: we are typically more constrained by memory.

Memory is the most important limit.

We do not know in advance how much memory will be required by a task.
We therefore we only start a single split search task when we can reserve an arbitrary pessimistic  number of bytes. 
This amount is configurable via the warmup_single_split_initial_allocation parameter in the `SearcherConfig`.

We then track, before any async download, whether we are about to exceed our currently reserved
amount of memory. If we do, we grow our reservation in the global warmup memory using an
exponential strategy (`1.7x`) to amortize allocations.

If there is not enough memory globally when we try to grow our reservation, the task waits until
memory is freed — except for the oldest active split search task (identified by a monotonic ID
assigned when its `WarmupMemoryGuard` is created), which always allocates immediately without
waiting. This ensures at least one split search task always progresses, finishes its search, and
releases its memory, preventing deadlocks.

To keep total warmup memory strictly within `warmup_memory_budget` even when the oldest split
search allocates without waiting, `GlobalWarmupMemory` is given a shared budget of
`warmup_memory_budget - warmup_single_split_max_allocation`, keeping
`warmup_single_split_max_allocation` in reserve for the oldest split search.

Once warmup has ended, we release the memory that is unused in our reservation.

`warmup_single_split_max_allocation` (strictly lower than `warmup_memory_budget`; by default,
`max(warmup_single_split_initial_allocation, warmup_memory_budget / 4)`) fails a split search
immediately if its reservation would exceed that per-split cap. That error is marked as not
retryable on another searcher.
