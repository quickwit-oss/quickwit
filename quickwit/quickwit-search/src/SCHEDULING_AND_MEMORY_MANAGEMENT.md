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
- a scheduled search request can error if not enough memory is available in the system.
This is not entirely a function of the query itself and depends on the existing workload.


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

We then track, before any download, whether we are about to exceed our currently reserved amount of memory.
If we do, we increase that amount through atomic using an exponential strategy to amortized this operation.

Once warm up has ended, release the memory that is unused in our allocation.

If there is not enough memory globally when we try to allocate more, we fail the search request as an OOM Error.
That error is marked as not retryable and will fail sibling search request as well.

This logic in itself is sufficient to bound the warm up cache memory usage to our initial budget.
However, this gives us no control on which leaf request will get OOMed.

For this reason, we also have a warmup_max_split_initial_allocation that will fail the request if the 
single split allocation is greater than a given amount; hence increasing the chance that the faulty query is the one that
will be OOMed first.
