
# Searcher split cache

Quickwit includes a split cache. It can be useful for specific workloads:
- to improve performance
- to reduce the cost associated with GET requests.

The split cache stores entire split files on disk.

It works under the following configurable constraints:
- number of concurrent downloads
- amount of disk space
- number of on-disk files.

Searcher get tipped by indexers about the existence of splits (for which they have the best affinity).
They also might learn about split existence, upon read requests.

The searcher is then in charge of maintaining an in-memory data structure with a bounded list of splits it knows about and their score.
The current strategy for admission/evicton is a simple LRU logic.

If the most recently accessed split not already in cache has been accessed, we consider downloading it.
If the limits have been reached, we only proceed to eviction if one of the split currently
in cache has been less recently accessed.

## Predicate preparation

Complete regex/wildcard predicate hitset preparation for unscored queries uses a separate reader
when predicate caching is enabled. It shares the split footer and hotcache metadata with the
ordinary reader. Its body reads bypass the whole-split cache, persistent body-byte cache, and
per-query byte cache. Tantivy decodes owned input buffers, and the request retains the resulting
hitsets even if the predicate cache rejects or evicts them. Ordinary reads, scored queries,
disabled predicate caching, and calculated-field prefilters keep cached postings warmup. When
an automaton serves both a complete predicate and a prefilter, both readers perform their work.


