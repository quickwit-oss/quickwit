# Partial result cache and refreshes

Leaf search caches each split's response under its rewritten search request. A
request collecting top hits and a count-only request have different cache keys.
Cached top hits can establish a timestamp threshold before other splits warm up;
splits older than that threshold can then use their count-only cache entries.

For descending timestamp searches that request hits, the leaf preserves hit
collection for the youngest mature split fully covered by the query's timestamp
range. That split is exempt from upfront match-all pruning and later top-hit
pruning, including after warmup. Timestamp normalization and other request
rewrites still apply. All splits use the normal scheduling and cache lookup paths;
there is no separate search phase.

Mature splits are no longer eligible for merging. The selected split's cached hit
list can therefore provide a threshold on a refresh even after newer splits were
replaced by merged splits. Splits crossing a query time boundary are not selected,
because a moving boundary changes their cache key.

Only one mature split is protected. If it contributes fewer than
`start_offset + max_hits` matching hits, its cached response alone cannot establish
a pruning threshold. Older splits may still need to be searched on refresh; search
results remain correct. Cache eviction, changed query filters, or a window moving
past the selected split can also require fresh searches.

The root sends `SplitIdAndFooterOffsets.is_mature`, evaluated from split metadata
at request creation. Older roots omit this protobuf field; its default `false`
disables this optimization for their splits. Older leaves ignore the new field.
Ascending timestamp searches, other sort orders, requests without hits, and nodes
with the partial result cache disabled retain their existing pruning behavior.
