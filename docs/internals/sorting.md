# Sorting

Quickwit can sort results based on fastfield values or score. This document discuss where and ho
 it happens.
It also tries to describe optimizations that may be enabled (but are not necessarily implemente)
by this behavior.

# Current state
(this section shall be removed once proposed changes are accepted)

## Behavior

Sorting is controlled by the `sort_by` query parameter. It accepts a comma separated list of fields
to use for sorting. Sorting is Ascending by default. The sorting order can be reversed by prefixing
a field name with a hyphen `-`.
The special value `_score` means sorting by score, with a sort that is then Descending by default
(documents matching better goes first).

In case of equality between two documents, the GlobalDocId, composed of (SplitId, SegmentId, DocId)
is used as a tie breaker. It is always used in Ascending order *inside a split*. It is used in the
same order as the first field *when merging results*
// TODO is this really the case? That's at least what my own tests seems to show.

If a document doesn't have a value for a sorting field, that document is considered to go after any
document which has a value, independantly of sort order. That is, when sorting the value 1,2 and
None, ascending sort would give `[1, 2, None]`, and descending sort would give `[2, 1, None]`.

If the client does not request sorting, documents are sorted using (DocId, SplitId, SegmentId), in
Descending order.

There are also plain bugs in sorting, in particular when values are absent (like 
https://github.com/quickwit-oss/quickwit/blob/5d2c66e38e17ba5952196f01123d87f5a98b6a3c/quickwit/quickwit-search/src/collector.rs#L379-L380
which prevent further admission in top-k once a single document with an absent value makes it into
the top-k, such as if it's one of the first matching documents in the split).

## Code
Sorting happends in multiple places. First in `QuickwitSegmentCollector::collect_top_k`, for
getting top documents from a single segment (and a single split). Then in 
`top_k_partial_hits` when merging splits inside a single leaf node. The retry mechanism in
cluster\_client can also end up merging results in `merge_leaf_search_response`, this time
only concatenating partial hits. Finally results from splits on different leaf nodes are
merged using the same logic as merging splits inside a leaf node, executed on the root node.

# Proposed state

## Behavior

Sorting is controlled by the `sort_by` query parameter. It accepts a comma separated list of fields
to use for sorting. Sorting is Ascending by default. The sorting order can be reversed by prefixing
a field name with a hyphen `-`.
The special value `_score` means sorting by score, with a sort that is then Descending by default
(documents matching better goes first).

In case of equality between two documents, the GlobalDocId, composed of (SplitId, SegmentId, DocId)
is used as a tie breaker. It is used to sort in the same order as the first field being sorted by.
This means it is in Ascending order by default, except when the first field being sorted by is
`_score` .
// TODO or should it always be Ascending by default? I don't think it matters much, but would require
some more special handling of `_score`.

If a document doesn't have a value for a sorting field, that document is considered to go after any
document which has a value, independantly of sort order. That is, when sorting the value 1,2 and
None, ascending sort would give `[1, 2, None]`, and descending sort would give `[2, 1, None]`.

If a client does not request sorting, documents are sorted using (SplitId, SegmentId, DocId), on
Ascending order.
// TODO we could also say "it's not sorted" and add a special `_doc_id` for that. See optimizations

# Code

(The changes described here are currently part of quickwit#3545, which is an optimization PR. They
*should* be backported to a standalone PR to ease review and discussion).
A new structure TopK is introduced which is used both for in-split sorting and for merging of
results. It reduces the risks of inconsistencies between in-split and between-split behavior.
`SortOrder` gets new `compare` and `compare_opt` method which can be used to compare two values with
 respect to the particular sort order required, and with proper handling of the `None` special case.

# Optimization permited

Both orders allow an optimization when sorting by date (either direction), by leveraging splits
meta-data to know in advance if a split can, or not, contain better results. Changing the sorting
order for "not sorted" queries allows to leverage SplitId as a way to know whether a split can
contain or not better results (if its SplitId is more/less than the current worst best-hit, the
split does not need to be searched).

If we allow unsorted requests, we can go further and stop searching as soon as we have k hits,
without even looking at other splits metadata. Argument can be made in favor of this because
GlobalDocId is not stable, and can change during a merge, so order is not guaranteed anyway,
at least not until Quickwit has support for a Point In Time mechanism.

None of these optimization described has any impact if we give an exact count of matching
documents, an option to request only a lower bound would be required for these optimizations to
make sense.
