# Sorting

Quickwit can sort results based on fastfield values or score. This document discuss where and how
 it happens.
It also tries to describe optimizations that may be enabled (but are not necessarily implemente)
by this behavior.

Described below is the target behavior, which is *not* implemented right now, but will be shortly.

## Behavior

Sorting is controlled by the `sort_by` query parameter. It accepts a comma separated list of fields
to use for sorting. Sorting is Descending by default. The sorting order can be reversed by prefixing
a field name with a hyphen `-`.
The special value `_score` means sorting by score, it is also Descending by default.

In case of equality between two documents, the GlobalDocId, composed of (SplitId, SegmentId, DocId)
is used as a tie breaker. It is used to sort in the same order as the first field being sorted by.
This means it is in Descending order by default.

If a document doesn't have a value for a sorting field, that document is considered to go after any
document which has a value, independently of sort order. That is, when sorting the value 1,2 and
None, ascending sort would give `[1, 2, None]`, and descending sort would give `[2, 1, None]`.

If a client does not request sorting, documents are sorted using (SplitId, SegmentId, DocId), on
Descending order. In other words, everything happens as if documents were sorted by a constant
value.

<!--
TODO we could also say "it's not sorted" and add a special `_doc_id` for that. See optimizations
-->

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

<!--
If we allow unsorted requests, we can go further and stop searching as soon as we have k hits
(even going as far as stopping mid collection), without even looking at other splits metadata.
Argument can be made in favor of this because GlobalDocId is not stable, and can change during
a merge, so order is not guaranteed anyway, at least not until Quickwit has support for a Point
In Time mechanism.
-->

These optimization have limited to no impact if we give an exact count of matching documents.
An option to request only a lower bound would be required for these optimizations to make sense.
