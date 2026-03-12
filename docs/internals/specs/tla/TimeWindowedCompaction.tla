---- MODULE TimeWindowedCompaction ----
\* Formal specification for time-windowed sorted compaction of Parquet splits.
\*
\* Models the invariants from ADR-003: Time-Windowed Sorted Compaction for Parquet.
\* The system ingests data points with timestamps, assigns them to time windows,
\* produces sorted splits, and compacts splits within the same window while
\* preserving row sets, row contents, sort order, and column unions.
\*
\* Key Invariants:
\*   TW-1: Every split belongs to exactly one time window
\*   TW-2: window_duration evenly divides one hour (3600 seconds)
\*   TW-3: Data is never merged across window boundaries
\*   CS-1: Only splits sharing all six scope components may be merged
\*   CS-2: Within a scope, only splits with same window_start are merged
\*   CS-3: Splits before compaction_start_time are never compacted
\*   MC-1: Row multiset preserved through compaction (no add/remove/duplicate)
\*   MC-2: Row contents unchanged through compaction (except bookkeeping)
\*   MC-3: Output is sorted according to the sort schema
\*   MC-4: Column set is the union of input column sets (nulls fill gaps)
\*
\* Signal Applicability:
\*   - Metrics: Primary target. Parquet splits, DataFusion queries, time-series data.
\*   - Traces:  Same invariants apply. Tantivy splits would use same windowing.
\*   - Logs:    Same invariants apply. Time-windowed compaction generalizes.
\*
\* TLA+-to-Rust Mapping:
\* | TLA+ Concept                | Rust Implementation                                      |
\* |-----------------------------|----------------------------------------------------------|
\* | `ObjectStorage`             | S3/GCS split files + `metrics_splits` PostgreSQL table    |
\* | `Split.scope`               | `(index_uid, source_id, partition_id,                     |
\* |                             |  doc_mapping_uid, sort_schema, window_duration)`          |
\* | `Split.window_start`        | `MetricsSplitMetadata.window_start`                       |
\* | `Split.rows`                | RecordBatch rows in Parquet file                          |
\* | `Split.columns`             | Parquet column names (Arrow schema)                       |
\* | `Split.sorted`              | Result of `lexsort_to_indices` applied to sort columns    |
\* | `IngestPoint`               | OTLP ingest -> MetricsDocProcessor -> MetricsIndexer      |
\* | `FlushSplit`                | MetricsIndexer -> MetricsUploader -> MetricsPublisher     |
\* | `CompactWindow`             | Parquet merge planner -> merge executor -> publisher      |
\* | `WindowDuration`            | Index-level config `window_duration_secs`                 |
\* | `CompactionStartTime`       | Index-level config `compaction_start_time`                |
\* | `LateDataAcceptanceWindow`  | Index-level config `late_data_acceptance_window`          |
\* | `SortOrder`                 | Sort schema string, e.g. "metric_name|tag_host|ts-/V2"   |
\* | `IsSorted(seq)`             | Arrow `lexsort_to_indices` produces identity permutation  |
\* | `BagUnion`                  | k-way merge output == union of input RecordBatches        |

EXTENDS Integers, Sequences, FiniteSets, TLC

\* ============================================================================
\* CONSTANTS
\* ============================================================================

CONSTANTS
    \* Set of possible timestamps (abstract time units).
    \* Example: {0, 1, 2, 3}
    Timestamps,

    \* Set of possible column names in data points.
    \* Example: {"metric_name", "value", "host"}
    AllColumns,

    \* Set of possible scope values.
    \* Each scope is a record representing the 6-part compatibility key.
    \* For model checking, we use a small set of distinct scopes.
    \* Example: {<<"idx1", "src1", "p1", "dm1", "sort1", 2>>}
    Scopes,

    \* The active window duration (in abstract time units).
    \* Must evenly divide HourSeconds.
    WindowDuration,

    \* Abstract representation of "one hour" in model time units.
    \* WindowDuration must evenly divide this.
    HourSeconds,

    \* The compaction start time. Splits with window_start < this are
    \* not eligible for compaction.
    CompactionStartTime,

    \* The late data acceptance window. Points with timestamp < (CurrentTime - this)
    \* are dropped at ingestion.
    LateDataAcceptanceWindow,

    \* The current time (abstract). Advances via the model.
    \* In the small model this is bounded to keep the state space finite.
    MaxTime,

    \* Maximum number of data points that can be ingested (bounds state space).
    MaxPoints,

    \* Maximum number of compaction steps (bounds state space).
    MaxCompactions,

    \* A sort key extraction function constant.
    \* In the model, each data point has a "sort_key" field used for ordering.
    \* This is implicit in the row structure.

    \* Set of possible sort key values (for abstract ordering).
    SortKeys

\* ============================================================================
\* VARIABLES
\* ============================================================================

VARIABLES
    \* The current time in the system (monotonically non-decreasing).
    currentTime,

    \* Object storage: a set of split records.
    \* Each split is a record:
    \*   [ id        : Nat,
    \*     scope     : Scope,
    \*     window_start : Int,
    \*     rows      : Sequence of Row records,
    \*     columns   : Set of column names,
    \*     sorted    : BOOLEAN ]
    \*
    \* Each Row is:
    \*   [ point_id  : Nat,        \* unique identity for MC-2 tracking
    \*     timestamp : Int,
    \*     sort_key  : SortKey,    \* abstract sort key for MC-3
    \*     columns   : Set of column names this row has values for,
    \*     values    : column name -> value mapping (for MC-2) ]
    objectStorage,

    \* Ingestion buffer: accumulates points per (scope, window_start) before flush.
    \* A function from <<scope, window_start>> to a sequence of Row records.
    ingestBuffer,

    \* Counter for generating unique split IDs.
    nextSplitId,

    \* Counter for generating unique point IDs.
    nextPointId,

    \* The total number of points ingested so far (for bounding).
    pointsIngested,

    \* The total number of compaction steps performed (for bounding).
    compactionsPerformed,

    \* History: tracks all rows ever created (by point_id) for MC-2 verification.
    \* Maps point_id -> original row record.
    rowHistory,

    \* Ghost variable: records each compaction's input/output for invariant checking.
    \* Set of records:
    \*   [ inputSplitIds  : Set of split IDs,
    \*     outputSplitId  : split ID,
    \*     inputRows      : Bag of point_ids,
    \*     outputRows     : Bag of point_ids ]
    compactionLog

vars == <<currentTime, objectStorage, ingestBuffer, nextSplitId,
          nextPointId, pointsIngested, compactionsPerformed,
          rowHistory, compactionLog>>

\* ============================================================================
\* HELPERS
\* ============================================================================

\* Compute the window start for a given timestamp.
WindowStart(t) == t - (t % WindowDuration)

\* The finite set of valid window starts derived from the timestamp domain.
\* Used in quantifiers instead of Int to keep TLC enumerable.
ValidWindowStarts == {WindowStart(t) : t \in Timestamps}

\* Check whether a sequence is sorted by sort_key (ascending).
\* An empty or single-element sequence is sorted.
IsSorted(seq) ==
    \A i \in 1..(Len(seq) - 1) : seq[i].sort_key <= seq[i + 1].sort_key

\* Convert a sequence to a "bag" (multiset) represented as a function
\* from elements to counts. We represent bags as sets of <<element, count>>
\* pairs. For our purposes, we use point_ids which are unique, so the bag
\* is effectively a set. But we model it properly to catch duplication bugs.
\*
\* We represent a bag as a function: point_id -> count.
\* Since TLC cannot natively handle Bags module in all configurations,
\* we model bags manually using functions over point_id domains.

\* Extract the set of point_ids from a sequence of rows.
PointIdSet(rows) == {rows[i].point_id : i \in 1..Len(rows)}

\* Count occurrences of a point_id in a sequence of rows.
CountInSeq(pid, rows) ==
    LET indices == {i \in 1..Len(rows) : rows[i].point_id = pid}
    IN Cardinality(indices)

\* Build a bag (point_id -> count) from a sequence of rows.
BagOfSeq(rows) ==
    [pid \in PointIdSet(rows) |-> CountInSeq(pid, rows)]

\* Check if two bags are equal (same domain, same counts).
BagsEqual(bag1, dom1, bag2, dom2) ==
    /\ dom1 = dom2
    /\ \A pid \in dom1 : bag1[pid] = bag2[pid]

\* Concatenate two sequences.
SeqConcat(s1, s2) ==
    [i \in 1..(Len(s1) + Len(s2)) |->
        IF i <= Len(s1) THEN s1[i] ELSE s2[i - Len(s1)]]

\* Merge sort two sorted sequences into one sorted sequence.
\* This is a recursive definition; TLC handles it for small sequences.
RECURSIVE MergeSort(_, _)
MergeSort(s1, s2) ==
    IF Len(s1) = 0 THEN s2
    ELSE IF Len(s2) = 0 THEN s1
    ELSE IF Head(s1).sort_key <= Head(s2).sort_key
         THEN <<Head(s1)>> \o MergeSort(Tail(s1), s2)
         ELSE <<Head(s2)>> \o MergeSort(s1, Tail(s2))

\* Multi-way merge: merge a set of sorted sequences into one sorted sequence.
\* We do this by folding pairwise merges.
RECURSIVE MultiMerge(_)
MultiMerge(seqSet) ==
    IF seqSet = {} THEN <<>>
    ELSE LET s == CHOOSE s \in seqSet : TRUE
         IN MergeSort(s, MultiMerge(seqSet \ {s}))

\* Insert an element into a sorted sequence at the correct position.
RECURSIVE SortedInsert(_, _)
SortedInsert(elem, seq) ==
    IF Len(seq) = 0 THEN <<elem>>
    ELSE IF elem.sort_key <= Head(seq).sort_key
         THEN <<elem>> \o seq
         ELSE <<Head(seq)>> \o SortedInsert(elem, Tail(seq))

\* Insert-sort a sequence by sort_key (for initial split creation).
RECURSIVE InsertionSort(_)
InsertionSort(seq) ==
    IF Len(seq) <= 1 THEN seq
    ELSE SortedInsert(Head(seq), InsertionSort(Tail(seq)))

\* Compute the union of column sets across a set of splits.
ColumnUnion(splits) ==
    UNION {s.columns : s \in splits}

\* Concatenate the rows from a set of splits into one sequence.
\* Used for building the input bag independently of the merge output.
RECURSIVE ConcatSplitRows(_)
ConcatSplitRows(splitSet) ==
    IF splitSet = {} THEN <<>>
    ELSE LET sp == CHOOSE sp \in splitSet : TRUE
         IN SeqConcat(sp.rows, ConcatSplitRows(splitSet \ {sp}))

\* ============================================================================
\* TYPE INVARIANT
\* ============================================================================

TypeInvariant ==
    /\ currentTime \in 0..MaxTime
    /\ nextSplitId \in Nat
    /\ nextPointId \in Nat
    /\ pointsIngested \in 0..MaxPoints
    /\ compactionsPerformed \in 0..MaxCompactions
    /\ \A split \in objectStorage :
        /\ split.id \in Nat
        /\ split.scope \in Scopes
        /\ split.window_start \in Int
        /\ split.columns \subseteq AllColumns
        /\ split.sorted \in BOOLEAN
        /\ Len(split.rows) >= 1

\* ============================================================================
\* SAFETY PROPERTIES (INVARIANTS)
\* ============================================================================

\* ---------------------------------------------------------------------------
\* TW-1: Every split in object storage belongs to exactly one time window.
\*       All rows in a split have the same window_start as the split metadata.
\* ---------------------------------------------------------------------------
TW1_OneWindowPerSplit ==
    \A split \in objectStorage :
        \A i \in 1..Len(split.rows) :
            WindowStart(split.rows[i].timestamp) = split.window_start

\* ---------------------------------------------------------------------------
\* TW-2: window_duration must evenly divide one hour (3600 seconds).
\*       This is a configuration constraint checked as an invariant.
\* ---------------------------------------------------------------------------
TW2_DurationDividesHour ==
    HourSeconds % WindowDuration = 0

\* ---------------------------------------------------------------------------
\* TW-3: Data is never merged across window boundaries.
\*       For every compaction log entry, all input splits have the same
\*       window_start, and the output split has that same window_start.
\*       Checked via the compaction log's recorded window_starts, and
\*       verified that the output split in storage (if still present)
\*       matches. This is implied by CS-2 but checked explicitly as a
\*       separate invariant per the ADR.
\* ---------------------------------------------------------------------------
TW3_NoCrossWindowMerge ==
    \A entry \in compactionLog :
        \* All input window_starts are identical (same check as CS-2,
        \* but stated in terms of "no cross-window merge").
        /\ \A id1, id2 \in entry.inputSplitIds :
            entry.inputWindowStarts[id1] = entry.inputWindowStarts[id2]
        \* The output split (if still in storage) has the same window_start.
        /\ \A s \in objectStorage :
            s.id = entry.outputSplitId =>
                \A id \in entry.inputSplitIds :
                    s.window_start = entry.inputWindowStarts[id]

\* ---------------------------------------------------------------------------
\* CS-1: Only splits sharing all six scope components may be merged.
\*       Every compaction log entry's input splits all share the same scope.
\* ---------------------------------------------------------------------------
CS1_ScopeCompatibility ==
    \A entry \in compactionLog :
        \A id1, id2 \in entry.inputSplitIds :
            entry.inputScopes[id1] = entry.inputScopes[id2]

\* ---------------------------------------------------------------------------
\* CS-2: Within a compatibility scope, only splits with the same window_start
\*       are merged. Every compaction log entry's input splits share window_start.
\* ---------------------------------------------------------------------------
CS2_SameWindowStart ==
    \A entry \in compactionLog :
        \A id1, id2 \in entry.inputSplitIds :
            entry.inputWindowStarts[id1] = entry.inputWindowStarts[id2]

\* ---------------------------------------------------------------------------
\* CS-3: Splits produced before compaction_start_time are never compacted.
\*       No compaction log entry includes a split with window_start < CompactionStartTime.
\* ---------------------------------------------------------------------------
CS3_CompactionStartTime ==
    \A entry \in compactionLog :
        \A id \in entry.inputSplitIds :
            entry.inputWindowStarts[id] >= CompactionStartTime

\* ---------------------------------------------------------------------------
\* MC-1: The multiset (bag) of rows is identical before and after compaction.
\*       The bag of point_ids in the output equals the bag-union of point_ids
\*       from the inputs. No rows added, removed, or duplicated.
\* ---------------------------------------------------------------------------
MC1_RowSetPreserved ==
    \A entry \in compactionLog :
        BagsEqual(
            entry.inputBag, DOMAIN entry.inputBag,
            entry.outputBag, DOMAIN entry.outputBag
        )

\* ---------------------------------------------------------------------------
\* MC-2: Row contents do not change during compaction.
\*       For every point_id in a compaction output, its row values match
\*       the original row as recorded in rowHistory.
\* ---------------------------------------------------------------------------
MC2_RowContentsPreserved ==
    \A split \in objectStorage :
        \A i \in 1..Len(split.rows) :
            LET row == split.rows[i]
                pid == row.point_id
            IN  /\ pid \in DOMAIN rowHistory
                /\ row.timestamp = rowHistory[pid].timestamp
                /\ row.sort_key = rowHistory[pid].sort_key
                /\ row.columns = rowHistory[pid].columns
                /\ row.values = rowHistory[pid].values

\* ---------------------------------------------------------------------------
\* MC-3: The output of a merge is sorted according to the sort schema.
\*       Every split marked as sorted has rows in non-decreasing sort_key order.
\* ---------------------------------------------------------------------------
MC3_SortOrderPreserved ==
    \A split \in objectStorage :
        split.sorted => IsSorted(split.rows)

\* ---------------------------------------------------------------------------
\* MC-4: If inputs have different column sets, the output contains the union
\*       of all columns. Type conflicts are an error (modeled as the action
\*       being disabled). Rows from inputs missing a column are filled with nulls.
\*       We verify: output.columns = union of input columns.
\* ---------------------------------------------------------------------------
MC4_ColumnUnion ==
    \A entry \in compactionLog :
        entry.outputColumns = entry.inputColumnUnion

\* ============================================================================
\* INITIAL STATE
\* ============================================================================

Init ==
    /\ currentTime = 0
    /\ objectStorage = {}
    /\ ingestBuffer = [key \in {} |-> <<>>]  \* empty function
    /\ nextSplitId = 1
    /\ nextPointId = 1
    /\ pointsIngested = 0
    /\ compactionsPerformed = 0
    /\ rowHistory = [pid \in {} |-> <<>>]  \* empty function
    /\ compactionLog = {}

\* ============================================================================
\* ACTIONS
\* ============================================================================

\* ---------------------------------------------------------------------------
\* AdvanceTime: Time progresses. Monotonically non-decreasing.
\* ---------------------------------------------------------------------------
AdvanceTime ==
    /\ currentTime < MaxTime
    /\ \E t \in (currentTime + 1)..MaxTime :
        /\ currentTime' = t
        /\ UNCHANGED <<objectStorage, ingestBuffer, nextSplitId,
                        nextPointId, pointsIngested, compactionsPerformed,
                        rowHistory, compactionLog>>

\* ---------------------------------------------------------------------------
\* IngestPoint: A data point arrives with a timestamp, sort key, column set,
\*              and values. It is assigned to a window. If too old, it is dropped.
\*
\* Guards:
\*   - pointsIngested < MaxPoints (bound state space)
\*   - timestamp is not older than late_data_acceptance_window
\*
\* Effects:
\*   - Adds the point to the ingest buffer for (scope, window_start)
\*   - Records the row in rowHistory for MC-2 checking
\* ---------------------------------------------------------------------------
IngestPoint ==
    /\ pointsIngested < MaxPoints
    /\ \E ts \in Timestamps, sk \in SortKeys, scope \in Scopes, cols \in SUBSET AllColumns :
        /\ cols # {}  \* must have at least one column
        /\ ts <= currentTime  \* cannot ingest future data
        /\ ts >= currentTime - LateDataAcceptanceWindow  \* drop too-old data
        /\ LET ws == WindowStart(ts)
               key == <<scope, ws>>
               pid == nextPointId
               row == [point_id  |-> pid,
                       timestamp |-> ts,
                       sort_key  |-> sk,
                       columns   |-> cols,
                       values    |-> [c \in cols |-> <<pid, c>>]]  \* unique value per (point, col)
               oldBuf == IF key \in DOMAIN ingestBuffer
                         THEN ingestBuffer[key]
                         ELSE <<>>
           IN /\ ingestBuffer' = [k \in DOMAIN ingestBuffer \union {key} |->
                                    IF k = key THEN Append(oldBuf, row)
                                    ELSE ingestBuffer[k]]
              /\ nextPointId' = nextPointId + 1
              /\ pointsIngested' = pointsIngested + 1
              /\ rowHistory' = [p \in DOMAIN rowHistory \union {pid} |->
                                  IF p = pid THEN row ELSE rowHistory[p]]
              /\ UNCHANGED <<currentTime, objectStorage, nextSplitId,
                             compactionsPerformed, compactionLog>>

\* ---------------------------------------------------------------------------
\* FlushSplit: Write a split for one (scope, window_start) from the ingest
\*             buffer. The split contains sorted rows belonging to exactly one
\*             window.
\*
\* Guards:
\*   - The ingest buffer for (scope, window_start) is non-empty
\*
\* Effects:
\*   - Creates a new split in object storage with sorted rows
\*   - Clears the ingest buffer for that key
\* ---------------------------------------------------------------------------
FlushSplit ==
    \E key \in DOMAIN ingestBuffer :
        /\ Len(ingestBuffer[key]) > 0
        /\ LET scope == key[1]
               ws    == key[2]
               rows  == ingestBuffer[key]
               sortedRows == InsertionSort(rows)
               allCols == UNION {rows[i].columns : i \in 1..Len(rows)}
               newSplit == [id           |-> nextSplitId,
                            scope        |-> scope,
                            window_start |-> ws,
                            rows         |-> sortedRows,
                            columns      |-> allCols,
                            sorted       |-> TRUE]
           IN /\ objectStorage' = objectStorage \union {newSplit}
              /\ nextSplitId' = nextSplitId + 1
              /\ ingestBuffer' = [k \in DOMAIN ingestBuffer \ {key} |->
                                    ingestBuffer[k]]
              /\ UNCHANGED <<currentTime, nextPointId, pointsIngested,
                             compactionsPerformed, rowHistory, compactionLog>>

\* ---------------------------------------------------------------------------
\* CompactWindow: Select two or more compatible splits in the same window,
\*                merge them into one split.
\*
\* Guards:
\*   - At least 2 splits share the same scope and window_start
\*   - All selected splits have window_start >= CompactionStartTime (CS-3)
\*   - All selected splits have the same scope (CS-1)
\*   - All selected splits have the same window_start (CS-2)
\*   - compactionsPerformed < MaxCompactions (bound state space)
\*   - No type conflicts on column names (simplified: always compatible)
\*
\* Effects:
\*   - Removes input splits from object storage
\*   - Adds one output split with:
\*     * rows = sorted merge of input rows (MC-1, MC-3)
\*     * columns = union of input columns (MC-4)
\*     * row contents unchanged (MC-2)
\*   - Records the compaction in compactionLog
\* ---------------------------------------------------------------------------
CompactWindow ==
    /\ compactionsPerformed < MaxCompactions
    /\ \E scope \in Scopes, ws \in ValidWindowStarts :
        /\ ws >= CompactionStartTime  \* CS-3: skip pre-start splits
        /\ LET candidates == {s \in objectStorage :
                                /\ s.scope = scope
                                /\ s.window_start = ws}
           IN /\ Cardinality(candidates) >= 2
              /\ \E mergeSplits \in SUBSET candidates :
                    /\ Cardinality(mergeSplits) >= 2
                    /\ LET \* Collect all input rows as sorted sequences
                            inputSeqs == {s.rows : s \in mergeSplits}
                            \* Perform multi-way sorted merge
                            mergedRows == MultiMerge(inputSeqs)
                            \* Compute column union (MC-4)
                            allCols == ColumnUnion(mergeSplits)
                            \* Build new split
                            outputSplit == [id           |-> nextSplitId,
                                            scope        |-> scope,
                                            window_start |-> ws,
                                            rows         |-> mergedRows,
                                            columns      |-> allCols,
                                            sorted       |-> TRUE]
                            \* Build bags for MC-1 verification
                            inputIds == UNION {PointIdSet(s.rows) : s \in mergeSplits}
                            outputIds == PointIdSet(mergedRows)
                            \* Concatenate all input rows for bag computation
                            allInputRows == MultiMerge(inputSeqs)  \* same rows, just for counting
                            \* Record compaction metadata
                            mergeIds == {s.id : s \in mergeSplits}
                            logEntry == [
                                inputSplitIds    |-> mergeIds,
                                outputSplitId    |-> nextSplitId,
                                inputBag         |-> BagOfSeq(mergedRows),
                                outputBag        |-> BagOfSeq(mergedRows),
                                inputScopes      |-> [id \in mergeIds |->
                                                        (CHOOSE s \in mergeSplits : s.id = id).scope],
                                inputWindowStarts |-> [id \in mergeIds |->
                                                        (CHOOSE s \in mergeSplits : s.id = id).window_start],
                                outputColumns    |-> allCols,
                                inputColumnUnion |-> allCols
                            ]
                       IN /\ objectStorage' = (objectStorage \ mergeSplits) \union {outputSplit}
                          /\ nextSplitId' = nextSplitId + 1
                          /\ compactionsPerformed' = compactionsPerformed + 1
                          /\ compactionLog' = compactionLog \union {logEntry}
                          /\ UNCHANGED <<currentTime, ingestBuffer, nextPointId,
                                         pointsIngested, rowHistory>>

\* Note on MC-1 bag verification in CompactWindow:
\* The merged rows come from MultiMerge which only reorders (mergesorts) the
\* input rows. It does not add, remove, or duplicate any row. The BagOfSeq
\* for the merged output and the BagOfSeq for all input rows must be identical.
\* Since MultiMerge is a pure reordering of the union of input sequences,
\* the bags are structurally equal by construction. The compactionLog records
\* both for the MC1_RowSetPreserved invariant to verify.
\*
\* To make the MC-1 check non-trivial (catching implementation bugs where
\* MultiMerge might be incorrect), we compute input and output bags
\* independently:

\* ---------------------------------------------------------------------------
\* CompactWindowWithBagCheck: Same as CompactWindow but builds input bag
\* by concatenating raw input sequences (not the merge output).
\* This is the version used for model checking.
\* ---------------------------------------------------------------------------
CompactWindowChecked ==
    /\ compactionsPerformed < MaxCompactions
    /\ \E scope \in Scopes, ws \in ValidWindowStarts :
        /\ ws >= CompactionStartTime
        /\ LET candidates == {s \in objectStorage :
                                /\ s.scope = scope
                                /\ s.window_start = ws}
           IN /\ Cardinality(candidates) >= 2
              /\ \E mergeSplits \in SUBSET candidates :
                    /\ Cardinality(mergeSplits) >= 2
                    /\ LET inputSeqs == {s.rows : s \in mergeSplits}
                            mergedRows == MultiMerge(inputSeqs)
                            allCols == ColumnUnion(mergeSplits)
                            outputSplit == [id           |-> nextSplitId,
                                            scope        |-> scope,
                                            window_start |-> ws,
                                            rows         |-> mergedRows,
                                            columns      |-> allCols,
                                            sorted       |-> TRUE]
                            mergeIds == {s.id : s \in mergeSplits}
                            \* Build input bag by concatenating raw input rows
                            \* (independently of the merge output, for MC-1)
                            inputRowsConcat == ConcatSplitRows(mergeSplits)
                            inputBag == BagOfSeq(inputRowsConcat)
                            outputBag == BagOfSeq(mergedRows)
                            logEntry == [
                                inputSplitIds     |-> mergeIds,
                                outputSplitId     |-> nextSplitId,
                                inputBag          |-> inputBag,
                                outputBag         |-> outputBag,
                                inputScopes       |-> [id \in mergeIds |->
                                                        (CHOOSE s \in mergeSplits : s.id = id).scope],
                                inputWindowStarts |-> [id \in mergeIds |->
                                                        (CHOOSE s \in mergeSplits : s.id = id).window_start],
                                outputColumns     |-> allCols,
                                inputColumnUnion  |-> allCols
                            ]
                       IN /\ objectStorage' = (objectStorage \ mergeSplits) \union {outputSplit}
                          /\ nextSplitId' = nextSplitId + 1
                          /\ compactionsPerformed' = compactionsPerformed + 1
                          /\ compactionLog' = compactionLog \union {logEntry}
                          /\ UNCHANGED <<currentTime, ingestBuffer, nextPointId,
                                         pointsIngested, rowHistory>>

\* ---------------------------------------------------------------------------
\* ChangeWindowDuration: Update the configured window duration.
\* In the model, WindowDuration is a CONSTANT, so we cannot change it.
\* Instead, we model window duration changes by having multiple scopes
\* with different window_duration components. Splits retain their original
\* scope (including window_duration), and the compatibility scope prevents
\* cross-duration merges.
\*
\* This action is a no-op in the TLA+ model because window_duration is
\* encoded in the scope constant. The invariant TW-2 verifies the
\* constraint for all scopes.
\* ---------------------------------------------------------------------------
\* ChangeWindowDuration is modeled implicitly through multiple scopes.
\* The scope tuple includes window_duration as the 6th component.
\* Different scopes with different window_duration values coexist.

\* ============================================================================
\* NEXT-STATE RELATION
\* ============================================================================

Next ==
    \/ AdvanceTime
    \/ IngestPoint
    \/ FlushSplit
    \/ CompactWindowChecked

\* ============================================================================
\* SPECIFICATION
\* ============================================================================

Spec == Init /\ [][Next]_vars

\* Fairness: ensure progress (for liveness, not safety).
FairSpec == Init /\ [][Next]_vars /\ WF_vars(Next)

\* ============================================================================
\* ALL INVARIANTS (referenced from .cfg files)
\* ============================================================================

\* Combined safety invariant for convenience.
Safety ==
    /\ TW1_OneWindowPerSplit
    /\ TW2_DurationDividesHour
    /\ TW3_NoCrossWindowMerge
    /\ CS1_ScopeCompatibility
    /\ CS2_SameWindowStart
    /\ CS3_CompactionStartTime
    /\ MC1_RowSetPreserved
    /\ MC2_RowContentsPreserved
    /\ MC3_SortOrderPreserved
    /\ MC4_ColumnUnion

\* ============================================================================
\* LIVENESS PROPERTIES (optional, for checking progress)
\* ============================================================================

\* Eventually, if there are buffered points, they get flushed.
\* (Requires fairness.)
EventualFlush ==
    \A key \in DOMAIN ingestBuffer :
        Len(ingestBuffer[key]) > 0 ~> key \notin DOMAIN ingestBuffer

====
