---- MODULE ParquetDataModel ----
\* Formal specification for Parquet Metrics Data Model Invariants (ADR-001)
\*
\* Models:
\*   - Ingestion of metric data points into pending batches on multiple nodes
\*   - Flushing pending batches into immutable Parquet splits in object storage
\*   - Compaction of splits: merging multiple splits into one without data loss
\*   - Deterministic timeseries_id computation from canonicalized tag sets
\*   - Preservation of timeseries_id through compaction (no recomputation)
\*
\* Key Invariants (from ADR-001):
\*   - DM-1: Each row in a Parquet split is exactly one data point
\*   - DM-2: No last-write-wins; duplicate (metric, tags, ts) from separate
\*           ingests both survive
\*   - DM-3: No interpolation; storage contains only ingested points
\*   - DM-4: timeseries_id is deterministic for a given tag set
\*   - DM-5: timeseries_id persists through compaction without recomputation
\*
\* Signal Applicability:
\*   - Metrics: Primary target. Each row is one data point
\*             (metric_name, tags, timestamp, value)
\*   - Traces:  Same model applies. Each row is one span. No LWW.
\*              timeseries_id equivalent is a hash of span attributes
\*   - Logs:    Same model applies. Each row is one log entry. No LWW.
\*              timeseries_id equivalent groups by service/host
\*
\* TLA+-to-Rust Mapping:
\*   | TLA+ Concept         | Rust Implementation                                   |
\*   |----------------------|-------------------------------------------------------|
\*   | Nodes                | Indexing pipeline nodes in quickwit-indexing           |
\*   | DataPoint            | Row in RecordBatch (quickwit-parquet-engine)           |
\*   | pending[n]           | MetricsIndexer accumulation buffer                    |
\*   | splits               | Published Parquet files in object storage              |
\*   | request_id           | Ingest request identity (WAL position / batch ID)     |
\*   | IngestPoint          | MetricsDocProcessor receives Arrow IPC batch           |
\*   | FlushSplit           | MetricsUploader + MetricsPublisher commit split        |
\*   | CompactSplits        | MergeExecutor merges Parquet files (future)            |
\*   | TSIDHash(tags)       | xxHash64 / SipHash of canonicalized tag key-value set  |
\*   | DM1_PointPerRow      | debug_assert! in schema validation                    |
\*   | DM2_NoLWW            | No dedup logic in ingest or compact paths              |
\*   | DM3_NoInterpolation  | debug_assert! no synthetic points in split writer      |
\*   | DM4_DeterministicID  | Deterministic hash fn in timeseries_id computation     |
\*   | DM5_TSIDPersistence  | Merge carries timeseries_id column without recompute   |

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    \* @type: Set(NODE);
    \* The set of ingestion nodes (indexing pipeline instances).
    Nodes,

    \* @type: Set(Str);
    \* The set of metric names in the model.
    MetricNames,

    \* @type: Set(Str);
    \* The set of possible tag sets (each represented as an opaque string
    \* for model simplicity; in the implementation these are sorted
    \* key-value maps).
    TagSets,

    \* @type: Set(Int);
    \* The set of possible timestamps.
    Timestamps,

    \* @type: Int;
    \* Maximum number of ingest requests the model explores.
    \* Bounds the state space for model checking.
    RequestCountMax

VARIABLES
    \* @type: NODE -> Seq(DATAPOINT);
    \* Per-node pending batch of data points waiting to be flushed.
    pending,

    \* @type: Set(SPLIT);
    \* The set of published splits in object storage.
    \* Each split is a record with fields:
    \*   split_id : Nat
    \*   rows     : Set of data-point records
    splits,

    \* @type: Set(DATAPOINT);
    \* The complete history of all points that have ever been ingested.
    \* Used to check DM-3 (no interpolation): storage is a subset of this.
    all_ingested_points,

    \* @type: Int;
    \* Monotonically increasing counter for split IDs.
    next_split_id,

    \* @type: Int;
    \* Monotonically increasing counter for request IDs.
    \* Each IngestPoint action increments this so that separate
    \* ingest requests produce distinct request_id values,
    \* enabling DM-2 verification.
    next_request_id

vars == <<pending, splits, all_ingested_points, next_split_id, next_request_id>>

----
\* ---- Helper: timeseries_id hash function ----
\*
\* TSIDHash models the deterministic hash from tag sets to timeseries IDs.
\* We model TSIDHash as a deterministic function from tag sets to integers.
\* The key property is that equal inputs always produce equal outputs (DM-4).
\* We use CHOOSE to pick a fixed arbitrary integer for each tag set —
\* this is deterministic in TLA+ (CHOOSE always returns the same value
\* for the same predicate).
\*
\* In the implementation this is xxHash64 or SipHash of the canonicalized
\* (sorted by key name) set of all tag key-value pairs.

TSIDHash(tags) == CHOOSE n \in 0..100 : TRUE

----
\* ---- Data Point record constructor ----
\*
\* A DataPoint is a record with:
\*   metric_name  : Str
\*   tags         : Str (represents the full tag set)
\*   timestamp    : Int
\*   value        : Int (abstract; real values are f64)
\*   request_id   : Int (distinguishes separate ingest requests)
\*   timeseries_id: Int (deterministic hash of tags)
\*
\* request_id is NOT part of the "logical identity" of a point. Two points
\* with the same (metric_name, tags, timestamp, value) but different
\* request_id represent the DM-2 scenario: duplicate points from separate
\* ingests that must both be stored.

MakePoint(mn, ts_set, t, v, rid) ==
    [metric_name   |-> mn,
     tags          |-> ts_set,
     timestamp     |-> t,
     value         |-> v,
     request_id    |-> rid,
     timeseries_id |-> TSIDHash(ts_set)]

----
\* ---- Type Invariant ----

TypeInvariant ==
    /\ \A n \in Nodes: pending[n] \in SUBSET
        [metric_name: MetricNames,
         tags: TagSets,
         timestamp: Timestamps,
         value: {1},
         request_id: Int,
         timeseries_id: Int]
    /\ \A s \in splits:
        /\ s.split_id \in Int
        /\ s.rows \in SUBSET
            [metric_name: MetricNames,
             tags: TagSets,
             timestamp: Timestamps,
             value: {1},
             request_id: Int,
             timeseries_id: Int]

----
\* ---- Derived state: all rows currently in storage ----

AllStoredRows == UNION {s.rows : s \in splits}

----
\* ---- Safety Properties (ADR-001 Invariants) ----

\* DM-1: Each row in a Parquet split is exactly one data point.
\*
\* In this model, each element of s.rows is already a single DataPoint
\* record by construction. The invariant asserts that every row has
\* all required fields populated (no partial / multi-point rows).
DM1_PointPerRow ==
    \A s \in splits:
        \A row \in s.rows:
            /\ row.metric_name \in MetricNames
            /\ row.tags \in TagSets
            /\ row.timestamp \in Timestamps
            /\ row.timeseries_id = TSIDHash(row.tags)

\* DM-2: No last-write-wins.
\*
\* If two data points with the same (metric_name, tags, timestamp)
\* were ingested via separate requests (different request_id), both
\* must exist in storage after all flushes complete.
\*
\* We check: for every pair of distinct ingested points that share
\* the same logical identity but have different request_id, both
\* are present in AllStoredRows OR both are still in pending batches.
\* (Points not yet flushed are not yet "lost" -- they will appear
\* when flushed.)
\*
\* The critical part: if both have been flushed (neither is in any
\* pending batch), then both must be in storage.
AllPendingRows == UNION {pending[n] : n \in Nodes}

DM2_NoLWW ==
    \A p1 \in all_ingested_points:
        \A p2 \in all_ingested_points:
            (/\ p1.metric_name = p2.metric_name
             /\ p1.tags = p2.tags
             /\ p1.timestamp = p2.timestamp
             /\ p1.request_id # p2.request_id
             /\ p1 \notin AllPendingRows
             /\ p2 \notin AllPendingRows)
            => (/\ p1 \in AllStoredRows
                /\ p2 \in AllStoredRows)

\* DM-3: No interpolation.
\*
\* The set of points in storage is always a subset of points that
\* were actually ingested. No synthetic points are ever created.
DM3_NoInterpolation ==
    AllStoredRows \subseteq all_ingested_points

\* DM-4: Deterministic timeseries_id.
\*
\* For any two rows anywhere in the system (stored or pending) with
\* the same tag set, timeseries_id is identical.
DM4_DeterministicTSID ==
    \A r1 \in AllStoredRows \union AllPendingRows:
        \A r2 \in AllStoredRows \union AllPendingRows:
            (r1.tags = r2.tags) => (r1.timeseries_id = r2.timeseries_id)

\* DM-5: timeseries_id persists through compaction without recomputation.
\*
\* After compaction, every row in the output split has the same
\* timeseries_id it had in the input splits. Since CompactSplits
\* unions the row sets without modifying them, this holds by
\* construction. We still state it explicitly as a checkable
\* invariant: every stored row's timeseries_id equals TSIDHash
\* of its tags (which is what was assigned at ingestion).
DM5_TSIDPersistence ==
    \A row \in AllStoredRows:
        row.timeseries_id = TSIDHash(row.tags)

\* Combined safety invariant.
Safety ==
    /\ DM1_PointPerRow
    /\ DM2_NoLWW
    /\ DM3_NoInterpolation
    /\ DM4_DeterministicTSID
    /\ DM5_TSIDPersistence

----
\* ---- Actions ----

\* IngestPoint: A node receives a data point and adds it to its pending batch.
\*
\* Models: MetricsDocProcessor receives a data point from an ingest request.
\* Each invocation uses a fresh request_id to model separate ingest requests.
\* The value field is fixed to 1 (abstract; real values vary).
IngestPoint(n, mn, ts_set, t) ==
    /\ next_request_id < RequestCountMax
    /\ LET point == MakePoint(mn, ts_set, t, 1, next_request_id)
       IN /\ pending' = [pending EXCEPT ![n] = pending[n] \union {point}]
          /\ all_ingested_points' = all_ingested_points \union {point}
          /\ next_request_id' = next_request_id + 1
    /\ UNCHANGED <<splits, next_split_id>>

\* FlushSplit: A node writes its entire pending batch as a new split.
\*
\* Models: MetricsIndexer commits a Parquet split via MetricsUploader
\* and MetricsPublisher. The batch becomes an immutable split in object
\* storage. The pending buffer is cleared.
\*
\* Precondition: the node has at least one pending point.
FlushSplit(n) ==
    /\ pending[n] # {}
    /\ LET new_split == [split_id |-> next_split_id, rows |-> pending[n]]
       IN /\ splits' = splits \union {new_split}
          /\ next_split_id' = next_split_id + 1
    /\ pending' = [pending EXCEPT ![n] = {}]
    /\ UNCHANGED <<all_ingested_points, next_request_id>>

\* CompactSplits: The compactor selects a non-empty subset of splits and
\* merges them into one new split. The input splits are removed from
\* storage and replaced by the merged split.
\*
\* Models: MergeExecutor performs a k-way merge of Parquet files.
\*
\* Critical properties enforced by this action:
\*   - All rows from input splits appear in the output (no data loss)
\*   - No new rows are created (no interpolation -- DM-3)
\*   - timeseries_id values are carried through unchanged (DM-5)
\*   - Rows from separate requests are not deduplicated (DM-2)
\*
\* The action takes the union of all rows from the selected splits.
\* This union preserves every row (including duplicates from separate
\* requests, since they have different request_id and are therefore
\* distinct set elements).
CompactSplits ==
    /\ Cardinality(splits) >= 2
    /\ \E selected \in SUBSET splits:
        /\ Cardinality(selected) >= 2
        /\ LET merged_rows == UNION {s.rows : s \in selected}
               new_split == [split_id |-> next_split_id, rows |-> merged_rows]
           IN /\ splits' = (splits \ selected) \union {new_split}
              /\ next_split_id' = next_split_id + 1
    /\ UNCHANGED <<pending, all_ingested_points, next_request_id>>

----
\* ---- Init and Next ----

Init ==
    /\ pending = [n \in Nodes |-> {}]
    /\ splits = {}
    /\ all_ingested_points = {}
    /\ next_split_id = 1
    /\ next_request_id = 1

Next ==
    \/ \E n \in Nodes, mn \in MetricNames, ts \in TagSets, t \in Timestamps:
        IngestPoint(n, mn, ts, t)
    \/ \E n \in Nodes:
        FlushSplit(n)
    \/ CompactSplits

----
\* ---- Specification ----

\* Weak fairness on Next ensures the system eventually makes progress
\* (points are eventually flushed, splits are eventually compacted).
Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

----
\* ---- Liveness Properties ----

\* Eventually, if a point is ingested, it reaches storage.
\* (With weak fairness on FlushSplit, pending batches are eventually flushed.)
Liveness ==
    \A n \in Nodes:
        [](pending[n] # {} => <>(pending[n] = {}))

====
