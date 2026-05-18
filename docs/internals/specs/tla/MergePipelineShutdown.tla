---- MODULE MergePipelineShutdown ----
\* Formal specification for the Parquet merge pipeline lifecycle, including
\* graceful shutdown AND crash/restart recovery.
\*
\* Models the actor pipeline:
\*   Planner -> Scheduler -> Downloader -> Executor -> Uploader -> Publisher
\*
\* Two shutdown variants are explored:
\*   1. Graceful: DisconnectMergePlanner -> RunFinalizeAndQuit -> drain.
\*   2. Crash: any Crash action invalidates in-memory state at any time.
\*      Restart re-seeds the planner from durable metastore state.
\*
\* The CompleteMerge transition is split into two phases to expose the
\* non-atomic boundary in production:
\*   1. UploadMergeOutput      - merge output written to object store
\*   2. PublishMergeAndFeedback - metastore replace + planner feedback
\* A Crash between (1) and (2) leaves the upload as an orphan in object
\* storage, with inputs still durable in the metastore. Subsequent restart
\* re-merges the same inputs (orphan eventually GC'd).
\*
\* Key Safety Invariants:
\*   TypeInvariant            - well-typed state
\*   RowsConserved            - sum(rows in published splits) = total ingested rows
\*                              (the strong "no data loss, no duplication" claim)
\*   NoSplitLoss              - every input of an in-flight merge is still in
\*                              published_splits (durable until publish step)
\*   NoDuplicateMerge         - no split appears in two concurrent in-flight merges
\*   NoOrphanInPlanner        - no split is simultaneously in cold_windows and
\*                              in_flight_merges (planner can't re-merge a locked split)
\*   NoOrphanWhenConnected    - while planner is alive AND connected to publisher,
\*                              every immature published split is reachable from
\*                              cold_windows or in_flight_merges. (Disconnect or
\*                              crash relaxes this; Restart re-establishes it.)
\*   LeakIsObjectStoreOnly    - orphan_outputs and published_splits are disjoint
\*                              (orphans are S3 garbage, never durable data loss)
\*   FinalizeWithinBound      - finalize emits at most MaxFinalizeOps operations
\*   BoundedWriteAmp          - every published split has merge_ops <= MaxMergeOps
\*   ShutdownOnlyWhenDrained  - graceful shutdown only when no merges in flight
\*   MP1_LevelHomogeneity     - all inputs of a merge share the same level
\*
\* Action Properties:
\*   RestartReSeedsAllImmature - every Restart transition lands in a state where
\*                               every immature published split is in cold_windows.
\*                               This is the formal claim that fetch_immature_splits
\*                               correctly recovers from any prior crash or shutdown.
\*
\* Liveness Properties:
\*   ShutdownEventuallyCompletes - in any run that doesn't restart, finalizing
\*                                 eventually leads to shutdown_complete.
\*   NoPersistentOrphan          - any orphan (split published immature but not
\*                                 in planner or in-flight) is eventually re-tracked
\*                                 (or matures, or merges away). Captures the
\*                                 cross-lifetime claim that a split is never
\*                                 *permanently* invisible to the planner.
\*
\* TLA+-to-Rust Mapping:
\* | TLA+ Concept              | Rust Implementation                                  |
\* |---------------------------|------------------------------------------------------|
\* | planner_connected         | Publisher.parquet_merge_planner_mailbox_opt is Some  |
\* | DisconnectMergePlanner    | DisconnectMergePlanner message to Publisher          |
\* | RunFinalizeAndQuit        | RunFinalizeMergePolicyAndQuit to ParquetMergePlanner |
\* | in_flight_merges          | MergeSchedulerService pending + executing            |
\* | cold_windows              | ParquetMergePlanner.scoped_young_splits              |
\* | UploadMergeOutput         | ParquetMergeExecutor uploads to object storage       |
\* | PublishMergeAndFeedback   | Publisher.publish_splits replaces in metastore       |
\* | orphan_outputs            | Object-store blobs without a metastore reference     |
\* | Crash + Restart           | Process death + ParquetMergePipeline::initialize     |
\* | (Restart re-seed)         | ParquetMergePipeline::fetch_immature_splits          |

EXTENDS Integers, FiniteSets, TLC

\* ============================================================================
\* CONSTANTS
\* ============================================================================

CONSTANTS
    \* Maximum concurrent in-flight merge operations.
    MaxMerges,

    \* Maximum finalize merge operations emitted at shutdown.
    MaxFinalizeOps,

    \* Set of window identifiers.
    Windows,

    \* Minimum number of splits to trigger a normal merge.
    MergeFactor,

    \* Maximum number of ingested splits (bounds state space).
    MaxIngests,

    \* Maximum merge_ops level for any published split. Splits with
    \* merge_ops >= MaxMergeOps are mature and never re-merged.
    MaxMergeOps,

    \* Maximum number of crash events (bounds state space).
    MaxCrashes,

    \* Maximum number of Restart events. Each Restart represents a fresh
    \* process invocation re-seeding from durable metastore state. Bounding
    \* this lets us model multiple process lifetimes without unbounded TLC
    \* exploration.
    MaxRestarts

\* ============================================================================
\* VARIABLES
\* ============================================================================

VARIABLES
    \* Whether the publisher sends feedback to the planner.
    planner_connected,

    \* Whether the planner is alive and processing.
    planner_alive,

    \* Set of in-flight merge records:
    \* [id |-> Nat, inputs |-> SUBSET Nat, level |-> Nat,
    \*  window |-> Window, uploaded |-> BOOLEAN, output_id |-> Nat].
    \* When uploaded = FALSE, output_id = 0 (sentinel).
    in_flight_merges,

    \* Map: window -> set of immature split IDs available for merging.
    cold_windows,

    \* Set of split IDs durable in the metastore (queryable).
    published_splits,

    \* Function: split_id -> row count.
    split_rows,

    \* Function: split_id -> merge_ops level (0 for ingested splits).
    split_merge_ops,

    \* Function: split_id -> window assignment.
    split_window,

    \* Set of split IDs uploaded to object storage but never published
    \* to the metastore (orphaned by a crash between upload and publish).
    \* These are invisible to query but consume storage until GC.
    orphan_outputs,

    \* Whether finalization has been requested.
    finalize_requested,

    \* Number of finalize merge operations emitted.
    finalize_ops_emitted,

    \* Whether the pipeline has completed graceful shutdown.
    shutdown_complete,

    \* Counter for generating unique IDs.
    next_id,

    \* Total number of ingests performed (bounds state space).
    ingests_performed,

    \* Number of crashes (bounds state space).
    crashes_performed,

    \* Number of restarts (bounds state space).
    restarts_performed,

    \* Ghost: cumulative rows ever ingested. Used by RowsConserved.
    total_ingested_rows

vars == <<planner_connected, planner_alive, in_flight_merges, cold_windows,
          published_splits, split_rows, split_merge_ops, split_window,
          orphan_outputs, finalize_requested, finalize_ops_emitted,
          shutdown_complete, next_id, ingests_performed, crashes_performed,
          restarts_performed, total_ingested_rows>>

\* ============================================================================
\* HELPERS
\* ============================================================================

\* Sum of rows across a set of split IDs (uses split_rows function).
RECURSIVE SumRows(_)
SumRows(S) ==
    IF S = {} THEN 0
    ELSE LET s == CHOOSE x \in S : TRUE
         IN split_rows[s] + SumRows(S \ {s})

\* All split IDs currently held by any in-flight merge (as inputs).
AllInFlightInputs == UNION {m.inputs : m \in in_flight_merges}

\* Number of in-flight merges.
NumInFlight == Cardinality(in_flight_merges)

\* All split IDs currently visible to the planner.
AllPlannerSplits == UNION {cold_windows[w] : w \in Windows}

\* Splits available in a window.
SplitsInWindow(w) == cold_windows[w]

\* Splits in window w grouped by merge_ops level.
SplitsAtLevel(w, lvl) ==
    {s \in cold_windows[w] : split_merge_ops[s] = lvl}

\* The set of distinct merge_ops levels present in window w.
LevelsInWindow(w) ==
    {split_merge_ops[s] : s \in cold_windows[w]}

\* Whether a split is mature (will not be re-merged).
IsMature(s) == split_merge_ops[s] >= MaxMergeOps

\* All immature published splits.
ImmaturePublished ==
    {s \in published_splits : ~IsMature(s)}

\* Set of *orphans*: immature published splits that the planner has lost
\* track of (neither in cold_windows nor in any in-flight merge). Used by
\* the NoPersistentOrphan liveness property.
\*
\* These splits remain queryable (they're in published_splits, durable in
\* metastore) but are stuck — no future compaction can run on them until
\* a Restart re-seeds the planner.
OrphanSet ==
    {s \in ImmaturePublished :
        /\ s \notin AllPlannerSplits
        /\ s \notin AllInFlightInputs}

\* ============================================================================
\* TYPE INVARIANT
\* ============================================================================

KnownIds == 1..next_id

TypeInvariant ==
    /\ planner_connected \in BOOLEAN
    /\ planner_alive \in BOOLEAN
    /\ \A m \in in_flight_merges :
        /\ m.id \in Nat
        /\ m.inputs \subseteq Nat
        /\ m.level \in Nat
        /\ m.window \in Windows
        /\ m.uploaded \in BOOLEAN
        /\ m.output_id \in Nat
    /\ \A w \in Windows : cold_windows[w] \subseteq Nat
    /\ published_splits \subseteq Nat
    /\ orphan_outputs \subseteq Nat
    /\ finalize_requested \in BOOLEAN
    /\ finalize_ops_emitted \in Nat
    /\ shutdown_complete \in BOOLEAN
    /\ next_id \in Nat
    /\ ingests_performed \in Nat
    /\ crashes_performed \in Nat
    /\ restarts_performed \in Nat
    /\ total_ingested_rows \in Nat

\* ============================================================================
\* INITIAL STATE
\* ============================================================================

Init ==
    /\ planner_connected = TRUE
    /\ planner_alive = TRUE
    /\ in_flight_merges = {}
    /\ cold_windows = [w \in Windows |-> {}]
    /\ published_splits = {}
    /\ split_rows = <<>>            \* empty function
    /\ split_merge_ops = <<>>
    /\ split_window = <<>>
    /\ orphan_outputs = {}
    /\ finalize_requested = FALSE
    /\ finalize_ops_emitted = 0
    /\ shutdown_complete = FALSE
    /\ next_id = 1
    /\ ingests_performed = 0
    /\ crashes_performed = 0
    /\ restarts_performed = 0
    /\ total_ingested_rows = 0

\* ============================================================================
\* ACTIONS
\* ============================================================================

\* Ingest a new split (1 row each, in window w).
\* Modeled as: writes to metastore, planner observes via feedback.
IngestSplit(w) ==
    /\ planner_alive
    /\ ingests_performed < MaxIngests
    /\ LET id == next_id IN
        /\ cold_windows' = [cold_windows EXCEPT ![w] = @ \union {id}]
        /\ published_splits' = published_splits \union {id}
        /\ split_rows' = split_rows @@ (id :> 1)
        /\ split_merge_ops' = split_merge_ops @@ (id :> 0)
        /\ split_window' = split_window @@ (id :> w)
        /\ next_id' = id + 1
        /\ ingests_performed' = ingests_performed + 1
        /\ total_ingested_rows' = total_ingested_rows + 1
    /\ UNCHANGED <<planner_connected, planner_alive, in_flight_merges,
                   orphan_outputs, finalize_requested, finalize_ops_emitted,
                   shutdown_complete, crashes_performed, restarts_performed>>

\* Plan a normal merge: take exactly MergeFactor splits at the same level
\* from a single window. Inputs are removed from cold_windows but remain
\* in published_splits (durable in metastore until publish-merge step).
PlanMerge(w) ==
    /\ planner_alive
    /\ ~finalize_requested
    /\ NumInFlight < MaxMerges
    /\ \E lvl \in 0..(MaxMergeOps - 1) :
        /\ Cardinality(SplitsAtLevel(w, lvl)) >= MergeFactor
        /\ \E S \in SUBSET SplitsAtLevel(w, lvl) :
            /\ Cardinality(S) = MergeFactor
            \* Splits available in cold_windows aren't in any in-flight merge
            \* by construction (they were removed at PlanMerge time).
            /\ in_flight_merges' = in_flight_merges \union
                {[id |-> next_id, inputs |-> S, level |-> lvl,
                  window |-> w, uploaded |-> FALSE, output_id |-> 0]}
            /\ cold_windows' = [cold_windows EXCEPT ![w] = @ \ S]
            /\ next_id' = next_id + 1
    /\ UNCHANGED <<planner_connected, planner_alive, published_splits,
                   split_rows, split_merge_ops, split_window, orphan_outputs,
                   finalize_requested, finalize_ops_emitted, shutdown_complete,
                   ingests_performed, crashes_performed, restarts_performed,
                   total_ingested_rows>>

\* Phase 1 of merge completion: upload output to object storage. The merge
\* output exists as a blob but is not yet referenced by the metastore.
\* A crash here leaks the blob (orphan_outputs) but loses no data.
UploadMergeOutput(m) ==
    /\ m \in in_flight_merges
    /\ ~m.uploaded
    /\ LET out == next_id
           m_uploaded == [m EXCEPT !.uploaded = TRUE, !.output_id = out]
       IN
        /\ in_flight_merges' = (in_flight_merges \ {m}) \union {m_uploaded}
        /\ next_id' = out + 1
        \* Ghost bookkeeping for the future output (rows = sum of inputs).
        /\ split_rows' = split_rows @@ (out :> SumRows(m.inputs))
        /\ split_merge_ops' = split_merge_ops @@ (out :> m.level + 1)
        /\ split_window' = split_window @@ (out :> m.window)
    /\ UNCHANGED <<planner_connected, planner_alive, cold_windows,
                   published_splits, orphan_outputs, finalize_requested,
                   finalize_ops_emitted, shutdown_complete, ingests_performed,
                   crashes_performed, restarts_performed, total_ingested_rows>>

\* Phase 2 of merge completion: atomic metastore replace + (optional) planner
\* feedback. The metastore transaction inserts the output and marks inputs
\* for deletion atomically.
PublishMergeAndFeedback(m) ==
    /\ m \in in_flight_merges
    /\ m.uploaded
    /\ LET out == m.output_id IN
        /\ in_flight_merges' = in_flight_merges \ {m}
        /\ published_splits' = (published_splits \ m.inputs) \union {out}
        \* Feed back to planner if connected, alive, and output is immature.
        /\ IF /\ planner_connected
              /\ planner_alive
              /\ split_merge_ops[out] < MaxMergeOps
           THEN cold_windows' =
                    [cold_windows EXCEPT ![m.window] = @ \union {out}]
           ELSE cold_windows' = cold_windows
    /\ UNCHANGED <<planner_connected, planner_alive, split_rows,
                   split_merge_ops, split_window, orphan_outputs,
                   finalize_requested, finalize_ops_emitted, shutdown_complete,
                   next_id, ingests_performed, crashes_performed,
                   restarts_performed, total_ingested_rows>>

\* Graceful shutdown phase 1: disconnect the planner from publisher feedback.
DisconnectMergePlanner ==
    /\ planner_connected
    /\ planner_alive
    /\ planner_connected' = FALSE
    /\ UNCHANGED <<planner_alive, in_flight_merges, cold_windows,
                   published_splits, split_rows, split_merge_ops, split_window,
                   orphan_outputs, finalize_requested, finalize_ops_emitted,
                   shutdown_complete, next_id, ingests_performed,
                   crashes_performed, restarts_performed, total_ingested_rows>>

\* Graceful shutdown phase 2: emit finalize merges from undersized cold
\* windows, then planner exits.
RunFinalizeAndQuit ==
    /\ planner_alive
    /\ ~planner_connected
    /\ ~finalize_requested
    /\ finalize_requested' = TRUE
    /\ LET eligible ==
            {w \in Windows :
                /\ Cardinality(SplitsInWindow(w)) >= 2
                /\ Cardinality(SplitsInWindow(w)) < MergeFactor}
           to_finalize ==
                IF Cardinality(eligible) > MaxFinalizeOps
                THEN CHOOSE S \in SUBSET eligible :
                        Cardinality(S) = MaxFinalizeOps
                ELSE eligible
       IN
        /\ finalize_ops_emitted' = Cardinality(to_finalize)
        \* Move finalize splits to in-flight (uploaded = FALSE; will follow
        \* the same Upload -> Publish phases).
        /\ in_flight_merges' =
            in_flight_merges \union
            {[id |-> next_id + i, inputs |-> SplitsInWindow(w),
              level |-> 0, window |-> w, uploaded |-> FALSE, output_id |-> 0]
             : i \in 0..(Cardinality(to_finalize) - 1), w \in to_finalize}
        /\ cold_windows' =
            [w \in Windows |->
                IF w \in to_finalize THEN {} ELSE cold_windows[w]]
        /\ next_id' = next_id + Cardinality(to_finalize)
        /\ planner_alive' = FALSE
    /\ UNCHANGED <<planner_connected, published_splits, split_rows,
                   split_merge_ops, split_window, orphan_outputs,
                   shutdown_complete, ingests_performed, crashes_performed,
                   restarts_performed, total_ingested_rows>>

\* Graceful shutdown completes when planner is dead and no merges in flight.
DrainComplete ==
    /\ ~planner_alive
    /\ in_flight_merges = {}
    /\ ~shutdown_complete
    /\ shutdown_complete' = TRUE
    /\ UNCHANGED <<planner_connected, planner_alive, in_flight_merges,
                   cold_windows, published_splits, split_rows, split_merge_ops,
                   split_window, orphan_outputs, finalize_requested,
                   finalize_ops_emitted, next_id, ingests_performed,
                   crashes_performed, restarts_performed, total_ingested_rows>>

\* Crash: process dies. All in-memory state is lost. Each in-flight merge
\* whose output was uploaded becomes an orphan blob.
\* Bound by MaxCrashes to keep state space finite.
Crash ==
    /\ crashes_performed < MaxCrashes
    /\ planner_alive
    /\ planner_alive' = FALSE
    /\ planner_connected' = FALSE
    \* Uploaded outputs become orphans.
    /\ orphan_outputs' = orphan_outputs \union
        {m.output_id : m \in {x \in in_flight_merges : x.uploaded}}
    /\ in_flight_merges' = {}
    /\ cold_windows' = [w \in Windows |-> {}]
    \* Finalize state lost on crash (planner forgets it requested finalize).
    /\ finalize_requested' = FALSE
    /\ finalize_ops_emitted' = 0
    /\ crashes_performed' = crashes_performed + 1
    /\ UNCHANGED <<published_splits, split_rows, split_merge_ops, split_window,
                   shutdown_complete, next_id, ingests_performed,
                   restarts_performed, total_ingested_rows>>

\* Restart: re-seed planner state from the durable metastore.
\* Models a fresh process invocation calling ParquetMergePipeline::initialize
\* and fetch_immature_splits. Restart can fire after Crash *or* after a
\* graceful shutdown — both correspond to "previous process is gone, new
\* process is starting up". This lets the model express the cross-lifetime
\* recovery claim (NoPersistentOrphan).
\*
\* in_flight_merges is reset on Restart: the previous process's in-flight
\* set is in-memory state that doesn't survive process death. (After a
\* Crash, in_flight_merges is already empty. After a graceful drain, it's
\* also empty. So in practice this is UNCHANGED, but we set it explicitly
\* for clarity.)
Restart ==
    /\ ~planner_alive
    /\ restarts_performed < MaxRestarts
    /\ planner_alive' = TRUE
    /\ planner_connected' = TRUE
    /\ in_flight_merges' = {}
    /\ cold_windows' =
        [w \in Windows |-> {s \in ImmaturePublished : split_window[s] = w}]
    \* New process lifetime — finalize state and shutdown_complete are reset.
    /\ finalize_requested' = FALSE
    /\ finalize_ops_emitted' = 0
    /\ shutdown_complete' = FALSE
    /\ restarts_performed' = restarts_performed + 1
    /\ UNCHANGED <<published_splits, split_rows, split_merge_ops, split_window,
                   orphan_outputs, next_id, ingests_performed, crashes_performed,
                   total_ingested_rows>>

\* ============================================================================
\* NEXT-STATE RELATION
\* ============================================================================

Next ==
    \/ \E w \in Windows : IngestSplit(w)
    \/ \E w \in Windows : PlanMerge(w)
    \/ \E m \in in_flight_merges : UploadMergeOutput(m)
    \/ \E m \in in_flight_merges : PublishMergeAndFeedback(m)
    \/ DisconnectMergePlanner
    \/ RunFinalizeAndQuit
    \/ DrainComplete
    \/ Crash
    \/ Restart

\* ============================================================================
\* SPECIFICATION
\* ============================================================================

\* Weak fairness on every action *except* Crash. Crash is adversarial: TLC
\* explores it but doesn't insist on it. Without Crash, graceful shutdown
\* must eventually complete.
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(\E w \in Windows : IngestSplit(w))
    /\ WF_vars(\E w \in Windows : PlanMerge(w))
    /\ WF_vars(\E m \in in_flight_merges : UploadMergeOutput(m))
    /\ WF_vars(\E m \in in_flight_merges : PublishMergeAndFeedback(m))
    /\ WF_vars(DisconnectMergePlanner)
    /\ WF_vars(RunFinalizeAndQuit)
    /\ WF_vars(DrainComplete)
    /\ WF_vars(Restart)

\* ============================================================================
\* SAFETY INVARIANTS
\* ============================================================================

\* The strong "no data loss, no duplication" invariant. Across all crash
\* paths, the rows visible via published_splits equal the cumulative rows
\* ever ingested.
RowsConserved ==
    SumRows(published_splits) = total_ingested_rows

\* Every input of an in-flight merge is still in published_splits. Inputs
\* are durable in the metastore until PublishMergeAndFeedback executes.
NoSplitLoss ==
    \A m \in in_flight_merges :
        m.inputs \subseteq published_splits

\* No split appears in two concurrent in-flight merges.
NoDuplicateMerge ==
    \A m1, m2 \in in_flight_merges :
        m1.id # m2.id => m1.inputs \cap m2.inputs = {}

\* The planner never sees a split that's also locked in an in-flight merge.
NoOrphanInPlanner ==
    AllPlannerSplits \cap AllInFlightInputs = {}

\* While the planner is alive AND connected to the publisher, every
\* immature published split is reachable. Disconnection (graceful shutdown)
\* and crash both relax this — Restart re-establishes the invariant by
\* re-seeding cold_windows from durable published_splits.
\*
\* Together with `Restart` setting `cold_windows = {immature splits}`, this
\* expresses the recoverability claim: an immature split is *never*
\* permanently lost from the planner's view, because (a) when connected,
\* the planner sees it; (b) when disconnected/crashed, Restart re-seeds.
NoOrphanWhenConnected ==
    (planner_alive /\ planner_connected) =>
        \A s \in ImmaturePublished :
            \/ s \in AllPlannerSplits
            \/ s \in AllInFlightInputs

\* Orphan outputs are object-store-only — never durable in the metastore.
LeakIsObjectStoreOnly ==
    orphan_outputs \cap published_splits = {}

\* All inputs of a single merge share the same level (MP-1).
MP1_LevelHomogeneity ==
    \A m \in in_flight_merges :
        \A s \in m.inputs :
            split_merge_ops[s] = m.level

\* Bounded write amplification: no published split exceeds MaxMergeOps.
BoundedWriteAmp ==
    \A s \in published_splits :
        split_merge_ops[s] <= MaxMergeOps

\* Finalize emits at most MaxFinalizeOps operations.
FinalizeWithinBound ==
    finalize_ops_emitted <= MaxFinalizeOps

\* Graceful shutdown only when drained.
ShutdownOnlyWhenDrained ==
    shutdown_complete => in_flight_merges = {}

\* ============================================================================
\* ACTION PROPERTIES
\* ============================================================================

\* Detect a Restart transition by the planner_alive flip from FALSE to TRUE.
\* (No other action sets planner_alive to TRUE.)
RestartTransition ==
    ~planner_alive /\ planner_alive'

\* Action property: every Restart transition lands in a state where every
\* immature published split is reachable via cold_windows. This is the
\* formal claim that fetch_immature_splits is correct: the post-state of a
\* Restart contains *all* immature splits that need further compaction.
\*
\* Note the use of primed variables (cold_windows', published_splits',
\* split_merge_ops', split_window') — the property is a predicate on the
\* (state, next-state) pair selected by the Restart transition.
RestartReSeedsAllImmature ==
    [][RestartTransition =>
        \A s \in published_splits' :
            split_merge_ops'[s] < MaxMergeOps =>
                s \in cold_windows'[split_window'[s]]
      ]_vars

\* ============================================================================
\* LIVENESS
\* ============================================================================

\* In a run without Crash, the pipeline eventually completes shutdown
\* once DisconnectMergePlanner and RunFinalizeAndQuit have been triggered.
\* (TLC reports as a property check.)
ShutdownEventuallyCompletes ==
    (crashes_performed = 0 /\ finalize_requested) ~> shutdown_complete

\* Cross-lifetime recoverability: any orphan (immature published split
\* that the planner has lost track of) is eventually re-tracked, matures,
\* or merges away — *as long as restart budget remains*. Holds because
\* Restart re-seeds cold_windows from published_splits, so each successive
\* process invocation re-establishes planner visibility for every
\* still-immature split.
\*
\* This is the formal claim against the failure mode "data lives in
\* metastore forever but never gets compacted because the planner forgot
\* about it". The `restarts_performed < MaxRestarts` guard reflects the
\* bounded-model artifact: production has effectively unbounded restarts,
\* so orphans always eventually clear; in the model they can persist past
\* the bound. Together with the action property RestartReSeedsAllImmature
\* (every Restart fully clears orphans), this verifies the cross-lifetime
\* claim under fairness.
NoPersistentOrphan ==
    [](OrphanSet # {} /\ restarts_performed < MaxRestarts
       ~> OrphanSet = {})

====
