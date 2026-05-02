---- MODULE MergePipelineShutdown ----
\* Formal specification for the Parquet merge pipeline shutdown drain protocol.
\*
\* Models the two-phase shutdown of the ParquetMergePipeline supervisor:
\*   Phase 1: DisconnectMergePlanner — breaks the publisher→planner feedback
\*   Phase 2: RunFinalizeMergePolicyAndQuit — drains cold windows, planner exits
\* After both phases, in-flight merges complete and the pipeline shuts down.
\*
\* Key Invariants:
\*   NoSplitLoss      — every split that entered a merge is in published or in-flight
\*   NoDuplicateMerge  — no split appears in more than one in-flight merge
\*   FinalizeWithinBound — finalize emits at most MaxFinalizeOps operations
\*   ShutdownOnlyWhenDrained — shutdown only when no in-flight merges remain
\*
\* Liveness:
\*   ShutdownEventuallyCompletes — under weak fairness, shutdown eventually occurs
\*
\* TLA+-to-Rust Mapping:
\* | TLA+ Concept            | Rust Implementation                                       |
\* |-------------------------|-----------------------------------------------------------|
\* | planner_connected       | Publisher.parquet_merge_planner_mailbox_opt is Some        |
\* | DisconnectMergePlanner  | DisconnectMergePlanner message to Publisher                |
\* | RunFinalizeAndQuit      | RunFinalizeMergePolicyAndQuit message to Planner           |
\* | in_flight_merges        | MergeSchedulerService pending + executing merges           |
\* | cold_windows            | ParquetMergePlanner.scoped_young_splits                    |
\* | CompleteMerge           | Publisher receives ParquetSplitsUpdate with replaced_ids   |
\* | PlanMerge               | ParquetMergePlanner.plan_merges()                         |

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
    MaxIngests

\* ============================================================================
\* VARIABLES
\* ============================================================================

VARIABLES
    \* Whether the publisher sends feedback to the planner.
    \* TRUE initially; set to FALSE by DisconnectMergePlanner.
    planner_connected,

    \* Whether the planner is alive and processing.
    \* Set to FALSE after RunFinalizeAndQuit completes.
    planner_alive,

    \* Set of merge operation IDs currently in the pipeline.
    \* Each merge is a record [id |-> Nat, inputs |-> set of split IDs].
    in_flight_merges,

    \* Map: window -> set of split IDs available for merging.
    cold_windows,

    \* Set of all published split IDs (includes merge outputs).
    published_splits,

    \* Whether finalization has been requested.
    finalize_requested,

    \* Number of finalize merge operations emitted.
    finalize_ops_emitted,

    \* Whether the pipeline has completed shutdown.
    shutdown_complete,

    \* Counter for generating unique IDs.
    next_id,

    \* Total number of ingests performed (bounds state space).
    ingests_performed

vars == <<planner_connected, planner_alive, in_flight_merges,
          cold_windows, published_splits, finalize_requested,
          finalize_ops_emitted, shutdown_complete, next_id,
          ingests_performed>>

\* ============================================================================
\* TYPE INVARIANT
\* ============================================================================

TypeInvariant ==
    /\ planner_connected \in BOOLEAN
    /\ planner_alive \in BOOLEAN
    /\ in_flight_merges \subseteq (Nat \X SUBSET Nat)
    /\ \A w \in Windows : cold_windows[w] \subseteq Nat
    /\ published_splits \subseteq Nat
    /\ finalize_requested \in BOOLEAN
    /\ finalize_ops_emitted \in Nat
    /\ shutdown_complete \in BOOLEAN
    /\ next_id \in Nat
    /\ ingests_performed \in Nat

\* ============================================================================
\* HELPERS
\* ============================================================================

\* All split IDs currently in any in-flight merge.
AllInFlightSplits == UNION {m[2] : m \in in_flight_merges}

\* Number of in-flight merges.
NumInFlight == Cardinality(in_flight_merges)

\* Splits available in a window.
SplitsInWindow(w) == cold_windows[w]

\* ============================================================================
\* INITIAL STATE
\* ============================================================================

Init ==
    /\ planner_connected = TRUE
    /\ planner_alive = TRUE
    /\ in_flight_merges = {}
    /\ cold_windows = [w \in Windows |-> {}]
    /\ published_splits = {}
    /\ finalize_requested = FALSE
    /\ finalize_ops_emitted = 0
    /\ shutdown_complete = FALSE
    /\ next_id = 1
    /\ ingests_performed = 0

\* ============================================================================
\* ACTIONS
\* ============================================================================

\* Ingest a new split into a window (models publisher feedback or new ingest).
IngestSplit(w) ==
    /\ planner_alive
    /\ ingests_performed < MaxIngests
    /\ cold_windows' = [cold_windows EXCEPT ![w] = @ \union {next_id}]
    /\ published_splits' = published_splits \union {next_id}
    /\ next_id' = next_id + 1
    /\ ingests_performed' = ingests_performed + 1
    /\ UNCHANGED <<planner_connected, planner_alive, in_flight_merges,
                   finalize_requested, finalize_ops_emitted, shutdown_complete>>

\* Plan a normal merge: take MergeFactor splits from a window.
PlanMerge(w) ==
    /\ planner_alive
    /\ ~finalize_requested
    /\ Cardinality(SplitsInWindow(w)) >= MergeFactor
    /\ NumInFlight < MaxMerges
    /\ \E S \in SUBSET SplitsInWindow(w) :
        /\ Cardinality(S) = MergeFactor
        \* No split already in another in-flight merge.
        /\ S \cap AllInFlightSplits = {}
        /\ in_flight_merges' = in_flight_merges \union {<<next_id, S>>}
        /\ cold_windows' = [cold_windows EXCEPT ![w] = @ \ S]
        /\ next_id' = next_id + 1
        /\ UNCHANGED <<planner_connected, planner_alive, published_splits,
                       finalize_requested, finalize_ops_emitted,
                       shutdown_complete, ingests_performed>>

\* Complete an in-flight merge: remove from in-flight, publish output.
\* If planner is connected, feed output back to a window for further merging.
CompleteMerge(m) ==
    /\ m \in in_flight_merges
    /\ LET merge_id == m[1]
           output_id == next_id
       IN
        /\ in_flight_merges' = in_flight_merges \ {m}
        /\ published_splits' = published_splits \union {output_id}
        \* Feed back to planner if connected (picks an arbitrary window).
        /\ IF planner_connected /\ planner_alive
           THEN \E w \in Windows :
                cold_windows' = [cold_windows EXCEPT ![w] = @ \union {output_id}]
           ELSE cold_windows' = cold_windows
        /\ next_id' = next_id + 1
        /\ UNCHANGED <<planner_connected, planner_alive, finalize_requested,
                       finalize_ops_emitted, shutdown_complete, ingests_performed>>

\* Phase 1: Disconnect the merge planner from the publisher.
DisconnectMergePlanner ==
    /\ planner_connected
    /\ planner_alive
    /\ planner_connected' = FALSE
    /\ UNCHANGED <<planner_alive, in_flight_merges, cold_windows,
                   published_splits, finalize_requested, finalize_ops_emitted,
                   shutdown_complete, next_id, ingests_performed>>

\* Phase 2: Finalize cold windows and shut down planner.
\* Emits up to MaxFinalizeOps merge operations from cold windows with
\* fewer than MergeFactor splits (the "stragglers").
RunFinalizeAndQuit ==
    /\ planner_alive
    /\ ~planner_connected  \* Must disconnect first (Phase 1 before Phase 2).
    /\ ~finalize_requested
    /\ finalize_requested' = TRUE
    \* Emit finalize merges for windows with 2+ splits (< MergeFactor).
    /\ LET eligible == {w \in Windows :
            Cardinality(SplitsInWindow(w)) >= 2
            /\ Cardinality(SplitsInWindow(w)) < MergeFactor}
           to_finalize == IF Cardinality(eligible) > MaxFinalizeOps
                          THEN CHOOSE S \in SUBSET eligible :
                               Cardinality(S) = MaxFinalizeOps
                          ELSE eligible
       IN
        /\ finalize_ops_emitted' = Cardinality(to_finalize)
        \* Move finalized splits to in-flight.
        /\ in_flight_merges' = in_flight_merges \union
            {<<next_id + i, SplitsInWindow(w)>> :
             i \in 0..(Cardinality(to_finalize) - 1),
             w \in to_finalize}
        /\ cold_windows' = [w \in Windows |->
            IF w \in to_finalize THEN {} ELSE cold_windows[w]]
        /\ next_id' = next_id + Cardinality(to_finalize)
        \* Planner shuts down after emitting finalize operations.
        /\ planner_alive' = FALSE
        /\ UNCHANGED <<planner_connected, published_splits,
                       shutdown_complete, ingests_performed>>

\* Pipeline shutdown completes when planner is dead and no merges in flight.
DrainComplete ==
    /\ ~planner_alive
    /\ in_flight_merges = {}
    /\ ~shutdown_complete
    /\ shutdown_complete' = TRUE
    /\ UNCHANGED <<planner_connected, planner_alive, in_flight_merges,
                   cold_windows, published_splits, finalize_requested,
                   finalize_ops_emitted, next_id, ingests_performed>>

\* ============================================================================
\* NEXT-STATE RELATION
\* ============================================================================

Next ==
    \/ \E w \in Windows : IngestSplit(w)
    \/ \E w \in Windows : PlanMerge(w)
    \/ \E m \in in_flight_merges : CompleteMerge(m)
    \/ DisconnectMergePlanner
    \/ RunFinalizeAndQuit
    \/ DrainComplete

\* ============================================================================
\* SPECIFICATION
\* ============================================================================

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

\* ============================================================================
\* SAFETY INVARIANTS
\* ============================================================================

\* Every split that entered a merge is either published or still in-flight.
\* (No split is lost during the merge pipeline.)
NoSplitLoss ==
    \A m \in in_flight_merges :
        \A s \in m[2] : s \in published_splits

\* No split appears in more than one in-flight merge simultaneously.
NoDuplicateMerge ==
    \A m1, m2 \in in_flight_merges :
        m1 # m2 => m1[2] \cap m2[2] = {}

\* Finalize emits at most MaxFinalizeOps operations.
FinalizeWithinBound ==
    finalize_ops_emitted <= MaxFinalizeOps

\* Shutdown only occurs when all in-flight merges have completed.
ShutdownOnlyWhenDrained ==
    shutdown_complete => in_flight_merges = {}

\* ============================================================================
\* LIVENESS
\* ============================================================================

\* Under weak fairness, the pipeline eventually shuts down.
ShutdownEventuallyCompletes ==
    <>(shutdown_complete)

====
