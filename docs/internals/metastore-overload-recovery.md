# Metastore overload and cluster recovery

This document explains how PostgreSQL metastore overload can turn into a prolonged cluster outage, and how the recovery fixes work together.

The initial database contention and the recovery bugs are different problems. Database contention starts the incident. The control-plane, scheduler, and ingest-routing bugs amplify it by preventing the cluster from converging after the database becomes responsive again.

## Trigger and recovery amplifiers

For this incident, telemetry points to PostgreSQL contention as the trigger. The code review identified several independent mechanisms that could prolong recovery; it does not establish that every branch occurred or that they happened in a strict sequence.

```text
Concurrent metastore work contends in PostgreSQL
  -> the connection pool fills and requests queue in SQLx
  -> readiness probes and control-plane operations time out
     |-> periodic reconciliation can stop after a recoverable error
     |-> a control-plane restart can leave stale generation-owned work
     |-> a restarted indexer may not receive an unchanged plan
     `-> an ingest router may keep selecting a stale source route
  -> database health returns, but one or more recovery paths remain stuck
  -> indexing and ingestion stay degraded
```

## 1. Bound aggregate metastore admission

**Before.** The metastore server configured at least 100 in-flight requests for each generated RPC method, even when the PostgreSQL pool contained only 10 connections. The layer was called “shared,” but cloning `LoadShedLayer` for each method created an independent semaphore for every method.

**Why this prolongs an outage.** A PostgreSQL statement cannot execute until its request acquires a connection. The other requests wait in SQLx's acquisition queue, retain RPC capacity, and time out. If admitted calls contend on the same database lock, they can also hold all connections while making progress one at a time. `check_connectivity` accesses the pool directly and has no priority over this convoy, so repeated acquisition failures make the metastore node report itself as not ready. Retries can keep refilling the queue after the initiating traffic burst has ended.

**Fix.** `LoadShedLayer::new_shared` stores one semaphore shared by all RPC methods. PostgreSQL-backed metastores admit `max(1, max_connections - 1)` unary RPCs in aggregate. Excess requests fail before entering the metastore with `TooManyRequests`; when the pool has at least two connections, one connection remains as best-effort headroom for direct work such as readiness checks.

SQLx `PoolTimedOut` errors are deliberately not converted to `TooManyRequests`. A `TooManyRequests` produced by pre-dispatch load shedding is safely known to have no side effects. A pool acquisition can instead time out after an operation has already produced side effects, so its transaction outcome may be uncertain and the control plane may need to reload state.

**Proof.** `test_shared_load_shed_across_services`, `test_postgres_metastore_admission_matches_connection_pool_capacity`, and `test_metastore_admission_is_shared_across_rpc_methods` prove aggregate cross-method admission, the pool-size mapping, rejection before execution, and permit release.

## 2. Keep periodic reconciliation alive after safe overload rejection

**Before.** `ControlPlanLoop` scheduled its next run only after a completely successful iteration. A `TooManyRequests` emitted by pre-dispatch load shedding was correctly considered recoverable, so the control-plane actor stayed alive, but the handler returned without scheduling another iteration.

**Why this prolongs an outage.** A single transient overload response could silently stop the periodic chain responsible for shard rebalancing and indexing-plan reconciliation. Restoring database capacity did not restart that chain. The cluster then depended on an unrelated event or a later control-plane restart to converge.

**Fix.** The current control-plane generation schedules its next iteration after a recoverable error. Errors with an uncertain transaction outcome still restart the actor so that it reloads authoritative metastore state.

**Proof.** `test_control_plan_loop_continues_after_too_many_requests` verifies that overload rejection does not kill the periodic loop or force an actor respawn.

## 3. Isolate work owned by each control-plane generation

A supervised control plane can restart while reusing the same mailbox. Work started by the old actor must not act on the successor's freshly loaded model.

### Timers and cluster watchers

**Before.** Periodic timers and cluster-change watchers were not tied to an actor generation. Repeated failures could leave multiple watchers sending untagged rebalance messages to the shared mailbox. A watcher blocked on mailbox capacity could survive shutdown.

**Why this prolongs an outage.** One cluster event could trigger duplicate plan rebuilds and shard rebalances, multiplying metastore and indexer traffic during recovery. A timer queued just before shutdown could also run against the successor's state.

**Fix.** Each actor instance receives a monotonically increasing generation and an explicit shutdown signal. Control-loop messages emitted by timers and rebalance messages emitted by watchers carry that generation. Successors discard stale messages, and watchers can cancel even while waiting for mailbox capacity.

**Proof.** `test_control_plane_respawns_do_not_multiply_generation_owned_work` and `test_watch_indexers_cancels_while_rebalance_send_is_backpressured` cover duplicate work and cancellation under backpressure.

### Readiness during restart

**Before.** A generation that failed after becoming ready kept its readiness signal set to `true` until the supervisor created its replacement.

**Why this matters during recovery.** For up to a supervisor heartbeat, the stale signal misrepresented whether an initialized control-plane generation existed. In the current OSS server, this signal is awaited during initial setup and is not wired to ongoing node discovery, so this is a readiness-correctness fix rather than an independent explanation for the outage.

**Fix.** Finalization clears readiness immediately. A replacement generation starts not ready and becomes ready only after initialization finishes.

**Proof.** `test_control_plane_failure_clears_readiness_before_supervisor_respawn` verifies the transition.

### Delayed shard closures

**Before.** A rebalance opens replacement shards, waits for gossip propagation, and closes the old shards asynchronously. That delayed task could survive actor failure and send an untagged callback to the successor.

**Why this prolongs an outage.** The stale task could issue close RPCs selected from the previous generation's model. If its callback crossed the restart boundary, it could also apply those results to the successor's freshly loaded model, setting recovery back and creating more corrective rebalance work.

**Fix.** Delayed close work observes generation shutdown during its delay, RPC phase, and callback delivery. The callback carries its generation, and the handler ignores stale callbacks.

**Proof.** `test_delayed_rebalance_close_is_cancelled_on_generation_shutdown` and `test_control_plane_handles_current_and_ignores_stale_rebalance_shards_callback` cover cancellation and stale-callback rejection.

## 4. Reapply indexing plans to restarted processes

### Detect process generations, not only stable node IDs

**Before.** Scheduler equality considered indexer node IDs and indexing-task contents. An indexer can restart with the same `NodeId` but a new `generation_id`. If gossip still described the same tasks, the scheduler considered the plan unchanged and skipped dispatch.

**Why this prolongs an outage.** The replacement process could miss the current physical plan, including an empty plan used to keep dropped pipelines off a draining node. Pipeline recovery, shard draining, or backlog processing could then stall until an unrelated plan change occurred.

**Fix.** The scheduler records the process generation targeted by the latest dispatch attempt for each eligible indexer. A generation change causes the current plan to be sent again even when its tasks are unchanged. A restarted retiring or decommissioning node that is absent from the plan receives an empty plan.

**Proof.** `test_control_running_plan_reapplies_unchanged_plan_to_new_generation`, `test_rebuild_plan_reapplies_unchanged_plan_to_new_generation`, and `test_rebuild_plan_sends_empty_plan_to_new_generation_of_dropped_retiring_indexer` cover the replay cases.

### Bound every plan-application wait

**Before.** Applying a plan to a draining indexer had an outer timeout, but applying one to a ready indexer had no independent hard deadline. If its client future did not complete or honor the ordinary client timeout, it retained the rebuild completion guard indefinitely.

**Why this prolongs an outage.** Recovery work waiting for plan completion could remain blocked behind one unreachable indexer, even when the rest of the plan was delivered.

**Fix.** Every eligible indexer has a deadline. Ready indexers receive a 35-second deadline, which is longer than the ordinary 30-second client timeout; retiring and decommissioning indexers retain the shorter two-second deadline.

**Proof.** `test_apply_plan_times_out_for_all_eligible_indexers` and `test_ready_indexer_apply_allows_slow_success` prove bounded completion without rejecting a valid slow response.

## 5. Remove stale source routes after `NoShardsAvailable`

**Before.** A `NoShardsAvailable` persist response did not invalidate the route that selected the leader. The router expected a piggybacked routing update to refresh it, but an ingester omits a source for which it has no shard. The stale `(index, source) -> leader` entry therefore remained selectable. Routing bookkeeping also trusted the leader ID echoed in the response instead of the node that received the request.

**Why this prolongs an outage.** Retries repeatedly selected the same leader, received the same failure, and never asked the control plane for a replacement. The loop could continue after the ingester and control plane were otherwise healthy.

**Fix.** The router records a source-scoped tombstone for the actual RPC target. It makes that leader unavailable only for the affected index and source, while preserving healthy sources on the same ingester. The tombstone survives an eventually consistent control-plane response that still advertises the stale shard; an authoritative positive ingester update or a new index incarnation clears it.

**Proof.** `test_no_shards_available_uses_request_leader_and_clears_omitted_stale_entry`, `test_source_unavailable_tombstone_survives_stale_cp_merge`, and `test_no_shards_available_recovers_via_source_scoped_leader_exclusion` cover invalidation, stale updates, and recovery.

## 6. Preserve source-specific exclusions in shard-creation requests

**Before.** `GetOrCreateOpenShardsRequest.unavailable_leaders` applies to the whole request, while `NoShardsAvailable` applies to one source. A single batch could not express “exclude leader A for source A, but keep it eligible for source B.” Zero-capacity leaders were rejected locally but not reported to the control plane, so the control plane could also return the same unusable leader.

**Why this prolongs an outage.** Omitting the failed leader allowed the control plane to return the stale route again. Excluding it globally would disrupt healthy sources. For zero-capacity nodes, local rejection followed by an unchanged control-plane answer formed another no-progress loop.

**Fix.** Node-wide failures remain in a workbench-wide exclusion set, while leaders tombstoned after a source-specific `NoShardsAvailable` stay scoped to that exact index and source. Each subrequest receives the union of those scopes. The debouncer groups subrequests by identical normalized exclusion sets and sends separate control-plane requests. A failure in one group does not prevent later groups from being processed, so unrelated sources can still obtain replacement routes.

Splitting a request could duplicate request-wide closed-shard feedback and amplify control-plane work. The feedback is therefore attached to exactly one generated request.

**Proof.** `test_router_populate_routing_table_debounced_splits_source_exclusions`, `test_debounced_get_or_create_open_shards_request`, `test_ingester_node_is_routing_candidate`, and `test_has_any_routing_candidate` cover request grouping, one-time feedback, source and node scopes, and zero-capacity reporting.

## What these fixes do not guarantee

- The shared PostgreSQL limit provides best-effort headroom, not a reserved connection. Direct pool users bypass it, and streaming RPCs release the admission permit before the returned stream has finished using PostgreSQL.
- A one-connection pool cannot retain headroom because it must still admit one request.
- Admission bounds overload but does not fix slow SQL, external database failures, or the underlying database lock contention.
- Cancelling a delayed shard close cannot undo a close accepted at the exact shutdown boundary. The successor reloads metastore state to reconcile that race.
- A plan-application timeout releases the rebuild waiter; it does not prove that the indexer applied the plan. Periodic reconciliation can retry while the reported running plan or process generation still differs.

The combined effect is that excess unary metastore RPCs are rejected before joining SQLx's acquisition queue, periodic recovery continues after safe load shedding, the patched generation-owned tasks are fenced across restarts, replacement indexer generations are targeted with the current plan, and routers can request a replacement while excluding a stale source leader.
