// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt};
use quickwit_common::metrics::{GaugeGuard, MEMORY_METRICS};
use quickwit_common::pubsub::{EventBroker, EventSubscriber};
use quickwit_common::{rate_limited_error, rate_limited_warn};
use quickwit_proto::control_plane::{
    ControlPlaneService, ControlPlaneServiceClient, GetOrCreateOpenShardsRequest,
    GetOrCreateOpenShardsSubrequest,
};
use quickwit_proto::indexing::ShardPositionsUpdate;
use quickwit_proto::ingest::ingester::{
    IngesterService, PersistFailureReason, PersistRequest, PersistResponse, PersistSubrequest,
};
use quickwit_proto::ingest::router::{
    IngestFailureReason, IngestRequestV2, IngestResponseV2, IngestRouterService,
};
use quickwit_proto::ingest::{
    CommitTypeV2, IngestV2Error, IngestV2Result, RateLimitingCause, ShardState,
};
use quickwit_proto::types::{IndexUid, NodeId, ShardId, SourceId, SubrequestId};
use serde_json::{json, Value as JsonValue};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::error::Elapsed;
use tracing::{error, info};

use super::broadcast::LocalShardsUpdate;
use super::debouncing::{
    DebouncedGetOrCreateOpenShardsRequest, GetOrCreateOpenShardsRequestDebouncer,
};
use super::ingester::PERSIST_REQUEST_TIMEOUT;
use super::metrics::IngestResultMetrics;
use super::routing_table::{NextOpenShardError, RoutingTable};
use super::workbench::IngestWorkbench;
use super::{pending_subrequests, IngesterPool};
use crate::{get_ingest_router_buffer_size, LeaderId};

/// Duration after which ingest requests time out with [`IngestV2Error::Timeout`].
fn ingest_request_timeout() -> Duration {
    const DEFAULT_INGEST_REQUEST_TIMEOUT: Duration = if cfg!(any(test, feature = "testsuite")) {
        Duration::from_millis(10)
    } else {
        Duration::from_secs(35)
    };
    static TIMEOUT: OnceLock<Duration> = OnceLock::new();
    *TIMEOUT.get_or_init(|| {
        let duration_ms = quickwit_common::get_from_env(
            "QW_INGEST_REQUEST_TIMEOUT_MS",
            DEFAULT_INGEST_REQUEST_TIMEOUT.as_millis() as u64,
        );
        let minimum_ingest_request_timeout: Duration =
            PERSIST_REQUEST_TIMEOUT * (MAX_PERSIST_ATTEMPTS as u32) + Duration::from_secs(5);
        let requested_ingest_request_timeout = Duration::from_millis(duration_ms);
        if requested_ingest_request_timeout < minimum_ingest_request_timeout {
            error!(
                "ingest request timeout too short {}ms, setting to {}ms",
                requested_ingest_request_timeout.as_millis(),
                minimum_ingest_request_timeout.as_millis()
            );
            minimum_ingest_request_timeout
        } else {
            requested_ingest_request_timeout
        }
    })
}

const MAX_PERSIST_ATTEMPTS: usize = 5;

type PersistResult = (PersistRequestSummary, IngestV2Result<PersistResponse>);

#[derive(Clone)]
pub struct IngestRouter {
    self_node_id: NodeId,
    control_plane: ControlPlaneServiceClient,
    ingester_pool: IngesterPool,
    state: Arc<Mutex<RouterState>>,
    replication_factor: usize,
    // Limits the number of ingest requests in-flight to some capacity in bytes.
    ingest_semaphore: Arc<Semaphore>,
    event_broker: EventBroker,
}

struct RouterState {
    // Debounces `GetOrCreateOpenShardsRequest` requests to the control plane.
    debouncer: GetOrCreateOpenShardsRequestDebouncer,
    // Holds the routing table mapping index and source IDs to shards.
    routing_table: RoutingTable,
}

impl fmt::Debug for IngestRouter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IngestRouter")
            .field("self_node_id", &self.self_node_id)
            .field("replication_factor", &self.replication_factor)
            .finish()
    }
}

impl IngestRouter {
    pub fn new(
        self_node_id: NodeId,
        control_plane: ControlPlaneServiceClient,
        ingester_pool: IngesterPool,
        replication_factor: usize,
        event_broker: EventBroker,
    ) -> Self {
        let state = Arc::new(Mutex::new(RouterState {
            debouncer: GetOrCreateOpenShardsRequestDebouncer::default(),
            routing_table: RoutingTable {
                self_node_id: self_node_id.clone(),
                table: HashMap::default(),
            },
        }));
        let ingest_semaphore_permits = get_ingest_router_buffer_size().as_u64() as usize;
        let ingest_semaphore = Arc::new(Semaphore::new(ingest_semaphore_permits));

        Self {
            self_node_id,
            control_plane,
            ingester_pool,
            state,
            replication_factor,
            ingest_semaphore,
            event_broker,
        }
    }

    pub fn subscribe(&self) {
        let weak_router_state = WeakRouterState(Arc::downgrade(&self.state));
        self.event_broker
            .subscribe::<LocalShardsUpdate>(weak_router_state.clone())
            .forever();
        self.event_broker
            .subscribe::<ShardPositionsUpdate>(weak_router_state)
            .forever();
    }

    /// Inspects the shard table for each subrequest and returns the appropriate
    /// [`GetOrCreateOpenShardsRequest`] request if open shards do not exist for all the them.
    async fn make_get_or_create_open_shard_request(
        &self,
        workbench: &mut IngestWorkbench,
        ingester_pool: &IngesterPool,
    ) -> DebouncedGetOrCreateOpenShardsRequest {
        let mut debounced_request = DebouncedGetOrCreateOpenShardsRequest::default();

        // `closed_shards` and `unavailable_leaders` are populated by calls to `has_open_shards`
        // as we're looking for open shards to route the subrequests to.
        let unavailable_leaders: &mut HashSet<NodeId> = &mut workbench.unavailable_leaders;

        let mut state_guard = self.state.lock().await;

        for subrequest in pending_subrequests(&workbench.subworkbenches) {
            if !state_guard.routing_table.has_open_shards(
                &subrequest.index_id,
                &subrequest.source_id,
                ingester_pool,
                &mut debounced_request.closed_shards,
                unavailable_leaders,
            ) {
                // No shard available! Let's attempt to create one.
                let acquire_result = state_guard
                    .debouncer
                    .acquire(&subrequest.index_id, &subrequest.source_id);

                match acquire_result {
                    Ok(permit) => {
                        let subrequest = GetOrCreateOpenShardsSubrequest {
                            subrequest_id: subrequest.subrequest_id,
                            index_id: subrequest.index_id.clone(),
                            source_id: subrequest.source_id.clone(),
                        };
                        debounced_request.push_subrequest(subrequest, permit);
                    }
                    Err(barrier) => {
                        debounced_request.push_barrier(barrier);
                    }
                }
            }
        }
        drop(state_guard);

        if !debounced_request.is_empty() && !debounced_request.closed_shards.is_empty() {
            info!(closed_shards=?debounced_request.closed_shards, "reporting closed shard(s) to control plane");
        }
        if !debounced_request.is_empty() && !unavailable_leaders.is_empty() {
            info!(unavailable_leaders=?unavailable_leaders, "reporting unavailable leader(s) to control plane");

            for unavailable_leader in unavailable_leaders.iter() {
                debounced_request
                    .unavailable_leaders
                    .push(unavailable_leader.to_string());
            }
        }
        debounced_request
    }

    async fn populate_routing_table_debounced(
        &self,
        workbench: &mut IngestWorkbench,
        debounced_request: DebouncedGetOrCreateOpenShardsRequest,
    ) {
        let (request_opt, rendezvous) = debounced_request.take();

        if let Some(request) = request_opt {
            self.populate_routing_table(workbench, request).await;
        }
        rendezvous.wait().await;
    }

    /// Issues a [`GetOrCreateOpenShardsRequest`] request to the control plane and populates the
    /// shard table according to the response received.
    async fn populate_routing_table(
        &self,
        workbench: &mut IngestWorkbench,
        request: GetOrCreateOpenShardsRequest,
    ) {
        if request.subrequests.is_empty() {
            return;
        }
        let response_result = self.control_plane.get_or_create_open_shards(request).await;
        let response = match response_result {
            Ok(response) => response,
            Err(control_plane_error) => {
                if workbench.is_last_attempt() {
                    rate_limited_error!(
                        limit_per_min = 10,
                        "failed to get open shards from control plane: {control_plane_error}"
                    );
                } else {
                    rate_limited_warn!(
                        limit_per_min = 10,
                        "failed to get open shards from control plane: {control_plane_error}"
                    );
                };
                return;
            }
        };
        let mut state_guard = self.state.lock().await;

        for success in response.successes {
            state_guard.routing_table.replace_shards(
                success.index_uid().clone(),
                success.source_id,
                success.open_shards,
            );
        }
        drop(state_guard);

        for failure in response.failures {
            workbench.record_get_or_create_open_shards_failure(failure);
        }
    }

    async fn process_persist_results(
        &self,
        workbench: &mut IngestWorkbench,
        mut persist_futures: FuturesUnordered<impl Future<Output = PersistResult>>,
    ) {
        let mut closed_shards: HashMap<(IndexUid, SourceId), Vec<ShardId>> = HashMap::new();
        let mut deleted_shards: HashMap<(IndexUid, SourceId), Vec<ShardId>> = HashMap::new();

        while let Some((persist_summary, persist_result)) = persist_futures.next().await {
            match persist_result {
                Ok(persist_response) => {
                    for persist_success in persist_response.successes {
                        workbench.record_persist_success(persist_success);
                    }
                    for persist_failure in persist_response.failures {
                        workbench.record_persist_failure(&persist_failure);

                        match persist_failure.reason() {
                            PersistFailureReason::ShardClosed => {
                                let shard_id = persist_failure.shard_id().clone();
                                let index_uid: IndexUid = persist_failure.index_uid().clone();
                                let source_id: SourceId = persist_failure.source_id;
                                closed_shards
                                    .entry((index_uid, source_id))
                                    .or_default()
                                    .push(shard_id);
                            }
                            PersistFailureReason::ShardNotFound => {
                                let shard_id = persist_failure.shard_id().clone();
                                let index_uid: IndexUid = persist_failure.index_uid().clone();
                                let source_id: SourceId = persist_failure.source_id;
                                deleted_shards
                                    .entry((index_uid, source_id))
                                    .or_default()
                                    .push(shard_id);
                            }
                            PersistFailureReason::WalFull
                            | PersistFailureReason::ShardRateLimited => {
                                // Let's record that the shard is rate limited or that the ingester
                                // that hosts has its wal full.
                                //
                                // That way we will avoid to retry the persist request on the very
                                // same node.
                                let shard_id = persist_failure.shard_id().clone();
                                workbench.rate_limited_shards.insert(shard_id);
                            }
                            _ => {}
                        }
                    }
                }
                Err(persist_error) => {
                    if workbench.is_last_attempt() {
                        rate_limited_error!(
                            limit_per_min = 10,
                            "failed to persist records on ingester `{}`: {persist_error}",
                            persist_summary.leader_id
                        );
                    } else {
                        rate_limited_warn!(
                            limit_per_min = 10,
                            "failed to persist records on ingester `{}`: {persist_error}",
                            persist_summary.leader_id
                        );
                    }
                    workbench.record_persist_error(persist_error, persist_summary);
                }
            };
        }
        if !closed_shards.is_empty() || !deleted_shards.is_empty() {
            let mut state_guard = self.state.lock().await;

            for ((index_uid, source_id), shard_ids) in closed_shards {
                state_guard
                    .routing_table
                    .close_shards(&index_uid, source_id, &shard_ids);
            }
            for ((index_uid, source_id), shard_ids) in deleted_shards {
                state_guard
                    .routing_table
                    .delete_shards_by_id(&index_uid, source_id, &shard_ids);
            }
        }
    }

    async fn batch_persist(&self, workbench: &mut IngestWorkbench, commit_type: CommitTypeV2) {
        // Let's first create the shards that might be missing.
        let debounced_request = self
            .make_get_or_create_open_shard_request(workbench, &self.ingester_pool)
            .await;

        self.populate_routing_table_debounced(workbench, debounced_request)
            .await;

        // Subrequests for which no shards are available to route the subrequests to.
        let mut no_shards_available_subrequest_ids: Vec<SubrequestId> = Vec::new();
        // Subrequests for which the shards are rate limited.
        let mut rate_limited_subrequest_ids: Vec<SubrequestId> = Vec::new();

        let mut per_leader_persist_subrequests: HashMap<&LeaderId, Vec<PersistSubrequest>> =
            HashMap::new();

        let rate_limited_shards: &HashSet<ShardId> = &workbench.rate_limited_shards;
        let state_guard = self.state.lock().await;

        for subrequest in pending_subrequests(&workbench.subworkbenches) {
            let next_open_shard_res_opt = state_guard
                .routing_table
                .find_entry(&subrequest.index_id, &subrequest.source_id)
                .map(|entry| {
                    entry.next_open_shard_round_robin(&self.ingester_pool, rate_limited_shards)
                });
            let next_open_shard = match next_open_shard_res_opt {
                Some(Ok(next_open_shard)) => next_open_shard,
                Some(Err(NextOpenShardError::RateLimited)) => {
                    rate_limited_subrequest_ids.push(subrequest.subrequest_id);
                    continue;
                }
                Some(Err(NextOpenShardError::NoShardsAvailable)) | None => {
                    no_shards_available_subrequest_ids.push(subrequest.subrequest_id);
                    continue;
                }
            };
            let persist_subrequest = PersistSubrequest {
                subrequest_id: subrequest.subrequest_id,
                index_uid: next_open_shard.index_uid.clone().into(),
                source_id: next_open_shard.source_id.clone(),
                shard_id: Some(next_open_shard.shard_id.clone()),
                doc_batch: subrequest.doc_batch.clone(),
            };
            per_leader_persist_subrequests
                .entry(&next_open_shard.leader_id)
                .or_default()
                .push(persist_subrequest);
        }
        let persist_futures = FuturesUnordered::new();

        for (leader_id, subrequests) in per_leader_persist_subrequests {
            let leader_id: NodeId = leader_id.clone();
            let subrequest_ids: Vec<SubrequestId> = subrequests
                .iter()
                .map(|subrequest| subrequest.subrequest_id)
                .collect();
            let Some(ingester) = self.ingester_pool.get(&leader_id) else {
                no_shards_available_subrequest_ids.extend(subrequest_ids);
                continue;
            };
            let persist_summary = PersistRequestSummary {
                leader_id: leader_id.clone(),
                subrequest_ids,
            };
            let persist_request = PersistRequest {
                leader_id: leader_id.into(),
                subrequests,
                commit_type: commit_type as i32,
            };
            workbench.record_persist_request(&persist_request);

            let persist_future = async move {
                let persist_result = tokio::time::timeout(
                    PERSIST_REQUEST_TIMEOUT,
                    ingester.persist(persist_request),
                )
                .await
                .unwrap_or_else(|_| {
                    let message = format!(
                        "persist request timed out after {} seconds",
                        PERSIST_REQUEST_TIMEOUT.as_secs()
                    );
                    Err(IngestV2Error::Timeout(message))
                });
                (persist_summary, persist_result)
            };
            persist_futures.push(persist_future);
        }
        drop(state_guard);

        for subrequest_id in no_shards_available_subrequest_ids {
            workbench.record_no_shards_available(subrequest_id);
        }
        for subrequest_id in rate_limited_subrequest_ids {
            workbench.record_rate_limited(subrequest_id);
        }
        self.process_persist_results(workbench, persist_futures)
            .await;
    }

    async fn retry_batch_persist(
        &self,
        ingest_request: IngestRequestV2,
        max_num_attempts: usize,
    ) -> IngestResponseV2 {
        let commit_type = ingest_request.commit_type();
        let mut workbench = if matches!(commit_type, CommitTypeV2::Force | CommitTypeV2::WaitFor) {
            IngestWorkbench::new_with_publish_tracking(
                ingest_request.subrequests,
                max_num_attempts,
                self.event_broker.clone(),
            )
        } else {
            IngestWorkbench::new(ingest_request.subrequests, max_num_attempts)
        };
        while !workbench.is_complete() {
            workbench.new_attempt();
            self.batch_persist(&mut workbench, commit_type).await;
        }
        workbench.into_ingest_result().await
    }

    async fn ingest_timeout(
        &self,
        ingest_request: IngestRequestV2,
        timeout_duration: Duration,
    ) -> IngestV2Result<IngestResponseV2> {
        tokio::time::timeout(
            timeout_duration,
            self.retry_batch_persist(ingest_request, MAX_PERSIST_ATTEMPTS),
        )
        .await
        .map_err(|_elapsed: Elapsed| {
            let message = format!(
                "ingest request timed out after {} millis",
                timeout_duration.as_millis()
            );
            error!(
                "ingest request should not timeout as there is a timeout on independent ingest \
                 requests too. timeout after {}",
                timeout_duration.as_millis()
            );
            IngestV2Error::Timeout(message)
        })
    }

    pub async fn debug_info(&self) -> JsonValue {
        let state_guard = self.state.lock().await;
        let routing_table_json = state_guard.routing_table.debug_info();

        json!({
            "routing_table": routing_table_json,
        })
    }
}

fn update_ingest_metrics(ingest_result: &IngestV2Result<IngestResponseV2>, num_subrequests: usize) {
    let num_subrequests = num_subrequests as u64;
    let ingest_results_metrics: &IngestResultMetrics =
        &crate::ingest_v2::metrics::INGEST_V2_METRICS.ingest_results;
    match ingest_result {
        Ok(ingest_response) => {
            ingest_results_metrics
                .success
                .inc_by(ingest_response.successes.len() as u64);
            for ingest_failure in &ingest_response.failures {
                match ingest_failure.reason() {
                    IngestFailureReason::CircuitBreaker => {
                        ingest_results_metrics.circuit_breaker.inc();
                    }
                    IngestFailureReason::Unspecified => ingest_results_metrics.unspecified.inc(),
                    IngestFailureReason::IndexNotFound => {
                        ingest_results_metrics.index_not_found.inc()
                    }
                    IngestFailureReason::SourceNotFound => {
                        ingest_results_metrics.source_not_found.inc()
                    }
                    IngestFailureReason::Internal => ingest_results_metrics.internal.inc(),
                    IngestFailureReason::NoShardsAvailable => {
                        ingest_results_metrics.no_shards_available.inc()
                    }
                    IngestFailureReason::ShardRateLimited => {
                        ingest_results_metrics.shard_rate_limited.inc()
                    }
                    IngestFailureReason::WalFull => ingest_results_metrics.wal_full.inc(),
                    IngestFailureReason::Timeout => ingest_results_metrics.timeout.inc(),
                    IngestFailureReason::RouterLoadShedding => {
                        ingest_results_metrics.router_load_shedding.inc()
                    }
                    IngestFailureReason::LoadShedding => ingest_results_metrics.load_shedding.inc(),
                }
            }
        }
        Err(ingest_error) => match ingest_error {
            IngestV2Error::TooManyRequests(rate_limiting_cause) => match rate_limiting_cause {
                RateLimitingCause::RouterLoadShedding => {
                    ingest_results_metrics
                        .router_load_shedding
                        .inc_by(num_subrequests);
                }
                RateLimitingCause::LoadShedding => {
                    ingest_results_metrics.load_shedding.inc_by(num_subrequests)
                }
                RateLimitingCause::WalFull => {
                    ingest_results_metrics.wal_full.inc_by(num_subrequests);
                }
                RateLimitingCause::CircuitBreaker => {
                    ingest_results_metrics
                        .circuit_breaker
                        .inc_by(num_subrequests);
                }
                RateLimitingCause::ShardRateLimiting => {
                    ingest_results_metrics
                        .shard_rate_limited
                        .inc_by(num_subrequests);
                }
                RateLimitingCause::Unknown => {
                    ingest_results_metrics.unspecified.inc_by(num_subrequests);
                }
            },
            IngestV2Error::Timeout(_) => {
                ingest_results_metrics
                    .router_timeout
                    .inc_by(num_subrequests);
            }
            IngestV2Error::ShardNotFound { .. } => {
                ingest_results_metrics
                    .shard_not_found
                    .inc_by(num_subrequests);
            }
            IngestV2Error::Unavailable(_) => {
                ingest_results_metrics.unavailable.inc_by(num_subrequests);
            }
            IngestV2Error::Internal(_) => {
                ingest_results_metrics.internal.inc_by(num_subrequests);
            }
        },
    }
}

#[async_trait]
impl IngestRouterService for IngestRouter {
    async fn ingest(&self, ingest_request: IngestRequestV2) -> IngestV2Result<IngestResponseV2> {
        let request_size_bytes = ingest_request.num_bytes();

        let mut gauge_guard = GaugeGuard::from_gauge(&MEMORY_METRICS.in_flight.ingest_router);
        gauge_guard.add(request_size_bytes as i64);
        let num_subrequests = ingest_request.subrequests.len();

        let _permit = self
            .ingest_semaphore
            .clone()
            .try_acquire_many_owned(request_size_bytes as u32)
            .map_err(|_| IngestV2Error::TooManyRequests(RateLimitingCause::RouterLoadShedding))?;

        let ingest_res = if ingest_request.commit_type() == CommitTypeV2::Auto {
            self.ingest_timeout(ingest_request, ingest_request_timeout())
                .await
        } else {
            Ok(self
                .retry_batch_persist(ingest_request, MAX_PERSIST_ATTEMPTS)
                .await)
        };
        update_ingest_metrics(&ingest_res, num_subrequests);

        ingest_res
    }
}

#[derive(Clone)]
struct WeakRouterState(Weak<Mutex<RouterState>>);

#[async_trait]
impl EventSubscriber<LocalShardsUpdate> for WeakRouterState {
    async fn handle_event(&mut self, local_shards_update: LocalShardsUpdate) {
        let Some(state) = self.0.upgrade() else {
            return;
        };
        let leader_id = local_shards_update.leader_id;
        let index_uid = local_shards_update.source_uid.index_uid;
        let source_id = local_shards_update.source_uid.source_id;

        if local_shards_update.is_deletion {
            let mut state_guard = state.lock().await;

            if state_guard
                .routing_table
                .delete_shards_by_leader_id(&index_uid, &source_id, &leader_id)
                .is_some()
            {
                state_guard
                    .debouncer
                    .delete_if_released(&index_uid.index_id, &source_id);
            }
            return;
        };
        let mut open_shard_ids: Vec<ShardId> = Vec::new();
        let mut closed_shard_ids: Vec<ShardId> = Vec::new();

        for shard_info in local_shards_update.shard_infos {
            match shard_info.shard_state {
                ShardState::Open => open_shard_ids.push(shard_info.shard_id),
                ShardState::Closed => closed_shard_ids.push(shard_info.shard_id),
                ShardState::Unavailable | ShardState::Unspecified => {
                    // Ingesters never broadcast the `Unavailable`` state because, from their point
                    // of view, they are never unavailable.
                }
            }
        }
        let mut state_guard = state.lock().await;

        state_guard
            .routing_table
            .close_shards(&index_uid, &source_id, &closed_shard_ids);

        state_guard.routing_table.insert_open_shards(
            &leader_id,
            index_uid,
            source_id,
            &open_shard_ids,
        );
    }
}

#[async_trait]
impl EventSubscriber<ShardPositionsUpdate> for WeakRouterState {
    async fn handle_event(&mut self, shard_positions_update: ShardPositionsUpdate) {
        let Some(state) = self.0.upgrade() else {
            return;
        };
        let mut deleted_shard_ids: Vec<ShardId> = Vec::new();

        for (shard_id, shard_position) in shard_positions_update.updated_shard_positions {
            if shard_position.is_eof() {
                deleted_shard_ids.push(shard_id);
            }
        }
        let mut state_guard = state.lock().await;

        let index_uid = shard_positions_update.source_uid.index_uid;
        let source_id = shard_positions_update.source_uid.source_id;

        state_guard
            .routing_table
            .delete_shards_by_id(&index_uid, &source_id, &deleted_shard_ids);
    }
}

pub(super) struct PersistRequestSummary {
    pub leader_id: NodeId,
    pub subrequest_ids: Vec<SubrequestId>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use mockall::Sequence;
    use quickwit_proto::control_plane::{
        GetOrCreateOpenShardsFailure, GetOrCreateOpenShardsFailureReason,
        GetOrCreateOpenShardsResponse, GetOrCreateOpenShardsSuccess, MockControlPlaneService,
    };
    use quickwit_proto::ingest::ingester::{
        IngesterServiceClient, MockIngesterService, PersistFailure, PersistResponse, PersistSuccess,
    };
    use quickwit_proto::ingest::router::IngestSubrequest;
    use quickwit_proto::ingest::{
        CommitTypeV2, DocBatchV2, ParseFailure, ParseFailureReason, Shard, ShardIds, ShardState,
    };
    use quickwit_proto::types::{DocUid, Position, SourceUid};
    use tokio::task::yield_now;

    use super::*;
    use crate::ingest_v2::broadcast::ShardInfo;
    use crate::ingest_v2::routing_table::{RoutingEntry, RoutingTableEntry};
    use crate::ingest_v2::workbench::SubworkbenchFailure;
    use crate::RateMibPerSec;

    #[tokio::test]
    async fn test_router_make_get_or_create_open_shard_request() {
        let self_node_id = "test-router".into();
        let control_plane: ControlPlaneServiceClient =
            ControlPlaneServiceClient::from_mock(MockControlPlaneService::new());
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
            EventBroker::default(),
        );
        let mut workbench = IngestWorkbench::default();
        let (get_or_create_open_shard_request_opt, rendezvous) = router
            .make_get_or_create_open_shard_request(&mut workbench, &ingester_pool)
            .await
            .take();
        assert!(get_or_create_open_shard_request_opt.is_none());
        assert!(rendezvous.is_empty());

        let mut state_guard = router.state.lock().await;

        let index_uid: IndexUid = IndexUid::for_test("test-index-0", 0);
        state_guard.routing_table.table.insert(
            ("test-index-0".into(), "test-source".into()),
            RoutingTableEntry {
                index_uid: index_uid.clone(),
                source_id: "test-source".to_string(),
                local_shards: vec![
                    RoutingEntry {
                        index_uid: index_uid.clone(),
                        source_id: "test-source".to_string(),
                        shard_id: ShardId::from(1),
                        shard_state: ShardState::Closed,
                        leader_id: "test-ingester-0".into(),
                    },
                    RoutingEntry {
                        index_uid: index_uid.clone(),
                        source_id: "test-source".to_string(),
                        shard_id: ShardId::from(2),
                        shard_state: ShardState::Open,
                        leader_id: "test-ingester-0".into(),
                    },
                ],
                ..Default::default()
            },
        );
        drop(state_guard);

        let ingest_subrequests: Vec<IngestSubrequest> = vec![
            IngestSubrequest {
                subrequest_id: 0,
                index_id: "test-index-0".to_string(),
                source_id: "test-source".to_string(),
                ..Default::default()
            },
            IngestSubrequest {
                subrequest_id: 1,
                index_id: "test-index-1".to_string(),
                source_id: "test-source".to_string(),
                ..Default::default()
            },
        ];
        let mut workbench = IngestWorkbench::new(ingest_subrequests.clone(), 3);
        let (get_or_create_open_shard_request_opt, rendezvous_1) = router
            .make_get_or_create_open_shard_request(&mut workbench, &ingester_pool)
            .await
            .take();

        let get_or_create_open_shard_request = get_or_create_open_shard_request_opt.unwrap();
        assert_eq!(get_or_create_open_shard_request.subrequests.len(), 2);

        assert_eq!(rendezvous_1.num_permits(), 2);
        assert_eq!(rendezvous_1.num_barriers(), 0);

        let subrequest = &get_or_create_open_shard_request.subrequests[0];
        assert_eq!(subrequest.index_id, "test-index-0");
        assert_eq!(subrequest.source_id, "test-source");

        let subrequest = &get_or_create_open_shard_request.subrequests[1];
        assert_eq!(subrequest.index_id, "test-index-1");
        assert_eq!(subrequest.source_id, "test-source");

        assert_eq!(get_or_create_open_shard_request.closed_shards.len(), 1);
        assert_eq!(
            get_or_create_open_shard_request.closed_shards[0],
            ShardIds {
                index_uid: Some(IndexUid::for_test("test-index-0", 0)),
                source_id: "test-source".to_string(),
                shard_ids: vec![ShardId::from(1)],
            }
        );
        assert_eq!(
            get_or_create_open_shard_request.unavailable_leaders.len(),
            1
        );
        assert_eq!(
            get_or_create_open_shard_request.unavailable_leaders[0],
            "test-ingester-0"
        );
        assert_eq!(workbench.unavailable_leaders.len(), 1);

        let (get_or_create_open_shard_request_opt, rendezvous_2) = router
            .make_get_or_create_open_shard_request(&mut workbench, &ingester_pool)
            .await
            .take();

        assert!(get_or_create_open_shard_request_opt.is_none());

        assert_eq!(rendezvous_2.num_permits(), 0);
        assert_eq!(rendezvous_2.num_barriers(), 2);

        drop(rendezvous_1);
        drop(rendezvous_2);

        ingester_pool.insert("test-ingester-0".into(), IngesterServiceClient::mocked());
        {
            // Ingester-0 has been marked as unavailable due to the previous requests.
            let (get_or_create_open_shard_request_opt, _rendezvous) = router
                .make_get_or_create_open_shard_request(&mut workbench, &ingester_pool)
                .await
                .take();
            let get_or_create_open_shard_request = get_or_create_open_shard_request_opt.unwrap();
            assert_eq!(get_or_create_open_shard_request.subrequests.len(), 2);
            assert_eq!(workbench.unavailable_leaders.len(), 1);
            assert_eq!(
                workbench
                    .unavailable_leaders
                    .iter()
                    .next()
                    .unwrap()
                    .to_string(),
                "test-ingester-0"
            );
        }
        {
            // With a fresh workbench, the ingester is not marked as unavailable, and present in the
            // pool.
            let mut workbench = IngestWorkbench::new(ingest_subrequests, 3);
            let (get_or_create_open_shard_request_opt, _rendezvous) = router
                .make_get_or_create_open_shard_request(&mut workbench, &ingester_pool)
                .await
                .take();
            let get_or_create_open_shard_request = get_or_create_open_shard_request_opt.unwrap();
            assert_eq!(get_or_create_open_shard_request.subrequests.len(), 1);

            let subrequest = &get_or_create_open_shard_request.subrequests[0];
            assert_eq!(subrequest.index_id, "test-index-1");
            assert_eq!(subrequest.source_id, "test-source");

            assert_eq!(
                get_or_create_open_shard_request.unavailable_leaders.len(),
                0
            );
        }
    }

    #[tokio::test]
    async fn test_router_populate_routing_table() {
        let self_node_id = "test-router".into();

        let index_uid: IndexUid = IndexUid::for_test("test-index-0", 0);
        let index_uid2: IndexUid = IndexUid::for_test("test-index-1", 0);
        let mut mock_control_plane = MockControlPlaneService::new();
        mock_control_plane
            .expect_get_or_create_open_shards()
            .once()
            .returning(move |request| {
                assert_eq!(request.subrequests.len(), 4);

                let subrequest_0 = &request.subrequests[0];
                assert_eq!(subrequest_0.index_id, "test-index-0");
                assert_eq!(subrequest_0.source_id, "test-source");

                let subrequest_1 = &request.subrequests[1];
                assert_eq!(subrequest_1.index_id, "test-index-1");
                assert_eq!(subrequest_1.source_id, "test-source");

                let subrequest_2 = &request.subrequests[2];
                assert_eq!(subrequest_2.index_id, "index-not-found");
                assert_eq!(subrequest_2.source_id, "test-source");

                let subrequest_3 = &request.subrequests[3];
                assert_eq!(subrequest_3.index_id, "test-index-0");
                assert_eq!(subrequest_3.source_id, "source-not-found");

                let response = GetOrCreateOpenShardsResponse {
                    successes: vec![
                        GetOrCreateOpenShardsSuccess {
                            subrequest_id: 0,
                            index_uid: Some(index_uid.clone()),
                            source_id: "test-source".to_string(),
                            open_shards: vec![Shard {
                                index_uid: Some(index_uid.clone()),
                                source_id: "test-source".to_string(),
                                shard_id: Some(ShardId::from(1)),
                                shard_state: ShardState::Open as i32,
                                ..Default::default()
                            }],
                        },
                        GetOrCreateOpenShardsSuccess {
                            subrequest_id: 1,
                            index_uid: Some(index_uid2.clone()),
                            source_id: "test-source".to_string(),
                            open_shards: vec![
                                Shard {
                                    index_uid: Some(index_uid2.clone()),
                                    source_id: "test-source".to_string(),
                                    shard_id: Some(ShardId::from(1)),
                                    shard_state: ShardState::Open as i32,
                                    ..Default::default()
                                },
                                Shard {
                                    index_uid: Some(index_uid2.clone()),
                                    source_id: "test-source".to_string(),
                                    shard_id: Some(ShardId::from(2)),
                                    shard_state: ShardState::Open as i32,
                                    ..Default::default()
                                },
                            ],
                        },
                    ],
                    failures: vec![
                        GetOrCreateOpenShardsFailure {
                            subrequest_id: 2,
                            index_id: "index-not-found".to_string(),
                            source_id: "test-source".to_string(),
                            reason: GetOrCreateOpenShardsFailureReason::IndexNotFound as i32,
                        },
                        GetOrCreateOpenShardsFailure {
                            subrequest_id: 3,
                            index_id: "test-index-0".to_string(),
                            source_id: "source-not-found".to_string(),
                            reason: GetOrCreateOpenShardsFailureReason::SourceNotFound as i32,
                        },
                    ],
                };
                Ok(response)
            });
        let control_plane = ControlPlaneServiceClient::from_mock(mock_control_plane);
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
            EventBroker::default(),
        );
        let ingest_subrequests = vec![
            IngestSubrequest {
                subrequest_id: 0,
                index_id: "test-index-0".to_string(),
                source_id: "test-source".to_string(),
                ..Default::default()
            },
            IngestSubrequest {
                subrequest_id: 1,
                index_id: "test-index-1".to_string(),
                source_id: "test-source".to_string(),
                ..Default::default()
            },
            IngestSubrequest {
                subrequest_id: 2,
                index_id: "index-not-found".to_string(),
                source_id: "test-source".to_string(),
                ..Default::default()
            },
            IngestSubrequest {
                subrequest_id: 3,
                index_id: "source-not-found".to_string(),
                source_id: "test-source".to_string(),
                ..Default::default()
            },
        ];
        let mut workbench = IngestWorkbench::new(ingest_subrequests, 2);

        let get_or_create_open_shards_request = GetOrCreateOpenShardsRequest {
            subrequests: vec![
                GetOrCreateOpenShardsSubrequest {
                    subrequest_id: 0,
                    index_id: "test-index-0".to_string(),
                    source_id: "test-source".to_string(),
                },
                GetOrCreateOpenShardsSubrequest {
                    subrequest_id: 1,
                    index_id: "test-index-1".to_string(),
                    source_id: "test-source".to_string(),
                },
                GetOrCreateOpenShardsSubrequest {
                    subrequest_id: 2,
                    index_id: "index-not-found".to_string(),
                    source_id: "test-source".to_string(),
                },
                GetOrCreateOpenShardsSubrequest {
                    subrequest_id: 3,
                    index_id: "test-index-0".to_string(),
                    source_id: "source-not-found".to_string(),
                },
            ],
            closed_shards: Vec::new(),
            unavailable_leaders: Vec::new(),
        };
        router
            .populate_routing_table(&mut workbench, get_or_create_open_shards_request)
            .await;

        let state_guard = router.state.lock().await;
        let routing_table = &state_guard.routing_table;
        assert_eq!(routing_table.len(), 2);

        let routing_entry_0 = routing_table
            .find_entry("test-index-0", "test-source")
            .unwrap();
        assert_eq!(routing_entry_0.len(), 1);
        assert_eq!(routing_entry_0.all_shards()[0].shard_id, ShardId::from(1));

        let routing_entry_1 = routing_table
            .find_entry("test-index-1", "test-source")
            .unwrap();
        assert_eq!(routing_entry_1.len(), 2);
        assert_eq!(routing_entry_1.all_shards()[0].shard_id, ShardId::from(1));
        assert_eq!(routing_entry_1.all_shards()[1].shard_id, ShardId::from(2));

        let subworkbench = workbench.subworkbenches.get(&2).unwrap();
        assert!(matches!(
            subworkbench.last_failure_opt,
            Some(SubworkbenchFailure::IndexNotFound)
        ));

        let subworkbench = workbench.subworkbenches.get(&3).unwrap();
        assert!(matches!(
            subworkbench.last_failure_opt,
            Some(SubworkbenchFailure::SourceNotFound)
        ));
    }

    #[tokio::test]
    async fn test_router_batch_persist_records_no_shards_available_empty_routing_table() {
        let self_node_id = "test-router".into();
        let mut mock_control_plane = MockControlPlaneService::new();
        mock_control_plane
            .expect_get_or_create_open_shards()
            .once()
            .returning(move |request| {
                assert_eq!(request.subrequests.len(), 1);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_id, "test-index");
                assert_eq!(subrequest.source_id, "test-source");

                let response = GetOrCreateOpenShardsResponse::default();
                Ok(response)
            });
        let control_plane = ControlPlaneServiceClient::from_mock(mock_control_plane);
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
            EventBroker::default(),
        );
        let ingest_subrequests = vec![IngestSubrequest {
            subrequest_id: 0,
            index_id: "test-index".to_string(),
            source_id: "test-source".to_string(),
            ..Default::default()
        }];
        let mut workbench = IngestWorkbench::new(ingest_subrequests, 2);
        let commit_type = CommitTypeV2::Auto;
        router.batch_persist(&mut workbench, commit_type).await;

        let subworkbench = workbench.subworkbenches.get(&0).unwrap();
        assert!(matches!(
            subworkbench.last_failure_opt,
            Some(SubworkbenchFailure::NoShardsAvailable)
        ));
    }

    #[tokio::test]
    async fn test_router_batch_persist_records_no_shards_available_unavailable_ingester() {
        let self_node_id = "test-router".into();
        let mut mock_control_plane = MockControlPlaneService::new();
        mock_control_plane
            .expect_get_or_create_open_shards()
            .once()
            .returning(move |request| {
                assert_eq!(request.subrequests.len(), 1);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_id, "test-index");
                assert_eq!(subrequest.source_id, "test-source");

                let response = GetOrCreateOpenShardsResponse {
                    successes: vec![GetOrCreateOpenShardsSuccess {
                        subrequest_id: 0,
                        index_uid: Some(IndexUid::for_test("test-index", 0)),
                        source_id: "test-source".to_string(),
                        open_shards: vec![Shard {
                            index_uid: Some(IndexUid::for_test("test-index", 0)),
                            source_id: "test-source".to_string(),
                            shard_id: Some(ShardId::from(1)),
                            shard_state: ShardState::Open as i32,
                            leader_id: "test-ingester".into(),
                            ..Default::default()
                        }],
                    }],
                    ..Default::default()
                };
                Ok(response)
            });
        let control_plane = ControlPlaneServiceClient::from_mock(mock_control_plane);
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
            EventBroker::default(),
        );
        let ingest_subrequests = vec![IngestSubrequest {
            subrequest_id: 0,
            index_id: "test-index".to_string(),
            source_id: "test-source".to_string(),
            ..Default::default()
        }];
        let mut workbench = IngestWorkbench::new(ingest_subrequests, 2);
        let commit_type = CommitTypeV2::Auto;
        router.batch_persist(&mut workbench, commit_type).await;

        let subworkbench = workbench.subworkbenches.get(&0).unwrap();
        assert!(matches!(
            subworkbench.last_failure_opt,
            Some(SubworkbenchFailure::NoShardsAvailable)
        ));
    }

    #[tokio::test]
    async fn test_router_process_persist_results_record_persist_successes() {
        let self_node_id = "test-router".into();
        let control_plane = ControlPlaneServiceClient::from_mock(MockControlPlaneService::new());
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
            EventBroker::default(),
        );
        let ingest_subrequests = vec![IngestSubrequest {
            subrequest_id: 0,
            index_id: "test-index-0".to_string(),
            source_id: "test-source".to_string(),
            ..Default::default()
        }];
        let mut workbench = IngestWorkbench::new(ingest_subrequests, 2);
        let persist_futures = FuturesUnordered::new();
        let index_uid: IndexUid = IndexUid::for_test("test-index-0", 0);

        persist_futures.push(async move {
            let persist_summary = PersistRequestSummary {
                leader_id: "test-ingester-0".into(),
                subrequest_ids: vec![0],
            };
            let persist_result = Ok::<_, IngestV2Error>(PersistResponse {
                leader_id: "test-ingester-0".to_string(),
                successes: vec![PersistSuccess {
                    subrequest_id: 0,
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
                    ..Default::default()
                }],
                failures: Vec::new(),
            });
            (persist_summary, persist_result)
        });
        router
            .process_persist_results(&mut workbench, persist_futures)
            .await;

        let subworkbench = workbench.subworkbenches.get(&0).unwrap();
        assert!(matches!(
            subworkbench.persist_success_opt,
            Some(PersistSuccess { .. })
        ));
    }

    #[tokio::test]
    async fn test_router_process_persist_results_record_persist_failures() {
        let self_node_id = "test-router".into();
        let control_plane = ControlPlaneServiceClient::from_mock(MockControlPlaneService::new());
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
            EventBroker::default(),
        );
        let ingest_subrequests = vec![IngestSubrequest {
            subrequest_id: 0,
            index_id: "test-index-0".to_string(),
            source_id: "test-source".to_string(),
            ..Default::default()
        }];
        let mut workbench = IngestWorkbench::new(ingest_subrequests, 2);
        let persist_futures = FuturesUnordered::new();
        let index_uid: IndexUid = IndexUid::for_test("test-index-0", 0);

        persist_futures.push(async move {
            let persist_summary = PersistRequestSummary {
                leader_id: "test-ingester-0".into(),
                subrequest_ids: vec![0],
            };
            let persist_result = Ok::<_, IngestV2Error>(PersistResponse {
                leader_id: "test-ingester-0".to_string(),
                successes: Vec::new(),
                failures: vec![PersistFailure {
                    subrequest_id: 0,
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
                    reason: PersistFailureReason::ShardRateLimited as i32,
                }],
            });
            (persist_summary, persist_result)
        });
        router
            .process_persist_results(&mut workbench, persist_futures)
            .await;

        let subworkbench = workbench.subworkbenches.get(&0).unwrap();
        assert!(matches!(
            subworkbench.last_failure_opt,
            Some(SubworkbenchFailure::Persist { .. })
        ));
    }

    #[tokio::test]
    async fn test_router_process_persist_results_closes_and_deletes_shards() {
        let self_node_id = "test-router".into();
        let control_plane = ControlPlaneServiceClient::from_mock(MockControlPlaneService::new());
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
            EventBroker::default(),
        );
        let index_uid: IndexUid = IndexUid::for_test("test-index-0", 0);
        let mut state_guard = router.state.lock().await;
        state_guard.routing_table.replace_shards(
            index_uid.clone(),
            "test-source",
            vec![
                Shard {
                    index_uid: Some(index_uid.clone()),
                    shard_id: Some(ShardId::from(1)),
                    shard_state: ShardState::Open as i32,
                    leader_id: "test-ingester-0".to_string(),
                    ..Default::default()
                },
                Shard {
                    index_uid: Some(index_uid.clone()),
                    shard_id: Some(ShardId::from(2)),
                    shard_state: ShardState::Open as i32,
                    leader_id: "test-ingester-0".to_string(),
                    ..Default::default()
                },
            ],
        );
        drop(state_guard);

        let mut workbench = IngestWorkbench::new(Vec::new(), 2);
        let persist_futures = FuturesUnordered::new();

        persist_futures.push(async {
            let persist_summary = PersistRequestSummary {
                leader_id: "test-ingester-0".into(),
                subrequest_ids: vec![0],
            };
            let persist_result = Ok::<_, IngestV2Error>(PersistResponse {
                leader_id: "test-ingester-0".to_string(),
                successes: Vec::new(),
                failures: vec![
                    PersistFailure {
                        subrequest_id: 0,
                        index_uid: Some(index_uid.clone()),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(1)),
                        reason: PersistFailureReason::ShardNotFound as i32,
                    },
                    PersistFailure {
                        subrequest_id: 1,
                        index_uid: Some(index_uid.clone()),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(2)),
                        reason: PersistFailureReason::ShardClosed as i32,
                    },
                ],
            });
            (persist_summary, persist_result)
        });
        router
            .process_persist_results(&mut workbench, persist_futures)
            .await;

        let state_guard = router.state.lock().await;
        let routing_table_entry = state_guard
            .routing_table
            .find_entry("test-index-0", "test-source")
            .unwrap();
        assert_eq!(routing_table_entry.len(), 1);

        let shard = routing_table_entry.all_shards()[0];
        assert_eq!(shard.shard_id, ShardId::from(2));
        assert_eq!(shard.shard_state, ShardState::Closed);
    }

    #[tokio::test]
    async fn test_router_process_persist_results_does_not_remove_unavailable_leaders() {
        let self_node_id = "test-router".into();
        let control_plane = ControlPlaneServiceClient::from_mock(MockControlPlaneService::new());

        let ingester_pool = IngesterPool::default();
        ingester_pool.insert("test-ingester-0".into(), IngesterServiceClient::mocked());
        ingester_pool.insert("test-ingester-1".into(), IngesterServiceClient::mocked());

        let replication_factor = 1;
        let router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
            EventBroker::default(),
        );
        let ingest_subrequests = vec![
            IngestSubrequest {
                subrequest_id: 0,
                index_id: "test-index-0".to_string(),
                source_id: "test-source".to_string(),
                ..Default::default()
            },
            IngestSubrequest {
                subrequest_id: 1,
                index_id: "test-index-1".to_string(),
                source_id: "test-source".to_string(),
                ..Default::default()
            },
        ];
        let mut workbench = IngestWorkbench::new(ingest_subrequests, 2);
        let persist_futures = FuturesUnordered::new();

        persist_futures.push(async {
            let persist_summary = PersistRequestSummary {
                leader_id: "test-ingester-0".into(),
                subrequest_ids: vec![0],
            };
            let persist_result =
                Err::<_, IngestV2Error>(IngestV2Error::Internal("internal error".to_string()));
            (persist_summary, persist_result)
        });
        router
            .process_persist_results(&mut workbench, persist_futures)
            .await;

        let subworkbench = workbench.subworkbenches.get(&0).unwrap();
        assert!(matches!(
            &subworkbench.last_failure_opt,
            Some(SubworkbenchFailure::Internal)
        ));

        assert!(!workbench
            .unavailable_leaders
            .contains(&NodeId::from("test-ingester-1")));
        let persist_futures = FuturesUnordered::new();
        persist_futures.push(async {
            let persist_summary = PersistRequestSummary {
                leader_id: "test-ingester-1".into(),
                subrequest_ids: vec![1],
            };
            let persist_result =
                Err::<_, IngestV2Error>(IngestV2Error::Unavailable("connection error".to_string()));
            (persist_summary, persist_result)
        });
        router
            .process_persist_results(&mut workbench, persist_futures)
            .await;

        // We do not remove the leader from the pool.
        assert!(!ingester_pool.is_empty());
        // ... but we mark it as unavailable.
        assert!(workbench
            .unavailable_leaders
            .contains(&NodeId::from("test-ingester-1")));

        let subworkbench = workbench.subworkbenches.get(&1).unwrap();
        assert!(matches!(
            subworkbench.last_failure_opt,
            Some(SubworkbenchFailure::Unavailable)
        ));
    }

    #[tokio::test]
    async fn test_router_ingest() {
        let self_node_id = "test-router".into();
        let control_plane = ControlPlaneServiceClient::from_mock(MockControlPlaneService::new());
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
            EventBroker::default(),
        );
        let index_uid: IndexUid = IndexUid::for_test("test-index-0", 0);
        let index_uid2: IndexUid = IndexUid::for_test("test-index-1", 0);
        let mut state_guard = router.state.lock().await;
        state_guard.routing_table.replace_shards(
            index_uid.clone(),
            "test-source",
            vec![Shard {
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-ingester-0".to_string(),
                ..Default::default()
            }],
        );
        state_guard.routing_table.replace_shards(
            index_uid2.clone(),
            "test-source",
            vec![
                Shard {
                    index_uid: Some(index_uid2.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
                    shard_state: ShardState::Open as i32,
                    leader_id: "test-ingester-0".to_string(),
                    follower_id: Some("test-ingester-1".to_string()),
                    ..Default::default()
                },
                Shard {
                    index_uid: Some(index_uid2.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(2)),
                    shard_state: ShardState::Open as i32,
                    leader_id: "test-ingester-1".to_string(),
                    follower_id: Some("test-ingester-2".to_string()),
                    ..Default::default()
                },
            ],
        );
        drop(state_guard);

        let mut mock_ingester_0 = MockIngesterService::new();
        let index_uid_clone = index_uid.clone();
        let index_uid2_clone = index_uid2.clone();
        mock_ingester_0
            .expect_persist()
            .once()
            .returning(move |request| {
                assert_eq!(request.leader_id, "test-ingester-0");
                assert_eq!(request.subrequests.len(), 2);
                assert_eq!(request.commit_type(), CommitTypeV2::Auto);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 0);
                assert_eq!(subrequest.index_uid(), &index_uid_clone);
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id(), ShardId::from(1));
                assert_eq!(
                    subrequest.doc_batch,
                    Some(DocBatchV2::for_test(["", "test-doc-foo", "test-doc-bar"]))
                );

                let subrequest = &request.subrequests[1];
                assert_eq!(subrequest.subrequest_id, 1);
                assert_eq!(subrequest.index_uid(), &index_uid2_clone);
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id(), ShardId::from(1));
                assert_eq!(
                    subrequest.doc_batch,
                    Some(DocBatchV2::for_test(["test-doc-qux"]))
                );

                let response = PersistResponse {
                    leader_id: request.leader_id,
                    successes: vec![
                        PersistSuccess {
                            subrequest_id: 0,
                            index_uid: Some(index_uid_clone.clone()),
                            source_id: "test-source".to_string(),
                            shard_id: Some(ShardId::from(1)),
                            replication_position_inclusive: Some(Position::offset(1u64)),
                            num_persisted_docs: 2,
                            parse_failures: vec![ParseFailure {
                                doc_uid: Some(DocUid::for_test(0)),
                                reason: ParseFailureReason::InvalidJson as i32,
                                message: "invalid JSON".to_string(),
                            }],
                        },
                        PersistSuccess {
                            subrequest_id: 1,
                            index_uid: Some(index_uid2_clone.clone()),
                            source_id: "test-source".to_string(),
                            shard_id: Some(ShardId::from(1)),
                            replication_position_inclusive: Some(Position::offset(0u64)),
                            num_persisted_docs: 1,
                            parse_failures: Vec::new(),
                        },
                    ],
                    failures: Vec::new(),
                };
                Ok(response)
            });
        mock_ingester_0
            .expect_persist()
            .once()
            .returning(move |request| {
                assert_eq!(request.leader_id, "test-ingester-0");
                assert_eq!(request.subrequests.len(), 1);
                assert_eq!(request.commit_type(), CommitTypeV2::Auto);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 0);
                assert_eq!(subrequest.index_uid(), &index_uid);
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id(), ShardId::from(1));
                assert_eq!(
                    subrequest.doc_batch,
                    Some(DocBatchV2::for_test(["test-doc-moo", "test-doc-baz"]))
                );

                let response = PersistResponse {
                    leader_id: request.leader_id,
                    successes: vec![PersistSuccess {
                        subrequest_id: 0,
                        index_uid: Some(index_uid.clone()),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(1)),
                        replication_position_inclusive: Some(Position::offset(3u64)),
                        num_persisted_docs: 4,
                        parse_failures: Vec::new(),
                    }],
                    failures: Vec::new(),
                };
                Ok(response)
            });
        let ingester_0 = IngesterServiceClient::from_mock(mock_ingester_0);
        ingester_pool.insert("test-ingester-0".into(), ingester_0.clone());

        let mut mock_ingester_1 = MockIngesterService::new();
        mock_ingester_1
            .expect_persist()
            .once()
            .returning(move |request| {
                assert_eq!(request.leader_id, "test-ingester-1");
                assert_eq!(request.subrequests.len(), 1);
                assert_eq!(request.commit_type(), CommitTypeV2::Auto);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 1);
                assert_eq!(subrequest.index_uid(), &index_uid2);
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id(), ShardId::from(2));
                assert_eq!(
                    subrequest.doc_batch,
                    Some(DocBatchV2::for_test(["test-doc-tux"]))
                );

                let response = PersistResponse {
                    leader_id: request.leader_id,
                    successes: vec![PersistSuccess {
                        subrequest_id: 1,
                        index_uid: Some(index_uid2.clone()),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(2)),
                        replication_position_inclusive: Some(Position::offset(0u64)),
                        num_persisted_docs: 1,
                        parse_failures: Vec::new(),
                    }],
                    failures: Vec::new(),
                };
                Ok(response)
            });
        let ingester_1 = IngesterServiceClient::from_mock(mock_ingester_1);
        ingester_pool.insert("test-ingester-1".into(), ingester_1);

        let ingest_request = IngestRequestV2 {
            subrequests: vec![
                IngestSubrequest {
                    subrequest_id: 0,
                    index_id: "test-index-0".to_string(),
                    source_id: "test-source".to_string(),
                    doc_batch: Some(DocBatchV2::for_test(["", "test-doc-foo", "test-doc-bar"])),
                },
                IngestSubrequest {
                    subrequest_id: 1,
                    index_id: "test-index-1".to_string(),
                    source_id: "test-source".to_string(),
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-qux"])),
                },
            ],
            commit_type: CommitTypeV2::Auto as i32,
        };
        let response = router.ingest(ingest_request).await.unwrap();
        assert_eq!(response.successes.len(), 2);
        assert_eq!(response.failures.len(), 0);

        let parse_failures = &response.successes[0].parse_failures;
        assert_eq!(parse_failures.len(), 1);

        let parse_failure = &parse_failures[0];
        assert_eq!(parse_failure.doc_uid(), DocUid::for_test(0));
        assert_eq!(parse_failure.reason(), ParseFailureReason::InvalidJson);

        let ingest_request = IngestRequestV2 {
            subrequests: vec![
                IngestSubrequest {
                    subrequest_id: 0,
                    index_id: "test-index-0".to_string(),
                    source_id: "test-source".to_string(),
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-moo", "test-doc-baz"])),
                },
                IngestSubrequest {
                    subrequest_id: 1,
                    index_id: "test-index-1".to_string(),
                    source_id: "test-source".to_string(),
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-tux"])),
                },
            ],
            commit_type: CommitTypeV2::Auto as i32,
        };
        let response = router.ingest(ingest_request).await.unwrap();
        assert_eq!(response.successes.len(), 2);
        assert_eq!(response.failures.len(), 0);
    }

    #[tokio::test]
    async fn test_router_ingest_retry() {
        let self_node_id = "test-router".into();
        let control_plane = ControlPlaneServiceClient::from_mock(MockControlPlaneService::new());
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
            EventBroker::default(),
        );
        let mut state_guard = router.state.lock().await;
        let index_uid: IndexUid = IndexUid::for_test("test-index-0", 0);
        state_guard.routing_table.replace_shards(
            index_uid.clone(),
            "test-source",
            vec![Shard {
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-ingester-0".to_string(),
                ..Default::default()
            }],
        );
        drop(state_guard);

        let mut mock_ingester_0 = MockIngesterService::new();
        let index_uid_clone = index_uid.clone();
        mock_ingester_0
            .expect_persist()
            .once()
            .returning(move |request| {
                assert_eq!(request.leader_id, "test-ingester-0");
                assert_eq!(request.subrequests.len(), 1);
                assert_eq!(request.commit_type(), CommitTypeV2::Auto);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 0);
                assert_eq!(subrequest.index_uid(), &index_uid_clone);
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id(), ShardId::from(1));
                assert_eq!(
                    subrequest.doc_batch,
                    Some(DocBatchV2::for_test(["test-doc-foo"]))
                );

                let response = PersistResponse {
                    leader_id: request.leader_id,
                    successes: Vec::new(),
                    failures: vec![PersistFailure {
                        subrequest_id: 0,
                        index_uid: Some(index_uid_clone.clone()),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(1)),
                        reason: PersistFailureReason::Timeout as i32,
                    }],
                };
                Ok(response)
            });
        mock_ingester_0
            .expect_persist()
            .once()
            .returning(move |request| {
                assert_eq!(request.leader_id, "test-ingester-0");
                assert_eq!(request.subrequests.len(), 1);
                assert_eq!(request.commit_type(), CommitTypeV2::Auto);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 0);
                assert_eq!(subrequest.index_uid(), &index_uid);
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id(), ShardId::from(1));
                assert_eq!(
                    subrequest.doc_batch,
                    Some(DocBatchV2::for_test(["test-doc-foo"]))
                );

                let response = PersistResponse {
                    leader_id: request.leader_id,
                    successes: vec![PersistSuccess {
                        subrequest_id: 0,
                        index_uid: Some(index_uid.clone()),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(1)),
                        replication_position_inclusive: Some(Position::offset(0u64)),
                        num_persisted_docs: 1,
                        parse_failures: Vec::new(),
                    }],
                    failures: Vec::new(),
                };
                Ok(response)
            });
        let ingester_0 = IngesterServiceClient::from_mock(mock_ingester_0);
        ingester_pool.insert("test-ingester-0".into(), ingester_0.clone());

        let ingest_request = IngestRequestV2 {
            subrequests: vec![IngestSubrequest {
                subrequest_id: 0,
                index_id: "test-index-0".to_string(),
                source_id: "test-source".to_string(),
                doc_batch: Some(DocBatchV2::for_test(["test-doc-foo"])),
            }],
            commit_type: CommitTypeV2::Auto as i32,
        };
        router.ingest(ingest_request).await.unwrap();
    }

    #[tokio::test]
    async fn test_router_updates_routing_table_on_chitchat_events() {
        let self_node_id = "test-router".into();
        let control_plane = ControlPlaneServiceClient::from_mock(MockControlPlaneService::new());
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let event_broker = EventBroker::default();
        let router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
            event_broker.clone(),
        );
        router.subscribe();
        let index_uid: IndexUid = IndexUid::for_test("test-index-0", 0);

        let mut state_guard = router.state.lock().await;
        state_guard.routing_table.replace_shards(
            index_uid.clone(),
            "test-source",
            vec![Shard {
                index_uid: Some(index_uid.clone()),
                shard_id: Some(ShardId::from(1)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-ingester".to_string(),
                ..Default::default()
            }],
        );
        drop(state_guard);

        let local_shards_update = LocalShardsUpdate {
            leader_id: "test-ingester".into(),
            source_uid: SourceUid {
                index_uid: index_uid.clone(),
                source_id: "test-source".to_string(),
            },
            shard_infos: BTreeSet::from_iter([
                ShardInfo {
                    shard_id: ShardId::from(1),
                    shard_state: ShardState::Closed,
                    short_term_ingestion_rate: RateMibPerSec(0),
                    long_term_ingestion_rate: RateMibPerSec(0),
                },
                ShardInfo {
                    shard_id: ShardId::from(2),
                    shard_state: ShardState::Open,
                    short_term_ingestion_rate: RateMibPerSec(0),
                    long_term_ingestion_rate: RateMibPerSec(0),
                },
            ]),
            is_deletion: false,
        };
        event_broker.publish(local_shards_update);

        // Yield so that the event is processed.
        yield_now().await;

        let state_guard = router.state.lock().await;
        let shards = state_guard
            .routing_table
            .find_entry("test-index-0", "test-source")
            .unwrap()
            .all_shards();
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0].shard_id, ShardId::from(1));
        assert_eq!(shards[0].shard_state, ShardState::Closed);
        assert_eq!(shards[1].shard_id, ShardId::from(2));
        assert_eq!(shards[1].shard_state, ShardState::Open);
        drop(state_guard);

        let shard_positions_update = ShardPositionsUpdate {
            source_uid: SourceUid {
                index_uid: index_uid.clone(),
                source_id: "test-source".to_string(),
            },
            updated_shard_positions: vec![(ShardId::from(1), Position::eof(0u64))],
        };
        event_broker.publish(shard_positions_update);

        // Yield so that the event is processed.
        yield_now().await;

        let state_guard = router.state.lock().await;
        let shards = state_guard
            .routing_table
            .find_entry("test-index-0", "test-source")
            .unwrap()
            .all_shards();
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].shard_id, ShardId::from(2));
        drop(state_guard);
    }

    #[tokio::test]
    async fn test_router_debug_info() {
        let self_node_id = "test-router".into();
        let control_plane = ControlPlaneServiceClient::from_mock(MockControlPlaneService::new());
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
            EventBroker::default(),
        );
        let index_uid_0: IndexUid = IndexUid::for_test("test-index-0", 0);
        let index_uid_1: IndexUid = IndexUid::for_test("test-index-1", 0);

        let mut state_guard = router.state.lock().await;
        state_guard.routing_table.replace_shards(
            index_uid_0.clone(),
            "test-source",
            vec![Shard {
                index_uid: Some(index_uid_0.clone()),
                shard_id: Some(ShardId::from(1)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-ingester".to_string(),
                ..Default::default()
            }],
        );
        state_guard.routing_table.replace_shards(
            index_uid_1.clone(),
            "test-source",
            vec![Shard {
                index_uid: Some(index_uid_1.clone()),
                shard_id: Some(ShardId::from(2)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-ingester".to_string(),
                ..Default::default()
            }],
        );
        drop(state_guard);

        let debug_info = router.debug_info().await;
        let routing_table = &debug_info["routing_table"];
        assert_eq!(routing_table.as_object().unwrap().len(), 2);

        assert_eq!(routing_table["test-index-0"].as_array().unwrap().len(), 1);
        assert_eq!(routing_table["test-index-1"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_router_does_not_retry_rate_limited_shards() {
        // We avoid retrying a shard limited shard at the scale of a workbench.
        let self_node_id = "test-router".into();
        let control_plane = ControlPlaneServiceClient::from_mock(MockControlPlaneService::new());
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
            EventBroker::default(),
        );
        let mut state_guard = router.state.lock().await;
        let index_uid: IndexUid = IndexUid::for_test("test-index-0", 0);

        state_guard.routing_table.replace_shards(
            index_uid.clone(),
            "test-source",
            vec![
                Shard {
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
                    shard_state: ShardState::Open as i32,
                    leader_id: "test-ingester-0".to_string(),
                    ..Default::default()
                },
                Shard {
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(2)),
                    shard_state: ShardState::Open as i32,
                    leader_id: "test-ingester-0".to_string(),
                    ..Default::default()
                },
            ],
        );
        drop(state_guard);

        // We have two shards.
        // - shard 1 is rate limited
        // - shard 2 is timeout.
        // We expect a retry on shard 2 that is then successful.
        let mut seq = Sequence::new();

        let mut mock_ingester_0 = MockIngesterService::new();
        mock_ingester_0
            .expect_persist()
            .times(1)
            .returning(move |request| {
                assert_eq!(request.leader_id, "test-ingester-0");
                assert_eq!(request.commit_type(), CommitTypeV2::Auto);
                assert_eq!(request.subrequests.len(), 1);
                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 0);
                let index_uid = subrequest.index_uid().clone();
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id(), ShardId::from(1));
                assert_eq!(
                    subrequest.doc_batch,
                    Some(DocBatchV2::for_test(["test-doc-foo"]))
                );

                let response = PersistResponse {
                    leader_id: request.leader_id,
                    successes: Vec::new(),
                    failures: vec![PersistFailure {
                        subrequest_id: 0,
                        index_uid: Some(index_uid),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(1)),
                        reason: PersistFailureReason::ShardRateLimited as i32,
                    }],
                };
                Ok(response)
            })
            .in_sequence(&mut seq);

        mock_ingester_0
            .expect_persist()
            .times(1)
            .returning(move |request| {
                assert_eq!(request.leader_id, "test-ingester-0");
                assert_eq!(request.commit_type(), CommitTypeV2::Auto);
                assert_eq!(request.subrequests.len(), 1);
                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 0);
                let index_uid = subrequest.index_uid().clone();
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id(), ShardId::from(2));
                assert_eq!(
                    subrequest.doc_batch,
                    Some(DocBatchV2::for_test(["test-doc-foo"]))
                );

                let response = PersistResponse {
                    leader_id: request.leader_id,
                    successes: Vec::new(),
                    failures: vec![PersistFailure {
                        subrequest_id: 0,
                        index_uid: Some(index_uid),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(1)),
                        reason: PersistFailureReason::Timeout as i32,
                    }],
                };
                Ok(response)
            })
            .in_sequence(&mut seq);

        mock_ingester_0
            .expect_persist()
            .times(1)
            .returning(move |request| {
                assert_eq!(request.leader_id, "test-ingester-0");
                assert_eq!(request.commit_type(), CommitTypeV2::Auto);
                assert_eq!(request.subrequests.len(), 1);
                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 0);
                let index_uid = subrequest.index_uid().clone();
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id(), ShardId::from(2));
                assert_eq!(
                    subrequest.doc_batch,
                    Some(DocBatchV2::for_test(["test-doc-foo"]))
                );

                let response = PersistResponse {
                    leader_id: request.leader_id,
                    successes: vec![PersistSuccess {
                        subrequest_id: 0,
                        index_uid: Some(index_uid),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(1)),
                        num_persisted_docs: 1,
                        replication_position_inclusive: Some(Position::offset(0u64)),
                        parse_failures: Vec::new(),
                    }],
                    failures: Vec::new(),
                };
                Ok(response)
            })
            .in_sequence(&mut seq);

        let ingester_0 = IngesterServiceClient::from_mock(mock_ingester_0);
        ingester_pool.insert("test-ingester-0".into(), ingester_0.clone());

        let ingest_request = IngestRequestV2 {
            subrequests: vec![IngestSubrequest {
                subrequest_id: 0,
                index_id: "test-index-0".to_string(),
                source_id: "test-source".to_string(),
                doc_batch: Some(DocBatchV2::for_test(["test-doc-foo"])),
            }],
            commit_type: CommitTypeV2::Auto as i32,
        };
        router.ingest(ingest_request).await.unwrap();
    }

    #[tokio::test]
    async fn test_router_returns_rate_limited_failure() {
        let self_node_id = "test-router".into();
        let control_plane = ControlPlaneServiceClient::from_mock(MockControlPlaneService::new());
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
            EventBroker::default(),
        );
        let mut state_guard = router.state.lock().await;
        let index_uid: IndexUid = IndexUid::for_test("test-index-0", 0);

        state_guard.routing_table.replace_shards(
            index_uid.clone(),
            "test-source",
            vec![Shard {
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-ingester-0".to_string(),
                ..Default::default()
            }],
        );
        drop(state_guard);

        let mut mock_ingester_0 = MockIngesterService::new();
        mock_ingester_0
            .expect_persist()
            .times(1)
            .returning(move |request| {
                assert_eq!(request.leader_id, "test-ingester-0");
                assert_eq!(request.commit_type(), CommitTypeV2::Auto);
                assert_eq!(request.subrequests.len(), 1);
                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 0);
                let index_uid = subrequest.index_uid().clone();
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id(), ShardId::from(1));
                assert_eq!(
                    subrequest.doc_batch,
                    Some(DocBatchV2::for_test(["test-doc-foo"]))
                );

                let response = PersistResponse {
                    leader_id: request.leader_id,
                    successes: Vec::new(),
                    failures: vec![PersistFailure {
                        subrequest_id: 0,
                        index_uid: Some(index_uid),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(1)),
                        reason: PersistFailureReason::ShardRateLimited as i32,
                    }],
                };
                Ok(response)
            });
        let ingester_0 = IngesterServiceClient::from_mock(mock_ingester_0);
        ingester_pool.insert("test-ingester-0".into(), ingester_0.clone());

        let ingest_request = IngestRequestV2 {
            subrequests: vec![IngestSubrequest {
                subrequest_id: 0,
                index_id: "test-index-0".to_string(),
                source_id: "test-source".to_string(),
                doc_batch: Some(DocBatchV2::for_test(["test-doc-foo"])),
            }],
            commit_type: CommitTypeV2::Auto as i32,
        };
        let ingest_response = router.ingest(ingest_request).await.unwrap();
        assert_eq!(ingest_response.successes.len(), 0);
        assert_eq!(ingest_response.failures.len(), 1);
        assert_eq!(
            ingest_response.failures[0].reason(),
            IngestFailureReason::ShardRateLimited
        );
    }
}
