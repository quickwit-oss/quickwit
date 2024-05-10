// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Weak};
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
use quickwit_proto::ingest::router::{IngestRequestV2, IngestResponseV2, IngestRouterService};
use quickwit_proto::ingest::{CommitTypeV2, IngestV2Error, IngestV2Result, ShardState};
use quickwit_proto::types::{IndexUid, NodeId, ShardId, SourceId, SubrequestId};
use serde_json::{json, Value as JsonValue};
use tokio::sync::{Mutex, Semaphore};
use tracing::info;

use super::broadcast::LocalShardsUpdate;
use super::debouncing::{
    DebouncedGetOrCreateOpenShardsRequest, GetOrCreateOpenShardsRequestDebouncer,
};
use super::ingester::PERSIST_REQUEST_TIMEOUT;
use super::routing_table::RoutingTable;
use super::workbench::IngestWorkbench;
use super::IngesterPool;
use crate::{get_ingest_router_buffer_size, LeaderId};

/// Duration after which ingest requests time out with [`IngestV2Error::Timeout`].
pub(super) const INGEST_REQUEST_TIMEOUT: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(10)
} else {
    Duration::from_secs(35)
};

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
        }
    }

    pub fn subscribe(&self, event_broker: &EventBroker) {
        let weak_router_state = WeakRouterState(Arc::downgrade(&self.state));
        event_broker
            .subscribe::<LocalShardsUpdate>(weak_router_state.clone())
            .forever();
        event_broker
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

        for subrequest in workbench.subworkbenches.values().filter_map(|subworbench| {
            if subworbench.is_pending() {
                Some(&subworbench.subrequest)
            } else {
                None
            }
        }) {
            if !state_guard.routing_table.has_open_shards(
                &subrequest.index_id,
                &subrequest.source_id,
                ingester_pool,
                &mut debounced_request.closed_shards,
                unavailable_leaders,
            ) {
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
            info!(closed_shards=?debounced_request.closed_shards, "reporting closed shard(s) to
        control plane");
        }
        if !debounced_request.is_empty() && !unavailable_leaders.is_empty() {
            info!(unvailable_leaders=?unavailable_leaders, "reporting unavailable leader(s) to
        control plane");

            for unavailable_leader in unavailable_leaders.iter() {
                debounced_request
                    .unavailable_leaders
                    .push(unavailable_leader.to_string());
            }
        }
        debounced_request
    }

    async fn populate_routing_table_debounced(
        &mut self,
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
        &mut self,
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
        &mut self,
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

                        if persist_failure.reason() == PersistFailureReason::ShardClosed {
                            let shard_id = persist_failure.shard_id().clone();
                            let index_uid: IndexUid = persist_failure.index_uid().clone();
                            let source_id: SourceId = persist_failure.source_id;
                            closed_shards
                                .entry((index_uid, source_id))
                                .or_default()
                                .push(shard_id);
                        } else if persist_failure.reason() == PersistFailureReason::ShardNotFound {
                            let shard_id = persist_failure.shard_id().clone();
                            let index_uid: IndexUid = persist_failure.index_uid().clone();
                            let source_id: SourceId = persist_failure.source_id;
                            deleted_shards
                                .entry((index_uid, source_id))
                                .or_default()
                                .push(shard_id);
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
                    .delete_shards(&index_uid, source_id, &shard_ids);
            }
        }
    }

    async fn batch_persist(&mut self, workbench: &mut IngestWorkbench, commit_type: CommitTypeV2) {
        let debounced_request = self
            .make_get_or_create_open_shard_request(workbench, &self.ingester_pool)
            .await;

        self.populate_routing_table_debounced(workbench, debounced_request)
            .await;

        // List of subrequest IDs for which no shards are available to route the subrequests to.
        let mut no_shards_available_subrequest_ids = Vec::new();

        let mut per_leader_persist_subrequests: HashMap<&LeaderId, Vec<PersistSubrequest>> =
            HashMap::new();

        let state_guard = self.state.lock().await;

        // TODO: Here would be the most optimal place to split the body of the HTTP request into
        // lines, validate, transform and then pack the docs into compressed batches routed
        // to the right shards.

        for subrequest in workbench.pending_subrequests() {
            let Some(shard) = state_guard
                .routing_table
                .find_entry(&subrequest.index_id, &subrequest.source_id)
                .and_then(|entry| entry.next_open_shard_round_robin(&self.ingester_pool))
            else {
                no_shards_available_subrequest_ids.push(subrequest.subrequest_id);
                continue;
            };
            let persist_subrequest = PersistSubrequest {
                subrequest_id: subrequest.subrequest_id,
                index_uid: shard.index_uid.clone().into(),
                source_id: shard.source_id.clone(),
                shard_id: Some(shard.shard_id.clone()),
                doc_batch: subrequest.doc_batch.clone(),
            };
            per_leader_persist_subrequests
                .entry(&shard.leader_id)
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
            let Some(mut ingester) = self.ingester_pool.get(&leader_id) else {
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
        self.process_persist_results(workbench, persist_futures)
            .await;
    }

    async fn retry_batch_persist(
        &mut self,
        ingest_request: IngestRequestV2,
        max_num_attempts: usize,
    ) -> IngestV2Result<IngestResponseV2> {
        let commit_type = ingest_request.commit_type();
        let mut workbench = IngestWorkbench::new(ingest_request.subrequests, max_num_attempts);
        while !workbench.is_complete() {
            workbench.new_attempt();
            self.batch_persist(&mut workbench, commit_type).await;
        }
        workbench.into_ingest_result()
    }

    async fn ingest_timeout(
        &mut self,
        ingest_request: IngestRequestV2,
        timeout_duration: Duration,
    ) -> IngestV2Result<IngestResponseV2> {
        tokio::time::timeout(
            timeout_duration,
            self.retry_batch_persist(ingest_request, MAX_PERSIST_ATTEMPTS),
        )
        .await
        .map_err(|_| {
            let message = format!(
                "ingest request timed out after {} seconds",
                INGEST_REQUEST_TIMEOUT.as_secs()
            );
            IngestV2Error::Timeout(message)
        })?
    }

    pub async fn debug_info(&self) -> JsonValue {
        let state_guard = self.state.lock().await;
        let routing_table_json = state_guard.routing_table.debug_info();

        json!({
            "routing_table": routing_table_json,
        })
    }
}

#[async_trait]
impl IngestRouterService for IngestRouter {
    async fn ingest(
        &mut self,
        ingest_request: IngestRequestV2,
    ) -> IngestV2Result<IngestResponseV2> {
        let request_size_bytes = ingest_request.num_bytes();

        let mut gauge_guard = GaugeGuard::from_gauge(&MEMORY_METRICS.in_flight.ingest_router);
        gauge_guard.add(request_size_bytes as i64);

        let _permit = self
            .ingest_semaphore
            .clone()
            .try_acquire_many_owned(request_size_bytes as u32)
            .map_err(|_| IngestV2Error::TooManyRequests)?;

        self.ingest_timeout(ingest_request, INGEST_REQUEST_TIMEOUT)
            .await
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
            .delete_shards(&index_uid, &source_id, &deleted_shard_ids);
    }
}

pub(super) struct PersistRequestSummary {
    pub leader_id: NodeId,
    pub subrequest_ids: Vec<SubrequestId>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use quickwit_proto::control_plane::{
        GetOrCreateOpenShardsFailure, GetOrCreateOpenShardsFailureReason,
        GetOrCreateOpenShardsResponse, GetOrCreateOpenShardsSuccess, MockControlPlaneService,
    };
    use quickwit_proto::ingest::ingester::{
        IngesterServiceClient, MockIngesterService, PersistFailure, PersistResponse, PersistSuccess,
    };
    use quickwit_proto::ingest::router::IngestSubrequest;
    use quickwit_proto::ingest::{CommitTypeV2, DocBatchV2, Shard, ShardIds, ShardState};
    use quickwit_proto::types::{Position, SourceUid};
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
        let mut router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
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
        let mut router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
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
        let mut router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
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
        let mut router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
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
        let mut router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
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
                    reason: PersistFailureReason::RateLimited as i32,
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
        let mut router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
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
        let mut router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
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
        let mut router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
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
                    Some(DocBatchV2::for_test(["test-doc-foo", "test-doc-bar"]))
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
                        },
                        PersistSuccess {
                            subrequest_id: 1,
                            index_uid: Some(index_uid2_clone.clone()),
                            source_id: "test-source".to_string(),
                            shard_id: Some(ShardId::from(1)),
                            replication_position_inclusive: Some(Position::offset(0u64)),
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
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-foo", "test-doc-bar"])),
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
        router.ingest(ingest_request).await.unwrap();

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
        router.ingest(ingest_request).await.unwrap();
    }

    #[tokio::test]
    async fn test_router_ingest_retry() {
        let self_node_id = "test-router".into();
        let control_plane = ControlPlaneServiceClient::from_mock(MockControlPlaneService::new());
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let mut router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
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
                        reason: PersistFailureReason::RateLimited as i32,
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
        let router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
        );
        let event_broker = EventBroker::default();
        router.subscribe(&event_broker);
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
                    ingestion_rate: RateMibPerSec(0),
                },
                ShardInfo {
                    shard_id: ShardId::from(2),
                    shard_state: ShardState::Open,
                    ingestion_rate: RateMibPerSec(0),
                },
            ]),
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
}
