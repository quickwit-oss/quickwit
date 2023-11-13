// Copyright (C) 2023 Quickwit, Inc.
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
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt};
use quickwit_proto::control_plane::{
    ControlPlaneService, ControlPlaneServiceClient, GetOrCreateOpenShardsRequest,
    GetOrCreateOpenShardsSubrequest,
};
use quickwit_proto::ingest::ingester::{
    IngesterService, PersistFailureReason, PersistRequest, PersistResponse, PersistSubrequest,
};
use quickwit_proto::ingest::router::{
    IngestRequestV2, IngestResponseV2, IngestRouterService, IngestSubrequest,
};
use quickwit_proto::ingest::{ClosedShards, CommitTypeV2, IngestV2Error, IngestV2Result};
use quickwit_proto::types::{IndexUid, NodeId, ShardId, SourceId, SubrequestId};
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use super::ingester::PERSIST_REQUEST_TIMEOUT;
use super::shard_table::ShardTable;
use super::workbench::IngestWorkbench;
use super::IngesterPool;

/// Duration after which ingest requests time out with [`IngestV2Error::Timeout`].
pub(super) const INGEST_REQUEST_TIMEOUT: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(10)
} else {
    Duration::from_secs(35)
};

const MAX_PERSIST_ATTEMPTS: usize = 5;

type LeaderId = String;

type PersistResult = (PersistRequestSummary, IngestV2Result<PersistResponse>);

#[derive(Clone)]
pub struct IngestRouter {
    self_node_id: NodeId,
    control_plane: ControlPlaneServiceClient,
    ingester_pool: IngesterPool,
    state: Arc<RwLock<RouterState>>,
    replication_factor: usize,
}

struct RouterState {
    shard_table: ShardTable,
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
        let state = RouterState {
            shard_table: ShardTable {
                self_node_id: self_node_id.clone(),
                table: HashMap::default(),
            },
        };
        Self {
            self_node_id,
            control_plane,
            ingester_pool,
            state: Arc::new(RwLock::new(state)),
            replication_factor,
        }
    }

    /// Inspects the shard table for each subrequest and returns the appropriate
    /// [`GetOrCreateOpenShardsRequest`] request if open shards do not exist for all the them.
    async fn make_get_or_create_open_shard_request(
        &self,
        subrequests: impl Iterator<Item = &IngestSubrequest>,
        ingester_pool: &IngesterPool,
    ) -> GetOrCreateOpenShardsRequest {
        let state_guard = self.state.read().await;
        let mut get_open_shards_subrequests = Vec::new();

        // `closed_shards` and `unavailable_leaders` are populated by calls to `has_open_shards`
        // as we're looking for open shards to route the subrequests to.
        let mut closed_shards: Vec<ClosedShards> = Vec::new();
        let mut unavailable_leaders: HashSet<NodeId> = HashSet::new();

        for subrequest in subrequests {
            if !state_guard.shard_table.has_open_shards(
                &subrequest.index_id,
                &subrequest.source_id,
                &mut closed_shards,
                ingester_pool,
                &mut unavailable_leaders,
            ) {
                let subrequest = GetOrCreateOpenShardsSubrequest {
                    subrequest_id: subrequest.subrequest_id,
                    index_id: subrequest.index_id.clone(),
                    source_id: subrequest.source_id.clone(),
                };
                get_open_shards_subrequests.push(subrequest);
            }
        }
        if !closed_shards.is_empty() {
            info!(
                "reporting {} closed shard(s) to control-plane",
                closed_shards.len()
            )
        }
        if !unavailable_leaders.is_empty() {
            info!(
                "reporting {} unavailable leader(s) to control-plane",
                unavailable_leaders.len()
            );
        }
        GetOrCreateOpenShardsRequest {
            subrequests: get_open_shards_subrequests,
            closed_shards,
            unavailable_leaders: unavailable_leaders.into_iter().map(Into::into).collect(),
        }
    }

    /// Issues a [`GetOrCreateOpenShardsRequest`] request to the control plane and populates the
    /// shard table according to the response received.
    async fn populate_shard_table(
        &mut self,
        workbench: &mut IngestWorkbench,
        request: GetOrCreateOpenShardsRequest,
    ) {
        if request.subrequests.is_empty() {
            return;
        }
        let response = match self.control_plane.get_or_create_open_shards(request).await {
            Ok(response) => response,
            Err(control_plane_error) => {
                if workbench.is_last_attempt() {
                    error!("failed to get open shards from control plane: {control_plane_error}");
                } else {
                    warn!("failed to get open shards from control plane: {control_plane_error}");
                };
                return;
            }
        };
        let mut state_guard = self.state.write().await;

        for success in response.successes {
            state_guard.shard_table.insert_shards(
                success.index_uid,
                success.source_id,
                success.open_shards,
            );
        }
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

        while let Some((persist_summary, persist_result)) = persist_futures.next().await {
            match persist_result {
                Ok(persist_response) => {
                    for persist_success in persist_response.successes {
                        workbench.record_persist_success(persist_success);
                    }
                    for persist_failure in persist_response.failures {
                        if persist_failure.reason() == PersistFailureReason::ShardClosed {
                            let index_uid: IndexUid = persist_failure.index_uid.clone().into();
                            let source_id: SourceId = persist_failure.source_id.clone();
                            closed_shards
                                .entry((index_uid, source_id))
                                .or_default()
                                .push(persist_failure.shard_id);
                        }
                        workbench.record_persist_failure(persist_failure);
                    }
                }
                Err(persist_error) => {
                    if workbench.is_last_attempt() {
                        error!(
                            "failed to persist records on ingester `{}`: {persist_error}",
                            persist_summary.leader_id
                        );
                    } else {
                        warn!(
                            "failed to persist records on ingester `{}`: {persist_error}",
                            persist_summary.leader_id
                        );
                    }
                    match persist_error {
                        IngestV2Error::Timeout
                        | IngestV2Error::Transport { .. }
                        | IngestV2Error::IngesterUnavailable { .. } => {
                            for subrequest_id in persist_summary.subrequest_ids {
                                workbench.record_no_shards_available(subrequest_id);
                            }
                            self.ingester_pool.remove(&persist_summary.leader_id);
                        }
                        _ => {
                            for subrequest_id in persist_summary.subrequest_ids {
                                workbench.record_internal_error(
                                    subrequest_id,
                                    persist_error.to_string(),
                                );
                            }
                        }
                    }
                }
            };
        }
        if !closed_shards.is_empty() {
            let mut state_guard = self.state.write().await;

            for ((index_uid, source_id), shard_ids) in closed_shards {
                state_guard
                    .shard_table
                    .close_shards(&index_uid, source_id, &shard_ids);
            }
        }
    }

    async fn batch_persist(&mut self, workbench: &mut IngestWorkbench, commit_type: CommitTypeV2) {
        let get_or_create_open_shards_request = self
            .make_get_or_create_open_shard_request(
                workbench.pending_subrequests(),
                &self.ingester_pool,
            )
            .await;

        self.populate_shard_table(workbench, get_or_create_open_shards_request)
            .await;

        // List of subrequest IDs for which no shards were available to route the subrequests to.
        let mut unavailable_subrequest_ids = Vec::new();

        let mut per_leader_persist_subrequests: HashMap<&LeaderId, Vec<PersistSubrequest>> =
            HashMap::new();

        let state_guard = self.state.read().await;

        // TODO: Here would be the most optimal place to split the body of the HTTP request into
        // lines, validate, transform and then pack the docs into compressed batches routed
        // to the right shards.

        for subrequest in workbench.pending_subrequests() {
            let Some(shard) = state_guard
                .shard_table
                .find_entry(&subrequest.index_id, &subrequest.source_id)
                .and_then(|entry| entry.next_open_shard_round_robin(&self.ingester_pool))
            else {
                unavailable_subrequest_ids.push(subrequest.subrequest_id);
                continue;
            };
            let persist_subrequest = PersistSubrequest {
                subrequest_id: subrequest.subrequest_id,
                index_uid: shard.index_uid.clone(),
                source_id: subrequest.source_id.clone(),
                shard_id: shard.shard_id,
                follower_id: shard.follower_id.clone(),
                doc_batch: subrequest.doc_batch.clone(),
            };
            per_leader_persist_subrequests
                .entry(&shard.leader_id)
                .or_default()
                .push(persist_subrequest);
        }
        let persist_futures = FuturesUnordered::new();

        for (leader_id, subrequests) in per_leader_persist_subrequests {
            let leader_id: NodeId = leader_id.clone().into();
            let subrequest_ids: Vec<SubrequestId> = subrequests
                .iter()
                .map(|subrequest| subrequest.subrequest_id)
                .collect();
            let Some(mut ingester) = self.ingester_pool.get(&leader_id) else {
                unavailable_subrequest_ids.extend(subrequest_ids);
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
                .unwrap_or_else(|_| Err(IngestV2Error::Timeout));
                (persist_summary, persist_result)
            };
            persist_futures.push(persist_future);
        }
        drop(state_guard);

        for subrequest_id in unavailable_subrequest_ids {
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
        workbench.into_ingest_response()
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
        .map_err(|_| IngestV2Error::Timeout)?
    }
}

#[async_trait]
impl IngestRouterService for IngestRouter {
    async fn ingest(
        &mut self,
        ingest_request: IngestRequestV2,
    ) -> IngestV2Result<IngestResponseV2> {
        self.ingest_timeout(ingest_request, INGEST_REQUEST_TIMEOUT)
            .await
    }
}

struct PersistRequestSummary {
    leader_id: NodeId,
    subrequest_ids: Vec<SubrequestId>,
}

#[cfg(test)]
mod tests {
    use std::iter;
    use std::sync::atomic::AtomicUsize;

    use quickwit_proto::control_plane::{
        GetOrCreateOpenShardsFailure, GetOrCreateOpenShardsFailureReason,
        GetOrCreateOpenShardsResponse, GetOrCreateOpenShardsSuccess,
    };
    use quickwit_proto::ingest::ingester::{
        IngesterServiceClient, PersistFailure, PersistResponse, PersistSuccess,
    };
    use quickwit_proto::ingest::router::IngestSubrequest;
    use quickwit_proto::ingest::{CommitTypeV2, DocBatchV2, Shard, ShardState};
    use quickwit_proto::types::Position;

    use super::*;
    use crate::ingest_v2::shard_table::ShardTableEntry;
    use crate::ingest_v2::workbench::SubworkbenchFailure;

    #[tokio::test]
    async fn test_router_make_get_or_create_open_shard_request() {
        let self_node_id = "test-router".into();
        let control_plane: ControlPlaneServiceClient = ControlPlaneServiceClient::mock().into();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
        );
        let get_or_create_open_shard_request = router
            .make_get_or_create_open_shard_request(iter::empty(), &ingester_pool)
            .await;
        assert!(get_or_create_open_shard_request.subrequests.is_empty());

        let mut state_guard = router.state.write().await;

        state_guard.shard_table.table.insert(
            ("test-index-0".into(), "test-source".into()),
            ShardTableEntry {
                index_uid: "test-index-0:0".into(),
                source_id: "test-source".to_string(),
                local_shards: vec![
                    Shard {
                        index_uid: "test-index-0:0".to_string(),
                        source_id: "test-source".to_string(),
                        shard_id: 1,
                        leader_id: "test-ingester-0".to_string(),
                        shard_state: ShardState::Closed as i32,
                        ..Default::default()
                    },
                    Shard {
                        index_uid: "test-index-0:0".to_string(),
                        source_id: "test-source".to_string(),
                        shard_id: 2,
                        leader_id: "test-ingester-0".to_string(),
                        shard_state: ShardState::Open as i32,
                        ..Default::default()
                    },
                ],
                local_round_robin_idx: AtomicUsize::default(),
                remote_shards: Vec::new(),
                remote_round_robin_idx: AtomicUsize::default(),
            },
        );
        drop(state_guard);

        let ingest_subrequests = [
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
        let get_or_create_open_shard_request = router
            .make_get_or_create_open_shard_request(ingest_subrequests.iter(), &ingester_pool)
            .await;

        assert_eq!(get_or_create_open_shard_request.subrequests.len(), 2);

        let subrequest = &get_or_create_open_shard_request.subrequests[0];
        assert_eq!(subrequest.index_id, "test-index-0");
        assert_eq!(subrequest.source_id, "test-source");

        let subrequest = &get_or_create_open_shard_request.subrequests[1];
        assert_eq!(subrequest.index_id, "test-index-1");
        assert_eq!(subrequest.source_id, "test-source");

        assert_eq!(get_or_create_open_shard_request.closed_shards.len(), 1);
        assert_eq!(
            get_or_create_open_shard_request.closed_shards[0],
            ClosedShards {
                index_uid: "test-index-0:0".into(),
                source_id: "test-source".to_string(),
                shard_ids: vec![1],
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

        ingester_pool.insert(
            "test-ingester-0".into(),
            IngesterServiceClient::mock().into(),
        );

        let get_or_create_open_shard_request = router
            .make_get_or_create_open_shard_request(ingest_subrequests.iter(), &ingester_pool)
            .await;

        let subrequest = &get_or_create_open_shard_request.subrequests[0];
        assert_eq!(subrequest.index_id, "test-index-1");
        assert_eq!(subrequest.source_id, "test-source");

        assert_eq!(get_or_create_open_shard_request.subrequests.len(), 1);
        assert_eq!(
            get_or_create_open_shard_request.unavailable_leaders.len(),
            0
        );
    }

    #[tokio::test]
    async fn test_router_populate_shard_table() {
        let self_node_id = "test-router".into();

        let mut control_plane_mock = ControlPlaneServiceClient::mock();
        control_plane_mock
            .expect_get_or_create_open_shards()
            .once()
            .returning(|request| {
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
                            index_uid: "test-index-0:0".to_string(),
                            source_id: "test-source".to_string(),
                            open_shards: vec![Shard {
                                shard_id: 1,
                                shard_state: ShardState::Open as i32,
                                ..Default::default()
                            }],
                        },
                        GetOrCreateOpenShardsSuccess {
                            subrequest_id: 1,
                            index_uid: "test-index-1:0".to_string(),
                            source_id: "test-source".to_string(),
                            open_shards: vec![
                                Shard {
                                    shard_id: 1,
                                    shard_state: ShardState::Open as i32,
                                    ..Default::default()
                                },
                                Shard {
                                    shard_id: 2,
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
        let control_plane: ControlPlaneServiceClient = control_plane_mock.into();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let mut router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
        );
        let get_or_create_open_shards_request = GetOrCreateOpenShardsRequest {
            subrequests: Vec::new(),
            closed_shards: Vec::new(),
            unavailable_leaders: Vec::new(),
        };
        let mut workbench = IngestWorkbench::new(Vec::new(), 2);

        router
            .populate_shard_table(&mut workbench, get_or_create_open_shards_request)
            .await;
        assert!(router.state.read().await.shard_table.is_empty());

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
            .populate_shard_table(&mut workbench, get_or_create_open_shards_request)
            .await;

        let state_guard = router.state.read().await;
        let shard_table = &state_guard.shard_table;
        assert_eq!(shard_table.len(), 2);

        let routing_entry_0 = shard_table
            .find_entry("test-index-0", "test-source")
            .unwrap();
        assert_eq!(routing_entry_0.len(), 1);
        assert_eq!(routing_entry_0.shards()[0].shard_id, 1);

        let routing_entry_1 = shard_table
            .find_entry("test-index-1", "test-source")
            .unwrap();
        assert_eq!(routing_entry_1.len(), 2);
        assert_eq!(routing_entry_1.shards()[0].shard_id, 1);
        assert_eq!(routing_entry_1.shards()[1].shard_id, 2);

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
    async fn test_router_process_persist_results_record_persist_successes() {
        let self_node_id = "test-router".into();
        let control_plane = ControlPlaneServiceClient::mock().into();
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

        persist_futures.push(async {
            let persist_summary = PersistRequestSummary {
                leader_id: "test-ingester-0".into(),
                subrequest_ids: vec![0],
            };
            let persist_result = Ok::<_, IngestV2Error>(PersistResponse {
                leader_id: "test-ingester-0".to_string(),
                successes: vec![PersistSuccess {
                    subrequest_id: 0,
                    index_uid: "test-index-0:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
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
        let control_plane = ControlPlaneServiceClient::mock().into();
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

        persist_futures.push(async {
            let persist_summary = PersistRequestSummary {
                leader_id: "test-ingester-0".into(),
                subrequest_ids: vec![0],
            };
            let persist_result = Ok::<_, IngestV2Error>(PersistResponse {
                leader_id: "test-ingester-0".to_string(),
                successes: Vec::new(),
                failures: vec![PersistFailure {
                    subrequest_id: 0,
                    index_uid: "test-index-0:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
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
    async fn test_router_process_persist_results_closes_shards() {
        let self_node_id = "test-router".into();
        let control_plane = ControlPlaneServiceClient::mock().into();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let mut router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
        );
        let mut state_guard = router.state.write().await;
        state_guard.shard_table.insert_shards(
            "test-index-0:0",
            "test-source",
            vec![Shard {
                index_uid: "test-index-0:0".to_string(),
                shard_id: 1,
                shard_state: ShardState::Open as i32,
                leader_id: "test-ingester-0".to_string(),
                ..Default::default()
            }],
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
                failures: vec![PersistFailure {
                    subrequest_id: 0,
                    index_uid: "test-index-0:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    reason: PersistFailureReason::ShardClosed as i32,
                }],
            });
            (persist_summary, persist_result)
        });
        router
            .process_persist_results(&mut workbench, persist_futures)
            .await;

        let state_guard = router.state.read().await;
        let shard_table_entry = state_guard
            .shard_table
            .find_entry("test-index-0", "test-source")
            .unwrap();
        assert_eq!(shard_table_entry.len(), 1);
        assert_eq!(shard_table_entry.shards()[0].shard_id, 1);
        assert_eq!(
            shard_table_entry.shards()[0].shard_state,
            ShardState::Closed as i32
        );
    }

    #[tokio::test]
    async fn test_router_process_persist_results_removes_unavailable_leaders() {
        let self_node_id = "test-router".into();
        let control_plane = ControlPlaneServiceClient::mock().into();

        let ingester_pool = IngesterPool::default();
        ingester_pool.insert(
            "test-ingester-0".into(),
            IngesterServiceClient::mock().into(),
        );
        ingester_pool.insert(
            "test-ingester-1".into(),
            IngesterServiceClient::mock().into(),
        );

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
            let persist_result = Err::<_, IngestV2Error>(IngestV2Error::Timeout);
            (persist_summary, persist_result)
        });
        router
            .process_persist_results(&mut workbench, persist_futures)
            .await;

        let subworkbench = workbench.subworkbenches.get(&0).unwrap();
        assert!(matches!(
            subworkbench.last_failure_opt,
            Some(SubworkbenchFailure::NoShardsAvailable { .. })
        ));

        let persist_futures = FuturesUnordered::new();

        persist_futures.push(async {
            let persist_summary = PersistRequestSummary {
                leader_id: "test-ingester-1".into(),
                subrequest_ids: vec![1],
            };
            let persist_result =
                Err::<_, IngestV2Error>(IngestV2Error::Transport("transport error".to_string()));
            (persist_summary, persist_result)
        });
        router
            .process_persist_results(&mut workbench, persist_futures)
            .await;
        let subworkbench = workbench.subworkbenches.get(&1).unwrap();
        assert!(matches!(
            subworkbench.last_failure_opt,
            Some(SubworkbenchFailure::NoShardsAvailable { .. })
        ));

        assert!(ingester_pool.is_empty());
    }

    #[tokio::test]
    async fn test_router_ingest() {
        let self_node_id = "test-router".into();
        let control_plane = ControlPlaneServiceClient::mock().into();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let mut router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
        );
        let mut state_guard = router.state.write().await;
        state_guard.shard_table.insert_shards(
            "test-index-0:0",
            "test-source",
            vec![Shard {
                index_uid: "test-index-0:0".to_string(),
                shard_id: 1,
                shard_state: ShardState::Open as i32,
                leader_id: "test-ingester-0".to_string(),
                ..Default::default()
            }],
        );
        state_guard.shard_table.insert_shards(
            "test-index-1:0",
            "test-source",
            vec![
                Shard {
                    index_uid: "test-index-1:0".to_string(),
                    shard_id: 1,
                    shard_state: ShardState::Open as i32,
                    leader_id: "test-ingester-0".to_string(),
                    follower_id: Some("test-ingester-1".to_string()),
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index-1:0".to_string(),
                    shard_id: 2,
                    shard_state: ShardState::Open as i32,
                    leader_id: "test-ingester-1".to_string(),
                    follower_id: Some("test-ingester-2".to_string()),
                    ..Default::default()
                },
            ],
        );
        drop(state_guard);

        let mut ingester_mock_0 = IngesterServiceClient::mock();
        ingester_mock_0
            .expect_persist()
            .once()
            .returning(|request| {
                assert_eq!(request.leader_id, "test-ingester-0");
                assert_eq!(request.subrequests.len(), 2);
                assert_eq!(request.commit_type(), CommitTypeV2::Auto);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 0);
                assert_eq!(subrequest.index_uid, "test-index-0:0");
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id, 1);
                assert!(subrequest.follower_id.is_none());
                assert_eq!(
                    subrequest.doc_batch,
                    Some(DocBatchV2::for_test(["test-doc-foo", "test-doc-bar"]))
                );

                let subrequest = &request.subrequests[1];
                assert_eq!(subrequest.subrequest_id, 1);
                assert_eq!(subrequest.index_uid, "test-index-1:0");
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id, 1);
                assert_eq!(subrequest.follower_id(), "test-ingester-1");
                assert_eq!(
                    subrequest.doc_batch,
                    Some(DocBatchV2::for_test(["test-doc-qux"]))
                );

                let response = PersistResponse {
                    leader_id: request.leader_id,
                    successes: vec![
                        PersistSuccess {
                            subrequest_id: 0,
                            index_uid: "test-index-0:0".to_string(),
                            source_id: "test-source".to_string(),
                            shard_id: 1,
                            replication_position_inclusive: Some(Position::from(1u64)),
                        },
                        PersistSuccess {
                            subrequest_id: 1,
                            index_uid: "test-index-1:0".to_string(),
                            source_id: "test-source".to_string(),
                            shard_id: 1,
                            replication_position_inclusive: Some(Position::from(0u64)),
                        },
                    ],
                    failures: Vec::new(),
                };
                Ok(response)
            });
        ingester_mock_0
            .expect_persist()
            .once()
            .returning(|request| {
                assert_eq!(request.leader_id, "test-ingester-0");
                assert_eq!(request.subrequests.len(), 1);
                assert_eq!(request.commit_type(), CommitTypeV2::Auto);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 0);
                assert_eq!(subrequest.index_uid, "test-index-0:0");
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id, 1);
                assert!(subrequest.follower_id.is_none());
                assert_eq!(
                    subrequest.doc_batch,
                    Some(DocBatchV2::for_test(["test-doc-moo", "test-doc-baz"]))
                );

                let response = PersistResponse {
                    leader_id: request.leader_id,
                    successes: vec![PersistSuccess {
                        subrequest_id: 0,
                        index_uid: "test-index-0:0".to_string(),
                        source_id: "test-source".to_string(),
                        shard_id: 1,
                        replication_position_inclusive: Some(Position::from(3u64)),
                    }],
                    failures: Vec::new(),
                };
                Ok(response)
            });
        let ingester_0: IngesterServiceClient = ingester_mock_0.into();
        ingester_pool.insert("test-ingester-0".into(), ingester_0.clone());

        let mut ingester_mock_1 = IngesterServiceClient::mock();
        ingester_mock_1
            .expect_persist()
            .once()
            .returning(|request| {
                assert_eq!(request.leader_id, "test-ingester-1");
                assert_eq!(request.subrequests.len(), 1);
                assert_eq!(request.commit_type(), CommitTypeV2::Auto);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 1);
                assert_eq!(subrequest.index_uid, "test-index-1:0");
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id, 2);
                assert_eq!(subrequest.follower_id(), "test-ingester-2");
                assert_eq!(
                    subrequest.doc_batch,
                    Some(DocBatchV2::for_test(["test-doc-tux"]))
                );

                let response = PersistResponse {
                    leader_id: request.leader_id,
                    successes: vec![PersistSuccess {
                        subrequest_id: 1,
                        index_uid: "test-index-1:0".to_string(),
                        source_id: "test-source".to_string(),
                        shard_id: 2,
                        replication_position_inclusive: Some(Position::from(0u64)),
                    }],
                    failures: Vec::new(),
                };
                Ok(response)
            });
        let ingester_1: IngesterServiceClient = ingester_mock_1.into();
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
        let control_plane = ControlPlaneServiceClient::mock().into();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let mut router = IngestRouter::new(
            self_node_id,
            control_plane,
            ingester_pool.clone(),
            replication_factor,
        );
        let mut state_guard = router.state.write().await;
        state_guard.shard_table.insert_shards(
            "test-index-0:0",
            "test-source",
            vec![Shard {
                index_uid: "test-index-0:0".to_string(),
                shard_id: 1,
                shard_state: ShardState::Open as i32,
                leader_id: "test-ingester-0".to_string(),
                ..Default::default()
            }],
        );
        drop(state_guard);

        let mut ingester_mock_0 = IngesterServiceClient::mock();
        ingester_mock_0
            .expect_persist()
            .once()
            .returning(|request| {
                assert_eq!(request.leader_id, "test-ingester-0");
                assert_eq!(request.subrequests.len(), 1);
                assert_eq!(request.commit_type(), CommitTypeV2::Auto);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 0);
                assert_eq!(subrequest.index_uid, "test-index-0:0");
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id, 1);
                assert!(subrequest.follower_id.is_none());
                assert_eq!(
                    subrequest.doc_batch,
                    Some(DocBatchV2::for_test(["test-doc-foo"]))
                );

                let response = PersistResponse {
                    leader_id: request.leader_id,
                    successes: Vec::new(),
                    failures: vec![PersistFailure {
                        subrequest_id: 0,
                        index_uid: "test-index-0:0".to_string(),
                        source_id: "test-source".to_string(),
                        shard_id: 1,
                        reason: PersistFailureReason::RateLimited as i32,
                    }],
                };
                Ok(response)
            });
        ingester_mock_0
            .expect_persist()
            .once()
            .returning(|request| {
                assert_eq!(request.leader_id, "test-ingester-0");
                assert_eq!(request.subrequests.len(), 1);
                assert_eq!(request.commit_type(), CommitTypeV2::Auto);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.subrequest_id, 0);
                assert_eq!(subrequest.index_uid, "test-index-0:0");
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_id, 1);
                assert!(subrequest.follower_id.is_none());
                assert_eq!(
                    subrequest.doc_batch,
                    Some(DocBatchV2::for_test(["test-doc-foo"]))
                );

                let response = PersistResponse {
                    leader_id: request.leader_id,
                    successes: vec![PersistSuccess {
                        subrequest_id: 0,
                        index_uid: "test-index-0:0".to_string(),
                        source_id: "test-source".to_string(),
                        shard_id: 1,
                        replication_position_inclusive: Some(Position::from(0u64)),
                    }],
                    failures: Vec::new(),
                };
                Ok(response)
            });
        let ingester_0: IngesterServiceClient = ingester_mock_0.into();
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
}
