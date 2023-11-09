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

use std::fmt;
use std::time::Duration;

use fnv::FnvHashSet;
use itertools::Itertools;
use quickwit_common::{PrettySample, Progress};
use quickwit_ingest::IngesterPool;
use quickwit_proto::control_plane::{
    ClosedShards, ControlPlaneError, ControlPlaneResult, GetOrCreateOpenShardsFailure,
    GetOrCreateOpenShardsFailureReason, GetOrCreateOpenShardsRequest,
    GetOrCreateOpenShardsResponse, GetOrCreateOpenShardsSuccess,
};
use quickwit_proto::ingest::ingester::{IngesterService, PingRequest};
use quickwit_proto::ingest::{IngestV2Error, ShardState};
use quickwit_proto::metastore;
use quickwit_proto::metastore::{MetastoreService, MetastoreServiceClient};
use quickwit_proto::types::{IndexUid, NodeId};
use rand::seq::SliceRandom;
use tokio::time::timeout;
use tracing::info;

use crate::control_plane_model::ControlPlaneModel;

const PING_LEADER_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(2)
};

pub struct IngestController {
    metastore: MetastoreServiceClient,
    ingester_pool: IngesterPool,
    replication_factor: usize,
}

impl fmt::Debug for IngestController {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("IngestController")
            .field("replication", &self.metastore)
            .field("ingester_pool", &self.ingester_pool)
            .field("replication_factor", &self.replication_factor)
            .finish()
    }
}

impl IngestController {
    pub fn new(
        metastore: MetastoreServiceClient,
        ingester_pool: IngesterPool,
        replication_factor: usize,
    ) -> Self {
        IngestController {
            metastore,
            ingester_pool,
            replication_factor,
        }
    }

    /// Pings an ingester to determine whether it is available for hosting a shard. If a follower ID
    /// is provided, the leader candidate is in charge of pinging the follower candidate as
    /// well.
    async fn ping_leader_and_follower(
        &mut self,
        leader_id: &NodeId,
        follower_id_opt: Option<&NodeId>,
        progress: &Progress,
    ) -> Result<(), PingError> {
        let mut leader_ingester = self
            .ingester_pool
            .get(leader_id)
            .ok_or(PingError::LeaderUnavailable)?;

        let ping_request = PingRequest {
            leader_id: leader_id.clone().into(),
            follower_id: follower_id_opt
                .cloned()
                .map(|follower_id| follower_id.into()),
        };
        progress.protect_future(timeout(
            PING_LEADER_TIMEOUT,
            leader_ingester.ping(ping_request),
        ))
        .await
        .map_err(|_| PingError::LeaderUnavailable)? // The leader timed out.
        .map_err(|error| {
            if let Some(follower_id) = follower_id_opt {
                if matches!(error, IngestV2Error::IngesterUnavailable { ingester_id } if ingester_id == *follower_id) {
                    return PingError::FollowerUnavailable;
                }
            }
            PingError::LeaderUnavailable
        })?;
        Ok(())
    }

    /// Finds an available leader-follower pair to host a shard. If the replication factor is set to
    /// 1, only a leader is returned. If no nodes are available, `None` is returned.
    async fn find_leader_and_follower(
        &mut self,
        unavailable_ingesters: &mut FnvHashSet<NodeId>,
        progress: &Progress,
    ) -> Option<(NodeId, Option<NodeId>)> {
        let mut candidates: Vec<NodeId> = self
            .ingester_pool
            .keys()
            .into_iter()
            .filter(|node_id| !unavailable_ingesters.contains(node_id))
            .collect();
        candidates.shuffle(&mut rand::thread_rng());

        #[cfg(test)]
        candidates.sort();

        if self.replication_factor == 1 {
            for leader_id in candidates {
                if unavailable_ingesters.contains(&leader_id) {
                    continue;
                }
                if self
                    .ping_leader_and_follower(&leader_id, None, progress)
                    .await
                    .is_ok()
                {
                    return Some((leader_id, None));
                }
            }
        } else {
            for (leader_id, follower_id) in candidates.into_iter().tuple_combinations() {
                // We must perform this check here since the `unavailable_ingesters` set can grow as
                // we go through the loop.
                if unavailable_ingesters.contains(&leader_id)
                    || unavailable_ingesters.contains(&follower_id)
                {
                    continue;
                }
                match self
                    .ping_leader_and_follower(&leader_id, Some(&follower_id), progress)
                    .await
                {
                    Ok(_) => return Some((leader_id, Some(follower_id))),
                    Err(PingError::LeaderUnavailable) => {
                        unavailable_ingesters.insert(leader_id);
                    }
                    Err(PingError::FollowerUnavailable) => {
                        // We do not mark the follower as unavailable here. The issue could be
                        // specific to the link between the leader and follower. We define
                        // unavailability as being unavailable from the point of view of the control
                        // plane.
                    }
                }
            }
        }
        None
    }

    fn handle_closed_shards(
        &self,
        closed_shards: Vec<ClosedShards>,
        model: &mut ControlPlaneModel,
    ) {
        for closed_shard in closed_shards {
            let index_uid: IndexUid = closed_shard.index_uid.into();
            let source_id = closed_shard.source_id;
            let closed_shard_ids =
                model.close_shards(&index_uid, &source_id, &closed_shard.shard_ids);

            if !closed_shard_ids.is_empty() {
                info!(
                    index_id=%index_uid.index_id(),
                    source_id=%source_id,
                    shard_ids=?PrettySample::new(&closed_shard_ids, 5),
                    "closed {} shard(s) reported by router",
                    closed_shard_ids.len()
                );
            }
        }
    }

    fn handle_unavailable_leaders(
        &self,
        unavailable_leaders: &FnvHashSet<NodeId>,
        model: &mut ControlPlaneModel,
    ) {
        let mut confirmed_unavailable_leaders = FnvHashSet::default();

        for leader_id in unavailable_leaders {
            if !self.ingester_pool.contains_key(leader_id) {
                confirmed_unavailable_leaders.insert(leader_id.clone());
            } else {
                // TODO: If a majority of ingesters consistenly reports a leader as unavailable, we
                // should probably mark it as unavailable too.
            }
        }
        if !confirmed_unavailable_leaders.is_empty() {
            for shard in model.shards_mut() {
                if shard.shard_state().is_open()
                    && confirmed_unavailable_leaders.contains(&shard.leader_id)
                {
                    shard.shard_state = ShardState::Unavailable as i32;
                }
            }
        }
    }

    /// Finds the open shards that satisfies the [`GetOrCreateOpenShardsRequest`] request sent by an
    /// ingest router. First, the control plane checks its internal shard table to find
    /// candidates. If it does not contain any, the control plane will ask
    /// the metastore to open new shards.
    pub(crate) async fn get_or_create_open_shards(
        &mut self,
        get_open_shards_request: GetOrCreateOpenShardsRequest,
        model: &mut ControlPlaneModel,
        progress: &Progress,
    ) -> ControlPlaneResult<GetOrCreateOpenShardsResponse> {
        self.handle_closed_shards(get_open_shards_request.closed_shards, model);

        let mut unavailable_leaders: FnvHashSet<NodeId> = get_open_shards_request
            .unavailable_leaders
            .into_iter()
            .map(|ingester_id| ingester_id.into())
            .collect();

        self.handle_unavailable_leaders(&unavailable_leaders, model);

        let num_subrequests = get_open_shards_request.subrequests.len();
        let mut get_or_create_open_shards_successes = Vec::with_capacity(num_subrequests);
        let mut get_or_create_open_shards_failures = Vec::new();
        let mut open_shards_subrequests = Vec::new();

        for get_open_shards_subrequest in get_open_shards_request.subrequests {
            let Some(index_uid) = model.index_uid(&get_open_shards_subrequest.index_id) else {
                let get_or_create_open_shards_failure = GetOrCreateOpenShardsFailure {
                    subrequest_id: get_open_shards_subrequest.subrequest_id,
                    index_id: get_open_shards_subrequest.index_id,
                    source_id: get_open_shards_subrequest.source_id,
                    reason: GetOrCreateOpenShardsFailureReason::IndexNotFound as i32,
                };
                get_or_create_open_shards_failures.push(get_or_create_open_shards_failure);
                continue;
            };
            let Some((open_shards, next_shard_id)) = model.find_open_shards(
                &index_uid,
                &get_open_shards_subrequest.source_id,
                &unavailable_leaders,
            ) else {
                let get_or_create_open_shards_failure = GetOrCreateOpenShardsFailure {
                    subrequest_id: get_open_shards_subrequest.subrequest_id,
                    index_id: get_open_shards_subrequest.index_id,
                    source_id: get_open_shards_subrequest.source_id,
                    reason: GetOrCreateOpenShardsFailureReason::SourceNotFound as i32,
                };
                get_or_create_open_shards_failures.push(get_or_create_open_shards_failure);
                continue;
            };
            if !open_shards.is_empty() {
                let get_or_create_open_shards_success = GetOrCreateOpenShardsSuccess {
                    subrequest_id: get_open_shards_subrequest.subrequest_id,
                    index_uid: index_uid.into(),
                    source_id: get_open_shards_subrequest.source_id,
                    open_shards,
                };
                get_or_create_open_shards_successes.push(get_or_create_open_shards_success);
            } else {
                // TODO: Find leaders in batches.
                // TODO: Round-robin leader-follower pairs or choose according to load.
                let (leader_id, follower_id) = self
                    .find_leader_and_follower(&mut unavailable_leaders, progress)
                    .await
                    .ok_or_else(|| {
                        ControlPlaneError::Unavailable("no available ingester".to_string())
                    })?;
                let open_shards_subrequest = metastore::OpenShardsSubrequest {
                    subrequest_id: get_open_shards_subrequest.subrequest_id,
                    index_uid: index_uid.into(),
                    source_id: get_open_shards_subrequest.source_id,
                    leader_id: leader_id.into(),
                    follower_id: follower_id.map(|follower_id| follower_id.into()),
                    next_shard_id,
                };
                open_shards_subrequests.push(open_shards_subrequest);
            }
        }
        if !open_shards_subrequests.is_empty() {
            let open_shards_request = metastore::OpenShardsRequest {
                subrequests: open_shards_subrequests,
            };
            let open_shards_response = progress
                .protect_future(self.metastore.open_shards(open_shards_request))
                .await?;
            for open_shards_subresponse in open_shards_response.subresponses {
                let index_uid: IndexUid = open_shards_subresponse.index_uid.clone().into();
                let source_id = open_shards_subresponse.source_id.clone();

                model.insert_newly_opened_shards(
                    &index_uid,
                    &source_id,
                    open_shards_subresponse.opened_shards,
                    open_shards_subresponse.next_shard_id,
                );
                if let Some((open_shards, _next_shard_id)) =
                    model.find_open_shards(&index_uid, &source_id, &unavailable_leaders)
                {
                    let get_or_create_open_shards_success = GetOrCreateOpenShardsSuccess {
                        subrequest_id: open_shards_subresponse.subrequest_id,
                        index_uid: index_uid.into(),
                        source_id: open_shards_subresponse.source_id,
                        open_shards,
                    };
                    get_or_create_open_shards_successes.push(get_or_create_open_shards_success);
                }
            }
        }
        Ok(GetOrCreateOpenShardsResponse {
            successes: get_or_create_open_shards_successes,
            failures: get_or_create_open_shards_failures,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PingError {
    LeaderUnavailable,
    FollowerUnavailable,
}

#[cfg(test)]
mod tests {

    use quickwit_config::{SourceConfig, SourceParams};
    use quickwit_metastore::IndexMetadata;
    use quickwit_proto::control_plane::GetOrCreateOpenShardsSubrequest;
    use quickwit_proto::ingest::ingester::{
        IngesterServiceClient, MockIngesterService, PingResponse,
    };
    use quickwit_proto::ingest::{Shard, ShardState};
    use quickwit_proto::types::SourceId;

    use super::*;

    #[tokio::test]
    async fn test_ingest_controller_ping_leader() {
        let progress = Progress::default();

        let mock_metastore = MetastoreServiceClient::mock();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let mut ingest_controller = IngestController::new(
            MetastoreServiceClient::from(mock_metastore),
            ingester_pool.clone(),
            replication_factor,
        );

        let leader_id: NodeId = "test-ingester-0".into();
        let error = ingest_controller
            .ping_leader_and_follower(&leader_id, None, &progress)
            .await
            .unwrap_err();
        assert!(matches!(error, PingError::LeaderUnavailable));

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().once().returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-0");
            assert!(request.follower_id.is_none());

            Ok(PingResponse {})
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-0".into(), ingester.clone());

        ingest_controller
            .ping_leader_and_follower(&leader_id, None, &progress)
            .await
            .unwrap();

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().once().returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-0");
            assert!(request.follower_id.is_none());

            let leader_id: NodeId = "test-ingester-0".into();
            Err(IngestV2Error::IngesterUnavailable {
                ingester_id: leader_id,
            })
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-0".into(), ingester.clone());

        let error = ingest_controller
            .ping_leader_and_follower(&leader_id, None, &progress)
            .await
            .unwrap_err();
        assert!(matches!(error, PingError::LeaderUnavailable));

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().once().returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-0");
            assert_eq!(request.follower_id.unwrap(), "test-ingester-1");

            let follower_id: NodeId = "test-ingester-1".into();
            Err(IngestV2Error::IngesterUnavailable {
                ingester_id: follower_id,
            })
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-0".into(), ingester.clone());

        let follower_id: NodeId = "test-ingester-1".into();
        let error = ingest_controller
            .ping_leader_and_follower(&leader_id, Some(&follower_id), &progress)
            .await
            .unwrap_err();
        assert!(matches!(error, PingError::FollowerUnavailable));
    }

    #[tokio::test]
    async fn test_ingest_controller_find_leader_replication_factor_1() {
        let progress = Progress::default();

        let mock_metastore = MetastoreServiceClient::mock();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let mut ingest_controller = IngestController::new(
            MetastoreServiceClient::from(mock_metastore),
            ingester_pool.clone(),
            replication_factor,
        );

        let leader_follower_pair = ingest_controller
            .find_leader_and_follower(&mut FnvHashSet::default(), &progress)
            .await;
        assert!(leader_follower_pair.is_none());

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().times(2).returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-0");
            assert!(request.follower_id.is_none());

            Err(IngestV2Error::Internal("Io error".to_string()))
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-0".into(), ingester.clone());

        let leader_follower_pair = ingest_controller
            .find_leader_and_follower(&mut FnvHashSet::default(), &progress)
            .await;
        assert!(leader_follower_pair.is_none());

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().once().returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-1");
            assert!(request.follower_id.is_none());

            Ok(PingResponse {})
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-1".into(), ingester);

        let (leader_id, follower_id) = ingest_controller
            .find_leader_and_follower(&mut FnvHashSet::default(), &progress)
            .await
            .unwrap();
        assert_eq!(leader_id.as_str(), "test-ingester-1");
        assert!(follower_id.is_none());
    }

    #[tokio::test]
    async fn test_ingest_controller_find_leader_replication_factor_2() {
        let progress = Progress::default();

        let mock_metastore = MetastoreServiceClient::mock();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 2;
        let mut ingest_controller = IngestController::new(
            MetastoreServiceClient::from(mock_metastore),
            ingester_pool.clone(),
            replication_factor,
        );

        let leader_follower_pair = ingest_controller
            .find_leader_and_follower(&mut FnvHashSet::default(), &progress)
            .await;
        assert!(leader_follower_pair.is_none());

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().once().returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-0");
            assert_eq!(request.follower_id.unwrap(), "test-ingester-1");

            Err(IngestV2Error::IngesterUnavailable {
                ingester_id: "test-ingester-1".into(),
            })
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-0".into(), ingester.clone());

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().returning(|_request| {
            panic!("`test-ingester-1` should not be pinged.");
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-1".into(), ingester.clone());

        let leader_follower_pair = ingest_controller
            .find_leader_and_follower(&mut FnvHashSet::default(), &progress)
            .await;
        assert!(leader_follower_pair.is_none());

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().once().returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-0");
            assert_eq!(request.follower_id.unwrap(), "test-ingester-1");

            Err(IngestV2Error::IngesterUnavailable {
                ingester_id: "test-ingester-1".into(),
            })
        });
        mock_ingester.expect_ping().once().returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-0");
            assert_eq!(request.follower_id.unwrap(), "test-ingester-2");

            Ok(PingResponse {})
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-0".into(), ingester.clone());

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().returning(|_request| {
            panic!("`test-ingester-2` should not be pinged.");
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-2".into(), ingester.clone());

        let (leader_id, follower_id) = ingest_controller
            .find_leader_and_follower(&mut FnvHashSet::default(), &progress)
            .await
            .unwrap();
        assert_eq!(leader_id.as_str(), "test-ingester-0");
        assert_eq!(follower_id.unwrap().as_str(), "test-ingester-2");
    }

    #[tokio::test]
    async fn test_ingest_controller_get_or_create_open_shards() {
        let source_id: &'static str = "test-source";

        let index_id_0 = "test-index-0";
        let index_metadata_0 = IndexMetadata::for_test(index_id_0, "ram://indexes/test-index-0");
        let index_uid_0 = index_metadata_0.index_uid.clone();

        let index_id_1 = "test-index-1";
        let index_metadata_1 = IndexMetadata::for_test(index_id_1, "ram://indexes/test-index-1");
        let index_uid_1 = index_metadata_1.index_uid.clone();

        let progress = Progress::default();

        let mut mock_metastore = MetastoreServiceClient::mock();
        mock_metastore.expect_open_shards().once().returning({
            let index_uid_1 = index_uid_1.clone();

            move |request| {
                assert_eq!(request.subrequests.len(), 1);
                assert_eq!(&request.subrequests[0].index_uid, index_uid_1.as_str());
                assert_eq!(&request.subrequests[0].source_id, source_id);

                let subresponses = vec![metastore::OpenShardsSubresponse {
                    subrequest_id: 1,
                    index_uid: index_uid_1.clone().into(),
                    source_id: source_id.to_string(),
                    opened_shards: vec![Shard {
                        index_uid: index_uid_1.clone().into(),
                        source_id: source_id.to_string(),
                        shard_id: 1,
                        shard_state: ShardState::Open as i32,
                        leader_id: "test-ingester-2".to_string(),
                        ..Default::default()
                    }],
                    next_shard_id: 2,
                }];
                let response = metastore::OpenShardsResponse { subresponses };
                Ok(response)
            }
        });
        let ingester_pool = IngesterPool::default();

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().once().returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-1");
            assert_eq!(request.follower_id.unwrap(), "test-ingester-2");

            Ok(PingResponse {})
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-1".into(), ingester.clone());

        let mock_ingester = MockIngesterService::default();
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-2".into(), ingester.clone());

        let replication_factor = 2;
        let mut ingest_controller = IngestController::new(
            MetastoreServiceClient::from(mock_metastore),
            ingester_pool.clone(),
            replication_factor,
        );

        let mut model = ControlPlaneModel::default();

        let source_config = SourceConfig::for_test(source_id, SourceParams::stdin());

        model.add_index(index_metadata_0.clone());
        model.add_index(index_metadata_1.clone());
        model
            .add_source(&index_uid_0, source_config.clone())
            .unwrap();
        model.add_source(&index_uid_1, source_config).unwrap();

        let shards = vec![
            Shard {
                index_uid: index_uid_0.clone().into(),
                source_id: source_id.to_string(),
                shard_id: 1,
                leader_id: "test-ingester-0".to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
            Shard {
                index_uid: index_uid_0.clone().into(),
                source_id: source_id.to_string(),
                shard_id: 2,
                leader_id: "test-ingester-1".to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
        ];

        model.insert_newly_opened_shards(&index_uid_0, &source_id.into(), shards, 3);

        let request = GetOrCreateOpenShardsRequest {
            subrequests: Vec::new(),
            closed_shards: Vec::new(),
            unavailable_leaders: Vec::new(),
        };

        let response = ingest_controller
            .get_or_create_open_shards(request, &mut model, &progress)
            .await
            .unwrap();

        assert_eq!(response.successes.len(), 0);
        assert_eq!(response.failures.len(), 0);

        let subrequests = vec![
            GetOrCreateOpenShardsSubrequest {
                subrequest_id: 0,
                index_id: "test-index-0".to_string(),
                source_id: source_id.to_string(),
            },
            GetOrCreateOpenShardsSubrequest {
                subrequest_id: 1,
                index_id: "test-index-1".to_string(),
                source_id: source_id.to_string(),
            },
            GetOrCreateOpenShardsSubrequest {
                subrequest_id: 2,
                index_id: "index-not-found".to_string(),
                source_id: "source-not-found".to_string(),
            },
            GetOrCreateOpenShardsSubrequest {
                subrequest_id: 3,
                index_id: "test-index-0".to_string(),
                source_id: "source-not-found".to_string(),
            },
        ];
        let closed_shards = Vec::new();
        let unavailable_leaders = vec!["test-ingester-0".to_string()];
        let request = GetOrCreateOpenShardsRequest {
            subrequests,
            closed_shards,
            unavailable_leaders,
        };
        let response = ingest_controller
            .get_or_create_open_shards(request, &mut model, &progress)
            .await
            .unwrap();

        assert_eq!(response.successes.len(), 2);
        assert_eq!(response.failures.len(), 2);

        let success = &response.successes[0];
        assert_eq!(success.subrequest_id, 0);
        assert_eq!(success.index_uid, index_uid_0.as_str());
        assert_eq!(success.source_id, source_id);
        assert_eq!(success.open_shards.len(), 1);
        assert_eq!(success.open_shards[0].shard_id, 2);
        assert_eq!(success.open_shards[0].leader_id, "test-ingester-1");

        let success = &response.successes[1];
        assert_eq!(success.subrequest_id, 1);
        assert_eq!(success.index_uid, index_uid_1.as_str());
        assert_eq!(success.source_id, source_id);
        assert_eq!(success.open_shards.len(), 1);
        assert_eq!(success.open_shards[0].shard_id, 1);
        assert_eq!(success.open_shards[0].leader_id, "test-ingester-2");

        let failure = &response.failures[0];
        assert_eq!(failure.subrequest_id, 2);
        assert_eq!(failure.index_id, "index-not-found");
        assert_eq!(failure.source_id, "source-not-found");
        assert_eq!(
            failure.reason(),
            GetOrCreateOpenShardsFailureReason::IndexNotFound
        );

        let failure = &response.failures[1];
        assert_eq!(failure.subrequest_id, 3);
        assert_eq!(failure.index_id, index_id_0);
        assert_eq!(failure.source_id, "source-not-found");
        assert_eq!(
            failure.reason(),
            GetOrCreateOpenShardsFailureReason::SourceNotFound
        );

        assert_eq!(model.observable_state().num_shards, 2);
    }

    #[tokio::test]
    async fn test_ingest_controller_get_open_shards_handles_closed_shards() {
        let metastore = MetastoreServiceClient::mock().into();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 2;

        let mut ingest_controller =
            IngestController::new(metastore, ingester_pool, replication_factor);
        let mut model = ControlPlaneModel::default();

        let index_uid: IndexUid = "test-index-0:0".into();
        let source_id: SourceId = "test-source".into();

        let shards = vec![Shard {
            shard_id: 1,
            leader_id: "test-ingester-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        }];
        model.insert_newly_opened_shards(&index_uid, &source_id, shards, 3);

        let request = GetOrCreateOpenShardsRequest {
            subrequests: Vec::new(),
            closed_shards: vec![ClosedShards {
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_ids: vec![1, 2],
            }],
            unavailable_leaders: Vec::new(),
        };
        let progress = Progress::default();

        ingest_controller
            .get_or_create_open_shards(request, &mut model, &progress)
            .await
            .unwrap();

        let shard_1 = model.shards().find(|shard| shard.shard_id == 1).unwrap();
        assert!(shard_1.shard_state().is_closed());
    }

    #[tokio::test]
    async fn test_ingest_controller_get_open_shards_handles_unavailable_leaders() {
        let metastore = MetastoreServiceClient::mock().into();

        let ingester_pool = IngesterPool::default();
        let ingester_1 = IngesterServiceClient::mock().into();
        ingester_pool.insert("test-ingester-1".into(), ingester_1);

        let replication_factor = 2;

        let mut ingest_controller =
            IngestController::new(metastore, ingester_pool.clone(), replication_factor);
        let mut model = ControlPlaneModel::default();

        let index_uid: IndexUid = "test-index-0:0".into();
        let source_id: SourceId = "test-source".into();

        let shards = vec![
            Shard {
                shard_id: 1,
                leader_id: "test-ingester-0".to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
            Shard {
                shard_id: 2,
                leader_id: "test-ingester-0".to_string(),
                shard_state: ShardState::Closed as i32,
                ..Default::default()
            },
            Shard {
                shard_id: 3,
                leader_id: "test-ingester-1".to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
        ];
        model.insert_newly_opened_shards(&index_uid, &source_id, shards, 4);

        let request = GetOrCreateOpenShardsRequest {
            subrequests: Vec::new(),
            closed_shards: Vec::new(),
            unavailable_leaders: vec!["test-ingester-0".to_string()],
        };
        let progress = Progress::default();

        ingest_controller
            .get_or_create_open_shards(request, &mut model, &progress)
            .await
            .unwrap();

        let shard_1 = model.shards().find(|shard| shard.shard_id == 1).unwrap();
        assert!(shard_1.shard_state().is_unavailable());

        let shard_2 = model.shards().find(|shard| shard.shard_id == 2).unwrap();
        assert!(shard_2.shard_state().is_closed());

        let shard_3 = model.shards().find(|shard| shard.shard_id == 3).unwrap();
        assert!(shard_3.shard_state().is_open());
    }
}
