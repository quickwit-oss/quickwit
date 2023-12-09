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

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use fnv::{FnvHashMap, FnvHashSet};
use itertools::Itertools;
use quickwit_common::{PrettySample, Progress};
use quickwit_ingest::{IngesterPool, LocalShardsUpdate};
use quickwit_proto::control_plane::{
    ControlPlaneError, ControlPlaneResult, GetOrCreateOpenShardsFailure,
    GetOrCreateOpenShardsFailureReason, GetOrCreateOpenShardsRequest,
    GetOrCreateOpenShardsResponse, GetOrCreateOpenShardsSuccess,
};
use quickwit_proto::ingest::ingester::{
    CloseShardsRequest, IngesterService, InitShardsRequest, PingRequest,
};
use quickwit_proto::ingest::{IngestV2Error, Shard, ShardIds, ShardState};
use quickwit_proto::metastore;
use quickwit_proto::metastore::{MetastoreService, MetastoreServiceClient};
use quickwit_proto::types::{IndexUid, NodeId, ShardId, SourceUid};
use rand::seq::SliceRandom;
use tokio::time::timeout;
use tracing::{info, warn};

use crate::metrics::CONTROL_PLANE_METRICS;
use crate::model::{ControlPlaneModel, ScalingMode, ShardEntry, ShardStats};

const MAX_SHARD_INGESTION_THROUGHPUT_MIB_PER_SEC: f32 = 5.;

/// Threshold in MiB/s above which we increase the number of shards.
const SCALE_UP_SHARDS_THRESHOLD_MIB_PER_SEC: f32 =
    MAX_SHARD_INGESTION_THROUGHPUT_MIB_PER_SEC * 8. / 10.;

/// Threshold in MiB/s below which we decrease the number of shards.
const SCALE_DOWN_SHARDS_THRESHOLD_MIB_PER_SEC: f32 =
    MAX_SHARD_INGESTION_THROUGHPUT_MIB_PER_SEC * 2. / 10.;

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

    fn handle_closed_shards(&self, closed_shards: Vec<ShardIds>, model: &mut ControlPlaneModel) {
        for closed_shard in closed_shards {
            let index_uid: IndexUid = closed_shard.index_uid.into();
            let source_id = closed_shard.source_id;

            let source_uid = SourceUid {
                index_uid,
                source_id,
            };
            let closed_shard_ids = model.close_shards(&source_uid, &closed_shard.shard_ids);

            if !closed_shard_ids.is_empty() {
                info!(
                    index_id=%source_uid.index_uid.index_id(),
                    source_id=%source_uid.source_id,
                    shard_ids=?PrettySample::new(&closed_shard_ids, 5),
                    "closed {} shard(s) reported by router",
                    closed_shard_ids.len()
                );
            }
        }
    }

    pub(crate) async fn handle_local_shards_update(
        &mut self,
        local_shards_update: LocalShardsUpdate,
        model: &mut ControlPlaneModel,
        progress: &Progress,
    ) {
        let shard_stats = model.update_shards(
            &local_shards_update.source_uid,
            &local_shards_update.shard_infos,
        );
        if shard_stats.avg_ingestion_rate >= SCALE_UP_SHARDS_THRESHOLD_MIB_PER_SEC {
            self.try_scale_up_shards(local_shards_update.source_uid, shard_stats, model, progress)
                .await;
        } else if shard_stats.avg_ingestion_rate <= SCALE_DOWN_SHARDS_THRESHOLD_MIB_PER_SEC
            && shard_stats.num_open_shards > 1
        {
            self.try_scale_down_shards(
                local_shards_update.source_uid,
                shard_stats,
                model,
                progress,
            )
            .await;
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
            for shard_entry in model.all_shards_mut() {
                if shard_entry.is_open()
                    && confirmed_unavailable_leaders.contains(&shard_entry.leader_id)
                {
                    shard_entry.set_shard_state(ShardState::Unavailable);
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
            let Some((open_shard_entries, next_shard_id)) = model.find_open_shards(
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
            if !open_shard_entries.is_empty() {
                let open_shards: Vec<Shard> = open_shard_entries
                    .into_iter()
                    .map(|shard_entry| shard_entry.shard)
                    .collect();
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
                        ControlPlaneError::Unavailable("no ingester available".to_string())
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

            // TODO: Handle failures.
            let _ = self.init_shards(&open_shards_response, progress).await;

            for open_shards_subresponse in open_shards_response.subresponses {
                let index_uid: IndexUid = open_shards_subresponse.index_uid.clone().into();
                let source_id = open_shards_subresponse.source_id.clone();

                model.insert_newly_opened_shards(
                    &index_uid,
                    &source_id,
                    open_shards_subresponse.opened_shards,
                    open_shards_subresponse.next_shard_id,
                );
                if let Some((open_shard_entries, _next_shard_id)) =
                    model.find_open_shards(&index_uid, &source_id, &unavailable_leaders)
                {
                    let open_shards = open_shard_entries
                        .into_iter()
                        .map(|shard_entry| shard_entry.shard)
                        .collect();
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

    /// Calls init shards on the leaders hosting newly opened shards.
    // TODO: Return partial failures instead of failing the whole request.
    async fn init_shards(
        &self,
        open_shards_response: &metastore::OpenShardsResponse,
        progress: &Progress,
    ) -> Result<(), IngestV2Error> {
        let mut per_leader_opened_shards: FnvHashMap<&String, Vec<Shard>> = FnvHashMap::default();

        for subresponse in &open_shards_response.subresponses {
            for shard in &subresponse.opened_shards {
                per_leader_opened_shards
                    .entry(&shard.leader_id)
                    .or_default()
                    .push(shard.clone());
            }
        }
        // TODO: Init shards in parallel.
        for (leader_id, shards) in per_leader_opened_shards {
            let init_shards_request = InitShardsRequest { shards };

            let Some(mut leader) = self.ingester_pool.get(leader_id) else {
                warn!("failed to init shards: ingester `{leader_id}` is unavailable");
                continue;
            };
            progress
                .protect_future(leader.init_shards(init_shards_request))
                .await?;
        }
        Ok(())
    }

    /// Attempts to increase the number of shards. This operation is rate limited to avoid creating
    /// to many shards in a short period of time. As a result, this method may not create any
    /// shard.
    async fn try_scale_up_shards(
        &mut self,
        source_uid: SourceUid,
        shard_stats: ShardStats,
        model: &mut ControlPlaneModel,
        progress: &Progress,
    ) {
        const NUM_PERMITS: u64 = 1;

        if !model
            .acquire_scaling_permits(&source_uid, ScalingMode::Up, NUM_PERMITS)
            .unwrap_or(false)
        {
            return;
        }
        let new_num_open_shards = shard_stats.num_open_shards + 1;

        info!(
            index_id=%source_uid.index_uid.index_id(),
            source_id=%source_uid.source_id,
            "scaling up number of shards to {new_num_open_shards}"
        );
        // Expect: the source should exist because we just acquired a permit.
        let next_shard_id = model
            .next_shard_id(&source_uid)
            .expect("source should exist");

        let mut unavailable_leaders: FnvHashSet<NodeId> = FnvHashSet::default();

        let Some((leader_id, follower_id)) = self
            .find_leader_and_follower(&mut unavailable_leaders, progress)
            .await
        else {
            warn!("failed to scale up number of shards: no ingester available");
            model.release_scaling_permits(&source_uid, ScalingMode::Up, NUM_PERMITS);
            return;
        };
        let open_shards_subrequest = metastore::OpenShardsSubrequest {
            subrequest_id: 0,
            index_uid: source_uid.index_uid.clone().into(),
            source_id: source_uid.source_id.clone(),
            leader_id: leader_id.into(),
            follower_id: follower_id.map(Into::into),
            next_shard_id,
        };
        let open_shards_request = metastore::OpenShardsRequest {
            subrequests: vec![open_shards_subrequest],
        };
        let open_shards_response = match progress
            .protect_future(self.metastore.open_shards(open_shards_request))
            .await
        {
            Ok(open_shards_response) => open_shards_response,
            Err(error) => {
                warn!("failed to scale up number of shards: {error}");
                model.release_scaling_permits(&source_uid, ScalingMode::Up, NUM_PERMITS);
                return;
            }
        };
        if let Err(error) = self.init_shards(&open_shards_response, progress).await {
            warn!("failed to scale up number of shards: {error}");
            model.release_scaling_permits(&source_uid, ScalingMode::Up, NUM_PERMITS);
            return;
        }
        for open_shards_subresponse in open_shards_response.subresponses {
            let index_uid: IndexUid = open_shards_subresponse.index_uid.into();
            let source_id = open_shards_subresponse.source_id;

            model.insert_newly_opened_shards(
                &index_uid,
                &source_id,
                open_shards_subresponse.opened_shards,
                open_shards_subresponse.next_shard_id,
            );
        }
        let label_values = [source_uid.index_uid.index_id(), &source_uid.source_id];
        CONTROL_PLANE_METRICS
            .open_shards_total
            .with_label_values(label_values)
            .set(new_num_open_shards as i64);
    }

    /// Attempts to decrease the number of shards. This operation is rate limited to avoid closing
    /// shards too aggressively. As a result, this method may not close any shard.
    async fn try_scale_down_shards(
        &self,
        source_uid: SourceUid,
        shard_stats: ShardStats,
        model: &mut ControlPlaneModel,
        progress: &Progress,
    ) {
        const NUM_PERMITS: u64 = 1;

        if !model
            .acquire_scaling_permits(&source_uid, ScalingMode::Down, NUM_PERMITS)
            .unwrap_or(false)
        {
            return;
        }
        let new_num_open_shards = shard_stats.num_open_shards - 1;

        info!(
            index_id=%source_uid.index_uid.index_id(),
            source_id=%source_uid.source_id,
            "scaling down number of shards to {new_num_open_shards}"
        );
        let Some((leader_id, shard_id)) = find_scale_down_candidate(&source_uid, model) else {
            model.release_scaling_permits(&source_uid, ScalingMode::Down, NUM_PERMITS);
            return;
        };
        let Some(mut ingester) = self.ingester_pool.get(&leader_id) else {
            model.release_scaling_permits(&source_uid, ScalingMode::Down, NUM_PERMITS);
            return;
        };
        let shards = vec![ShardIds {
            index_uid: source_uid.index_uid.clone().into(),
            source_id: source_uid.source_id.clone(),
            shard_ids: vec![shard_id],
        }];
        let close_shards_request = CloseShardsRequest { shards };

        if let Err(error) = progress
            .protect_future(ingester.close_shards(close_shards_request))
            .await
        {
            warn!("failed to scale down number of shards: {error}");
            model.release_scaling_permits(&source_uid, ScalingMode::Down, NUM_PERMITS);
            return;
        }
        model.close_shards(&source_uid, &[shard_id]);

        let label_values = [source_uid.index_uid.index_id(), &source_uid.source_id];
        CONTROL_PLANE_METRICS
            .open_shards_total
            .with_label_values(label_values)
            .set(new_num_open_shards as i64);
    }
}

/// Finds the shard with the highest ingestion rate on the ingester with the least number of open
/// shards.
fn find_scale_down_candidate(
    source_uid: &SourceUid,
    model: &ControlPlaneModel,
) -> Option<(NodeId, ShardId)> {
    let mut per_leader_candidates: HashMap<&String, (usize, &ShardEntry)> = HashMap::new();

    for shard in model.list_shards(source_uid)? {
        if shard.is_open() {
            per_leader_candidates
                .entry(&shard.leader_id)
                .and_modify(|(num_shards, candidate)| {
                    *num_shards += 1;

                    if candidate.ingestion_rate < shard.ingestion_rate {
                        *candidate = shard;
                    }
                })
                .or_insert((1, shard));
        }
    }
    per_leader_candidates
        .into_iter()
        .min_by_key(|(_leader_id, (num_shards, _shard))| *num_shards)
        .map(|(leader_id, (_num_shards, shard))| (leader_id.clone().into(), shard.shard_id))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PingError {
    LeaderUnavailable,
    FollowerUnavailable,
}

#[cfg(test)]
mod tests {

    use std::collections::BTreeSet;

    use quickwit_config::{SourceConfig, SourceParams, INGEST_SOURCE_ID};
    use quickwit_ingest::{RateMibPerSec, ShardInfo};
    use quickwit_metastore::IndexMetadata;
    use quickwit_proto::control_plane::GetOrCreateOpenShardsSubrequest;
    use quickwit_proto::ingest::ingester::{
        CloseShardsResponse, IngesterServiceClient, InitShardsResponse, MockIngesterService,
        PingResponse,
    };
    use quickwit_proto::ingest::{Shard, ShardState};
    use quickwit_proto::metastore::MetastoreError;
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

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester
            .expect_init_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.shards.len(), 1);
                assert_eq!(request.shards[0].index_uid, "test-index-1:0");
                assert_eq!(request.shards[0].source_id, "test-source");
                assert_eq!(request.shards[0].shard_id, 1);
                assert_eq!(request.shards[0].leader_id, "test-ingester-2");

                Ok(InitShardsResponse {})
            });
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
            closed_shards: vec![ShardIds {
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

        let shard_1 = model
            .all_shards_mut()
            .find(|shard| shard.shard_id == 1)
            .unwrap();
        assert!(shard_1.is_closed());
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

        let shard_1 = model
            .all_shards_mut()
            .find(|shard| shard.shard_id == 1)
            .unwrap();
        assert!(shard_1.is_unavailable());

        let shard_2 = model
            .all_shards_mut()
            .find(|shard| shard.shard_id == 2)
            .unwrap();
        assert!(shard_2.is_closed());

        let shard_3 = model
            .all_shards_mut()
            .find(|shard| shard.shard_id == 3)
            .unwrap();
        assert!(shard_3.is_open());
    }

    #[tokio::test]
    async fn test_ingest_controller_handle_local_shards_update() {
        let metastore = MetastoreServiceClient::mock().into();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;

        let mut ingest_controller =
            IngestController::new(metastore, ingester_pool.clone(), replication_factor);

        let index_uid: IndexUid = "test-index:0".into();
        let source_id: SourceId = "test-source".into();

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let mut model = ControlPlaneModel::default();
        let progress = Progress::default();

        let shards = vec![Shard {
            shard_id: 1,
            leader_id: "test-ingester".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        }];
        model.insert_newly_opened_shards(&index_uid, &source_id, shards, 2);

        let shard_entries: Vec<ShardEntry> = model
            .all_shards_mut()
            .map(|shard_entry| shard_entry.clone())
            .collect();
        assert_eq!(shard_entries.len(), 1);
        assert_eq!(shard_entries[0].ingestion_rate, 0);

        // Test update shard ingestion rate but no scale down because num open shards is 1.
        let shard_infos = BTreeSet::from_iter([ShardInfo {
            shard_id: 1,
            shard_state: ShardState::Open,
            ingestion_rate: RateMibPerSec(1),
        }]);
        let local_shards_update = LocalShardsUpdate {
            leader_id: "test-ingester".into(),
            source_uid: source_uid.clone(),
            shard_infos,
        };
        ingest_controller
            .handle_local_shards_update(local_shards_update, &mut model, &progress)
            .await;

        let shard_entries: Vec<ShardEntry> = model
            .all_shards_mut()
            .map(|shard_entry| shard_entry.clone())
            .collect();
        assert_eq!(shard_entries.len(), 1);
        assert_eq!(shard_entries[0].ingestion_rate, 1);

        // Test update shard ingestion rate with failing scale down.
        let shards = vec![Shard {
            shard_id: 2,
            leader_id: "test-ingester".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        }];
        model.insert_newly_opened_shards(&index_uid, &source_id, shards, 2);

        let shard_entries: Vec<ShardEntry> = model
            .all_shards_mut()
            .map(|shard_entry| shard_entry.clone())
            .collect();
        assert_eq!(shard_entries.len(), 2);

        let mut ingester_mock = IngesterServiceClient::mock();

        ingester_mock.expect_close_shards().returning(|request| {
            assert_eq!(request.shards.len(), 1);
            assert_eq!(request.shards[0].index_uid, "test-index:0");
            assert_eq!(request.shards[0].source_id, "test-source");
            assert_eq!(request.shards[0].shard_ids, vec![1]);

            Err(IngestV2Error::Internal(
                "failed to close shards".to_string(),
            ))
        });
        ingester_mock.expect_ping().returning(|request| {
            assert_eq!(request.leader_id, "test-ingester");

            Err(IngestV2Error::Internal("failed ping ingester".to_string()))
        });
        ingester_pool.insert("test-ingester".into(), ingester_mock.into());

        let shard_infos = BTreeSet::from_iter([
            ShardInfo {
                shard_id: 1,
                shard_state: ShardState::Open,
                ingestion_rate: RateMibPerSec(1),
            },
            ShardInfo {
                shard_id: 2,
                shard_state: ShardState::Open,
                ingestion_rate: RateMibPerSec(1),
            },
        ]);
        let local_shards_update = LocalShardsUpdate {
            leader_id: "test-ingester".into(),
            source_uid: source_uid.clone(),
            shard_infos,
        };
        ingest_controller
            .handle_local_shards_update(local_shards_update, &mut model, &progress)
            .await;

        // Test update shard ingestion rate with failing scale up.
        let shard_infos = BTreeSet::from_iter([
            ShardInfo {
                shard_id: 1,
                shard_state: ShardState::Open,
                ingestion_rate: RateMibPerSec(4),
            },
            ShardInfo {
                shard_id: 2,
                shard_state: ShardState::Open,
                ingestion_rate: RateMibPerSec(4),
            },
        ]);
        let local_shards_update = LocalShardsUpdate {
            leader_id: "test-ingester".into(),
            source_uid: source_uid.clone(),
            shard_infos,
        };
        ingest_controller
            .handle_local_shards_update(local_shards_update, &mut model, &progress)
            .await;
    }

    #[tokio::test]
    async fn test_ingest_controller_try_scale_up_shards() {
        let mut mock_metastore = MetastoreServiceClient::mock();

        mock_metastore
            .expect_open_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 1);
                assert_eq!(request.subrequests[0].index_uid, "test-index:0");
                assert_eq!(request.subrequests[0].source_id, INGEST_SOURCE_ID);
                assert_eq!(request.subrequests[0].leader_id, "test-ingester");
                assert_eq!(request.subrequests[0].next_shard_id, 1);

                Err(MetastoreError::InvalidArgument {
                    message: "failed to open shards".to_string(),
                })
            });
        mock_metastore.expect_open_shards().returning(|request| {
            assert_eq!(request.subrequests.len(), 1);
            assert_eq!(request.subrequests[0].index_uid, "test-index:0");
            assert_eq!(request.subrequests[0].source_id, INGEST_SOURCE_ID);
            assert_eq!(request.subrequests[0].leader_id, "test-ingester");
            assert_eq!(request.subrequests[0].next_shard_id, 1);

            let subresponses = vec![metastore::OpenShardsSubresponse {
                subrequest_id: 0,
                index_uid: "test-index:0".into(),
                source_id: INGEST_SOURCE_ID.to_string(),
                opened_shards: vec![Shard {
                    index_uid: "test-index:0".into(),
                    source_id: INGEST_SOURCE_ID.to_string(),
                    shard_id: 1,
                    leader_id: "test-ingester".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                }],
                next_shard_id: 2,
            }];
            let response = metastore::OpenShardsResponse { subresponses };
            Ok(response)
        });

        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;

        let mut ingest_controller = IngestController::new(
            mock_metastore.into(),
            ingester_pool.clone(),
            replication_factor,
        );

        let index_uid: IndexUid = "test-index:0".into();
        let source_id: SourceId = INGEST_SOURCE_ID.to_string();

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let shard_stats = ShardStats {
            num_open_shards: 2,
            ..Default::default()
        };
        let mut model = ControlPlaneModel::default();
        let index_metadata =
            IndexMetadata::for_test(index_uid.index_id(), "ram://indexes/test-index:0");
        model.add_index(index_metadata);

        let souce_config = SourceConfig::ingest_v2_default();
        model.add_source(&index_uid, souce_config).unwrap();

        let progress = Progress::default();

        // Test could not find leader.
        ingest_controller
            .try_scale_up_shards(source_uid.clone(), shard_stats, &mut model, &progress)
            .await;

        let mut ingester_mock = IngesterServiceClient::mock();

        ingester_mock.expect_ping().returning(|request| {
            assert_eq!(request.leader_id, "test-ingester");

            Ok(PingResponse {})
        });
        ingester_mock
            .expect_init_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.shards.len(), 1);
                assert_eq!(request.shards[0].index_uid, "test-index:0");
                assert_eq!(request.shards[0].source_id, INGEST_SOURCE_ID);
                assert_eq!(request.shards[0].shard_id, 1);
                assert_eq!(request.shards[0].leader_id, "test-ingester");

                Err(IngestV2Error::Internal("failed to init shards".to_string()))
            });
        ingester_mock.expect_init_shards().returning(|request| {
            assert_eq!(request.shards.len(), 1);
            assert_eq!(request.shards[0].index_uid, "test-index:0");
            assert_eq!(request.shards[0].source_id, INGEST_SOURCE_ID);
            assert_eq!(request.shards[0].shard_id, 1);
            assert_eq!(request.shards[0].leader_id, "test-ingester");

            Ok(InitShardsResponse {})
        });
        ingester_pool.insert("test-ingester".into(), ingester_mock.into());

        // Test failed to open shards.
        ingest_controller
            .try_scale_up_shards(source_uid.clone(), shard_stats, &mut model, &progress)
            .await;
        assert_eq!(model.all_shards_mut().count(), 0);

        // Test failed to init shards.
        ingest_controller
            .try_scale_up_shards(source_uid.clone(), shard_stats, &mut model, &progress)
            .await;
        assert_eq!(model.all_shards_mut().count(), 0);

        // Test successfully opened shard.
        ingest_controller
            .try_scale_up_shards(source_uid.clone(), shard_stats, &mut model, &progress)
            .await;
        assert_eq!(
            model
                .all_shards_mut()
                .filter(|shard| shard.is_open())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn test_ingest_controller_try_scale_down_shards() {
        let metastore = MetastoreServiceClient::mock().into();
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;

        let ingest_controller =
            IngestController::new(metastore, ingester_pool.clone(), replication_factor);

        let index_uid: IndexUid = "test-index:0".into();
        let source_id: SourceId = "test-source".into();

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let shard_stats = ShardStats {
            num_open_shards: 2,
            ..Default::default()
        };
        let mut model = ControlPlaneModel::default();
        let progress = Progress::default();

        // Test could not find a scale down candidate.
        ingest_controller
            .try_scale_down_shards(source_uid.clone(), shard_stats, &mut model, &progress)
            .await;

        let shards = vec![Shard {
            shard_id: 1,
            leader_id: "test-ingester".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        }];
        model.insert_newly_opened_shards(&index_uid, &source_id, shards, 2);

        // Test ingester is unavailable.
        ingest_controller
            .try_scale_down_shards(source_uid.clone(), shard_stats, &mut model, &progress)
            .await;

        let mut ingester_mock = IngesterServiceClient::mock();

        ingester_mock
            .expect_close_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.shards.len(), 1);
                assert_eq!(request.shards[0].index_uid, "test-index:0");
                assert_eq!(request.shards[0].source_id, "test-source");
                assert_eq!(request.shards[0].shard_ids, vec![1]);

                Err(IngestV2Error::Internal(
                    "failed to close shards".to_string(),
                ))
            });
        ingester_mock
            .expect_close_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.shards.len(), 1);
                assert_eq!(request.shards[0].index_uid, "test-index:0");
                assert_eq!(request.shards[0].source_id, "test-source");
                assert_eq!(request.shards[0].shard_ids, vec![1]);

                Ok(CloseShardsResponse {})
            });
        ingester_pool.insert("test-ingester".into(), ingester_mock.into());

        // Test failed to close shard.
        ingest_controller
            .try_scale_down_shards(source_uid.clone(), shard_stats, &mut model, &progress)
            .await;
        assert!(model.all_shards_mut().all(|shard| shard.is_open()));

        // Test successfully closed shard.
        ingest_controller
            .try_scale_down_shards(source_uid.clone(), shard_stats, &mut model, &progress)
            .await;
        assert!(model.all_shards_mut().all(|shard| shard.is_closed()));

        let shards = vec![Shard {
            shard_id: 2,
            leader_id: "test-ingester".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        }];
        model.insert_newly_opened_shards(&index_uid, &source_id, shards, 3);

        // Test rate limited.
        ingest_controller
            .try_scale_down_shards(source_uid.clone(), shard_stats, &mut model, &progress)
            .await;
        assert!(model.all_shards_mut().any(|shard| shard.is_open()));
    }

    #[test]
    fn test_find_scale_down_candidate() {
        let index_uid: IndexUid = "test-index:0".into();
        let source_id: SourceId = "test-source".into();

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let mut model = ControlPlaneModel::default();

        assert!(find_scale_down_candidate(&source_uid, &model).is_none());

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
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
            Shard {
                shard_id: 3,
                leader_id: "test-ingester-0".to_string(),
                shard_state: ShardState::Closed as i32,
                ..Default::default()
            },
            Shard {
                shard_id: 4,
                leader_id: "test-ingester-1".to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
            Shard {
                shard_id: 5,
                leader_id: "test-ingester-1".to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
            Shard {
                shard_id: 6,
                leader_id: "test-ingester-1".to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
        ];
        model.insert_newly_opened_shards(&index_uid, &source_id, shards, 7);

        let shard_infos = BTreeSet::from_iter([
            ShardInfo {
                shard_id: 1,
                shard_state: ShardState::Open,
                ingestion_rate: quickwit_ingest::RateMibPerSec(1),
            },
            ShardInfo {
                shard_id: 2,
                shard_state: ShardState::Open,
                ingestion_rate: quickwit_ingest::RateMibPerSec(2),
            },
            ShardInfo {
                shard_id: 3,
                shard_state: ShardState::Open,
                ingestion_rate: quickwit_ingest::RateMibPerSec(3),
            },
            ShardInfo {
                shard_id: 4,
                shard_state: ShardState::Open,
                ingestion_rate: quickwit_ingest::RateMibPerSec(4),
            },
            ShardInfo {
                shard_id: 5,
                shard_state: ShardState::Open,
                ingestion_rate: quickwit_ingest::RateMibPerSec(5),
            },
            ShardInfo {
                shard_id: 6,
                shard_state: ShardState::Open,
                ingestion_rate: quickwit_ingest::RateMibPerSec(6),
            },
        ]);
        model.update_shards(&source_uid, &shard_infos);

        let (leader_id, shard_id) = find_scale_down_candidate(&source_uid, &model).unwrap();
        assert_eq!(leader_id, "test-ingester-0");
        assert_eq!(shard_id, 2);
    }
}
