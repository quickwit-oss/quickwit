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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use quickwit_common::pretty::PrettyDisplay;
use quickwit_metrics::{counter, label_values};
use quickwit_proto::control_plane::{
    ADVISE_RESET_SHARDS_V2, AdviseResetShardsRequest, AdviseResetShardsResponse,
    ControlPlaneService, ControlPlaneServiceClient,
};
use quickwit_proto::ingest::ShardIds;
use quickwit_proto::types::{
    IndexUid, NodeId, Position, QueueId, ShardId, SourceId, split_queue_id,
};
use rand::RngExt;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tracing::{error, info, instrument, warn};

use super::models::IngesterShard;
use super::state::{IngesterState, WeakIngesterState};
use crate::ingest_v2::metrics::{
    RECONCILE_SHARDS_OPERATIONS_TOTAL, RECONCILE_SHARDS_UNSAFE_DELETES_TOTAL, STATUS,
};

const RECONCILE_SHARDS_INTERVAL_SECS: i64 = 30;

const MIN_RECONCILE_SHARDS_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::ZERO
} else {
    Duration::from_secs(5)
};

const RECONCILE_SHARDS_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

pub(super) struct ReconcileShardsTask {
    self_node_id: NodeId,
    control_plane: ControlPlaneServiceClient,
    weak_state: WeakIngesterState,
    notify: Arc<Notify>,
}

/// While ingesters own shards and the data on them, the control plane is the source of truth for
/// what shards exist, should be open, and their truncation positions (via the metastore). Every
/// so often, it's necessary to reconcile the ingester state with the control plane's to prevent
/// drift.
///
/// This operation is called ReconcileShards, but repurposes the legacy AdviseResetShards, which was
/// how ingesters previously communicated with the control plane.
///
/// This task runs every RECONCILE_SHARDS_INTERVAL_SECS, or more frequently if the ingester WAL is
/// under pressure. Those calls will reset the time until the next call.
impl ReconcileShardsTask {
    pub fn spawn(
        self_node_id: NodeId,
        control_plane: ControlPlaneServiceClient,
        weak_state: WeakIngesterState,
        notify: Arc<Notify>,
    ) -> JoinHandle<()> {
        let task = Self {
            self_node_id,
            control_plane,
            weak_state,
            notify,
        };
        tokio::spawn(task.run())
    }

    async fn run(self) {
        let Some(mut state) = self.weak_state.upgrade() else {
            return;
        };
        state.wait_for_ready().await;
        drop(state);

        loop {
            let started_at = Instant::now();

            if let Err(error) = self.run_once().await {
                error!("stopping reconcile shards task: {error}");
                return;
            }
            let jitter_secs: i64 = rand::rng().random_range(-5..=5);
            let interval =
                Duration::from_secs((RECONCILE_SHARDS_INTERVAL_SECS + jitter_secs) as u64);

            tokio::select! {
                _ = sleep(interval) => {}
                _ = self.notify.notified() => {}
            }
            if let Some(sleep_for) = MIN_RECONCILE_SHARDS_INTERVAL.checked_sub(started_at.elapsed())
            {
                sleep(sleep_for).await;
            }
        }
    }

    #[instrument(name = "ingester.reconcile_shards", skip_all)]
    pub(super) async fn run_once(&self) -> anyhow::Result<()> {
        let state = self
            .weak_state
            .upgrade()
            .context("ingester state was dropped")?;
        let now = Instant::now();

        let state_guard = state.lock_partially("reconcile_shards_list").await?;
        let mut per_source_shard_ids: HashMap<(IndexUid, SourceId), Vec<ShardId>> = HashMap::new();

        for queue_id in state_guard.shards.keys() {
            let Some((index_uid, source_id, shard_id)) = split_queue_id(queue_id) else {
                continue;
            };
            per_source_shard_ids
                .entry((index_uid, source_id))
                .or_default()
                .push(shard_id);
        }
        drop(state_guard);

        let shard_ids: Vec<ShardIds> = per_source_shard_ids
            .into_iter()
            .map(|((index_uid, source_id), shard_ids)| ShardIds {
                index_uid: Some(index_uid),
                source_id,
                shard_ids,
            })
            .collect();
        let reconcile_shards_request = AdviseResetShardsRequest {
            ingester_id: self.self_node_id.to_string(),
            shard_ids,
            version: ADVISE_RESET_SHARDS_V2,
        };
        let reconcile_shards_result = timeout(
            RECONCILE_SHARDS_REQUEST_TIMEOUT,
            self.control_plane
                .advise_reset_shards(reconcile_shards_request),
        )
        .await;

        let status: &'static str;

        match reconcile_shards_result {
            Ok(Ok(reconcile_shards_response)) => {
                apply_reset_shards_response(&state, reconcile_shards_response, now).await?;
                status = "success";
            }
            Ok(Err(error)) => {
                warn!("reconcile shards request failed: {error}");
                status = "error";
            }
            Err(_) => {
                warn!("reconcile shards request timed out");
                status = "timeout";
            }
        }
        counter!(
            parent: RECONCILE_SHARDS_OPERATIONS_TOTAL,
            labels: [label_values!(STATUS => status)],
        )
        .inc();
        Ok(())
    }
}

fn is_safe_to_delete(shard: &IngesterShard) -> bool {
    let has_unpublished_data = shard.replication_position_inclusive.as_u64()
        > shard.truncation_position_inclusive.as_u64();
    if has_unpublished_data {
        return false;
    }
    shard.is_closed() || shard.replication_position_inclusive.is_beginning()
}

fn handle_unsafe_requested_delete(queue_id: &QueueId, shard: &mut IngesterShard) {
    error!(
        is_open=%shard.is_open(),
        is_advertisable=%shard.is_advertisable,
        replication_position_inclusive=%shard.replication_position_inclusive,
        truncation_position_inclusive=%shard.truncation_position_inclusive,
        "control plane advised deleting shard `{queue_id}` which holds unpublished data or is \
         open and not empty: closing it instead"
    );
    shard.close();
    RECONCILE_SHARDS_UNSAFE_DELETES_TOTAL.inc();
}

async fn apply_reset_shards_response(
    state: &IngesterState,
    reconcile_shards_response: AdviseResetShardsResponse,
    now: Instant,
) -> anyhow::Result<()> {
    let queue_ids_to_keep: Vec<QueueId> = reconcile_shards_response
        .shards_to_keep
        .iter()
        .flat_map(|shard_ids| shard_ids.queue_ids())
        .collect();
    let queue_ids_to_delete: Vec<QueueId> = reconcile_shards_response
        .shards_to_delete
        .iter()
        .flat_map(|shard_ids| shard_ids.queue_ids())
        .collect();
    let queue_ids_to_force_delete: Vec<QueueId> = reconcile_shards_response
        .shards_to_force_delete
        .iter()
        .flat_map(|shard_ids| shard_ids.queue_ids())
        .collect();
    let queue_positions_to_truncate: Vec<(QueueId, Position)> = reconcile_shards_response
        .shards_to_truncate
        .iter()
        .flat_map(|shard_id_positions| shard_id_positions.queue_id_positions())
        .collect();

    let mut state_guard = state.lock_fully("reconcile_shards").await?;

    for queue_id in &queue_ids_to_keep {
        let Some(shard) = state_guard.shards.get_mut(queue_id) else {
            warn!("control plane advised keeping shard `{queue_id}` which no longer exists");
            continue;
        };
        shard.is_advertisable = true;
    }
    let mut num_deleted_shards = 0;
    let mut num_unsafe_deletes = 0;

    for queue_id in &queue_ids_to_delete {
        let Some(shard) = state_guard.shards.get_mut(queue_id) else {
            continue;
        };
        // A shard is not safe to delete if it's open or has un-indexed data.
        if !is_safe_to_delete(shard) {
            handle_unsafe_requested_delete(queue_id, shard);
            num_unsafe_deletes += 1;
            continue;
        }
        state_guard
            .delete_shard(queue_id, "control-plane-reconcile-shards-rpc")
            .await;
        num_deleted_shards += 1;
    }
    // force-deletes are for indexes that were deleted, where we no longer care about the data.
    for queue_id in &queue_ids_to_force_delete {
        state_guard
            .delete_shard(queue_id, "control-plane-reconcile-shards-rpc")
            .await;
    }
    for (queue_id, publish_position) in &queue_positions_to_truncate {
        state_guard
            .truncate_shard(
                queue_id,
                publish_position.clone(),
                "control-plane-reconcile-shards-rpc",
            )
            .await;
    }
    state_guard.check_decommissioning_status().await;
    drop(state_guard);

    info!(
        "reconciled shards in {}: kept {}, deleted {}, force deleted {}, closed {} unsafe \
         deletes, truncated {}",
        now.elapsed().pretty_display(),
        queue_ids_to_keep.len(),
        num_deleted_shards,
        queue_ids_to_force_delete.len(),
        num_unsafe_deletes,
        queue_positions_to_truncate.len(),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use quickwit_cluster::{ChitchatTransport, create_cluster_for_test};
    use quickwit_config::service::QuickwitService;
    use quickwit_proto::control_plane::MockControlPlaneService;
    use quickwit_proto::ingest::ShardState;
    use quickwit_proto::types::queue_id;

    use super::*;

    enum Advice {
        Keep,
        Delete,
        ForceDelete,
    }

    enum Outcome {
        Advertisable,
        Deleted,
        Closed,
    }

    #[tokio::test]
    async fn test_reconcile_shards_run_once() {
        let cluster = create_cluster_for_test(
            Vec::new(),
            &[QuickwitService::Indexer.as_str()],
            &ChitchatTransport::default(),
            true,
        )
        .await
        .unwrap();
        let (_temp_dir, state) = IngesterState::for_test(cluster).await;

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        // (shard state, replication position, truncation position, advice, expected outcome)
        let cases = [
            (
                ShardState::Open,
                Position::Beginning,
                Position::Beginning,
                Advice::Keep,
                Outcome::Advertisable,
            ),
            (
                ShardState::Open,
                Position::Beginning,
                Position::Beginning,
                Advice::Delete,
                Outcome::Deleted,
            ),
            (
                ShardState::Open,
                Position::offset(1u64),
                Position::Beginning,
                Advice::Delete,
                Outcome::Closed,
            ),
            (
                ShardState::Closed,
                Position::offset(1u64),
                Position::offset(1u64),
                Advice::Delete,
                Outcome::Deleted,
            ),
            (
                ShardState::Closed,
                Position::offset(1u64),
                Position::offset(0u64),
                Advice::Delete,
                Outcome::Closed,
            ),
            (
                ShardState::Open,
                Position::offset(1u64),
                Position::Beginning,
                Advice::ForceDelete,
                Outcome::Deleted,
            ),
        ];
        let mut shard_ids_to_keep = Vec::new();
        let mut shard_ids_to_delete = Vec::new();
        let mut shard_ids_to_force_delete = Vec::new();

        let mut state_guard = state.lock_partially("test").await.unwrap();

        for (i, (shard_state, replication_position, truncation_position, advice, _)) in
            cases.iter().enumerate()
        {
            let shard_id = ShardId::from(i as u64);
            let shard =
                IngesterShard::builder(index_uid.clone(), source_id.clone(), shard_id.clone())
                    .with_state(*shard_state)
                    .with_replication_position_inclusive(replication_position.clone())
                    .with_truncation_position_inclusive(truncation_position.clone())
                    .build();
            state_guard.shards.insert(shard.queue_id(), shard);

            match advice {
                Advice::Keep => shard_ids_to_keep.push(shard_id),
                Advice::Delete => shard_ids_to_delete.push(shard_id),
                Advice::ForceDelete => shard_ids_to_force_delete.push(shard_id),
            }
        }
        drop(state_guard);

        let to_shard_ids = |shard_ids: Vec<ShardId>| {
            vec![ShardIds {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_ids,
            }]
        };
        let response = AdviseResetShardsResponse {
            shards_to_keep: to_shard_ids(shard_ids_to_keep),
            shards_to_delete: to_shard_ids(shard_ids_to_delete),
            shards_to_force_delete: to_shard_ids(shard_ids_to_force_delete),
            shards_to_truncate: Vec::new(),
        };
        let mut mock_control_plane = MockControlPlaneService::new();
        mock_control_plane
            .expect_advise_reset_shards()
            .once()
            .returning(move |request| {
                assert_eq!(request.version, ADVISE_RESET_SHARDS_V2);
                Ok(response.clone())
            });
        let task = ReconcileShardsTask {
            self_node_id: NodeId::from_str("test-ingester"),
            control_plane: ControlPlaneServiceClient::from_mock(mock_control_plane),
            weak_state: state.weak(),
            notify: Arc::new(Notify::new()),
        };
        task.run_once().await.unwrap();

        let state_guard = state.lock_partially("test").await.unwrap();

        for (i, (_, _, _, _, outcome)) in cases.iter().enumerate() {
            let queue_id = queue_id(&index_uid, &source_id, &ShardId::from(i as u64));
            let shard_opt = state_guard.shards.get(&queue_id);

            match outcome {
                Outcome::Advertisable => assert!(shard_opt.unwrap().is_advertisable, "case {i}"),
                Outcome::Deleted => assert!(shard_opt.is_none(), "case {i}"),
                Outcome::Closed => assert!(shard_opt.unwrap().is_closed(), "case {i}"),
            }
        }
    }
}
