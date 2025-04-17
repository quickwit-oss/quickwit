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

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::time::{Duration, Instant};

use anyhow::Context;
use async_trait::async_trait;
use fnv::FnvHashMap;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, SpawnContext};
use quickwit_cluster::{Cluster, ListenerHandle};
use quickwit_common::pretty::PrettyDisplay;
use quickwit_common::pubsub::{Event, EventBroker};
use quickwit_proto::indexing::ShardPositionsUpdate;
use quickwit_proto::types::{Position, ShardId, SourceUid};
use tracing::{debug, error, info, warn};

/// Prefix used in chitchat to publish the shard positions.
const SHARD_POSITIONS_PREFIX: &str = "indexer.shard_positions:";

/// This event means that a pipeline running in the current node (hence "local")
/// performed a publish on an ingest pipeline, and hence the position of a shard has been updated.
///
/// This event is meant to be built by the `IngestSource`, upon reception of suggest truncate
/// event. It should only be consumed by the `ShardPositionsService`.
///
/// (This is why its member are private).
///
/// The new position is to be exposed to the entire cluster via chitchat.
///
/// Consumers of such events should listen to the more `ShardPositionsUpdate` event instead.
/// That event is broadcasted via the cluster event broker, and will include both local
/// changes and changes from other nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalShardPositionsUpdate {
    source_uid: SourceUid,
    // This list can be partial: not all shards for the source need to be listed here.
    shard_positions: Vec<(ShardId, Position)>,
}

impl LocalShardPositionsUpdate {
    pub fn new(source_uid: SourceUid, shard_positions: Vec<(ShardId, Position)>) -> Self {
        LocalShardPositionsUpdate {
            source_uid,
            shard_positions,
        }
    }
}

/// This event is an internal detail of the `ShardPositionsService`.
///
/// When a shard position change in the cluster is detected, a `ClusterShardPositionUpdate`
/// message is queued into the `ShardPositionsService`
#[derive(Debug)]
struct ClusterShardPositionsUpdate {
    pub source_uid: SourceUid,
    pub shard_id: ShardId,
    pub position: Position,
}

impl Event for LocalShardPositionsUpdate {}

/// The published shard positions is a model unique to the indexer service instance that
/// keeps track of the latest (known) published position for the shards of all managed sources.
///
/// It receives updates through the event broker, and only keeps the maximum published position
/// for each shard.
pub struct ShardPositionsService {
    shard_positions_per_source: FnvHashMap<SourceUid, BTreeMap<ShardId, Position>>,
    cluster: Cluster,
    event_broker: EventBroker,
    cluster_listener_handle_opt: Option<ListenerHandle>,
}

fn parse_shard_positions_from_kv(
    key: &str,
    value: &str,
) -> anyhow::Result<ClusterShardPositionsUpdate> {
    let (source_uid_str, shard_id_str) = key.rsplit_once(':').context("invalid key")?;
    let shard_id = ShardId::from(shard_id_str);
    let (index_uid_str, source_id) = source_uid_str.rsplit_once(':').context("invalid key")?;
    let index_uid = index_uid_str.parse()?;
    let source_uid = SourceUid {
        index_uid,
        source_id: source_id.to_string(),
    };
    let position = Position::from(value.to_string());
    Ok(ClusterShardPositionsUpdate {
        source_uid,
        shard_id,
        position,
    })
}

fn push_position_update(
    shard_positions_service_mailbox: &Mailbox<ShardPositionsService>,
    key: &str,
    value: &str,
) {
    let shard_positions = match parse_shard_positions_from_kv(key, value) {
        Ok(shard_positions) => shard_positions,
        Err(error) => {
            error!(key=key, value=value, error=%error, "failed to parse shard positions from cluster kv");
            return;
        }
    };
    if shard_positions_service_mailbox
        .try_send_message(shard_positions)
        .is_err()
    {
        error!("failed to send shard positions to the shard positions service");
    }
}

#[async_trait]
impl Actor for ShardPositionsService {
    type ObservableState = ();
    fn observable_state(&self) {}

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        let mailbox = ctx.mailbox().clone();

        self.cluster_listener_handle_opt = Some(
            self.cluster
                .subscribe(SHARD_POSITIONS_PREFIX, move |event| {
                    push_position_update(&mailbox, event.key, event.value);
                })
                .await,
        );

        // We are now listening to new updates. However, the cluster has been started earlier.
        // It might have already received shard updates from other nodes.
        //
        // Let's also sync our `ShardPositionsService` with the current state of the cluster.
        // Shard position updates are trivially idempotent, so we can replay all the events,
        // without worrying about duplicates.

        let now = Instant::now();
        let chitchat = self.cluster.chitchat().await;
        let chitchat_lock = chitchat.lock().await;
        let mut num_keys = 0;
        for node_state in chitchat_lock.node_states().values() {
            for (key, versioned_value) in node_state.iter_prefix(SHARD_POSITIONS_PREFIX) {
                let key_stripped = key.strip_prefix(SHARD_POSITIONS_PREFIX).unwrap();
                push_position_update(ctx.mailbox(), key_stripped, &versioned_value.value);
                num_keys += 1;
            }
            // It is tempting to yield here, but we are holding the chitchat lock.
            // Let's just log the amount of time it takes for the moment.
        }
        let elapsed = now.elapsed();
        if elapsed > Duration::from_millis(300) {
            warn!(
                "initializing shard positions took longer than expected: {} ({num_keys} keys)",
                elapsed.pretty_display(),
            );
        } else {
            info!(
                "initialized shard positions in {} ({num_keys} keys)",
                elapsed.pretty_display(),
            );
        }
        Ok(())
    }
}

impl ShardPositionsService {
    pub fn spawn(spawn_ctx: &SpawnContext, event_broker: EventBroker, cluster: Cluster) {
        let shard_positions_service = ShardPositionsService::new(event_broker.clone(), cluster);
        let (shard_positions_service_mailbox, _) =
            spawn_ctx.spawn_builder().spawn(shard_positions_service);
        // This subscription is in charge of updating the shard positions model.
        event_broker
            .subscribe_without_timeout::<LocalShardPositionsUpdate>(move |update| {
                if shard_positions_service_mailbox
                    .try_send_message(update)
                    .is_err()
                {
                    error!("failed to send update to shard positions service");
                }
            })
            .forever();
    }

    fn new(event_broker: EventBroker, cluster: Cluster) -> ShardPositionsService {
        ShardPositionsService {
            shard_positions_per_source: Default::default(),
            cluster,
            event_broker,
            cluster_listener_handle_opt: None,
        }
    }
}

#[async_trait]
impl Handler<ClusterShardPositionsUpdate> for ShardPositionsService {
    type Reply = ();

    async fn handle(
        &mut self,
        update: ClusterShardPositionsUpdate,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let ClusterShardPositionsUpdate {
            source_uid,
            shard_id,
            position,
        } = update;
        let updated_shard_positions = self.apply_update(&source_uid, vec![(shard_id, position)]);
        debug!(updated_shard_positions=?updated_shard_positions, "cluster position update");
        if !updated_shard_positions.is_empty() {
            self.publish_shard_updates_to_event_broker(source_uid, updated_shard_positions);
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<LocalShardPositionsUpdate> for ShardPositionsService {
    type Reply = ();

    async fn handle(
        &mut self,
        update: LocalShardPositionsUpdate,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let LocalShardPositionsUpdate {
            source_uid,
            shard_positions,
        } = update;
        let updated_shard_positions: Vec<(ShardId, Position)> =
            self.apply_update(&source_uid, shard_positions);
        if updated_shard_positions.is_empty() {
            return Ok(());
        }
        self.publish_positions_into_chitchat(&source_uid, &updated_shard_positions)
            .await;
        self.publish_shard_updates_to_event_broker(source_uid, updated_shard_positions);
        Ok(())
    }
}

impl ShardPositionsService {
    async fn publish_positions_into_chitchat(
        &self,
        source_uid: &SourceUid,
        shard_positions: &[(ShardId, Position)],
    ) {
        let SourceUid {
            index_uid,
            source_id,
        } = &source_uid;
        for (shard_id, position) in shard_positions {
            let key = format!("{SHARD_POSITIONS_PREFIX}{index_uid}:{source_id}:{shard_id}");
            self.cluster
                .set_self_key_value_delete_after_ttl(key, position)
                .await;
        }
    }

    fn publish_shard_updates_to_event_broker(
        &self,
        source_uid: SourceUid,
        shard_positions: Vec<(ShardId, Position)>,
    ) {
        debug!(shard_positions=?shard_positions, "shard positions updates");
        self.event_broker.publish(ShardPositionsUpdate {
            source_uid,
            updated_shard_positions: shard_positions,
        });
    }

    /// Updates the internal model holding the last position per shard, and
    /// returns the list of shards that were updated.
    fn apply_update(
        &mut self,
        source_uid: &SourceUid,
        published_positions_per_shard: Vec<(ShardId, Position)>,
    ) -> Vec<(ShardId, Position)> {
        if published_positions_per_shard.is_empty() {
            warn!("received an empty publish shard positions update");
            return Vec::new();
        }
        let current_shard_positions = self
            .shard_positions_per_source
            .entry(source_uid.clone())
            .or_default();

        let updated_positions_per_shard = published_positions_per_shard
            .into_iter()
            .filter(|(shard, new_position)| {
                let Some(position) = current_shard_positions.get(shard) else {
                    return true;
                };
                new_position > position
            })
            .collect::<Vec<_>>();

        for (shard, position) in updated_positions_per_shard.iter() {
            current_shard_positions.insert(shard.clone(), position.clone());
        }

        updated_positions_per_shard
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_actors::Universe;
    use quickwit_cluster::{ChannelTransport, create_cluster_for_test};
    use quickwit_common::pubsub::EventBroker;
    use quickwit_proto::types::IndexUid;

    use super::*;

    #[tokio::test]
    async fn test_shard_positions_from_cluster() {
        quickwit_common::setup_logging_for_tests();

        let transport = ChannelTransport::default();

        let universe1 = Universe::with_accelerated_time();
        let universe2 = Universe::with_accelerated_time();

        let event_broker1 = EventBroker::default();
        let event_broker2 = EventBroker::default();

        let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel::<ShardPositionsUpdate>();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel::<ShardPositionsUpdate>();

        event_broker1
            .subscribe(move |update: ShardPositionsUpdate| {
                tx1.send(update).unwrap();
            })
            .forever();

        event_broker2
            .subscribe(move |update: ShardPositionsUpdate| {
                tx2.send(update).unwrap();
            })
            .forever();

        let index_uid = IndexUid::new_with_random_ulid("index-test");
        let source_id = "test-source".to_string();
        let source_uid = SourceUid {
            index_uid,
            source_id,
        };

        let cluster1 =
            create_cluster_for_test(Vec::new(), &["indexer", "metastore"], &transport, true)
                .await
                .unwrap();
        ShardPositionsService::spawn(
            universe1.spawn_ctx(),
            event_broker1.clone(),
            cluster1.clone(),
        );

        // One of the event is published before cluster formation.
        event_broker1.publish(LocalShardPositionsUpdate::new(
            source_uid.clone(),
            vec![(ShardId::from(20), Position::offset(100u64))],
        ));

        let cluster2 = create_cluster_for_test(
            vec![cluster1.gossip_listen_addr.to_string()],
            &["indexer"],
            &transport,
            true,
        )
        .await
        .unwrap();

        cluster1
            .wait_for_ready_members(|members| members.len() == 2, Duration::from_secs(5))
            .await
            .unwrap();
        cluster2
            .wait_for_ready_members(|members| members.len() == 2, Duration::from_secs(5))
            .await
            .unwrap();

        ShardPositionsService::spawn(
            universe2.spawn_ctx(),
            event_broker2.clone(),
            cluster2.clone(),
        );

        // ----------------------
        // One of the node publishes a given shard position update.
        // This is done using a LocalPublishShardPositionUpdate

        event_broker1.publish(LocalShardPositionsUpdate::new(
            source_uid.clone(),
            vec![(ShardId::from(2), Position::offset(10u64))],
        ));
        event_broker1.publish(LocalShardPositionsUpdate::new(
            source_uid.clone(),
            vec![(ShardId::from(1), Position::offset(10u64))],
        ));
        event_broker2.publish(LocalShardPositionsUpdate::new(
            source_uid.clone(),
            vec![(ShardId::from(2), Position::offset(10u64))],
        ));
        event_broker2.publish(LocalShardPositionsUpdate::new(
            source_uid.clone(),
            vec![(ShardId::from(2), Position::offset(12u64))],
        ));
        event_broker2.publish(LocalShardPositionsUpdate::new(
            source_uid.clone(),
            vec![
                (ShardId::from(1), Position::Beginning),
                (ShardId::from(2), Position::offset(12u64)),
            ],
        ));

        let mut updates1: Vec<Vec<(ShardId, Position)>> = Vec::new();
        for _ in 0..4 {
            let update = rx1.recv().await.unwrap();
            assert_eq!(update.source_uid, source_uid);
            updates1.push(update.updated_shard_positions);
        }

        // The updates as seen from the first node.
        assert_eq!(
            updates1,
            vec![
                vec![(ShardId::from(20), Position::offset(100u64))],
                vec![(ShardId::from(2u64), Position::offset(10u64))],
                vec![(ShardId::from(1u64), Position::offset(10u64)),],
                vec![(ShardId::from(2u64), Position::offset(12u64)),],
            ]
        );

        // The updates as seen from the second.
        let mut updates2: Vec<Vec<(ShardId, Position)>> = Vec::new();
        for _ in 0..5 {
            let update = rx2.recv().await.unwrap();
            assert_eq!(update.source_uid, source_uid);
            updates2.push(update.updated_shard_positions);
        }
        assert_eq!(
            updates2,
            vec![
                vec![(ShardId::from(20u64), Position::offset(100u64))],
                vec![(ShardId::from(2u64), Position::offset(10u64))],
                vec![(ShardId::from(2u64), Position::offset(12u64))],
                vec![(ShardId::from(1u64), Position::Beginning)],
                vec![(ShardId::from(1u64), Position::offset(10u64))]
            ]
        );

        universe1.assert_quit().await;
        universe2.assert_quit().await;
    }

    #[tokio::test]
    async fn test_shard_positions_local_updates_publish_to_cluster() {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::with_accelerated_time();
        let transport = ChannelTransport::default();

        let cluster: Cluster = create_cluster_for_test(Vec::new(), &[], &transport, true)
            .await
            .unwrap();
        let event_broker = EventBroker::default();

        ShardPositionsService::spawn(universe.spawn_ctx(), event_broker.clone(), cluster.clone());

        let index_uid = IndexUid::new_with_random_ulid("index-test");
        let source_id = "test-source".to_string();
        let key_prefix = format!("{SHARD_POSITIONS_PREFIX}{index_uid}:{source_id}");
        let source_uid = SourceUid {
            index_uid,
            source_id,
        };

        let shard_id1 = ShardId::from(1);
        let shard_id2 = ShardId::from(2);
        let shard_id3 = ShardId::from(3);

        {
            event_broker.publish(LocalShardPositionsUpdate::new(
                source_uid.clone(),
                vec![(ShardId::from(1), Position::Beginning)],
            ));
            tokio::time::sleep(Duration::from_secs(1)).await;
            let key = format!("{key_prefix}:{shard_id1}");
            let value = cluster.get_self_key_value(&key).await.unwrap();
            assert_eq!(&value, "");
        }
        {
            event_broker.publish(LocalShardPositionsUpdate::new(
                source_uid.clone(),
                vec![
                    (shard_id1.clone(), Position::offset(1_000u64)),
                    (shard_id2.clone(), Position::offset(2_000u64)),
                ],
            ));
            tokio::time::sleep(Duration::from_secs(1)).await;
            let value1 = cluster
                .get_self_key_value(&format!("{key_prefix}:{shard_id1}"))
                .await
                .unwrap();
            assert_eq!(&value1, "00000000000000001000");
            let value2 = cluster
                .get_self_key_value(&format!("{key_prefix}:{shard_id2}"))
                .await
                .unwrap();
            assert_eq!(&value2, "00000000000000002000");
        }
        {
            event_broker.publish(LocalShardPositionsUpdate::new(
                source_uid.clone(),
                vec![
                    (shard_id1.clone(), Position::offset(999u64)),
                    (shard_id3.clone(), Position::offset(3_000u64)),
                ],
            ));
            tokio::time::sleep(Duration::from_secs(1)).await;
            let value1 = cluster
                .get_self_key_value(&format!("{key_prefix}:{shard_id1}"))
                .await
                .unwrap();
            // We do not update the position that got lower, nor the position that disappeared
            assert_eq!(&value1, "00000000000000001000");
            let value2 = cluster
                .get_self_key_value(&format!("{key_prefix}:{shard_id2}"))
                .await
                .unwrap();
            assert_eq!(&value2, "00000000000000002000");
            let value3 = cluster
                .get_self_key_value(&format!("{key_prefix}:{shard_id3}"))
                .await
                .unwrap();
            assert_eq!(&value3, "00000000000000003000");
        }
        universe.assert_quit().await;
    }
}
