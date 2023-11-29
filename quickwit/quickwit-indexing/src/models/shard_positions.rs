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

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;

use anyhow::Context;
use async_trait::async_trait;
use chitchat::ListenerHandle;
use fnv::FnvHashMap;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, SpawnContext};
use quickwit_cluster::Cluster;
use quickwit_common::pubsub::{Event, EventBroker};
use quickwit_proto::indexing::ShardPositionsUpdate;
use quickwit_proto::types::{IndexUid, Position, ShardId, SourceUid};
use tracing::{error, warn};

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
    // This list can be partial: not all shards for the source need to be listed here.
    pub shard_positions: Vec<(ShardId, Position)>,
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
    let (index_uid_str, source_id) = key.rsplit_once(':').context("invalid key")?;
    let index_uid = IndexUid::parse(index_uid_str)?;
    let source_uid = SourceUid {
        index_uid,
        source_id: source_id.to_string(),
    };
    let shard_positions_map: HashMap<ShardId, Position> =
        serde_json::from_str(value).context("failed to parse shard positions json")?;
    let shard_positions = shard_positions_map.into_iter().collect();
    Ok(ClusterShardPositionsUpdate {
        source_uid,
        shard_positions,
    })
}

#[async_trait]
impl Actor for ShardPositionsService {
    type ObservableState = ();
    fn observable_state(&self) {}

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        let mailbox = ctx.mailbox().clone();
        self.cluster_listener_handle_opt = Some(
            self.cluster
                .subscribe(SHARD_POSITIONS_PREFIX, move |key, value| {
                    let shard_positions= match parse_shard_positions_from_kv(key, value) {
                        Ok(shard_positions) => {
                            shard_positions
                        }
                        Err(error) => {
                            error!(key=key, value=value, error=%error, "failed to parse shard positions from cluster kv");
                            return;
                        }
                    };
                    if mailbox.try_send_message(shard_positions).is_err() {
                        error!("failed to send shard positions to the shard positions service");
                    }
                })
                .await
        );
        Ok(())
    }
}

impl ShardPositionsService {
    pub fn spawn(spawn_ctx: &SpawnContext, event_broker: EventBroker, cluster: Cluster) {
        let shard_positions_service = ShardPositionsService::new(event_broker.clone(), cluster);
        let (shard_positions_service_mailbox, _) =
            spawn_ctx.spawn_builder().spawn(shard_positions_service);
        event_broker
            .subscribe::<LocalShardPositionsUpdate>(move |update| {
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
            shard_positions,
        } = update;
        let was_updated = self.apply_update(&source_uid, shard_positions);
        if was_updated {
            self.publish_shard_updates_to_event_broker(source_uid);
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
    ) -> Result<Self::Reply, ActorExitStatus> {
        let LocalShardPositionsUpdate {
            source_uid,
            shard_positions,
        } = update;
        let was_updated = self.apply_update(&source_uid, shard_positions);
        if was_updated {
            self.publish_positions_into_chitchat(source_uid.clone())
                .await;
            self.publish_shard_updates_to_event_broker(source_uid);
        }
        Ok(())
    }
}

impl ShardPositionsService {
    async fn publish_positions_into_chitchat(&self, source_uid: SourceUid) {
        let Some(shard_positions) = self.shard_positions_per_source.get(&source_uid) else {
            return;
        };
        let SourceUid {
            index_uid,
            source_id,
        } = &source_uid;
        let key = format!("{SHARD_POSITIONS_PREFIX}{index_uid}:{source_id}");
        let shard_positions_json = serde_json::to_string(&shard_positions).unwrap();
        self.cluster
            .set_self_key_value(key, shard_positions_json)
            .await;
    }

    fn publish_shard_updates_to_event_broker(&self, source_uid: SourceUid) {
        let Some(shard_positions_map) = self.shard_positions_per_source.get(&source_uid) else {
            return;
        };
        let shard_positions: Vec<(ShardId, Position)> = shard_positions_map
            .iter()
            .map(|(&shard_id, position)| (shard_id, position.clone()))
            .collect();
        self.event_broker.publish(ShardPositionsUpdate {
            source_uid,
            shard_positions,
        });
    }

    /// Updates the internal model holding the last position per shard, and
    /// returns true if at least one of the publish position was updated.
    fn apply_update(
        &mut self,
        source_uid: &SourceUid,
        published_positions_per_shard: Vec<(ShardId, Position)>,
    ) -> bool {
        if published_positions_per_shard.is_empty() {
            warn!("received an empty publish shard positions update");
            return false;
        }
        let mut was_modified = false;
        let current_shard_positions = self
            .shard_positions_per_source
            .entry(source_uid.clone())
            .or_default();
        for (shard, new_position) in published_positions_per_shard {
            match current_shard_positions.entry(shard) {
                Entry::Occupied(mut occupied) => {
                    if *occupied.get() < new_position {
                        occupied.insert(new_position);
                        was_modified = true;
                    }
                }
                Entry::Vacant(vacant) => {
                    was_modified = true;
                    vacant.insert(new_position.clone());
                }
            }
        }
        was_modified
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chitchat::transport::ChannelTransport;
    use quickwit_actors::Universe;
    use quickwit_cluster::create_cluster_for_test;
    use quickwit_common::pubsub::EventBroker;
    use quickwit_proto::types::IndexUid;

    use super::*;

    #[tokio::test]
    async fn test_shard_positions_from_cluster() {
        quickwit_common::setup_logging_for_tests();

        let transport = ChannelTransport::default();
        let universe1 = Universe::with_accelerated_time();
        let universe2 = Universe::with_accelerated_time();
        let cluster1 = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let cluster2 = create_cluster_for_test(
            vec![cluster1.gossip_listen_addr.to_string()],
            &["indexer", "metastore"],
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

        let event_broker1 = EventBroker::default();
        let event_broker2 = EventBroker::default();
        ShardPositionsService::spawn(
            universe1.spawn_ctx(),
            event_broker1.clone(),
            cluster1.clone(),
        );
        ShardPositionsService::spawn(
            universe2.spawn_ctx(),
            event_broker2.clone(),
            cluster2.clone(),
        );

        let index_uid = IndexUid::new_with_random_ulid("index-test");
        let source_id = "test-source".to_string();
        let source_uid = SourceUid {
            index_uid,
            source_id,
        };

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

        // ----------------------
        // One of the node publishes a given shard position update.
        // This is done using a LocalPublishShardPositionUpdate

        event_broker1.publish(LocalShardPositionsUpdate::new(
            source_uid.clone(),
            vec![(1, Position::Beginning)],
        ));
        event_broker1.publish(LocalShardPositionsUpdate::new(
            source_uid.clone(),
            vec![(2, 10u64.into())],
        ));
        event_broker1.publish(LocalShardPositionsUpdate::new(
            source_uid.clone(),
            vec![(1, 10u64.into())],
        ));
        event_broker2.publish(LocalShardPositionsUpdate::new(
            source_uid.clone(),
            vec![(2, 10u64.into())],
        ));
        event_broker2.publish(LocalShardPositionsUpdate::new(
            source_uid.clone(),
            vec![(2, 12u64.into())],
        ));
        event_broker2.publish(LocalShardPositionsUpdate::new(
            source_uid.clone(),
            vec![(1, Position::Beginning), (2, 12u64.into())],
        ));

        let mut updates1: Vec<Vec<(ShardId, Position)>> = Vec::new();
        for _ in 0..4 {
            let update = rx1.recv().await.unwrap();
            assert_eq!(update.source_uid, source_uid);
            updates1.push(update.shard_positions);
        }

        // The updates as seen from the first node.
        assert_eq!(
            updates1,
            vec![
                vec![(1, Position::Beginning)],
                vec![(1, Position::Beginning), (2, 10u64.into())],
                vec![(1, 10u64.into()), (2, 10u64.into()),],
                vec![(1, 10u64.into()), (2, 12u64.into()),],
            ]
        );

        // The updates as seen from the second.
        let mut updates2: Vec<Vec<(ShardId, Position)>> = Vec::new();
        for _ in 0..4 {
            let update = rx2.recv().await.unwrap();
            assert_eq!(update.source_uid, source_uid);
            updates2.push(update.shard_positions);
        }
        assert_eq!(
            updates2,
            vec![
                vec![(2, 10u64.into())],
                vec![(2, 12u64.into())],
                vec![(1, Position::Beginning), (2, 12u64.into())],
                vec![(1, 10u64.into()), (2, 12u64.into())],
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
        let key = format!("{SHARD_POSITIONS_PREFIX}{index_uid}:{source_id}");
        let source_uid = SourceUid {
            index_uid,
            source_id,
        };
        event_broker.publish(LocalShardPositionsUpdate::new(
            source_uid.clone(),
            Vec::new(),
        ));

        assert!(cluster.get_self_key_value(&key).await.is_none());

        {
            event_broker.publish(LocalShardPositionsUpdate::new(
                source_uid.clone(),
                vec![(1, Position::Beginning)],
            ));
            tokio::time::sleep(Duration::from_millis(1000)).await;
            let value = cluster.get_self_key_value(&key).await.unwrap();
            assert_eq!(&value, r#"{"1":""}"#);
        }
        {
            event_broker.publish(LocalShardPositionsUpdate::new(
                source_uid.clone(),
                vec![(1, 1_000u64.into()), (2, 2000u64.into())],
            ));
            tokio::time::sleep(Duration::from_millis(1000)).await;
            let value = cluster.get_self_key_value(&key).await.unwrap();
            assert_eq!(
                &value,
                r#"{"1":"00000000000000001000","2":"00000000000000002000"}"#
            );
        }
        {
            event_broker.publish(LocalShardPositionsUpdate::new(
                source_uid.clone(),
                vec![(1, 999u64.into()), (3, 3000u64.into())],
            ));
            tokio::time::sleep(Duration::from_millis(1000)).await;
            let value = cluster.get_self_key_value(&key).await.unwrap();
            // We do not update the position that got lower, nor the position that disappeared
            assert_eq!(
                &value,
                r#"{"1":"00000000000000001000","2":"00000000000000002000","3":"00000000000000003000"}"#
            );
        }
        universe.assert_quit().await;
    }
}
