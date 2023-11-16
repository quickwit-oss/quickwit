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
use std::collections::BTreeMap;
use std::fmt::Debug;

use async_trait::async_trait;
use fnv::FnvHashMap;
use quickwit_cluster::Cluster;
use quickwit_common::pubsub::{Event, EventSubscriber};
use quickwit_common::shared_consts::SHARD_POSITIONS_PREFIX;
use quickwit_proto::types::{Position, ShardId, SourceUid};
use tracing::warn;

#[derive(Debug, Clone)]
pub struct PublishedShardPositionsUpdate {
    pub source_uid: SourceUid,
    pub published_positions_per_shard: Vec<(ShardId, Position)>,
}

impl Event for PublishedShardPositionsUpdate {}

/// The published shard positions is a model unique to the indexer service instance that
/// keeps track of the latest (known) published position for the shards of all managed sources.
///
/// It receives updates through the event broker, and only keeps the maximum published position
/// for each shard.
pub struct PublishedShardPositions {
    shard_positions_per_source: FnvHashMap<SourceUid, BTreeMap<ShardId, Position>>,
    cluster_client: Cluster,
}

impl PublishedShardPositions {
    pub fn new(cluster: Cluster) -> PublishedShardPositions {
        PublishedShardPositions {
            shard_positions_per_source: Default::default(),
            cluster_client: cluster,
        }
    }
}
#[async_trait]
impl EventSubscriber<PublishedShardPositionsUpdate> for PublishedShardPositions {
    async fn handle_event(&mut self, update: PublishedShardPositionsUpdate) {
        let source_uid = update.source_uid.clone();
        let was_updated = self.apply_update(update);
        if was_updated {
            self.published_updated_positions_for_source(source_uid)
                .await;
        }
    }
}

impl PublishedShardPositions {
    async fn published_updated_positions_for_source(&self, source_uid: SourceUid) {
        let Some(shard_positions) = self.shard_positions_per_source.get(&source_uid) else {
            return;
        };
        let SourceUid {
            index_uid,
            source_id,
        } = &source_uid;
        let key = format!("{SHARD_POSITIONS_PREFIX}{index_uid}:{source_id}");
        let shard_positions_json = serde_json::to_string(&shard_positions).unwrap();
        self.cluster_client
            .set_self_key_value(key, shard_positions_json)
            .await;
    }

    /// Updates the internal model holding the last position per shard, and
    /// returns true if at least one of the publish position was updated.
    fn apply_update(&mut self, update: PublishedShardPositionsUpdate) -> bool {
        if update.published_positions_per_shard.is_empty() {
            warn!("received an empty publish shard positions update");
            return false;
        }
        let mut was_modified = false;
        let PublishedShardPositionsUpdate {
            source_uid,
            published_positions_per_shard,
        } = update;
        if published_positions_per_shard.is_empty() {
            return false;
        }
        let current_shard_positions = self
            .shard_positions_per_source
            .entry(source_uid)
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
    use chitchat::transport::ChannelTransport;
    use quickwit_cluster::create_cluster_for_test;
    use quickwit_common::pubsub::EventBroker;
    use quickwit_common::shared_consts::SHARD_POSITIONS_PREFIX;
    use quickwit_proto::types::IndexUid;

    use super::*;

    #[tokio::test]
    async fn test_shard_positions() {
        let transport = ChannelTransport::default();
        let cluster: Cluster = create_cluster_for_test(Vec::new(), &[], &transport, true)
            .await
            .unwrap();
        let shard_positions = PublishedShardPositions::new(cluster.clone());
        let event_broker = EventBroker::default();
        event_broker.subscribe(shard_positions).forever();
        let index_uid = IndexUid::new_with_random_ulid("index-test");
        let source_id = "test-source".to_string();
        let key = format!("{SHARD_POSITIONS_PREFIX}{index_uid}:{source_id}");
        let source_uid = SourceUid {
            index_uid,
            source_id,
        };
        event_broker.publish(PublishedShardPositionsUpdate {
            source_uid: source_uid.clone(),
            published_positions_per_shard: vec![],
        });
        {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            assert!(cluster.get_self_key_value(&key).await.is_none());
        }
        {
            event_broker.publish(PublishedShardPositionsUpdate {
                source_uid: source_uid.clone(),
                published_positions_per_shard: vec![(1, Position::Beginning)],
            });
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let value = cluster.get_self_key_value(&key).await.unwrap();
            assert_eq!(&value, r#"{"1":""}"#);
        }
        {
            event_broker.publish(PublishedShardPositionsUpdate {
                source_uid: source_uid.clone(),
                published_positions_per_shard: vec![(1, 1_000u64.into()), (2, 2000u64.into())],
            });
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let value = cluster.get_self_key_value(&key).await.unwrap();
            assert_eq!(
                &value,
                r#"{"1":"00000000000000001000","2":"00000000000000002000"}"#
            );
        }
        {
            event_broker.publish(PublishedShardPositionsUpdate {
                source_uid: source_uid.clone(),
                published_positions_per_shard: vec![(1, 999u64.into()), (3, 3000u64.into())],
            });
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let value = cluster.get_self_key_value(&key).await.unwrap();
            // We do not update the position that got lower, nor the position that disappeared
            assert_eq!(
                &value,
                r#"{"1":"00000000000000001000","2":"00000000000000002000","3":"00000000000000003000"}"#
            );
        }
    }
}
