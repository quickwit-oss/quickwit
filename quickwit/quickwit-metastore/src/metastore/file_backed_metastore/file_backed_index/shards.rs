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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;

use quickwit_proto::ingest::{Shard, ShardState};
use quickwit_proto::metastore::{
    AcquireShardsSubrequest, AcquireShardsSubresponse, DeleteShardsSubrequest, EntityKind,
    ListShardsSubrequest, ListShardsSubresponse, MetastoreError, MetastoreResult,
    OpenShardsSubrequest, OpenShardsSubresponse,
};
use quickwit_proto::types::{queue_id, IndexUid, Position, ShardId, SourceId};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::checkpoint::{PartitionId, SourceCheckpoint, SourceCheckpointDelta};
use crate::file_backed_metastore::MutationOccurred;

/// Manages the shards of a source.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct Shards {
    index_uid: IndexUid,
    source_id: SourceId,
    checkpoint: SourceCheckpoint,
    pub next_shard_id: ShardId,
    pub shards: HashMap<ShardId, Shard>,
}

impl fmt::Debug for Shards {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shards")
            .field("index_uid", &self.index_uid)
            .field("source_id", &self.source_id)
            .field("num_shards", &self.shards.len())
            .finish()
    }
}

impl Shards {
    pub(super) fn empty(index_uid: IndexUid, source_id: SourceId) -> Self {
        Self {
            index_uid,
            source_id,
            checkpoint: SourceCheckpoint::default(),
            next_shard_id: 1, // `1` matches the PostgreSQL sequence min value.
            shards: HashMap::new(),
        }
    }

    pub(super) fn from_serde_shards(
        index_uid: IndexUid,
        source_id: SourceId,
        serde_shards: SerdeShards,
    ) -> Self {
        let mut checkpoint = SourceCheckpoint::default();
        let mut shards = HashMap::with_capacity(serde_shards.shards.len());

        for shard in serde_shards.shards {
            checkpoint.add_partition(
                PartitionId::from(shard.shard_id),
                shard.publish_position_inclusive(),
            );
            shards.insert(shard.shard_id, shard);
        }

        Self {
            index_uid,
            source_id,
            checkpoint,
            next_shard_id: serde_shards.next_shard_id,
            shards,
        }
    }

    fn get_shard(&self, shard_id: ShardId) -> MetastoreResult<&Shard> {
        self.shards.get(&shard_id).ok_or_else(|| {
            let queue_id = queue_id(self.index_uid.as_str(), &self.source_id, shard_id);
            MetastoreError::NotFound(EntityKind::Shard { queue_id })
        })
    }

    fn get_shard_mut(&mut self, shard_id: ShardId) -> MetastoreResult<&mut Shard> {
        self.shards.get_mut(&shard_id).ok_or_else(|| {
            let queue_id = queue_id(self.index_uid.as_str(), &self.source_id, shard_id);
            MetastoreError::NotFound(EntityKind::Shard { queue_id })
        })
    }

    pub(super) fn open_shards(
        &mut self,
        subrequest: OpenShardsSubrequest,
    ) -> MetastoreResult<MutationOccurred<OpenShardsSubresponse>> {
        let mut mutation_occurred = false;

        if subrequest.next_shard_id + 1 < self.next_shard_id
            || subrequest.next_shard_id > self.next_shard_id
        {
            warn!(
                "control plane and metastore next shard IDs do not match, expected `{}`, got `{}`",
                self.next_shard_id, subrequest.next_shard_id
            )
            // TODO: Return an error and crash the control plane.
        }

        let entry = self.shards.entry(subrequest.next_shard_id);
        let shard = match entry {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let shard = Shard {
                    index_uid: self.index_uid.clone().into(),
                    source_id: self.source_id.clone(),
                    shard_id: self.next_shard_id,
                    leader_id: subrequest.leader_id.clone(),
                    follower_id: subrequest.follower_id.clone(),
                    ..Default::default()
                };
                mutation_occurred = true;
                entry.insert(shard.clone());
                self.next_shard_id += 1;

                info!(
                    index_id=%self.index_uid.index_id(),
                    source_id=%self.source_id,
                    shard_id=%shard.shard_id,
                    leader_id=%shard.leader_id,
                    follower_id=?shard.follower_id,
                    "opened shard"
                );
                shard
            }
        };
        let open_shards = vec![shard];
        let next_shard_id = self.next_shard_id;

        let response = OpenShardsSubresponse {
            index_uid: subrequest.index_uid,
            source_id: subrequest.source_id,
            open_shards,
            next_shard_id,
        };
        if mutation_occurred {
            Ok(MutationOccurred::Yes(response))
        } else {
            Ok(MutationOccurred::No(response))
        }
    }

    pub(super) fn acquire_shards(
        &mut self,
        subrequest: AcquireShardsSubrequest,
    ) -> MetastoreResult<MutationOccurred<AcquireShardsSubresponse>> {
        let mut mutation_occurred = false;
        let mut acquired_shards = Vec::with_capacity(subrequest.shard_ids.len());

        for shard_id in subrequest.shard_ids {
            if let Some(shard) = self.shards.get_mut(&shard_id) {
                if shard.publish_token.as_ref() != Some(&subrequest.publish_token) {
                    shard.publish_token = Some(subrequest.publish_token.clone());
                    mutation_occurred = true;
                }
                acquired_shards.push(shard.clone());
            } else {
                warn!(
                    index_id=%self.index_uid.index_id(),
                    source_id=%self.source_id,
                    shard_id=%shard_id,
                    "shard not found"
                );
            }
        }
        let subresponse = AcquireShardsSubresponse {
            index_uid: subrequest.index_uid,
            source_id: subrequest.source_id,
            acquired_shards,
        };
        if mutation_occurred {
            Ok(MutationOccurred::Yes(subresponse))
        } else {
            Ok(MutationOccurred::No(subresponse))
        }
    }

    pub(super) fn delete_shards(
        &mut self,
        subrequest: DeleteShardsSubrequest,
        force: bool,
    ) -> MetastoreResult<MutationOccurred<()>> {
        let mut mutation_occurred = false;

        for shard_id in subrequest.shard_ids {
            if let Entry::Occupied(entry) = self.shards.entry(shard_id) {
                let shard = entry.get();
                if force || shard.publish_position_inclusive() == Position::Eof {
                    mutation_occurred = true;
                    info!(
                        index_id=%self.index_uid.index_id(),
                        source_id=%self.source_id,
                        shard_id=%shard.shard_id,
                        "deleted shard",
                    );
                    entry.remove();
                    continue;
                }
                let message = format!("shard `{shard_id}` is not deletable");
                return Err(MetastoreError::InvalidArgument { message });
            }
        }
        Ok(MutationOccurred::from(mutation_occurred))
    }

    pub(super) fn list_shards(
        &self,
        subrequest: ListShardsSubrequest,
    ) -> MetastoreResult<ListShardsSubresponse> {
        let shards = self.list_shards_inner(subrequest.shard_state);
        let response = ListShardsSubresponse {
            index_uid: subrequest.index_uid,
            source_id: subrequest.source_id,
            shards,
            next_shard_id: self.next_shard_id,
        };
        Ok(response)
    }

    pub(super) fn try_apply_delta(
        &mut self,
        checkpoint_delta: SourceCheckpointDelta,
        publish_token: String,
    ) -> MetastoreResult<MutationOccurred<()>> {
        if checkpoint_delta.is_empty() {
            return Ok(MutationOccurred::No(()));
        }
        self.checkpoint
            .check_compatibility(&checkpoint_delta)
            .map_err(|_error| {
                let message = "incompatible partition delta: FIXME".to_string();
                MetastoreError::InvalidArgument { message }
            })?;

        let mut shard_ids = Vec::with_capacity(checkpoint_delta.num_partitions());

        for (partition_id, partition_delta) in checkpoint_delta.iter() {
            let shard_id = partition_id.as_u64().ok_or_else(|| {
                let message = format!("invalid partition ID: expected a u64, got `{partition_id}`");
                MetastoreError::InvalidArgument { message }
            })?;
            let shard = self.get_shard(shard_id)?;

            if shard.publish_token() != publish_token {
                let message = "failed to apply checkpoint delta: invalid publish token".to_string();
                return Err(MetastoreError::InvalidArgument { message });
            }
            let publish_position_inclusive = partition_delta.to;
            shard_ids.push((shard_id, publish_position_inclusive))
        }
        self.checkpoint
            .try_apply_delta(checkpoint_delta)
            .expect("delta compatibility should have been checked");

        for (shard_id, publish_position_inclusive) in shard_ids {
            let shard = self.get_shard_mut(shard_id).expect("shard should exist");

            if publish_position_inclusive == Position::Eof {
                shard.shard_state = ShardState::Closed as i32;
            }
            shard.publish_position_inclusive = Some(publish_position_inclusive);
        }
        Ok(MutationOccurred::Yes(()))
    }

    fn list_shards_inner(&self, shard_state: Option<i32>) -> Vec<Shard> {
        if let Some(shard_state) = shard_state {
            self.shards
                .values()
                .filter(|shard| shard.shard_state == shard_state)
                .cloned()
                .collect()
        } else {
            self.shards.values().cloned().collect()
        }
    }
}

/// The serialized representation of [`SourceShards`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct SerdeShards {
    pub next_shard_id: ShardId,
    pub shards: Vec<Shard>,
}

impl From<Shards> for SerdeShards {
    fn from(shards: Shards) -> Self {
        Self {
            next_shard_id: shards.next_shard_id,
            shards: shards.shards.into_values().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::ShardState;

    use super::*;

    #[test]
    fn test_open_shards() {
        let index_uid: IndexUid = "test-index:0".into();
        let source_id = "test-source".to_string();
        let mut shards = Shards::empty(index_uid.clone(), source_id.clone());

        let subrequest = OpenShardsSubrequest {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            leader_id: "leader_id".to_string(),
            follower_id: None,
            next_shard_id: 1,
        };
        let MutationOccurred::Yes(subresponse) = shards.open_shards(subrequest.clone()).unwrap()
        else {
            panic!("Expected `MutationOccured::Yes`");
        };
        assert_eq!(subresponse.index_uid, index_uid.as_str());
        assert_eq!(subresponse.source_id, source_id);
        assert_eq!(subresponse.open_shards.len(), 1);

        let shard = &subresponse.open_shards[0];
        assert_eq!(shard.index_uid, index_uid.as_str());
        assert_eq!(shard.source_id, source_id);
        assert_eq!(shard.shard_id, 1);
        assert_eq!(shard.shard_state, 0);
        assert_eq!(shard.leader_id, "leader_id");
        assert_eq!(shard.follower_id, None);
        assert_eq!(shard.publish_position_inclusive(), Position::Beginning);

        assert_eq!(shards.shards.get(&1).unwrap(), shard);

        let MutationOccurred::No(subresponse) = shards.open_shards(subrequest).unwrap() else {
            panic!("Expected `MutationOccured::No`");
        };
        assert_eq!(subresponse.open_shards.len(), 1);

        let shard = &subresponse.open_shards[0];
        assert_eq!(shards.shards.get(&1).unwrap(), shard);

        let subrequest = OpenShardsSubrequest {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            leader_id: "leader_id".to_string(),
            follower_id: Some("follower_id".to_string()),
            next_shard_id: 2,
        };
        let MutationOccurred::Yes(subresponse) = shards.open_shards(subrequest).unwrap() else {
            panic!("Expected `MutationOccured::No`");
        };
        assert_eq!(subresponse.index_uid, index_uid.as_str());
        assert_eq!(subresponse.source_id, source_id);
        assert_eq!(subresponse.open_shards.len(), 1);

        let shard = &subresponse.open_shards[0];
        assert_eq!(shard.index_uid, index_uid.as_str());
        assert_eq!(shard.source_id, source_id);
        assert_eq!(shard.shard_id, 2);
        assert_eq!(shard.shard_state, 0);
        assert_eq!(shard.leader_id, "leader_id");
        assert_eq!(shard.follower_id.as_ref().unwrap(), "follower_id");
        assert_eq!(shard.publish_position_inclusive(), Position::Beginning);

        assert_eq!(shards.shards.get(&2).unwrap(), shard);
    }

    #[test]
    fn test_close_shard() {}

    #[test]
    fn test_list_shards() {
        let index_uid: IndexUid = "test-index:0".into();
        let source_id = "test-source".to_string();
        let mut shards = Shards::empty(index_uid.clone(), source_id.clone());

        let subrequest = ListShardsSubrequest {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_state: None,
        };
        let subresponse = shards.list_shards(subrequest).unwrap();
        assert_eq!(subresponse.index_uid, index_uid.as_str());
        assert_eq!(subresponse.source_id, source_id);
        assert_eq!(subresponse.shards.len(), 0);

        let shard_0 = Shard {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_id: 0,
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let shard_1 = Shard {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_id: 1,
            shard_state: ShardState::Closed as i32,
            ..Default::default()
        };
        shards.shards.insert(0, shard_0);
        shards.shards.insert(1, shard_1);

        let subrequest = ListShardsSubrequest {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_state: None,
        };
        let mut subresponse = shards.list_shards(subrequest).unwrap();
        subresponse
            .shards
            .sort_unstable_by_key(|shard| shard.shard_id);
        assert_eq!(subresponse.shards.len(), 2);
        assert_eq!(subresponse.shards[0].shard_id, 0);
        assert_eq!(subresponse.shards[1].shard_id, 1);

        let subrequest = ListShardsSubrequest {
            index_uid: index_uid.into(),
            source_id,
            shard_state: Some(ShardState::Closed as i32),
        };
        let subresponse = shards.list_shards(subrequest).unwrap();
        assert_eq!(subresponse.shards.len(), 1);
        assert_eq!(subresponse.shards[0].shard_id, 1);
    }
}
