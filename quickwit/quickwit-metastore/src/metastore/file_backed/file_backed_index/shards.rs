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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;

use itertools::Itertools;
use quickwit_proto::ingest::{Shard, ShardState};
use quickwit_proto::metastore::{
    AcquireShardsRequest, AcquireShardsResponse, DeleteShardsRequest, DeleteShardsResponse,
    EntityKind, ListShardsSubrequest, ListShardsSubresponse, MetastoreError, MetastoreResult,
    OpenShardSubrequest, OpenShardSubresponse, PruneShardsRequest,
};
use quickwit_proto::types::{queue_id, IndexUid, Position, PublishToken, ShardId, SourceId};
use time::OffsetDateTime;
use tracing::{info, warn};

use crate::checkpoint::{PartitionId, SourceCheckpoint, SourceCheckpointDelta};
use crate::file_backed::MutationOccurred;

// TODO: Rename `SourceShards`
/// Manages the shards of a source.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct Shards {
    index_uid: IndexUid,
    source_id: SourceId,
    checkpoint: SourceCheckpoint,
    shards: HashMap<ShardId, Shard>,
}

impl fmt::Debug for Shards {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shards")
            .field("index_uid", &self.index_uid)
            .field("source_id", &self.source_id)
            .field("num_shards", &self.shards.len())
            .field("shards", &self.shards)
            .finish()
    }
}

impl Shards {
    pub(super) fn empty(index_uid: IndexUid, source_id: SourceId) -> Self {
        Self {
            index_uid,
            source_id,
            checkpoint: SourceCheckpoint::default(),
            shards: HashMap::new(),
        }
    }

    pub(super) fn from_shards_vec(
        index_uid: IndexUid,
        source_id: SourceId,
        shards_vec: Vec<Shard>,
    ) -> Self {
        let mut shards: HashMap<ShardId, Shard> = HashMap::with_capacity(shards_vec.len());
        let mut checkpoint = SourceCheckpoint::default();

        for shard in shards_vec {
            let shard_id = shard.shard_id().clone();
            let partition_id = PartitionId::from(shard_id.as_str());
            let position = shard.publish_position_inclusive();
            checkpoint.add_partition(partition_id, position);
            shards.insert(shard_id, shard);
        }

        Self {
            index_uid,
            source_id,
            checkpoint,
            shards,
        }
    }

    pub fn into_shards_vec(self) -> Vec<Shard> {
        self.shards.into_values().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.shards.is_empty()
    }

    fn get_shard(&self, shard_id: &ShardId) -> MetastoreResult<&Shard> {
        self.shards.get(shard_id).ok_or_else(|| {
            let queue_id = queue_id(&self.index_uid, &self.source_id, shard_id);
            MetastoreError::NotFound(EntityKind::Shard { queue_id })
        })
    }

    fn get_shard_mut(&mut self, shard_id: &ShardId) -> MetastoreResult<&mut Shard> {
        self.shards.get_mut(shard_id).ok_or_else(|| {
            let queue_id = queue_id(&self.index_uid, &self.source_id, shard_id);
            MetastoreError::NotFound(EntityKind::Shard { queue_id })
        })
    }

    pub(super) fn open_shard(
        &mut self,
        subrequest: OpenShardSubrequest,
    ) -> MetastoreResult<MutationOccurred<OpenShardSubresponse>> {
        let mut mutation_occurred = false;

        let shard_id = subrequest.shard_id().clone();
        let entry = self.shards.entry(shard_id.clone());
        let shard = match entry {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let shard = Shard {
                    index_uid: Some(self.index_uid.clone()),
                    source_id: self.source_id.clone(),
                    shard_id: Some(shard_id.clone()),
                    shard_state: ShardState::Open as i32,
                    leader_id: subrequest.leader_id,
                    follower_id: subrequest.follower_id,
                    doc_mapping_uid: subrequest.doc_mapping_uid,
                    publish_position_inclusive: Some(Position::Beginning),
                    publish_token: subrequest.publish_token.clone(),
                    update_timestamp: OffsetDateTime::now_utc().unix_timestamp(),
                };
                mutation_occurred = true;
                entry.insert(shard.clone());

                info!(
                    index_uid=%self.index_uid,
                    source_id=%self.source_id,
                    %shard_id,
                    leader_id=%shard.leader_id,
                    follower_id=?shard.follower_id,
                    "opened shard"
                );
                shard
            }
        };
        let response = OpenShardSubresponse {
            subrequest_id: subrequest.subrequest_id,
            open_shard: Some(shard),
        };
        if mutation_occurred {
            Ok(MutationOccurred::Yes(response))
        } else {
            Ok(MutationOccurred::No(response))
        }
    }

    pub(super) fn acquire_shards(
        &mut self,
        request: AcquireShardsRequest,
    ) -> MetastoreResult<MutationOccurred<AcquireShardsResponse>> {
        let mut mutation_occurred = false;
        let mut acquired_shards = Vec::with_capacity(request.shard_ids.len());

        for shard_id in &request.shard_ids {
            if let Some(shard) = self.shards.get_mut(shard_id) {
                if shard.publish_token() != request.publish_token {
                    shard.publish_token = Some(request.publish_token.clone());
                    mutation_occurred = true;
                }
                acquired_shards.push(shard.clone());
            } else {
                warn!(
                    index_uid=%self.index_uid,
                    source_id=%self.source_id,
                    %shard_id,
                    "shard not found"
                );
            }
        }
        let response = AcquireShardsResponse { acquired_shards };

        if mutation_occurred {
            Ok(MutationOccurred::Yes(response))
        } else {
            Ok(MutationOccurred::No(response))
        }
    }

    pub(super) fn delete_shards(
        &mut self,
        request: DeleteShardsRequest,
    ) -> MetastoreResult<MutationOccurred<DeleteShardsResponse>> {
        let mut successes = Vec::with_capacity(request.shard_ids.len());
        let mut failures = Vec::new();
        let mut mutation_occurred = false;

        for shard_id in request.shard_ids {
            if let Entry::Occupied(entry) = self.shards.entry(shard_id.clone()) {
                let shard = entry.get();
                if !request.force && !shard.publish_position_inclusive().is_eof() {
                    failures.push(shard_id);
                    continue;
                }
                info!(
                    index_uid=%self.index_uid,
                    source_id=%self.source_id,
                    %shard_id,
                    "deleted shard",
                );
                entry.remove();
                mutation_occurred = true;
            }
            successes.push(shard_id);
        }
        if !failures.is_empty() {
            warn!(
                index_uid=%self.index_uid,
                source_id=%self.source_id,
                "failed to delete shards `{}`: shards are not fully indexed",
                failures.iter().join(", ")
            );
        }
        let response = DeleteShardsResponse {
            index_uid: request.index_uid,
            source_id: request.source_id,
            successes,
            failures,
        };
        if mutation_occurred {
            Ok(MutationOccurred::Yes(response))
        } else {
            Ok(MutationOccurred::No(response))
        }
    }

    pub(super) fn prune_shards(
        &mut self,
        request: PruneShardsRequest,
    ) -> MetastoreResult<MutationOccurred<()>> {
        let initial_shard_count = self.shards.len();

        if let Some(max_age_secs) = request.max_age_secs {
            self.shards.retain(|_, shard| {
                let gc_deadline = shard.update_timestamp + max_age_secs as i64;
                let now = OffsetDateTime::now_utc().unix_timestamp();
                gc_deadline >= now
            });
        };
        if let Some(max_count) = request.max_count {
            let max_count = max_count as usize;
            if max_count < self.shards.len() {
                let num_to_remove = self.shards.len() - max_count;
                let shard_ids_to_delete = self
                    .shards
                    .values()
                    .sorted_by_key(|shard| shard.update_timestamp)
                    .take(num_to_remove)
                    .map(|shard| shard.shard_id().clone())
                    .collect_vec();
                for shard_id in shard_ids_to_delete {
                    self.shards.remove(&shard_id);
                }
            }
        }
        if initial_shard_count > self.shards.len() {
            Ok(MutationOccurred::Yes(()))
        } else {
            Ok(MutationOccurred::No(()))
        }
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
        };
        Ok(response)
    }

    pub(super) fn try_apply_delta(
        &mut self,
        checkpoint_delta: SourceCheckpointDelta,
        publish_token: PublishToken,
    ) -> MetastoreResult<MutationOccurred<()>> {
        if checkpoint_delta.is_empty() {
            return Ok(MutationOccurred::No(()));
        }
        self.checkpoint
            .check_compatibility(&checkpoint_delta)
            .map_err(|error| MetastoreError::InvalidArgument {
                message: error.to_string(),
            })?;

        let mut shard_ids = Vec::with_capacity(checkpoint_delta.num_partitions());

        for (partition_id, partition_delta) in checkpoint_delta.iter() {
            let shard_id = ShardId::from(partition_id.as_str());
            let shard = self.get_shard(&shard_id)?;

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
            let shard = self.get_shard_mut(&shard_id).expect("shard should exist");

            if publish_position_inclusive.is_eof() {
                shard.shard_state = ShardState::Closed as i32;
            }
            shard.publish_position_inclusive = Some(publish_position_inclusive);
            shard.update_timestamp = OffsetDateTime::now_utc().unix_timestamp();
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

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::ShardState;
    use quickwit_proto::types::DocMappingUid;

    use super::*;

    impl Shards {
        pub(crate) fn insert_shards(&mut self, shards: Vec<Shard>) {
            for shard in shards {
                let shard_id = shard.shard_id().clone();
                self.shards.insert(shard_id, shard);
            }
        }
    }

    #[test]
    fn test_open_shards() {
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let mut shards = Shards::empty(index_uid.clone(), source_id.clone());

        let subrequest = OpenShardSubrequest {
            subrequest_id: 0,
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            leader_id: "leader_id".to_string(),
            follower_id: None,
            doc_mapping_uid: Some(DocMappingUid::default()),
            publish_token: None,
        };
        let MutationOccurred::Yes(subresponse) = shards.open_shard(subrequest.clone()).unwrap()
        else {
            panic!("expected `MutationOccurred::Yes`");
        };
        assert_eq!(subresponse.subrequest_id, 0);

        let shard = subresponse.open_shard();
        assert_eq!(shard.index_uid(), &index_uid);
        assert_eq!(shard.source_id, source_id);
        assert_eq!(shard.shard_id(), ShardId::from(1));
        assert_eq!(shard.shard_state(), ShardState::Open);
        assert_eq!(shard.leader_id, "leader_id");
        assert_eq!(shard.follower_id, None);
        assert_eq!(shard.publish_token, None);
        assert_eq!(shard.publish_position_inclusive(), Position::Beginning);

        let MutationOccurred::No(subresponse) = shards.open_shard(subrequest).unwrap() else {
            panic!("Expected `MutationOccurred::No`");
        };
        assert_eq!(subresponse.subrequest_id, 0);

        let shard = subresponse.open_shard();
        assert_eq!(shards.shards.get(&ShardId::from(1)).unwrap(), shard);

        let subrequest = OpenShardSubrequest {
            subrequest_id: 0,
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            leader_id: "leader_id".to_string(),
            follower_id: Some("follower_id".to_string()),
            doc_mapping_uid: Some(DocMappingUid::default()),
            publish_token: Some("publish_token".to_string()),
        };
        let MutationOccurred::Yes(subresponse) = shards.open_shard(subrequest).unwrap() else {
            panic!("Expected `MutationOccurred::No`");
        };
        assert_eq!(subresponse.subrequest_id, 0);

        let shard = subresponse.open_shard();
        assert_eq!(shard.index_uid(), &index_uid);
        assert_eq!(shard.source_id, source_id);
        assert_eq!(shard.shard_id(), ShardId::from(2));
        assert_eq!(shard.shard_state(), ShardState::Open);
        assert_eq!(shard.leader_id, "leader_id");
        assert_eq!(shard.follower_id.as_ref().unwrap(), "follower_id");
        assert_eq!(shard.publish_position_inclusive(), Position::Beginning);

        assert_eq!(shards.shards.get(&ShardId::from(2)).unwrap(), shard);
    }

    #[test]
    fn test_list_shards() {
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let mut shards = Shards::empty(index_uid.clone(), source_id.clone());

        let subrequest = ListShardsSubrequest {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_state: None,
        };
        let subresponse = shards.list_shards(subrequest).unwrap();
        assert_eq!(subresponse.index_uid(), &index_uid);
        assert_eq!(subresponse.source_id, source_id);
        assert_eq!(subresponse.shards.len(), 0);

        let shard_0 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(0)),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let shard_1 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Closed as i32,
            ..Default::default()
        };
        shards.shards.insert(ShardId::from(0), shard_0);
        shards.shards.insert(ShardId::from(1), shard_1);

        let subrequest = ListShardsSubrequest {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_state: None,
        };
        let mut subresponse = shards.list_shards(subrequest).unwrap();
        subresponse
            .shards
            .sort_unstable_by(|left, right| left.shard_id.cmp(&right.shard_id));
        assert_eq!(subresponse.shards.len(), 2);
        assert_eq!(subresponse.shards[0].shard_id(), ShardId::from(0));
        assert_eq!(subresponse.shards[1].shard_id(), ShardId::from(1));

        let subrequest = ListShardsSubrequest {
            index_uid: index_uid.into(),
            source_id,
            shard_state: Some(ShardState::Closed as i32),
        };
        let subresponse = shards.list_shards(subrequest).unwrap();
        assert_eq!(subresponse.shards.len(), 1);
        assert_eq!(subresponse.shards[0].shard_id(), ShardId::from(1));
    }

    #[test]
    fn test_acquire_shards() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let mut shards = Shards::empty(index_uid.clone(), source_id.clone());

        let request = AcquireShardsRequest {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_ids: Vec::new(),
            publish_token: "test-publish-token".to_string(),
        };
        let MutationOccurred::No(response) = shards.acquire_shards(request).unwrap() else {
            panic!("Expected `MutationOccurred::No`");
        };
        assert!(response.acquired_shards.is_empty());

        let request = AcquireShardsRequest {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_ids: vec![ShardId::from(0), ShardId::from(1)],
            publish_token: "test-publish-token".to_string(),
        };
        let MutationOccurred::No(response) = shards.acquire_shards(request.clone()).unwrap() else {
            panic!("Expected `MutationOccurred::No`");
        };
        assert!(response.acquired_shards.is_empty());

        shards.shards.insert(
            ShardId::from(0),
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(0)),
                shard_state: ShardState::Open as i32,
                publish_position_inclusive: Some(Position::eof(0u64)),
                ..Default::default()
            },
        );
        let MutationOccurred::Yes(response) = shards.acquire_shards(request.clone()).unwrap()
        else {
            panic!("expected `MutationOccurred::Yes`");
        };
        assert_eq!(response.acquired_shards.len(), 1);
        let acquired_shard = &response.acquired_shards[0];
        assert_eq!(acquired_shard.shard_id(), ShardId::from(0));

        assert_eq!(
            shards
                .shards
                .get(&ShardId::from(0))
                .unwrap()
                .publish_token(),
            "test-publish-token"
        );
    }

    #[test]
    fn test_delete_shards() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let mut shards = Shards::empty(index_uid.clone(), source_id.clone());

        let request = DeleteShardsRequest {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_ids: Vec::new(),
            force: false,
        };
        let MutationOccurred::No(response) = shards.delete_shards(request).unwrap() else {
            panic!("expected `MutationOccurred::No`");
        };
        assert_eq!(response.index_uid(), &index_uid);
        assert_eq!(response.source_id, source_id);
        assert!(response.successes.is_empty());
        assert!(response.failures.is_empty());

        let request = DeleteShardsRequest {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_ids: vec![ShardId::from(0)],
            force: false,
        };
        let MutationOccurred::No(response) = shards.delete_shards(request).unwrap() else {
            panic!("expected `MutationOccurred::No`");
        };
        assert_eq!(response.index_uid(), &index_uid);
        assert_eq!(response.source_id, source_id);
        assert_eq!(response.successes.len(), 1);
        assert_eq!(response.successes[0], ShardId::from(0));
        assert!(response.failures.is_empty());

        shards.shards.insert(
            ShardId::from(0),
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(0)),
                shard_state: ShardState::Open as i32,
                publish_position_inclusive: Some(Position::eof(0u64)),
                ..Default::default()
            },
        );
        shards.shards.insert(
            ShardId::from(1),
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(1)),
                shard_state: ShardState::Open as i32,
                publish_position_inclusive: Some(Position::offset(0u64)),
                ..Default::default()
            },
        );
        let request = DeleteShardsRequest {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_ids: vec![ShardId::from(0), ShardId::from(1)],
            force: false,
        };
        let MutationOccurred::Yes(response) = shards.delete_shards(request).unwrap() else {
            panic!("expected `MutationOccurred::Yes`");
        };
        assert_eq!(response.index_uid(), &index_uid);
        assert_eq!(response.source_id, source_id);
        assert_eq!(response.successes.len(), 1);
        assert_eq!(response.successes[0], ShardId::from(0));
        assert_eq!(response.failures.len(), 1);
        assert_eq!(response.failures[0], ShardId::from(1));

        let request = DeleteShardsRequest {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_ids: vec![ShardId::from(1)],
            force: true,
        };
        let MutationOccurred::Yes(response) = shards.delete_shards(request).unwrap() else {
            panic!("expected `MutationOccurred::Yes`");
        };
        assert_eq!(response.index_uid(), &index_uid);
        assert_eq!(response.source_id, source_id);
        assert_eq!(response.successes.len(), 1);
        assert_eq!(response.successes[0], ShardId::from(1));
        assert!(response.failures.is_empty());

        assert!(shards.shards.is_empty());
    }

    #[test]
    fn test_prune_shards() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let mut shards = Shards::empty(index_uid.clone(), source_id.clone());

        let request = PruneShardsRequest {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            max_age_secs: None,
            max_count: None,
            interval: None,
        };
        let MutationOccurred::No(()) = shards.prune_shards(request).unwrap() else {
            panic!("expected `MutationOccurred::No`");
        };

        let request = PruneShardsRequest {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            max_age_secs: Some(50),
            max_count: None,
            interval: None,
        };
        let MutationOccurred::No(()) = shards.prune_shards(request).unwrap() else {
            panic!("expected `MutationOccurred::No`");
        };

        let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();
        shards.shards.insert(
            ShardId::from(0),
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(0)),
                shard_state: ShardState::Open as i32,
                publish_position_inclusive: Some(Position::eof(0u64)),
                update_timestamp: current_timestamp - 200,
                ..Default::default()
            },
        );
        shards.shards.insert(
            ShardId::from(1),
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(1)),
                shard_state: ShardState::Open as i32,
                publish_position_inclusive: Some(Position::offset(0u64)),
                update_timestamp: current_timestamp - 100,
                ..Default::default()
            },
        );

        let request = PruneShardsRequest {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            max_age_secs: Some(150),
            max_count: None,
            interval: None,
        };
        let MutationOccurred::Yes(()) = shards.prune_shards(request).unwrap() else {
            panic!("expected `MutationOccurred::Yes`");
        };

        let request = PruneShardsRequest {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            max_age_secs: Some(150),
            max_count: None,
            interval: None,
        };
        let MutationOccurred::No(()) = shards.prune_shards(request).unwrap() else {
            panic!("expected `MutationOccurred::No`");
        };
    }
}
