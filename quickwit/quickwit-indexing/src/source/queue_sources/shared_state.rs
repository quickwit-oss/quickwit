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

use std::collections::BTreeMap;

use anyhow::{bail, Context};
use async_trait::async_trait;
use quickwit_metastore::checkpoint::PartitionId;
use quickwit_proto::metastore::{
    MetastoreService, MetastoreServiceClient, OpenShardSubrequest, OpenShardsRequest,
};
use quickwit_proto::types::{DocMappingUid, IndexUid, Position, ShardId};

use super::message::PreProcessedMessage;

#[async_trait]
pub trait QueueSharedState: Send {
    /// Tries to acquire the ownership for the provided messages from the global
    /// shared context. For each partition id, if the ownership was successfully
    /// acquired or the partition was already successfully indexed, the position
    /// is returned along with the partition id, otherwise the partition id is
    /// dropped.
    async fn acquire_shards(
        &self,
        publish_token: &str,
        partitions: Vec<PartitionId>,
    ) -> anyhow::Result<Vec<(PartitionId, Position)>>;
}

pub struct QueueSharedStateImpl {
    pub metastore: MetastoreServiceClient,
    pub index_uid: IndexUid,
    pub source_id: String,
}

#[async_trait]
impl QueueSharedState for QueueSharedStateImpl {
    async fn acquire_shards(
        &self,
        publish_token: &str,
        partitions: Vec<PartitionId>,
    ) -> anyhow::Result<Vec<(PartitionId, Position)>> {
        let open_shard_subrequests = partitions
            .iter()
            .enumerate()
            .map(|(idx, partition_id)| OpenShardSubrequest {
                subrequest_id: idx as u32,
                index_uid: Some(self.index_uid.clone()),
                source_id: self.source_id.clone(),
                // TODO: make this optional?
                leader_id: String::new(),
                follower_id: None,
                shard_id: Some(ShardId::from(partition_id.as_str())),
                doc_mapping_uid: Some(DocMappingUid::default()),
                publish_token: Some(publish_token.to_string()),
            })
            .collect();

        let open_shard_resp = self
            .metastore
            .open_shards(OpenShardsRequest {
                subrequests: open_shard_subrequests,
            })
            .await
            .unwrap();

        let mut shards = Vec::new();
        for sub in open_shard_resp.subresponses {
            let partition_id = partitions[sub.subrequest_id as usize].clone();
            sub.open_shard().follower_id();
            let position = sub
                .open_shard()
                .publish_position_inclusive
                .clone()
                .unwrap_or_default();
            let is_owned = sub.open_shard().publish_token.as_deref() == Some(publish_token);
            if position.is_eof() || (is_owned && position.is_beginning()) {
                shards.push((partition_id, position));
            } else if is_owned && !position.is_beginning() {
                bail!("Partition is owned by this indexing pipeline but is not at the beginning. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")
            }
        }

        // TODO: Add logic to re-acquire shards that have a token that is not
        // the local token but haven't been updated recently

        Ok(shards)
    }
}

/// Acquires shards from the shared state for the provided list of messages and
/// maps results to that same list
pub async fn checkpoint_messages(
    shared_state: &(dyn QueueSharedState + Sync),
    publish_token: &str,
    messages: Vec<PreProcessedMessage>,
) -> anyhow::Result<Vec<(PreProcessedMessage, Position)>> {
    let mut message_map =
        BTreeMap::from_iter(messages.into_iter().map(|msg| (msg.partition_id(), msg)));
    let partition_ids = message_map.keys().cloned().collect();

    let shards = shared_state
        .acquire_shards(publish_token, partition_ids)
        .await?;

    shards
    .into_iter()
    .map(|(partition_id, position)| {
        let content = message_map.remove(&partition_id).context("Unexpected partition ID. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")?;
        Ok((
            content,
            position,
        ))
    })
    .collect::<anyhow::Result<_>>()
}

#[cfg(test)]
pub mod shared_state_for_tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Debug, Clone, Eq, PartialEq)]
    pub enum SharedStateForPartition {
        AvailableForProcessing(Position),
        // contains the publish token
        InProgress(String),
        Completed,
    }

    #[derive(Default, Clone)]
    pub struct InMemoryQueueSharedState {
        state: Arc<Mutex<BTreeMap<PartitionId, SharedStateForPartition>>>,
    }

    impl InMemoryQueueSharedState {
        pub fn set(&self, partition_id: PartitionId, state: SharedStateForPartition) {
            self.state.lock().unwrap().insert(partition_id, state);
        }

        pub fn get(&self, partition_id: &PartitionId) -> SharedStateForPartition {
            self.state
                .lock()
                .unwrap()
                .get(partition_id)
                .unwrap_or(&SharedStateForPartition::AvailableForProcessing(
                    Position::Beginning,
                ))
                .clone()
        }
    }

    #[async_trait]
    impl QueueSharedState for InMemoryQueueSharedState {
        async fn acquire_shards(
            &self,
            publish_token: &str,
            partitions: Vec<PartitionId>,
        ) -> anyhow::Result<Vec<(PartitionId, Position)>> {
            let mut state = self.state.lock().unwrap();
            let mut checkpointed_partitions = Vec::new();
            for partition_id in partitions {
                let processing_state = state
                    .get(&partition_id)
                    .unwrap_or(&SharedStateForPartition::AvailableForProcessing(
                        Position::Beginning,
                    ))
                    .clone();
                match processing_state {
                    SharedStateForPartition::AvailableForProcessing(position) => {
                        state.insert(
                            partition_id.clone(),
                            SharedStateForPartition::InProgress(publish_token.to_string()),
                        );
                        checkpointed_partitions.push((partition_id, position));
                    }
                    SharedStateForPartition::InProgress(in_progress_token) => {
                        assert_ne!(
                            in_progress_token, publish_token,
                            "Shared state should not be accessed when it could be tracked locally"
                        );
                        // TODO: Add logic to re-acquire shards that have a token that is not
                        // the local token but haven't been updated recently
                    }
                    SharedStateForPartition::Completed => {
                        checkpointed_partitions.push((partition_id, Position::Eof(None)));
                    }
                }
            }
            Ok(checkpointed_partitions)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::time::Instant;
    use std::vec;

    use quickwit_common::uri::Uri;
    use quickwit_proto::ingest::{Shard, ShardState};
    use quickwit_proto::metastore::{
        MockMetastoreService, OpenShardSubresponse, OpenShardsResponse,
    };

    use super::*;
    use crate::source::queue_sources::message::{MessageMetadata, PreProcessedPayload};

    fn mock_metastore(
        tracked: BTreeMap<PartitionId, (String, Position)>,
    ) -> MetastoreServiceClient {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_open_shards()
            .returning(move |request| {
                let subresponses = request
                    .subrequests
                    .into_iter()
                    .map(|sub_req| {
                        let partition_id: PartitionId = sub_req.shard_id().to_string().into();
                        let (token, position) = tracked
                            .get(&partition_id)
                            .cloned()
                            .unwrap_or((sub_req.publish_token.unwrap(), Position::Beginning));
                        OpenShardSubresponse {
                            subrequest_id: sub_req.subrequest_id,
                            open_shard: Some(Shard {
                                shard_id: sub_req.shard_id,
                                source_id: sub_req.source_id,
                                publish_token: Some(token),
                                index_uid: sub_req.index_uid,
                                follower_id: sub_req.follower_id,
                                leader_id: sub_req.leader_id,
                                doc_mapping_uid: sub_req.doc_mapping_uid,
                                publish_position_inclusive: Some(position),
                                shard_state: ShardState::Open as i32,
                            }),
                        }
                    })
                    .collect();
                Ok(OpenShardsResponse { subresponses })
            });
        MetastoreServiceClient::from_mock(mock_metastore)
    }

    fn test_messages(message_number: usize) -> Vec<PreProcessedMessage> {
        (0..message_number)
            .map(|i| PreProcessedMessage {
                metadata: MessageMetadata {
                    ack_id: format!("ackid{}", i),
                    delivery_attempts: 0,
                    initial_deadline: Instant::now(),
                    message_id: format!("mid{}", i),
                },
                payload: PreProcessedPayload::ObjectUri(
                    Uri::from_str(&format!("s3://bucket/key{}", i)).unwrap(),
                ),
            })
            .collect()
    }

    #[tokio::test]
    async fn test_acquire_shards() {
        let index_id = "test-sqs-index";
        let index_uid = IndexUid::new_with_random_ulid(index_id);
        let metastore = mock_metastore(BTreeMap::from_iter(
            [(
                "p1".into(),
                ("token2".to_string(), Position::Eof(Some(100usize.into()))),
            )]
            .into_iter(),
        ));

        let shared_state = QueueSharedStateImpl {
            metastore,
            index_uid,
            source_id: "test-sqs-source".to_string(),
        };

        let aquired = shared_state
            .acquire_shards("token1", vec!["p1".into(), "p2".into()])
            .await
            .unwrap();

        assert_eq!(
            aquired,
            vec![
                ("p1".into(), Position::Eof(Some(100usize.into()))),
                ("p2".into(), Position::Beginning)
            ]
        );
    }

    #[tokio::test]
    async fn test_checkpoint_with_completed() {
        let index_id = "test-sqs-index";
        let index_uid = IndexUid::new_with_random_ulid(index_id);

        let source_messages = test_messages(2);

        let metastore = mock_metastore(BTreeMap::from_iter(
            [(
                source_messages[0].partition_id(),
                ("token2".to_string(), Position::Eof(Some(100usize.into()))),
            )]
            .into_iter(),
        ));

        let shared_state = QueueSharedStateImpl {
            metastore,
            index_uid,
            source_id: "test-sqs-source".to_string(),
        };

        let checkpointed_msg = checkpoint_messages(&shared_state, "token1", source_messages)
            .await
            .unwrap();
        assert_eq!(checkpointed_msg.len(), 2);
        assert_eq!(checkpointed_msg[0].1, Position::Eof(Some(100usize.into())));
        assert_eq!(checkpointed_msg[1].1, Position::Beginning);
    }
}
