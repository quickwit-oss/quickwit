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
    /// shared context. For each message, if the ownership was successfully
    /// acquired or the message was already successfully indexed, the position
    /// is returned along with the message, otherwise the message is dropped.
    async fn checkpoint_messages(
        &mut self,
        publish_token: &str,
        messages: Vec<PreProcessedMessage>,
    ) -> anyhow::Result<Vec<(PreProcessedMessage, Position)>>;
}

pub struct QueueSharedStateImpl {
    pub metastore: MetastoreServiceClient,
    pub index_uid: IndexUid,
    pub source_id: String,
}

impl QueueSharedStateImpl {
    /// Tries to acquire the shards associated with the given partitions.
    /// Partitions that are assumed to be processed by another indexing
    /// pipeline are filtered out.
    async fn acquire_shards(
        &mut self,
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
            // TODO: inclusive??? a +1 will likely be required here
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

#[async_trait]
impl QueueSharedState for QueueSharedStateImpl {
    async fn checkpoint_messages(
        &mut self,
        publish_token: &str,
        messages: Vec<PreProcessedMessage>,
    ) -> anyhow::Result<Vec<(PreProcessedMessage, Position)>> {
        let mut message_map =
            BTreeMap::from_iter(messages.into_iter().map(|msg| (msg.partition_id(), msg)));
        let partition_ids = message_map.keys().cloned().collect();

        let shards = self.acquire_shards(publish_token, partition_ids).await?;

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
        async fn checkpoint_messages(
            &mut self,
            publish_token: &str,
            messages: Vec<PreProcessedMessage>,
        ) -> anyhow::Result<Vec<(PreProcessedMessage, Position)>> {
            let mut state = self.state.lock().unwrap();
            let mut checkpointed_messages = Vec::new();
            for message in messages {
                let partition_id = message.partition_id();
                let processing_state = state
                    .get(&partition_id)
                    .unwrap_or(&SharedStateForPartition::AvailableForProcessing(
                        Position::Beginning,
                    ))
                    .clone();
                match processing_state {
                    SharedStateForPartition::AvailableForProcessing(position) => {
                        state.insert(
                            partition_id,
                            SharedStateForPartition::InProgress(publish_token.to_string()),
                        );
                        checkpointed_messages.push((message, position));
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
                        checkpointed_messages.push((message, Position::Eof(None)));
                    }
                }
            }
            Ok(checkpointed_messages)
        }
    }
}

#[cfg(test)]
mod tests {
    // use quickwit_metastore::metastore_for_test;

    // use super::*;
    // use crate::source::test_setup_helper::setup_index;

    // #[tokio::test]
    // async fn test_queue_shared_state_impl() {
    //     let sqs_params = SqsSourceParams {
    //         queue_url,
    //         wait_time_seconds: 1,
    //         queue_params: QueueParams {
    //             message_type: QueueMessageType::RawUri,
    //         },
    //     };
    //     let source_params = SourceParams::Sqs(sqs_params.clone());
    //     let source_config = SourceConfig::for_test("test-sqs-source", source_params);
    //     let metastore = metastore_for_test();
    //     let index_id = append_random_suffix("test-sqs-index");
    //     let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;

    // }
}
