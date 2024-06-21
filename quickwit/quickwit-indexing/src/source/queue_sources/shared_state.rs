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

use anyhow::Context;
use async_trait::async_trait;
use quickwit_metastore::checkpoint::PartitionId;
use quickwit_proto::indexing::IndexingPipelineId;
use quickwit_proto::metastore::{
    MetastoreService, MetastoreServiceClient, OpenShardSubrequest, OpenShardsRequest,
};
use quickwit_proto::types::{Position, ShardId};

use super::message::{CheckpointedMessage, PreProcessedMessage};
use super::Categorized;

#[async_trait]
pub trait QueueSharedState: Send {
    /// Fetches the status of the given messages and split them into processable
    /// (with position) and already processed messages.
    async fn checkpoint_messages(
        &mut self,
        pipeline_id: &IndexingPipelineId,
        messages: Vec<PreProcessedMessage>,
    ) -> anyhow::Result<Categorized<CheckpointedMessage, PreProcessedMessage>>;
}

pub struct QueueSharedStateImpl {
    pub metastore: MetastoreServiceClient,
}

impl QueueSharedStateImpl {
    /// Tries to acquire the shards associated with the given partitions.
    /// Partitions that are assumed to be processed by another indexing
    /// pipeline are filtered out.
    async fn acquire_shards(
        &mut self,
        pipeline_id: &IndexingPipelineId,
        partitions: Vec<PartitionId>,
    ) -> anyhow::Result<Categorized<(PartitionId, Position), PartitionId>> {
        let local_publish_token = pipeline_id.pipeline_uid.to_string();
        let open_shard_subrequests = partitions
            .iter()
            .enumerate()
            .map(|(idx, partition_id)| OpenShardSubrequest {
                subrequest_id: idx as u32,
                index_uid: Some(pipeline_id.index_uid.clone()),
                source_id: pipeline_id.source_id.clone(),
                // TODO: make this optional?
                leader_id: String::new(),
                follower_id: None,
                shard_id: Some(ShardId::from(partition_id.as_str())),
                publish_token: Some(local_publish_token.clone()),
            })
            .collect();

        let open_shard_resp = self
            .metastore
            .open_shards(OpenShardsRequest {
                subrequests: open_shard_subrequests,
            })
            .await
            .unwrap();

        let mut completed_shards = Vec::new();
        let mut new_shards = Vec::new();
        for sub in open_shard_resp.subresponses {
            let partition_id = partitions[sub.subrequest_id as usize].clone();
            let position = sub.open_shard().publish_position_inclusive();
            if position.is_eof() {
                completed_shards.push(partition_id);
            } else if sub.open_shard().publish_token.as_ref() == Some(&local_publish_token) {
                new_shards.push((partition_id, position));
            }
        }

        // TODO: Add logic to re-acquire shards that have a token that is not
        // the local token but haven't been updated recently

        Ok(Categorized {
            processable: new_shards,
            already_processed: completed_shards,
        })
    }
}

#[async_trait]
impl QueueSharedState for QueueSharedStateImpl {
    async fn checkpoint_messages(
        &mut self,
        pipeline_id: &IndexingPipelineId,
        messages: Vec<PreProcessedMessage>,
    ) -> anyhow::Result<Categorized<CheckpointedMessage, PreProcessedMessage>> {
        let mut message_map =
            BTreeMap::from_iter(messages.into_iter().map(|m| (m.partition_id(), m)));
        let partition_ids = message_map.keys().cloned().collect();

        let Categorized {
            processable,
            already_processed,
        } = self.acquire_shards(pipeline_id, partition_ids).await?;

        let processable_messages = processable
            .into_iter()
            .map(|(partition_id, position)| {
                let content = message_map.remove(&partition_id).context("Unexpected partition ID. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")?;
                Ok(CheckpointedMessage {
                    position,
                    content,
                })
            })
            .collect::<anyhow::Result<_>>()?;

        let already_processed_messages = already_processed
            .into_iter()
            .map(|partition_id| {
                message_map.remove(&partition_id).context("Unexpected partition ID. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")
            })
            .collect::<anyhow::Result<_>>()?;

        Ok(Categorized {
            processable: processable_messages,
            already_processed: already_processed_messages,
        })
    }
}

#[cfg(test)]
pub mod shared_state_for_tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use quickwit_proto::types::PipelineUid;

    use super::*;

    #[derive(Debug, Clone, Eq, PartialEq)]
    pub enum SharedStateForPartition {
        AvailableForProcessing(Position),
        InProgress(PipelineUid),
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
            pipeline_id: &IndexingPipelineId,
            messages: Vec<PreProcessedMessage>,
        ) -> anyhow::Result<Categorized<CheckpointedMessage, PreProcessedMessage>> {
            let mut state = self.state.lock().unwrap();
            let mut processable_messages = Vec::new();
            let mut already_processed_messages = Vec::new();
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
                            SharedStateForPartition::InProgress(pipeline_id.pipeline_uid),
                        );
                        processable_messages.push(CheckpointedMessage {
                            position,
                            content: message,
                        });
                    }
                    SharedStateForPartition::InProgress(pipeline_uid) => {
                        assert_ne!(
                            pipeline_uid, pipeline_id.pipeline_uid,
                            "Shared state should not be accessed when it could be tracked locally"
                        );
                        // TODO: Add logic to re-acquire shards that have a token that is not
                        // the local token but haven't been updated recently
                    }
                    SharedStateForPartition::Completed => {
                        already_processed_messages.push(message);
                    }
                }
            }
            Ok(Categorized {
                processable: processable_messages,
                already_processed: already_processed_messages,
            })
        }
    }
}
