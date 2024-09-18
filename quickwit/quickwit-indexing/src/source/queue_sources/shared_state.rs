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
use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::{bail, Context};
use quickwit_metastore::checkpoint::PartitionId;
use quickwit_proto::metastore::{
    AcquireShardsRequest, MetastoreService, MetastoreServiceClient, OpenShardSubrequest,
    OpenShardsRequest, PruneShardsRequest,
};
use quickwit_proto::types::{DocMappingUid, Position, ShardId, SourceUid};
use time::OffsetDateTime;
use tracing::{error, info};

use super::message::PreProcessedMessage;

#[derive(Clone)]
pub struct QueueSharedState {
    metastore: MetastoreServiceClient,
    pub source_uid: SourceUid,
    /// Duration after which the processing of a shard is considered stale and
    /// should be reacquired
    reacquire_grace_period: Duration,
    _cleanup_handle: Arc<()>,
}

impl QueueSharedState {
    /// Create a shared state service and runs a cleanup task that prunes shards
    /// in the background
    pub fn new(
        metastore: MetastoreServiceClient,
        source_uid: SourceUid,
        reacquire_grace_period: Duration,
        max_age: Option<Duration>,
        max_count: Option<u32>,
        pruning_interval: Duration,
    ) -> Self {
        let cleanup_handle = Arc::new(());
        tokio::spawn(Self::run_cleanup_task(
            metastore.clone(),
            source_uid.clone(),
            max_age,
            max_count,
            pruning_interval,
            Arc::downgrade(&cleanup_handle),
        ));
        Self {
            metastore,
            source_uid,
            reacquire_grace_period,
            _cleanup_handle: cleanup_handle,
        }
    }

    async fn run_cleanup_task(
        metastore: MetastoreServiceClient,
        source_uid: SourceUid,
        max_age: Option<Duration>,
        max_count: Option<u32>,
        pruning_interval: Duration,
        owner_handle: Weak<()>,
    ) {
        if max_count.is_none() && max_age.is_none() {
            return;
        }
        let max_age_secs = max_age.map(|duration| duration.as_secs() as u32);
        let SourceUid {
            index_uid,
            source_id,
        } = source_uid;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(pruning_interval);
            loop {
                interval.tick().await;
                if owner_handle.upgrade().is_none() {
                    break;
                }
                let result: Result<_, _> = metastore
                    .prune_shards(PruneShardsRequest {
                        index_uid: Some(index_uid.clone()),
                        source_id: source_id.clone(),
                        max_age_secs,
                        max_count,
                    })
                    .await;
                if let Err(err) = result {
                    error!(error = ?err, "failed to prune shards");
                }
            }
        });
    }

    /// Tries to acquire the ownership for the provided messages from the global
    /// shared context. For each partition id, if the ownership was successfully
    /// acquired or the partition was already successfully indexed, the position
    /// is returned along with the partition id, otherwise the partition id is
    /// dropped.
    async fn acquire_partitions(
        &mut self,
        publish_token: &str,
        partitions: Vec<PartitionId>,
    ) -> anyhow::Result<Vec<(PartitionId, Position)>> {
        let open_shard_subrequests = partitions
            .iter()
            .enumerate()
            .map(|(idx, partition_id)| OpenShardSubrequest {
                subrequest_id: idx as u32,
                index_uid: Some(self.source_uid.index_uid.clone()),
                source_id: self.source_uid.source_id.clone(),
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
            .await?;

        let mut shards = Vec::new();
        let mut re_acquired_shards = Vec::new();
        for sub in open_shard_resp.subresponses {
            // we could also just cast the shard_id back to a partition_id
            let partition_id = partitions[sub.subrequest_id as usize].clone();
            let shard = sub.open_shard();
            let position = shard.publish_position_inclusive.clone().unwrap_or_default();
            let is_owned = sub.open_shard().publish_token.as_deref() == Some(publish_token);
            let update_datetime = OffsetDateTime::from_unix_timestamp(shard.update_timestamp)
                .context("Invalid shard update timestamp")?;
            let is_stale =
                OffsetDateTime::now_utc() - update_datetime > self.reacquire_grace_period;
            if position.is_eof() || (is_owned && position.is_beginning()) {
                shards.push((partition_id, position));
            } else if !is_owned && is_stale {
                info!(previous_token = shard.publish_token, "shard re-acquired");
                re_acquired_shards.push(shard.shard_id().clone());
            } else if is_owned && !position.is_beginning() {
                bail!("Partition is owned by this indexing pipeline but is not at the beginning. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")
            }
        }

        if re_acquired_shards.is_empty() {
            return Ok(shards);
        }

        // re-acquire shards that have a token that is not the local token
        let acquire_shard_resp = self
            .metastore
            .acquire_shards(AcquireShardsRequest {
                index_uid: Some(self.source_uid.index_uid.clone()),
                source_id: self.source_uid.source_id.clone(),
                shard_ids: re_acquired_shards,
                publish_token: publish_token.to_string(),
            })
            .await
            .unwrap();
        for shard in acquire_shard_resp.acquired_shards {
            let partition_id = PartitionId::from(shard.shard_id().as_str());
            let position = shard.publish_position_inclusive.unwrap_or_default();
            shards.push((partition_id, position));
        }

        Ok(shards)
    }
}

/// Acquires shards from the shared state for the provided list of messages and
/// maps results to that same list
pub async fn checkpoint_messages(
    shared_state: &mut QueueSharedState,
    publish_token: &str,
    messages: Vec<PreProcessedMessage>,
) -> anyhow::Result<Vec<(PreProcessedMessage, Position)>> {
    let mut message_map =
        BTreeMap::from_iter(messages.into_iter().map(|msg| (msg.partition_id(), msg)));
    let partition_ids = message_map.keys().cloned().collect();

    let shards = shared_state
        .acquire_partitions(publish_token, partition_ids)
        .await?;

    let mut result = Vec::with_capacity(shards.len());
    for (position_id, position) in shards {
        let content = message_map.remove(&position_id).context("Unexpected partition ID. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")?;
        result.push((content, position));
    }

    Ok(result)
}

#[cfg(test)]
pub mod shared_state_for_tests {
    use std::sync::{Arc, Mutex};

    use itertools::Itertools;
    use quickwit_proto::ingest::{Shard, ShardState};
    use quickwit_proto::metastore::{
        AcquireShardsResponse, MockMetastoreService, OpenShardSubresponse, OpenShardsResponse,
    };
    use quickwit_proto::types::IndexUid;

    use super::*;

    /// Creates a metastore that mocks the behavior of the Shard API on the open
    /// and acquire methods using a simplified in-memory state.
    pub(super) fn mock_metastore(
        // Shards (token, position, update_timestamp) in the initial state
        initial_state: &[(PartitionId, (String, Position, i64))],
        // Times open_shards is expected to be called (None <=> no expectation)
        open_shard_times: Option<usize>,
        // Times acquire_shards is expected to be called (None <=> no expectation)
        acquire_times: Option<usize>,
    ) -> MetastoreServiceClient {
        let mut mock_metastore = MockMetastoreService::new();
        let inner_state = Arc::new(Mutex::new(BTreeMap::from_iter(
            initial_state.iter().cloned(),
        )));
        let inner_state_ref = Arc::clone(&inner_state);
        let open_shards_expectation =
            mock_metastore
                .expect_open_shards()
                .returning(move |request| {
                    let mut subresponses = Vec::with_capacity(request.subrequests.len());
                    for sub_req in request.subrequests {
                        let partition_id: PartitionId = sub_req.shard_id().to_string().into();
                        let req_token = sub_req.publish_token.unwrap();
                        let (token, position, update_timestamp) = inner_state_ref
                            .lock()
                            .unwrap()
                            .get(&partition_id)
                            .cloned()
                            .unwrap_or((
                                req_token.clone(),
                                Position::Beginning,
                                OffsetDateTime::now_utc().unix_timestamp(),
                            ));

                        inner_state_ref.lock().unwrap().insert(
                            partition_id,
                            (token.clone(), position.clone(), update_timestamp),
                        );
                        subresponses.push(OpenShardSubresponse {
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
                                update_timestamp,
                            }),
                        });
                    }
                    Ok(OpenShardsResponse { subresponses })
                });
        if let Some(times) = open_shard_times {
            open_shards_expectation.times(times);
        }
        let acquire_shards_expectation =
            mock_metastore
                .expect_acquire_shards()
                .returning(move |request| {
                    let mut acquired_shards = Vec::with_capacity(request.shard_ids.len());
                    for shard_id in request.shard_ids {
                        let partition_id: PartitionId = shard_id.to_string().into();
                        let (existing_token, position, update_timestamp) = inner_state
                            .lock()
                            .unwrap()
                            .get(&partition_id)
                            .cloned()
                            .expect("we should never try to acquire a shard that doesn't exist");
                        inner_state.lock().unwrap().insert(
                            partition_id,
                            (
                                request.publish_token.clone(),
                                position.clone(),
                                update_timestamp,
                            ),
                        );
                        assert_ne!(existing_token, request.publish_token);
                        acquired_shards.push(Shard {
                            shard_id: Some(shard_id),
                            source_id: "dummy".to_string(),
                            publish_token: Some(request.publish_token.clone()),
                            index_uid: None,
                            follower_id: None,
                            leader_id: "dummy".to_string(),
                            doc_mapping_uid: None,
                            publish_position_inclusive: Some(position),
                            shard_state: ShardState::Open as i32,
                            update_timestamp,
                        });
                    }
                    Ok(AcquireShardsResponse { acquired_shards })
                });
        if let Some(times) = acquire_times {
            acquire_shards_expectation.times(times);
        }
        MetastoreServiceClient::from_mock(mock_metastore)
    }

    pub fn init_state(
        index_id: &str,
        // Shards (token, position, is_stale) in the initial state
        initial_state: &[(PartitionId, (String, Position, bool))],
    ) -> QueueSharedState {
        let index_uid = IndexUid::new_with_random_ulid(index_id);
        let metastore_state = initial_state
            .iter()
            .map(|(pid, (token, pos, is_stale))| {
                let update_timestamp = if *is_stale {
                    OffsetDateTime::now_utc().unix_timestamp() - 100
                } else {
                    OffsetDateTime::now_utc().unix_timestamp()
                };
                (pid.clone(), (token.clone(), pos.clone(), update_timestamp))
            })
            .collect_vec();
        let metastore = mock_metastore(&metastore_state, None, None);
        QueueSharedState {
            metastore,
            source_uid: SourceUid {
                index_uid,
                source_id: "test-queue-src".to_string(),
            },
            reacquire_grace_period: Duration::from_secs(10),
            _cleanup_handle: Arc::new(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::time::Instant;
    use std::vec;

    use quickwit_common::uri::Uri;
    use quickwit_proto::types::IndexUid;
    use shared_state_for_tests::mock_metastore;

    use super::*;
    use crate::source::queue_sources::message::{MessageMetadata, PreProcessedPayload};

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
    async fn test_acquire_shards_with_completed() {
        let index_id = "test-sqs-index";
        let index_uid = IndexUid::new_with_random_ulid(index_id);
        let init_state = &[(
            "p1".into(),
            (
                "token2".to_string(),
                Position::eof(100usize),
                OffsetDateTime::now_utc().unix_timestamp(),
            ),
        )];
        let metastore = mock_metastore(init_state, Some(1), Some(0));

        let mut shared_state = QueueSharedState {
            metastore,
            source_uid: SourceUid {
                index_uid,
                source_id: "test-sqs-source".to_string(),
            },
            reacquire_grace_period: Duration::from_secs(10),
            _cleanup_handle: Arc::new(()),
        };

        let aquired = shared_state
            .acquire_partitions("token1", vec!["p1".into(), "p2".into()])
            .await
            .unwrap();
        assert!(aquired.contains(&("p1".into(), Position::eof(100usize))));
        assert!(aquired.contains(&("p2".into(), Position::Beginning)));
    }

    #[tokio::test]
    async fn test_re_acquire_shards_within_grace_period() {
        let index_id = "test-sqs-index";
        let index_uid = IndexUid::new_with_random_ulid(index_id);
        let init_state = &[(
            "p1".into(),
            (
                "token2".to_string(),
                Position::offset(100usize),
                OffsetDateTime::now_utc().unix_timestamp(),
            ),
        )];
        let metastore = mock_metastore(init_state, Some(1), Some(0));

        let mut shared_state = QueueSharedState {
            metastore,
            source_uid: SourceUid {
                index_uid,
                source_id: "test-sqs-source".to_string(),
            },
            reacquire_grace_period: Duration::from_secs(10),
            _cleanup_handle: Arc::new(()),
        };

        let acquired = shared_state
            .acquire_partitions("token1", vec!["p1".into(), "p2".into()])
            .await
            .unwrap();
        assert_eq!(acquired.len(), 1);
        assert!(acquired.contains(&("p2".into(), Position::Beginning)));
    }

    #[tokio::test]
    async fn test_re_acquire_shards_after_grace_period() {
        let index_id = "test-sqs-index";
        let index_uid = IndexUid::new_with_random_ulid(index_id);
        let init_state = &[(
            "p1".into(),
            (
                "token2".to_string(),
                Position::offset(100usize),
                OffsetDateTime::now_utc().unix_timestamp() - 100,
            ),
        )];
        let metastore = mock_metastore(init_state, Some(1), Some(1));

        let mut shared_state = QueueSharedState {
            metastore,
            source_uid: SourceUid {
                index_uid,
                source_id: "test-sqs-source".to_string(),
            },
            reacquire_grace_period: Duration::from_secs(10),
            _cleanup_handle: Arc::new(()),
        };

        let aquired = shared_state
            .acquire_partitions("token1", vec!["p1".into(), "p2".into()])
            .await
            .unwrap();
        assert!(aquired.contains(&("p1".into(), Position::offset(100usize))));
        assert!(aquired.contains(&("p2".into(), Position::Beginning)));
    }

    #[tokio::test]
    async fn test_checkpoint_with_completed() {
        let index_id = "test-sqs-index";
        let index_uid = IndexUid::new_with_random_ulid(index_id);

        let source_messages = test_messages(2);
        let completed_partition_id = source_messages[0].partition_id();
        let new_partition_id = source_messages[1].partition_id();

        let init_state = &[(
            completed_partition_id.clone(),
            (
                "token2".to_string(),
                Position::eof(100usize),
                OffsetDateTime::now_utc().unix_timestamp(),
            ),
        )];
        let metastore = mock_metastore(init_state, Some(1), Some(0));
        let mut shared_state = QueueSharedState {
            metastore,
            source_uid: SourceUid {
                index_uid,
                source_id: "test-sqs-source".to_string(),
            },
            reacquire_grace_period: Duration::from_secs(10),
            _cleanup_handle: Arc::new(()),
        };

        let checkpointed_msg = checkpoint_messages(&mut shared_state, "token1", source_messages)
            .await
            .unwrap();
        assert_eq!(checkpointed_msg.len(), 2);
        let completed_msg = checkpointed_msg
            .iter()
            .find(|(msg, _)| msg.partition_id() == completed_partition_id)
            .unwrap();
        assert_eq!(completed_msg.1, Position::eof(100usize));
        let new_msg = checkpointed_msg
            .iter()
            .find(|(msg, _)| msg.partition_id() == new_partition_id)
            .unwrap();
        assert_eq!(new_msg.1, Position::Beginning);
    }
}
