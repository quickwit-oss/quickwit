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

use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use fail::fail_point;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_metastore::checkpoint::IndexCheckpointDelta;
use quickwit_proto::metastore::{
    MetastoreError, MetastoreResult, MetastoreService, MetastoreServiceClient, PublishSplitsRequest,
};
use serde::Serialize;
use tracing::{error, info, instrument, warn};

use crate::actors::MergePlanner;
use crate::metrics::record_published_split;
use crate::models::{NewSplits, PublishLock, SharedPublishToken, SplitsUpdate};
use crate::source::{SourceActor, SuggestTruncate};

#[derive(Clone, Debug, Default, Serialize)]
pub struct PublisherCounters {
    pub num_published_splits: u64,
    pub num_replace_operations: u64,
    pub num_empty_splits: u64,
}

#[derive(Clone, Copy, Debug)]
pub enum PublisherType {
    MainPublisher,
    MergePublisher,
}

impl PublisherType {
    pub fn actor_name(&self) -> &'static str {
        match self {
            PublisherType::MainPublisher => "Publisher",
            PublisherType::MergePublisher => "MergePublisher",
        }
    }
}

/// Disconnect the merge planner loop back.
/// This message is used to cut the merge pipeline loop, and let it terminate.
#[derive(Debug)]
pub(crate) struct DisconnectMergePlanner;

#[derive(Clone)]
pub struct Publisher {
    publisher_type: PublisherType,
    metastore: MetastoreServiceClient,
    merge_planner_mailbox_opt: Option<Mailbox<MergePlanner>>,
    source_mailbox_opt: Option<Mailbox<SourceActor>>,
    publish_token: SharedPublishToken,
    counters: PublisherCounters,
}

impl Publisher {
    pub fn new(
        publisher_type: PublisherType,
        metastore: MetastoreServiceClient,
        merge_planner_mailbox_opt: Option<Mailbox<MergePlanner>>,
        source_mailbox_opt: Option<Mailbox<SourceActor>>,
        publish_token: SharedPublishToken,
    ) -> Publisher {
        Publisher {
            publisher_type,
            metastore,
            merge_planner_mailbox_opt,
            source_mailbox_opt,
            publish_token,
            counters: PublisherCounters::default(),
        }
    }

    /// Ends the pipeline this publisher belongs to. We do this by signaling the source to exit,
    /// which will propagate the message downstream to the other actors.
    async fn terminate_pipeline(
        &self,
        ctx: &ActorContext<Publisher>,
        publish_error: ActorExitStatus,
        publish_lock: &PublishLock,
        split_ids: &[String],
    ) -> Result<(), ActorExitStatus> {
        let Some(source_mailbox) = self.source_mailbox_opt.as_ref() else {
            return Err(publish_error);
        };
        error!(
            error=?publish_error,
            split_ids=?split_ids,
            "failed to publish splits, terminating the pipeline"
        );
        // The actor kill signal will propagate and eventually end up back here, and will try
        // to publish before exiting, so we kill the publish lock to prevent one final flush.
        publish_lock.kill().await;
        let _ = ctx.send_exit_with_success(source_mailbox).await;
        Ok(())
    }
}

fn is_invalid_publish_token(publish_error: &ActorExitStatus) -> bool {
    let ActorExitStatus::Failure(error) = publish_error else {
        return false;
    };
    matches!(
        error.downcast_ref::<MetastoreError>(),
        Some(MetastoreError::InvalidPublishToken { .. })
    )
}

fn serialize_checkpoint_delta(
    checkpoint_delta_opt: &Option<IndexCheckpointDelta>,
) -> anyhow::Result<Option<String>> {
    checkpoint_delta_opt
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .context("failed to serialize `IndexCheckpointDelta`")
}

async fn suggest_truncate(
    ctx: &ActorContext<Publisher>,
    source_mailbox_opt: &Option<Mailbox<SourceActor>>,
    checkpoint_delta_opt: Option<IndexCheckpointDelta>,
) {
    if let Some(source_mailbox) = source_mailbox_opt.as_ref()
        && let Some(checkpoint) = checkpoint_delta_opt
    {
        let _ = ctx
            .send_message(
                source_mailbox,
                SuggestTruncate(checkpoint.source_delta.get_source_checkpoint()),
            )
            .await;
    }
}

// This is used primarily for publisher-specific metastore retry logic, specifically to have a
// handle on an invalid publish token, which will cause the pipeline to be terminated and not
async fn publish_with_retry<T, F, Fut>(
    ctx: &ActorContext<Publisher>,
    operation_name: &str,
    mut publish: F,
) -> Result<(), ActorExitStatus>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = MetastoreResult<T>>,
{
    for retry_delay in [
        Some(Duration::from_secs(1)),
        Some(Duration::from_secs(3)),
        None,
    ] {
        let Err(error) = ctx.protect_future(publish()).await else {
            return Ok(());
        };
        let retryable = matches!(error, MetastoreError::InvalidPublishToken { .. });
        match retry_delay {
            Some(retry_delay) if retryable => {
                warn!(%error, operation = operation_name, "metastore publish failed, retrying");
                ctx.protect_future(ctx.sleep(retry_delay)).await;
            }
            _ => {
                warn!(%error, operation = operation_name, retryable, "metastore publish failed, giving up after 3 tries");
                return Err(anyhow::Error::from(error)
                    .context(format!("failed to {operation_name}"))
                    .into());
            }
        }
    }
    unreachable!("retry loop returns on the final attempt")
}

#[async_trait]
impl Actor for Publisher {
    type ObservableState = PublisherCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn name(&self) -> String {
        self.publisher_type.actor_name().to_string()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        match self.publisher_type {
            PublisherType::MainPublisher => QueueCapacity::Bounded(1),
            PublisherType::MergePublisher => QueueCapacity::Unbounded,
        }
    }
}

#[async_trait]
impl Handler<DisconnectMergePlanner> for Publisher {
    type Reply = ();

    async fn handle(
        &mut self,
        _: DisconnectMergePlanner,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        info!("disconnecting merge planner mailbox");
        self.merge_planner_mailbox_opt = None;
        Ok(())
    }
}

#[async_trait]
impl Handler<SplitsUpdate> for Publisher {
    type Reply = ();

    #[instrument(name="publisher", parent=split_update.parent_span.id(), skip(self, ctx))]
    async fn handle(
        &mut self,
        split_update: SplitsUpdate,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        fail_point!("publisher:before");

        let SplitsUpdate {
            index_uid,
            new_splits,
            replaced_split_ids,
            checkpoint_delta_opt,
            publish_lock,
            ..
        } = split_update;

        let index_id = index_uid.index_id.clone();
        let index_checkpoint_delta_json_opt = serialize_checkpoint_delta(&checkpoint_delta_opt)?;
        let split_ids: Vec<String> = new_splits
            .iter()
            .map(|split| split.split_id.to_string())
            .collect();
        let Some(guard) = publish_lock.acquire().await else {
            info!(
                split_ids=?split_ids,
                "Splits' publish lock is dead."
            );
            return Ok(());
        };
        let publish_result = publish_with_retry(ctx, "publish splits", || {
            // Move the request construction in the closure so that fresh values are captured
            // on each retry, such as the publish token updating
            let metastore = self.metastore.clone();
            let publish_splits_request = PublishSplitsRequest {
                index_uid: Some(index_uid.clone()),
                staged_split_ids: split_ids.clone(),
                replaced_split_ids: replaced_split_ids.iter().map(String::from).collect(),
                index_checkpoint_delta_json_opt: index_checkpoint_delta_json_opt.clone(),
                publish_token_opt: self
                    .publish_token
                    .load()
                    .as_deref()
                    .map(|publish_token| publish_token.to_string()),
            };
            async move { metastore.publish_splits(publish_splits_request).await }
        })
        .await;
        drop(guard);

        if let Err(publish_error) = publish_result {
            if is_invalid_publish_token(&publish_error) {
                return self
                    .terminate_pipeline(ctx, publish_error, &publish_lock, &split_ids)
                    .await;
            }
            return Err(publish_error);
        }
        for split in &new_splits {
            record_published_split(&index_id, split);
        }
        let num_docs: usize = new_splits.iter().map(|split| split.num_docs).sum();
        // `footer_offsets.end` is the on-disk size of the split file in bytes.
        let split_size_bytes: u64 = new_splits
            .iter()
            .map(|split| split.footer_offsets.end)
            .sum();
        info!(
            num_splits = new_splits.len(),
            num_docs, split_size_bytes, "publish-new-splits"
        );
        suggest_truncate(ctx, &self.source_mailbox_opt, checkpoint_delta_opt).await;

        if !new_splits.is_empty() {
            if let Some(merge_planner_mailbox) = self.merge_planner_mailbox_opt.as_ref() {
                let _ = ctx
                    .send_message(merge_planner_mailbox, NewSplits { new_splits })
                    .await;
            }

            if replaced_split_ids.is_empty() {
                self.counters.num_published_splits += 1;
            } else {
                self.counters.num_replace_operations += 1;
            }
        } else {
            self.counters.num_empty_splits += 1;
        }
        fail_point!("publisher:after");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_actors::{ActorExitStatus, Command, Universe};
    use quickwit_common::test_utils::wait_until_predicate;
    use quickwit_metastore::checkpoint::{
        IndexCheckpointDelta, PartitionId, SourceCheckpoint, SourceCheckpointDelta,
    };
    use quickwit_metastore::{PublishSplitsRequestExt, SplitMetadata};
    use quickwit_proto::metastore::{
        EmptyResponse, MetastoreError, MetastoreServiceClient, MockMetastoreService,
    };
    use quickwit_proto::types::{IndexUid, Position, SplitId};
    use tracing::Span;

    use super::{Publisher, PublisherType};
    use crate::models::{PublishLock, SharedPublishToken, SplitsUpdate};
    use crate::source::SuggestTruncate;

    #[tokio::test]
    async fn test_publisher_publish_operation() {
        let universe = Universe::with_accelerated_time();
        let ref_index_uid: IndexUid = IndexUid::for_test("index", 1);
        let mut mock_metastore = MockMetastoreService::new();
        let ref_index_uid_clone = ref_index_uid.clone();
        mock_metastore
            .expect_publish_splits()
            .withf(move |publish_splits_request| {
                let checkpoint_delta: IndexCheckpointDelta = publish_splits_request
                    .deserialize_index_checkpoint()
                    .unwrap()
                    .unwrap();
                publish_splits_request.index_uid() == &ref_index_uid_clone
                    && checkpoint_delta.source_id == "source"
                    && publish_splits_request.staged_split_ids[..] == ["split"]
                    && publish_splits_request.replaced_split_ids.is_empty()
                    && checkpoint_delta.source_delta == SourceCheckpointDelta::from_range(1..3)
            })
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));
        let (merge_planner_mailbox, merge_planner_inbox) = universe.create_test_mailbox();

        let (source_mailbox, source_inbox) = universe.create_test_mailbox();

        let publisher = Publisher::new(
            PublisherType::MainPublisher,
            MetastoreServiceClient::from_mock(mock_metastore),
            Some(merge_planner_mailbox),
            Some(source_mailbox),
            SharedPublishToken::default(),
        );
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);

        assert!(
            publisher_mailbox
                .send_message(SplitsUpdate {
                    index_uid: ref_index_uid.clone(),
                    new_splits: vec![SplitMetadata {
                        split_id: "split".into(),
                        ..Default::default()
                    }],
                    replaced_split_ids: Vec::new(),
                    checkpoint_delta_opt: Some(IndexCheckpointDelta {
                        source_id: "source".to_string(),
                        source_delta: SourceCheckpointDelta::from_range(1..3),
                    }),
                    publish_lock: PublishLock::default(),
                    merge_task: None,
                    parent_span: tracing::Span::none(),
                })
                .await
                .is_ok()
        );

        let publisher_observation = publisher_handle.process_pending_and_observe().await.state;
        assert_eq!(publisher_observation.num_published_splits, 1);

        let suggest_truncate_checkpoints: Vec<SourceCheckpoint> = source_inbox
            .drain_for_test_typed::<SuggestTruncate>()
            .into_iter()
            .map(|msg| msg.0)
            .collect();

        assert_eq!(suggest_truncate_checkpoints.len(), 1);
        assert_eq!(
            suggest_truncate_checkpoints[0]
                .position_for_partition(&PartitionId::default())
                .unwrap(),
            &Position::offset(2u64)
        );

        use crate::models::NewSplits;
        let merger_msgs: Vec<NewSplits> = merge_planner_inbox.drain_for_test_typed::<NewSplits>();
        assert_eq!(merger_msgs.len(), 1);
        assert_eq!(merger_msgs[0].new_splits.len(), 1);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_publisher_publish_operation_with_empty_splits() {
        let universe = Universe::with_accelerated_time();
        let ref_index_uid: IndexUid = IndexUid::for_test("index", 1);
        let mut mock_metastore = MockMetastoreService::new();
        let ref_index_uid_clone = ref_index_uid.clone();
        mock_metastore
            .expect_publish_splits()
            .withf(move |publish_splits_request| {
                let checkpoint_delta: IndexCheckpointDelta = publish_splits_request
                    .deserialize_index_checkpoint()
                    .unwrap()
                    .unwrap();
                publish_splits_request.index_uid() == &ref_index_uid_clone
                    && checkpoint_delta.source_id == "source"
                    && publish_splits_request.staged_split_ids.is_empty()
                    && publish_splits_request.replaced_split_ids.is_empty()
                    && checkpoint_delta.source_delta == SourceCheckpointDelta::from_range(1..3)
            })
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));
        let (merge_planner_mailbox, merge_planner_inbox) = universe.create_test_mailbox();

        let (source_mailbox, source_inbox) = universe.create_test_mailbox();

        let publisher = Publisher::new(
            PublisherType::MainPublisher,
            MetastoreServiceClient::from_mock(mock_metastore),
            Some(merge_planner_mailbox),
            Some(source_mailbox),
            SharedPublishToken::default(),
        );
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);

        assert!(
            publisher_mailbox
                .send_message(SplitsUpdate {
                    index_uid: ref_index_uid.clone(),
                    new_splits: Vec::new(),
                    replaced_split_ids: Vec::new(),
                    checkpoint_delta_opt: Some(IndexCheckpointDelta {
                        source_id: "source".to_string(),
                        source_delta: SourceCheckpointDelta::from_range(1..3),
                    }),
                    publish_lock: PublishLock::default(),
                    merge_task: None,
                    parent_span: tracing::Span::none(),
                })
                .await
                .is_ok()
        );

        let publisher_observation = publisher_handle.process_pending_and_observe().await.state;
        assert_eq!(publisher_observation.num_published_splits, 0);
        assert_eq!(publisher_observation.num_replace_operations, 0);
        assert_eq!(publisher_observation.num_empty_splits, 1);

        let suggest_truncate_checkpoints: Vec<SourceCheckpoint> = source_inbox
            .drain_for_test_typed::<SuggestTruncate>()
            .into_iter()
            .map(|msg| msg.0)
            .collect();

        assert_eq!(suggest_truncate_checkpoints.len(), 1);

        use crate::models::NewSplits;
        let merger_msgs: Vec<NewSplits> = merge_planner_inbox.drain_for_test_typed::<NewSplits>();
        assert_eq!(merger_msgs.len(), 0);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_publisher_replace_operation() {
        let universe = Universe::with_accelerated_time();
        let mut mock_metastore = MockMetastoreService::new();
        let ref_index_uid: IndexUid = IndexUid::for_test("index", 1);
        let ref_index_uid_clone = ref_index_uid.clone();
        mock_metastore
            .expect_publish_splits()
            .withf(move |publish_splits_requests| {
                publish_splits_requests.index_uid() == &ref_index_uid_clone
                    && publish_splits_requests.staged_split_ids[..] == ["split3"]
                    && publish_splits_requests.replaced_split_ids[..] == ["split1", "split2"]
                    && publish_splits_requests
                        .index_checkpoint_delta_json_opt()
                        .is_empty()
            })
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));
        let (merge_planner_mailbox, merge_planner_inbox) = universe.create_test_mailbox();
        let publisher = Publisher::new(
            PublisherType::MainPublisher,
            MetastoreServiceClient::from_mock(mock_metastore),
            Some(merge_planner_mailbox),
            None,
            SharedPublishToken::default(),
        );
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);
        publisher_mailbox
            .send_message(SplitsUpdate {
                index_uid: ref_index_uid.clone(),
                new_splits: vec![SplitMetadata {
                    split_id: "split3".into(),
                    ..Default::default()
                }],
                replaced_split_ids: vec![SplitId::from("split1"), SplitId::from("split2")],
                checkpoint_delta_opt: None,
                publish_lock: PublishLock::default(),
                merge_task: None,
                parent_span: Span::none(),
            })
            .await
            .unwrap();
        let publisher_observation = publisher_handle.process_pending_and_observe().await.state;
        assert_eq!(publisher_observation.num_published_splits, 0);
        assert_eq!(publisher_observation.num_replace_operations, 1);

        use crate::models::NewSplits;
        let merge_planner_msgs = merge_planner_inbox.drain_for_test_typed::<NewSplits>();
        assert_eq!(merge_planner_msgs.len(), 1);
        assert_eq!(merge_planner_msgs[0].new_splits.len(), 1);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn publisher_acquires_publish_lock() {
        let universe = Universe::with_accelerated_time();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_publish_splits().never();
        let (merge_planner_mailbox, merge_planner_inbox) = universe.create_test_mailbox();

        let publisher = Publisher::new(
            PublisherType::MainPublisher,
            MetastoreServiceClient::from_mock(mock_metastore),
            Some(merge_planner_mailbox),
            None,
            SharedPublishToken::default(),
        );
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);

        let publish_lock = PublishLock::default();
        publish_lock.kill().await;

        publisher_mailbox
            .send_message(SplitsUpdate {
                index_uid: IndexUid::new_with_random_ulid("index"),
                new_splits: vec![SplitMetadata::for_test("test-split".into())],
                replaced_split_ids: Vec::new(),
                checkpoint_delta_opt: None,
                publish_lock,
                merge_task: None,
                parent_span: Span::none(),
            })
            .await
            .unwrap();

        let publisher_observation = publisher_handle.process_pending_and_observe().await.state;
        assert_eq!(publisher_observation.num_published_splits, 0);

        let merger_messages = merge_planner_inbox.drain_for_test();
        assert!(merger_messages.is_empty());
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_publisher_retries_then_succeeds_on_retryable_error() {
        let universe = Universe::with_accelerated_time();
        let index_uid: IndexUid = IndexUid::for_test("index", 1);
        let mut mock_metastore = MockMetastoreService::new();
        let mut attempt = 0;
        mock_metastore
            .expect_publish_splits()
            .times(2)
            .returning(move |_| {
                attempt += 1;
                if attempt == 1 {
                    Err(MetastoreError::InvalidPublishToken {
                        queue_id: "index:1/source/0".to_string(),
                    })
                } else {
                    Ok(EmptyResponse {})
                }
            });
        let publisher = Publisher::new(
            PublisherType::MainPublisher,
            MetastoreServiceClient::from_mock(mock_metastore),
            None,
            None,
            SharedPublishToken::default(),
        );
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);
        publisher_mailbox
            .send_message(SplitsUpdate {
                index_uid,
                new_splits: vec![SplitMetadata {
                    split_id: SplitId::from("split"),
                    ..Default::default()
                }],
                replaced_split_ids: Vec::new(),
                checkpoint_delta_opt: None,
                publish_lock: PublishLock::default(),
                merge_task: None,
                parent_span: Span::none(),
            })
            .await
            .unwrap();
        drop(publisher_mailbox);
        let (exit_status, observation) = publisher_handle.join().await;
        assert!(exit_status.is_success());
        assert_eq!(observation.num_published_splits, 1);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_publisher_terminates_pipeline_on_invalid_publish_token_error() {
        let universe = Universe::with_accelerated_time();
        let index_uid: IndexUid = IndexUid::for_test("index", 1);
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_publish_splits()
            .times(3)
            .returning(|_| {
                Err(MetastoreError::InvalidPublishToken {
                    queue_id: "index:1/source/0".to_string(),
                })
            });
        let (source_mailbox, source_inbox) = universe.create_test_mailbox();
        let publisher = Publisher::new(
            PublisherType::MainPublisher,
            MetastoreServiceClient::from_mock(mock_metastore),
            None,
            Some(source_mailbox),
            SharedPublishToken::default(),
        );
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);
        let publish_lock = PublishLock::default();
        let splits_update = |split_id: &str| SplitsUpdate {
            index_uid: index_uid.clone(),
            new_splits: vec![SplitMetadata {
                split_id: SplitId::from(split_id),
                ..Default::default()
            }],
            replaced_split_ids: Vec::new(),
            checkpoint_delta_opt: None,
            publish_lock: publish_lock.clone(),
            merge_task: None,
            parent_span: Span::none(),
        };
        publisher_mailbox
            .send_message(splits_update("split-1"))
            .await
            .unwrap();
        wait_until_predicate(
            || {
                let publish_lock = publish_lock.clone();
                async move { publish_lock.is_dead() }
            },
            Duration::from_secs(10),
            Duration::from_millis(10),
        )
        .await
        .expect("publisher should give up on the revoked token and kill the publish lock");

        publisher_mailbox
            .send_message(splits_update("split-2"))
            .await
            .unwrap();
        drop(publisher_mailbox);
        let (exit_status, observation) = publisher_handle.join().await;

        assert!(exit_status.is_success());
        assert_eq!(observation.num_published_splits, 0);
        let source_commands = source_inbox.drain_for_test_typed::<Command>();
        assert!(matches!(
            source_commands.as_slice(),
            [Command::ExitWithSuccess]
        ));
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_publisher_propagates_publish_errors_other_than_revoked_token() {
        let universe = Universe::with_accelerated_time();
        let index_uid: IndexUid = IndexUid::for_test("index", 1);
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_publish_splits()
            .times(1)
            .returning(|_| {
                Err(MetastoreError::InvalidArgument {
                    message: "failed to apply checkpoint delta".to_string(),
                })
            });
        let (source_mailbox, source_inbox) = universe.create_test_mailbox();
        let publisher = Publisher::new(
            PublisherType::MainPublisher,
            MetastoreServiceClient::from_mock(mock_metastore),
            None,
            Some(source_mailbox),
            SharedPublishToken::default(),
        );
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);
        let publish_lock = PublishLock::default();
        publisher_mailbox
            .send_message(SplitsUpdate {
                index_uid,
                new_splits: vec![SplitMetadata {
                    split_id: SplitId::from("split"),
                    ..Default::default()
                }],
                replaced_split_ids: Vec::new(),
                checkpoint_delta_opt: None,
                publish_lock: publish_lock.clone(),
                merge_task: None,
                parent_span: Span::none(),
            })
            .await
            .unwrap();
        let (exit_status, _) = publisher_handle.join().await;

        assert!(matches!(exit_status, ActorExitStatus::Failure(_)));
        assert!(publish_lock.is_alive());
        assert!(source_inbox.drain_for_test_typed::<Command>().is_empty());
    }
}
