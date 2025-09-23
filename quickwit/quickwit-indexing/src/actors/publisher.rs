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

use anyhow::Context;
use async_trait::async_trait;
use fail::fail_point;
use quickwit_actors::{Actor, ActorContext, Handler, Mailbox, QueueCapacity};
use quickwit_proto::metastore::{MetastoreService, MetastoreServiceClient, PublishSplitsRequest};
use serde::Serialize;
use tracing::{info, instrument, warn};

use crate::actors::MergePlanner;
use crate::models::{NewSplits, SplitsUpdate};
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
    counters: PublisherCounters,
}

impl Publisher {
    pub fn new(
        publisher_type: PublisherType,
        metastore: MetastoreServiceClient,
        merge_planner_mailbox_opt: Option<Mailbox<MergePlanner>>,
        source_mailbox_opt: Option<Mailbox<SourceActor>>,
    ) -> Publisher {
        Publisher {
            publisher_type,
            metastore,
            merge_planner_mailbox_opt,
            source_mailbox_opt,
            counters: PublisherCounters::default(),
        }
    }
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
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        info!("disconnecting merge planner mailbox");
        self.merge_planner_mailbox_opt = None;
        Ok(())
    }
}

#[async_trait]
impl Handler<SplitsUpdate> for Publisher {
    type Reply = ();

    #[instrument(name="publisher", parent=split_update.parent_span.id(),  skip(self, ctx))]
    async fn handle(
        &mut self,
        split_update: SplitsUpdate,
        ctx: &ActorContext<Self>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        fail_point!("publisher:before");

        let SplitsUpdate {
            index_uid,
            new_splits,
            replaced_split_ids,
            checkpoint_delta_opt,
            publish_lock,
            publish_token_opt,
            ..
        } = split_update;

        let index_checkpoint_delta_json_opt = checkpoint_delta_opt
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .context("failed to serialize `IndexCheckpointDelta`")?;
        let split_ids: Vec<String> = new_splits
            .iter()
            .map(|split| split.split_id.clone())
            .collect();
        if let Some(_guard) = publish_lock.acquire().await {
            let publish_splits_request = PublishSplitsRequest {
                index_uid: Some(index_uid),
                staged_split_ids: split_ids.clone(),
                replaced_split_ids: replaced_split_ids.clone(),
                index_checkpoint_delta_json_opt,
                publish_token_opt: publish_token_opt.clone(),
            };
            ctx.protect_future(self.metastore.publish_splits(publish_splits_request))
                .await
                .context("failed to publish splits")?;
        } else {
            // TODO: Remove the junk right away?
            info!(
                split_ids=?split_ids,
                "Splits' publish lock is dead."
            );
            return Ok(());
        }
        info!("publish-new-splits");
        if let Some(source_mailbox) = self.source_mailbox_opt.as_ref()
            && let Some(checkpoint) = checkpoint_delta_opt
        {
            // We voluntarily do not log anything here.
            //
            // Not being to send the truncation message is a common event and should not be
            // considered an error. For instance, if the source is a
            // FileSource, it will terminate upon EOF and drop its
            // mailbox.
            let suggest_truncate_res = ctx
                .send_message(
                    source_mailbox,
                    SuggestTruncate(checkpoint.source_delta.get_source_checkpoint()),
                )
                .await;
            if let Err(send_truncate_err) = suggest_truncate_res {
                warn!(error=?send_truncate_err, "failed to send truncate message from publisher to source");
            }
        }

        if !new_splits.is_empty() {
            // The merge planner is not necessarily awake and this is not an error.
            // For instance, when a source reaches its end, and the last "new" split
            // has been packaged, the packager finalizer sends a message to the merge
            // planner in order to stop it.
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
    use quickwit_actors::Universe;
    use quickwit_metastore::checkpoint::{
        IndexCheckpointDelta, PartitionId, SourceCheckpoint, SourceCheckpointDelta,
    };
    use quickwit_metastore::{PublishSplitsRequestExt, SplitMetadata};
    use quickwit_proto::metastore::{EmptyResponse, MockMetastoreService};
    use quickwit_proto::types::{IndexUid, Position};
    use tracing::Span;

    use super::*;
    use crate::models::PublishLock;

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
        );
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);

        assert!(
            publisher_mailbox
                .send_message(SplitsUpdate {
                    index_uid: ref_index_uid.clone(),
                    new_splits: vec![SplitMetadata {
                        split_id: "split".to_string(),
                        ..Default::default()
                    }],
                    replaced_split_ids: Vec::new(),
                    checkpoint_delta_opt: Some(IndexCheckpointDelta {
                        source_id: "source".to_string(),
                        source_delta: SourceCheckpointDelta::from_range(1..3),
                    }),
                    publish_lock: PublishLock::default(),
                    publish_token_opt: None,
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
                    publish_token_opt: None,
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
        assert_eq!(
            suggest_truncate_checkpoints[0]
                .position_for_partition(&PartitionId::default())
                .unwrap(),
            &Position::offset(2u64)
        );

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
        );
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);
        let publisher_message = SplitsUpdate {
            index_uid: ref_index_uid.clone(),
            new_splits: vec![SplitMetadata {
                split_id: "split3".to_string(),
                ..Default::default()
            }],
            replaced_split_ids: vec!["split1".to_string(), "split2".to_string()],
            checkpoint_delta_opt: None,
            publish_lock: PublishLock::default(),
            publish_token_opt: None,
            merge_task: None,
            parent_span: Span::none(),
        };
        assert!(
            publisher_mailbox
                .send_message(publisher_message)
                .await
                .is_ok()
        );
        let publisher_observation = publisher_handle.process_pending_and_observe().await.state;
        assert_eq!(publisher_observation.num_published_splits, 0);
        assert_eq!(publisher_observation.num_replace_operations, 1);
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
        );
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);

        let publish_lock = PublishLock::default();
        publish_lock.kill().await;

        publisher_mailbox
            .send_message(SplitsUpdate {
                index_uid: IndexUid::new_with_random_ulid("index"),
                new_splits: vec![SplitMetadata::for_test("test-split".to_string())],
                replaced_split_ids: Vec::new(),
                checkpoint_delta_opt: None,
                publish_lock,
                publish_token_opt: None,
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
}
