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

use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use fail::fail_point;
use quickwit_actors::{Actor, ActorContext, Handler, Mailbox, QueueCapacity};
use quickwit_metastore::Metastore;
use serde::Serialize;
use tracing::{info, instrument};

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

#[derive(Clone)]
pub struct Publisher {
    publisher_type: PublisherType,
    metastore: Arc<dyn Metastore>,
    merge_planner_mailbox_opt: Option<Mailbox<MergePlanner>>,
    source_mailbox_opt: Option<Mailbox<SourceActor>>,
    counters: PublisherCounters,
}

impl Publisher {
    pub fn new(
        publisher_type: PublisherType,
        metastore: Arc<dyn Metastore>,
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

        let split_ids: Vec<&str> = new_splits.iter().map(|split| split.split_id()).collect();

        let replaced_split_ids_ref_vec: Vec<&str> =
            replaced_split_ids.iter().map(String::as_str).collect();

        if let Some(_guard) = publish_lock.acquire().await {
            ctx.protect_future(self.metastore.publish_splits(
                index_uid,
                &split_ids[..],
                &replaced_split_ids_ref_vec,
                checkpoint_delta_opt.clone(),
                publish_token_opt,
            ))
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
        info!(new_splits=?split_ids, checkpoint_delta=?checkpoint_delta_opt, "publish-new-splits");
        if let Some(source_mailbox) = self.source_mailbox_opt.as_ref() {
            if let Some(checkpoint) = checkpoint_delta_opt {
                // We voluntarily do not log anything here.
                //
                // Not being to send the truncation message is a common event and should not be
                // considered an error. For instance, if the source is a
                // FileSource, it will terminate upon EOF and drop its
                // mailbox.
                let _ = ctx
                    .send_message(
                        source_mailbox,
                        SuggestTruncate(checkpoint.source_delta.get_source_checkpoint()),
                    )
                    .await;
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
        IndexCheckpointDelta, PartitionId, Position, SourceCheckpoint, SourceCheckpointDelta,
    };
    use quickwit_metastore::{MockMetastore, SplitMetadata};
    use quickwit_proto::IndexUid;
    use tracing::Span;

    use super::*;
    use crate::models::PublishLock;

    #[tokio::test]
    async fn test_publisher_publish_operation() {
        let universe = Universe::with_accelerated_time();
        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_publish_splits()
            .withf(
                |index_uid,
                 split_ids,
                 replaced_split_ids,
                 checkpoint_delta_opt,
                 _publish_token_opt| {
                    let checkpoint_delta = checkpoint_delta_opt.as_ref().unwrap();
                    *index_uid == "index:11111111111111111111111111"
                        && checkpoint_delta.source_id == "source"
                        && split_ids[..] == ["split"]
                        && replaced_split_ids.is_empty()
                        && checkpoint_delta.source_delta == SourceCheckpointDelta::from_range(1..3)
                },
            )
            .times(1)
            .returning(|_, _, _, _, _| Ok(()));
        let (merge_planner_mailbox, merge_planner_inbox) = universe.create_test_mailbox();

        let (source_mailbox, source_inbox) = universe.create_test_mailbox();

        let publisher = Publisher::new(
            PublisherType::MainPublisher,
            Arc::new(mock_metastore),
            Some(merge_planner_mailbox),
            Some(source_mailbox),
        );
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);

        assert!(publisher_mailbox
            .send_message(SplitsUpdate {
                index_uid: "index:11111111111111111111111111".to_string().into(),
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
                merge_operation: None,
                parent_span: tracing::Span::none(),
            })
            .await
            .is_ok());

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
            &Position::from(2u64)
        );

        let merger_msgs: Vec<NewSplits> = merge_planner_inbox.drain_for_test_typed::<NewSplits>();
        assert_eq!(merger_msgs.len(), 1);
        assert_eq!(merger_msgs[0].new_splits.len(), 1);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_publisher_publish_operation_with_empty_splits() {
        let universe = Universe::with_accelerated_time();
        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_publish_splits()
            .withf(
                |index_uid,
                 split_ids,
                 replaced_split_ids,
                 checkpoint_delta_opt,
                 _publish_token_opt| {
                    let checkpoint_delta = checkpoint_delta_opt.as_ref().unwrap();
                    *index_uid == "index:11111111111111111111111111"
                        && checkpoint_delta.source_id == "source"
                        && split_ids.is_empty()
                        && replaced_split_ids.is_empty()
                        && checkpoint_delta.source_delta == SourceCheckpointDelta::from_range(1..3)
                },
            )
            .times(1)
            .returning(|_, _, _, _, _| Ok(()));
        let (merge_planner_mailbox, merge_planner_inbox) = universe.create_test_mailbox();

        let (source_mailbox, source_inbox) = universe.create_test_mailbox();

        let publisher = Publisher::new(
            PublisherType::MainPublisher,
            Arc::new(mock_metastore),
            Some(merge_planner_mailbox),
            Some(source_mailbox),
        );
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);

        assert!(publisher_mailbox
            .send_message(SplitsUpdate {
                index_uid: "index:11111111111111111111111111".to_string().into(),
                new_splits: Vec::new(),
                replaced_split_ids: Vec::new(),
                checkpoint_delta_opt: Some(IndexCheckpointDelta {
                    source_id: "source".to_string(),
                    source_delta: SourceCheckpointDelta::from_range(1..3),
                }),
                publish_lock: PublishLock::default(),
                publish_token_opt: None,
                merge_operation: None,
                parent_span: tracing::Span::none(),
            })
            .await
            .is_ok());

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
            &Position::from(2u64)
        );

        let merger_msgs: Vec<NewSplits> = merge_planner_inbox.drain_for_test_typed::<NewSplits>();
        assert_eq!(merger_msgs.len(), 0);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_publisher_replace_operation() {
        let universe = Universe::with_accelerated_time();
        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_publish_splits()
            .withf(
                |index_uid,
                 new_split_ids,
                 replaced_split_ids,
                 checkpoint_delta_opt,
                 _publish_token_opt| {
                    *index_uid == "index:11111111111111111111111111"
                        && new_split_ids[..] == ["split3"]
                        && replaced_split_ids[..] == ["split1", "split2"]
                        && checkpoint_delta_opt.is_none()
                },
            )
            .times(1)
            .returning(|_, _, _, _, _| Ok(()));
        let (merge_planner_mailbox, merge_planner_inbox) = universe.create_test_mailbox();
        let publisher = Publisher::new(
            PublisherType::MainPublisher,
            Arc::new(mock_metastore),
            Some(merge_planner_mailbox),
            None,
        );
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);
        let publisher_message = SplitsUpdate {
            index_uid: "index:11111111111111111111111111".to_string().into(),
            new_splits: vec![SplitMetadata {
                split_id: "split3".to_string(),
                ..Default::default()
            }],
            replaced_split_ids: vec!["split1".to_string(), "split2".to_string()],
            checkpoint_delta_opt: None,
            publish_lock: PublishLock::default(),
            publish_token_opt: None,
            merge_operation: None,
            parent_span: Span::none(),
        };
        assert!(publisher_mailbox
            .send_message(publisher_message)
            .await
            .is_ok());
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
        let mut mock_metastore = MockMetastore::default();
        mock_metastore.expect_publish_splits().never();
        let (merge_planner_mailbox, merge_planner_inbox) = universe.create_test_mailbox();

        let publisher = Publisher::new(
            PublisherType::MainPublisher,
            Arc::new(mock_metastore),
            Some(merge_planner_mailbox),
            None,
        );
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);

        let publish_lock = PublishLock::default();
        publish_lock.kill().await;

        publisher_mailbox
            .send_message(SplitsUpdate {
                index_uid: IndexUid::new("index"),
                new_splits: vec![SplitMetadata::for_test("test-split".to_string())],
                replaced_split_ids: Vec::new(),
                checkpoint_delta_opt: None,
                publish_lock,
                publish_token_opt: None,
                merge_operation: None,
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
