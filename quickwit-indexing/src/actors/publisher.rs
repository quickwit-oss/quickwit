// Copyright (C) 2021 Quickwit, Inc.
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
use quickwit_metastore::checkpoint::SourceCheckpoint;
use quickwit_metastore::Metastore;
use tokio::sync::oneshot::Receiver;
use tracing::{error, info, instrument};

use crate::actors::uploader::MAX_CONCURRENT_SPLIT_UPLOAD;
use crate::actors::{GarbageCollector, MergePlanner};
use crate::models::{NewSplits, PublishOperation, PublisherMessage};
use crate::source::{SourceActor, SuggestTruncate};

#[derive(Debug, Clone, Default)]
pub struct PublisherCounters {
    pub num_published_splits: u64,
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

    pub fn queue_capacity(&self) -> QueueCapacity {
        match self {
            PublisherType::MainPublisher => QueueCapacity::Bounded(MAX_CONCURRENT_SPLIT_UPLOAD),
            PublisherType::MergePublisher => QueueCapacity::Unbounded,
        }
    }
}

pub struct Publisher {
    publisher_type: PublisherType,
    source_id: String,
    metastore: Arc<dyn Metastore>,
    merge_planner_mailbox: Mailbox<MergePlanner>,
    garbage_collector_mailbox: Mailbox<GarbageCollector>,
    source_mailbox_opt: Option<Mailbox<SourceActor>>,
    counters: PublisherCounters,
}

impl Publisher {
    pub fn new(
        publisher_type: PublisherType,
        source_id: String,
        metastore: Arc<dyn Metastore>,
        merge_planner_mailbox: Mailbox<MergePlanner>,
        garbage_collector_mailbox: Mailbox<GarbageCollector>,
        source_mailbox_opt: Option<Mailbox<SourceActor>>,
    ) -> Publisher {
        Publisher {
            publisher_type,
            source_id,
            metastore,
            merge_planner_mailbox,
            garbage_collector_mailbox,
            source_mailbox_opt,
            counters: PublisherCounters::default(),
        }
    }

    pub async fn run_publish_operation(
        &self,
        publisher_message: &PublisherMessage,
    ) -> anyhow::Result<()> {
        match &publisher_message.operation {
            PublishOperation::PublishNewSplit {
                new_split,
                checkpoint_delta,
                ..
            } => {
                info!("new-split-start");
                self.metastore
                    .publish_splits(
                        &publisher_message.index_id,
                        &self.source_id,
                        &[new_split.split_id()],
                        checkpoint_delta.clone(),
                    )
                    .await
                    .context("Failed to publish splits.")?;
                info!("new-split-success");
            }
            PublishOperation::ReplaceSplits {
                new_splits: new_split_id,
                replaced_split_ids,
            } => {
                info!("replace-split-start");
                // TODO change the metastore API to take &[String]
                let new_split_ids_ref_vec: Vec<&str> =
                    new_split_id.iter().map(|split| split.split_id()).collect();
                let replaced_split_ids_ref_vec: Vec<&str> =
                    replaced_split_ids.iter().map(String::as_str).collect();
                self.metastore
                    .replace_splits(
                        &publisher_message.index_id,
                        &new_split_ids_ref_vec,
                        &replaced_split_ids_ref_vec[..],
                    )
                    .await
                    .context("Failed to replace splits.")?;
                info!("replace-split-success");
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Actor for Publisher {
    type ObservableState = PublisherCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn queue_capacity(&self) -> quickwit_actors::QueueCapacity {
        self.publisher_type.queue_capacity()
    }

    fn name(&self) -> String {
        self.publisher_type.actor_name().to_string()
    }

    async fn finalize(
        &mut self,
        _exit_status: &quickwit_actors::ActorExitStatus,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        // The `garbage_collector` actor runs for ever.
        // Periodically scheduling new messages for itself.
        //
        // The publisher actor being the last standing actor of the pipeline,
        // its end of life should also means the end of life of never stopping actors.
        // After all, when the publisher is stopped, there shouldn't be anything to process.
        // It's fine if the garbage collector is already dead.
        let _ = ctx
            .send_exit_with_success(&self.garbage_collector_mailbox)
            .await;
        let _ = ctx
            .send_exit_with_success(&self.merge_planner_mailbox)
            .await;
        Ok(())
    }
}

#[async_trait]
impl Handler<Receiver<PublisherMessage>> for Publisher {
    type Reply = ();

    #[instrument(name="publish", source_id=self.source_id, skip_all)]
    async fn handle(
        &mut self,
        uploaded_split_future: Receiver<PublisherMessage>,
        ctx: &ActorContext<Self>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        fail_point!("publisher:before");
        let publisher_message = {
            let _protect_guard = ctx.protect_zone();
            uploaded_split_future
                .await
                .context("Failed to upload split.")? //< splits must be published in order, so one uploaded failing means we should fail
                                                     //< entirely.
        };
        self.run_publish_operation(&publisher_message).await?;

        match &publisher_message.operation {
            PublishOperation::PublishNewSplit {
                new_split,
                checkpoint_delta,
                split_date_of_birth,
            } => {
                info!(new_split=new_split.split_id(), tts=%split_date_of_birth.elapsed().as_secs_f32(), checkpoint_delta=?checkpoint_delta, "publish-new-splits");
                let checkpoint: SourceCheckpoint = checkpoint_delta.get_source_checkpoint();
                if let Some(source_mailbox) = self.source_mailbox_opt.as_ref() {
                    if ctx
                        .send_message(source_mailbox, SuggestTruncate(checkpoint))
                        .await
                        .is_err()
                    {
                        error!("fail-send-suggest-truncate");
                    }
                }
            }
            PublishOperation::ReplaceSplits {
                new_splits,
                replaced_split_ids,
            } => {
                let new_split_ids: Vec<&str> = new_splits
                    .iter()
                    .map(|new_split| new_split.split_id())
                    .collect();
                info!(new_splits=?new_split_ids, replaced_splits=?replaced_split_ids, "replace-splits");
            }
        }

        let new_splits = publisher_message.operation.extract_new_splits();

        // The merge planner is not necessarily awake and this is not an error.
        // For instance, when a source reaches its end, and the last "new" split
        // has been packaged, the packager finalizer sends a message to the merge
        // planner in order to stop it.
        let _ = ctx
            .send_message(&self.merge_planner_mailbox, NewSplits { new_splits })
            .await;
        self.counters.num_published_splits += 1;
        fail_point!("publisher:after");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use quickwit_actors::{create_test_mailbox, Universe};
    use quickwit_metastore::checkpoint::{
        CheckpointDelta, PartitionId, Position, SourceCheckpoint,
    };
    use quickwit_metastore::{MockMetastore, SplitMetadata};
    use tokio::sync::oneshot;

    use super::*;
    use crate::source::SuggestTruncate;

    #[tokio::test]
    async fn test_publisher_publishes_in_order() {
        quickwit_common::setup_logging_for_tests();
        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_publish_splits()
            .withf(|index_id, source_id, split_ids, checkpoint_delta| {
                index_id == "index"
                    && source_id == "source"
                    && split_ids[..] == ["split1"]
                    && checkpoint_delta == &CheckpointDelta::from(1..3)
            })
            .times(1)
            .returning(|_, _, _, _| Ok(()));
        mock_metastore
            .expect_publish_splits()
            .withf(|index_id, source_id, split_ids, checkpoint_delta| {
                index_id == "index"
                    && source_id == "source"
                    && split_ids[..] == ["split2"]
                    && checkpoint_delta == &CheckpointDelta::from(3..7)
            })
            .times(1)
            .returning(|_, _, _, _| Ok(()));
        let (merge_planner_mailbox, _merge_planner_inbox) = create_test_mailbox();
        let (garbage_collector_mailbox, _garbage_collector_inbox) = create_test_mailbox();

        let (source_mailbox, source_inbox) = create_test_mailbox();

        let publisher = Publisher::new(
            PublisherType::MainPublisher,
            "source".to_string(),
            Arc::new(mock_metastore),
            merge_planner_mailbox,
            garbage_collector_mailbox,
            Some(source_mailbox),
        );
        let universe = Universe::new();
        let (publisher_mailbox, publisher_handle) = universe.spawn_actor(publisher).spawn();
        let (split_future_tx1, split_future_rx1) = oneshot::channel::<PublisherMessage>();
        assert!(publisher_mailbox
            .send_message(split_future_rx1)
            .await
            .is_ok());
        let (split_future_tx2, split_future_rx2) = oneshot::channel::<PublisherMessage>();
        assert!(publisher_mailbox
            .send_message(split_future_rx2)
            .await
            .is_ok());

        // note the future is resolved in the inverse of the expected order.
        assert!(split_future_tx2
            .send(PublisherMessage {
                index_id: "index".to_string(),
                operation: PublishOperation::PublishNewSplit {
                    new_split: SplitMetadata {
                        split_id: "split2".to_string(),
                        ..Default::default()
                    },
                    checkpoint_delta: CheckpointDelta::from(3..7),
                    split_date_of_birth: Instant::now(),
                }
            })
            .is_ok());
        assert!(split_future_tx1
            .send(PublisherMessage {
                index_id: "index".to_string(),
                operation: PublishOperation::PublishNewSplit {
                    new_split: SplitMetadata {
                        split_id: "split1".to_string(),
                        ..Default::default()
                    },
                    checkpoint_delta: CheckpointDelta::from(1..3),
                    split_date_of_birth: Instant::now(),
                },
            })
            .is_ok());
        let publisher_observation = publisher_handle.process_pending_and_observe().await.state;
        assert_eq!(publisher_observation.num_published_splits, 2);

        let suggest_truncate_checkpoints: Vec<SourceCheckpoint> = source_inbox
            .drain_for_test()
            .into_iter()
            .map(|any_msg| any_msg.downcast::<SuggestTruncate>().unwrap().0)
            .collect();

        assert_eq!(suggest_truncate_checkpoints.len(), 2);
        assert_eq!(
            suggest_truncate_checkpoints[0]
                .position_for_partition(&PartitionId::default())
                .unwrap(),
            &Position::from(2u64)
        );
        assert_eq!(
            suggest_truncate_checkpoints[1]
                .position_for_partition(&PartitionId::default())
                .unwrap(),
            &Position::from(6u64)
        );
    }

    #[tokio::test]
    async fn test_publisher_replace_operation() {
        quickwit_common::setup_logging_for_tests();
        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_replace_splits()
            .withf(|index_id, new_split_ids, replaced_split_ids| {
                index_id == "index"
                    && new_split_ids[..] == ["split3"]
                    && replaced_split_ids[..] == ["split1", "split2"]
            })
            .times(1)
            .returning(|_, _, _| Ok(()));
        let (merge_planner_mailbox, merge_planner_inbox) = create_test_mailbox();
        let (garbage_collector_mailbox, _garbage_collector_inbox) = create_test_mailbox();
        let publisher = Publisher::new(
            PublisherType::MainPublisher,
            "source".to_string(),
            Arc::new(mock_metastore),
            merge_planner_mailbox,
            garbage_collector_mailbox,
            None,
        );
        let universe = Universe::new();
        let (publisher_mailbox, publisher_handle) = universe.spawn_actor(publisher).spawn();
        let (split_future_tx, split_future_rx) = oneshot::channel::<PublisherMessage>();
        assert!(publisher_mailbox
            .send_message(split_future_rx)
            .await
            .is_ok());
        assert!(split_future_tx
            .send(PublisherMessage {
                index_id: "index".to_string(),
                operation: PublishOperation::ReplaceSplits {
                    new_splits: vec![SplitMetadata {
                        split_id: "split3".to_string(),
                        ..Default::default()
                    }],
                    replaced_split_ids: vec!["split1".to_string(), "split2".to_string()],
                }
            })
            .is_ok());
        let publisher_observation = publisher_handle.process_pending_and_observe().await.state;
        assert_eq!(publisher_observation.num_published_splits, 1);
        let merge_planner_msgs = merge_planner_inbox.drain_for_test();
        assert_eq!(merge_planner_msgs.len(), 1);
        let merge_planner_msg = merge_planner_msgs[0].downcast_ref::<NewSplits>().unwrap();
        assert_eq!(merge_planner_msg.new_splits.len(), 1);
    }
}
