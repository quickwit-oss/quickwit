// Copyright (C) 2022 Quickwit, Inc.
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
use quickwit_actors::{Actor, ActorContext, Handler, Mailbox};
use quickwit_metastore::Metastore;
use tracing::info;

use crate::actors::{GarbageCollector, MergePlanner};
use crate::models::{NewSplits, SplitUpdate};
use crate::source::{SourceActor, SuggestTruncate};

#[derive(Clone, Debug, Default)]
pub struct PublisherCounters {
    pub num_published_splits: u64,
    pub num_replace_operations: u64,
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

pub struct Publisher {
    publisher_type: PublisherType,
    metastore: Arc<dyn Metastore>,
    merge_planner_mailbox: Mailbox<MergePlanner>,
    garbage_collector_mailbox: Mailbox<GarbageCollector>,
    source_mailbox_opt: Option<Mailbox<SourceActor>>,
    counters: PublisherCounters,
}

impl Publisher {
    pub fn new(
        publisher_type: PublisherType,
        metastore: Arc<dyn Metastore>,
        merge_planner_mailbox: Mailbox<MergePlanner>,
        garbage_collector_mailbox: Mailbox<GarbageCollector>,
        source_mailbox_opt: Option<Mailbox<SourceActor>>,
    ) -> Publisher {
        Publisher {
            publisher_type,
            metastore,
            merge_planner_mailbox,
            garbage_collector_mailbox,
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
impl Handler<SplitUpdate> for Publisher {
    type Reply = ();

    async fn handle(
        &mut self,
        split_update: SplitUpdate,
        ctx: &ActorContext<Self>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        fail_point!("publisher:before");

        let SplitUpdate {
            index_id,
            new_splits,
            replaced_split_ids,
            checkpoint_delta_opt,
            publish_lock,
            date_of_birth,
        } = split_update;

        let split_ids: Vec<&str> = new_splits.iter().map(|split| split.split_id()).collect();

        let replaced_split_ids_ref_vec: Vec<&str> =
            replaced_split_ids.iter().map(String::as_str).collect();

        if let Some(_guard) = publish_lock.acquire().await {
            ctx.protect_future(self.metastore.publish_splits(
                &index_id,
                &split_ids[..],
                &replaced_split_ids_ref_vec,
                checkpoint_delta_opt.clone(),
            ))
            .await
            .context("Failed to publish splits.")?;
        } else {
            // TODO: Remove the junk right away?
            info!(
                split_ids=?split_ids,
                "Splits' publish lock is dead."
            );
            return Ok(());
        }
        info!(new_splits=?split_ids, tts=%date_of_birth.elapsed().as_secs_f32(), checkpoint_delta=?checkpoint_delta_opt, "publish-new-splits");
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

        // The merge planner is not necessarily awake and this is not an error.
        // For instance, when a source reaches its end, and the last "new" split
        // has been packaged, the packager finalizer sends a message to the merge
        // planner in order to stop it.
        let _ = ctx
            .send_message(&self.merge_planner_mailbox, NewSplits { new_splits })
            .await;
        if replaced_split_ids.is_empty() {
            self.counters.num_published_splits += 1;
        } else {
            self.counters.num_replace_operations += 1;
        }
        fail_point!("publisher:after");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use quickwit_actors::{create_test_mailbox, Universe};
    use quickwit_metastore::checkpoint::{
        IndexCheckpointDelta, PartitionId, Position, SourceCheckpoint, SourceCheckpointDelta,
    };
    use quickwit_metastore::{MockMetastore, SplitMetadata};

    use super::*;
    use crate::models::PublishLock;

    #[tokio::test]
    async fn test_publisher_publish_operation() {
        quickwit_common::setup_logging_for_tests();
        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_publish_splits()
            .withf(
                |index_id, split_ids, replaced_split_ids, checkpoint_delta_opt| {
                    let checkpoint_delta = checkpoint_delta_opt.as_ref().unwrap();
                    index_id == "index"
                        && checkpoint_delta.source_id == "source"
                        && split_ids[..] == ["split"]
                        && replaced_split_ids.is_empty()
                        && checkpoint_delta.source_delta == SourceCheckpointDelta::from(1..3)
                },
            )
            .times(1)
            .returning(|_, _, _, _| Ok(()));
        let (merge_planner_mailbox, merge_planner_inbox) = create_test_mailbox();
        let (garbage_collector_mailbox, _garbage_collector_inbox) = create_test_mailbox();

        let (source_mailbox, source_inbox) = create_test_mailbox();

        let publisher = Publisher::new(
            PublisherType::MainPublisher,
            Arc::new(mock_metastore),
            merge_planner_mailbox,
            garbage_collector_mailbox,
            Some(source_mailbox),
        );
        let universe = Universe::new();
        let (publisher_mailbox, publisher_handle) = universe.spawn_actor(publisher).spawn();

        assert!(publisher_mailbox
            .send_message(SplitUpdate {
                index_id: "index".to_string(),
                new_splits: vec![SplitMetadata {
                    split_id: "split".to_string(),
                    ..Default::default()
                }],
                replaced_split_ids: Vec::new(),
                checkpoint_delta_opt: Some(IndexCheckpointDelta {
                    source_id: "source".to_string(),
                    source_delta: SourceCheckpointDelta::from(1..3),
                }),
                publish_lock: PublishLock::default(),
                date_of_birth: Instant::now(),
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
    }

    #[tokio::test]
    async fn test_publisher_replace_operation() {
        quickwit_common::setup_logging_for_tests();
        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_publish_splits()
            .withf(
                |index_id, new_split_ids, replaced_split_ids, checkpoint_delta_opt| {
                    index_id == "index"
                        && new_split_ids[..] == ["split3"]
                        && replaced_split_ids[..] == ["split1", "split2"]
                        && checkpoint_delta_opt.is_none()
                },
            )
            .times(1)
            .returning(|_, _, _, _| Ok(()));
        let (merge_planner_mailbox, merge_planner_inbox) = create_test_mailbox();
        let (garbage_collector_mailbox, _garbage_collector_inbox) = create_test_mailbox();
        let publisher = Publisher::new(
            PublisherType::MainPublisher,
            Arc::new(mock_metastore),
            merge_planner_mailbox,
            garbage_collector_mailbox,
            None,
        );
        let universe = Universe::new();
        let (publisher_mailbox, publisher_handle) = universe.spawn_actor(publisher).spawn();
        let publisher_message = SplitUpdate {
            index_id: "index".to_string(),
            new_splits: vec![SplitMetadata {
                split_id: "split3".to_string(),
                ..Default::default()
            }],
            replaced_split_ids: vec!["split1".to_string(), "split2".to_string()],
            checkpoint_delta_opt: None,
            publish_lock: PublishLock::default(),
            date_of_birth: Instant::now(),
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
    }
}
