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
use quickwit_actors::{Actor, ActorContext, AsyncActor, QueueCapacity};
use quickwit_metastore::Metastore;
use tokio::sync::oneshot::Receiver;
use tracing::info;

use crate::models::{PublishOperation, PublisherMessage};

#[derive(Debug, Clone, Default)]
pub struct PublisherCounters {
    pub num_published_splits: u64,
}
pub struct Publisher {
    metastore: Arc<dyn Metastore>,
    counters: PublisherCounters,
}

impl Publisher {
    pub fn new(metastore: Arc<dyn Metastore>) -> Publisher {
        Publisher {
            metastore,
            counters: PublisherCounters::default(),
        }
    }

    pub async fn run_publish_operation(
        &self,
        publisher_message: &PublisherMessage,
    ) -> anyhow::Result<()> {
        info!(index=publisher_message.index_id.as_str(), op=?publisher_message.operation, "publish-operation");
        match &publisher_message.operation {
            PublishOperation::PublishNewSplit {
                new_split,
                checkpoint_delta,
            } => {
                self.metastore
                    .publish_splits(
                        &publisher_message.index_id,
                        &[&new_split.split_id],
                        checkpoint_delta.clone(),
                    )
                    .await
                    .with_context(|| "Failed to publish splits")?;
            }
            PublishOperation::ReplaceSplits {
                new_splits: new_split_id,
                replaced_split_ids,
            } => {
                // TODO change the metastore API to take &[String]
                let new_split_ids_ref_vec: Vec<&str> = new_split_id
                    .iter()
                    .map(|split| split.split_id.as_str())
                    .collect();
                let replaced_split_ids_ref_vec: Vec<&str> =
                    replaced_split_ids.iter().map(String::as_str).collect();
                self.metastore
                    .replace_splits(
                        &publisher_message.index_id,
                        &new_split_ids_ref_vec,
                        &replaced_split_ids_ref_vec[..],
                    )
                    .await
                    .with_context(|| "Failed to replace splits")?;
            }
        }
        Ok(())
    }
}

impl Actor for Publisher {
    type Message = Receiver<PublisherMessage>;
    type ObservableState = PublisherCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn queue_capacity(&self) -> quickwit_actors::QueueCapacity {
        QueueCapacity::Bounded(3)
    }
}

#[async_trait]
impl AsyncActor for Publisher {
    async fn process_message(
        &mut self,
        uploaded_split_future: Receiver<PublisherMessage>,
        ctx: &ActorContext<Receiver<PublisherMessage>>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        fail_point!("publisher:before");
        let publisher_message = {
            let _protect_guard = ctx.protect_zone();
            uploaded_split_future
                .await
                .with_context(|| "Upload apparently failed")? //< splits must be published in order, so one uploaded failing means we should fail
                                                              //< entirely.
        };
        self.run_publish_operation(&publisher_message).await?;

        self.counters.num_published_splits += 1;
        fail_point!("publisher:after");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use quickwit_actors::Universe;
    use quickwit_metastore::checkpoint::CheckpointDelta;
    use quickwit_metastore::{MockMetastore, SplitMetadata};
    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn test_publisher_publishes_in_order() {
        quickwit_common::setup_logging_for_tests();
        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_publish_splits()
            .withf(|index_id, split_ids, checkpoint_delta| {
                index_id == "index"
                    && split_ids[..] == ["split1"]
                    && checkpoint_delta == &CheckpointDelta::from(1..3)
            })
            .times(1)
            .returning(|_, _, _| Ok(()));
        mock_metastore
            .expect_publish_splits()
            .withf(|index_id, split_ids, checkpoint_delta| {
                index_id == "index"
                    && split_ids[..] == ["split2"]
                    && checkpoint_delta == &CheckpointDelta::from(3..7)
            })
            .times(1)
            .returning(|_, _, _| Ok(()));
        let publisher = Publisher::new(Arc::new(mock_metastore));
        let universe = Universe::new();
        let (publisher_mailbox, publisher_handle) = universe.spawn_actor(publisher).spawn_async();
        let (split_future_tx1, split_future_rx1) = oneshot::channel::<PublisherMessage>();
        assert!(universe
            .send_message(&publisher_mailbox, split_future_rx1)
            .await
            .is_ok());
        let (split_future_tx2, split_future_rx2) = oneshot::channel::<PublisherMessage>();
        assert!(universe
            .send_message(&publisher_mailbox, split_future_rx2)
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
                },
            })
            .is_ok());
        let publisher_observation = publisher_handle.process_pending_and_observe().await.state;
        assert_eq!(publisher_observation.num_published_splits, 2);
    }
}
