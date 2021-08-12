// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::sync::Arc;

use crate::models::UploadedSplit;
use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::Actor;
use quickwit_actors::ActorContext;
use quickwit_actors::AsyncActor;
use quickwit_actors::QueueCapacity;
use quickwit_metastore::Metastore;
use tokio::sync::oneshot::Receiver;

#[derive(Debug, Clone, Default)]
pub struct PublisherCounters {
    pub num_published_splits: usize,
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
}

impl Actor for Publisher {
    type Message = Receiver<UploadedSplit>;
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
        uploaded_split_future: Receiver<UploadedSplit>,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        let uploaded_split = uploaded_split_future
            .await
            .with_context(|| "Upload apparently failed")?; //< splits must be published in order, so one uploaded failing means we should fail entirely.
        self.metastore
            .publish_splits(&uploaded_split.index_id, &[&uploaded_split.split_id])
            .await
            .with_context(|| "Failed to publish splits")?;
        self.counters.num_published_splits += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickwit_actors::Universe;
    use quickwit_metastore::MockMetastore;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn test_publisher_publishes_in_order() {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_publish_splits()
            .withf(|index_id, split_ids| index_id == "index" && &split_ids[..] == &["split1"])
            .times(1)
            .returning(|_, _| Ok(()));
        mock_metastore
            .expect_publish_splits()
            .withf(|index_id, split_ids| index_id == "index" && &split_ids[..] == &["split2"])
            .times(1)
            .returning(|_, _| Ok(()));
        let publisher = Publisher::new(Arc::new(mock_metastore));
        let (publisher_mailbox, publisher_handle) = universe.spawn(publisher);
        let (split_future_tx1, split_future_rx1) = oneshot::channel::<UploadedSplit>();
        assert!(universe
            .send_message(&publisher_mailbox, split_future_rx1)
            .await
            .is_ok());
        let (split_future_tx2, split_future_rx2) = oneshot::channel::<UploadedSplit>();
        assert!(universe
            .send_message(&publisher_mailbox, split_future_rx2)
            .await
            .is_ok());
        // note the future is resolved in the inverse of the expected order.
        assert!(split_future_tx2
            .send(UploadedSplit {
                index_id: "index".to_string(),
                split_id: "split2".to_string(),
            })
            .is_ok());
        assert!(split_future_tx1
            .send(UploadedSplit {
                index_id: "index".to_string(),
                split_id: "split1".to_string(),
            })
            .is_ok());
        let publisher_observation = publisher_handle.process_pending_and_observe().await.state;
        assert_eq!(publisher_observation.num_published_splits, 2);
    }
}
