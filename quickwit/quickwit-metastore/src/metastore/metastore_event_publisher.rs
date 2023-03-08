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

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_common::pubsub::{Event, EventBroker};
use quickwit_common::uri::Uri;
use quickwit_config::{IndexConfig, SourceConfig};
use quickwit_proto::metastore_api::{DeleteQuery, DeleteTask};

use crate::checkpoint::IndexCheckpointDelta;
use crate::{IndexMetadata, ListSplitsQuery, Metastore, MetastoreResult, Split, SplitMetadata};

/// Metastore events dispatched to subscribers.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MetastoreEvent {
    /// Delete index event.
    DeleteIndex {
        /// Index ID of the deleted index.
        index_id: String,
    },
    /// Add source event.
    AddSource {
        /// Index ID of the added source.
        index_id: String,
        /// Source config of the added source.
        source_config: SourceConfig,
    },
    /// Toggle source events.
    ToggleSource {
        /// Index ID of the toggled source.
        index_id: String,
        /// Source ID of the toggled source.
        source_id: String,
        /// Whether the source was enabled or not.
        enabled: bool,
    },
    /// Delete source event.
    DeleteSource {
        /// Index ID of the deleted source.
        index_id: String,
        /// Source ID of the deleted source.
        source_id: String,
    },
}

impl Event for MetastoreEvent {}

/// Wraps a metastore and dispatches events to subscribers.
pub struct MetastoreEventPublisher {
    underlying: Arc<dyn Metastore>,
    event_broker: EventBroker,
}

impl MetastoreEventPublisher {
    /// Creates a new metastore publisher.
    pub fn new(metastore: Arc<dyn Metastore>, event_broker: EventBroker) -> Self {
        Self {
            underlying: metastore,
            event_broker,
        }
    }
}

impl fmt::Debug for MetastoreEventPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetastorePublisher")
            .field("uri", self.underlying.uri())
            .finish()
    }
}

#[async_trait]
impl Metastore for MetastoreEventPublisher {
    fn uri(&self) -> &Uri {
        self.underlying.uri()
    }

    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.underlying.check_connectivity().await
    }

    // Index API
    async fn create_index(&self, index_config: IndexConfig) -> MetastoreResult<()> {
        self.underlying.create_index(index_config).await
    }

    async fn index_exists(&self, index_id: &str) -> MetastoreResult<bool> {
        self.underlying.index_exists(index_id).await
    }

    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        self.underlying.index_metadata(index_id).await
    }

    async fn list_indexes_metadatas(&self) -> MetastoreResult<Vec<IndexMetadata>> {
        self.underlying.list_indexes_metadatas().await
    }

    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()> {
        let event = MetastoreEvent::DeleteIndex {
            index_id: index_id.to_string(),
        };
        self.underlying.delete_index(index_id).await?;
        self.event_broker.publish(event);
        Ok(())
    }

    // Split API

    async fn stage_splits(
        &self,
        index_id: &str,
        split_metadata_list: Vec<SplitMetadata>,
    ) -> MetastoreResult<()> {
        self.underlying
            .stage_splits(index_id, split_metadata_list)
            .await
    }

    async fn publish_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
        checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    ) -> MetastoreResult<()> {
        self.underlying
            .publish_splits(
                index_id,
                split_ids,
                replaced_split_ids,
                checkpoint_delta_opt,
            )
            .await
    }

    async fn list_splits<'a>(&self, query: ListSplitsQuery<'a>) -> MetastoreResult<Vec<Split>> {
        self.underlying.list_splits(query).await
    }

    async fn list_all_splits(&self, index_id: &str) -> MetastoreResult<Vec<Split>> {
        self.underlying.list_all_splits(index_id).await
    }

    async fn mark_splits_for_deletion<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        self.underlying
            .mark_splits_for_deletion(index_id, split_ids)
            .await
    }

    async fn delete_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        self.underlying.delete_splits(index_id, split_ids).await
    }

    // Source API

    async fn add_source(&self, index_id: &str, source: SourceConfig) -> MetastoreResult<()> {
        let event = MetastoreEvent::AddSource {
            index_id: index_id.to_string(),
            source_config: source.clone(),
        };
        self.underlying.add_source(index_id, source).await?;
        self.event_broker.publish(event);
        Ok(())
    }

    async fn toggle_source(
        &self,
        index_id: &str,
        source_id: &str,
        enable: bool,
    ) -> MetastoreResult<()> {
        let event = MetastoreEvent::ToggleSource {
            index_id: index_id.to_string(),
            source_id: source_id.to_string(),
            enabled: enable,
        };
        self.underlying
            .toggle_source(index_id, source_id, enable)
            .await?;
        self.event_broker.publish(event);
        Ok(())
    }

    async fn reset_source_checkpoint(
        &self,
        index_id: &str,
        source_id: &str,
    ) -> MetastoreResult<()> {
        self.underlying
            .reset_source_checkpoint(index_id, source_id)
            .await
    }

    async fn delete_source(&self, index_id: &str, source_id: &str) -> MetastoreResult<()> {
        let event = MetastoreEvent::DeleteSource {
            index_id: index_id.to_string(),
            source_id: source_id.to_string(),
        };
        self.underlying.delete_source(index_id, source_id).await?;
        self.event_broker.publish(event);
        Ok(())
    }

    // Delete task API
    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        self.underlying.create_delete_task(delete_query).await
    }

    async fn list_delete_tasks(
        &self,
        index_id: &str,
        opstamp_start: u64,
    ) -> MetastoreResult<Vec<DeleteTask>> {
        self.underlying
            .list_delete_tasks(index_id, opstamp_start)
            .await
    }

    async fn last_delete_opstamp(&self, index_id: &str) -> MetastoreResult<u64> {
        self.underlying.last_delete_opstamp(index_id).await
    }

    async fn update_splits_delete_opstamp<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        delete_opstamp: u64,
    ) -> MetastoreResult<()> {
        self.underlying
            .update_splits_delete_opstamp(index_id, split_ids, delete_opstamp)
            .await
    }

    async fn list_stale_splits(
        &self,
        index_id: &str,
        delete_opstamp: u64,
        num_splits: usize,
    ) -> MetastoreResult<Vec<Split>> {
        self.underlying
            .list_stale_splits(index_id, delete_opstamp, num_splits)
            .await
    }
}

#[cfg(test)]
mod tests {

    use quickwit_common::pubsub::EventSubscriber;
    use quickwit_config::SourceParams;

    use super::*;
    use crate::metastore_for_test;
    use crate::tests::test_suite::DefaultForTest;

    #[async_trait]
    impl DefaultForTest for MetastoreEventPublisher {
        async fn default_for_test() -> Self {
            MetastoreEventPublisher {
                underlying: metastore_for_test(),
                event_broker: EventBroker::default(),
            }
        }
    }

    metastore_test_suite!(crate::metastore::metastore_event_publisher::MetastoreEventPublisher);

    #[derive(Debug, Clone)]
    struct TxSubscriber(tokio::sync::mpsc::Sender<MetastoreEvent>);

    #[async_trait]
    impl EventSubscriber<MetastoreEvent> for TxSubscriber {
        async fn handle_event(&mut self, event: MetastoreEvent) {
            let _ = self.0.send(event).await;
        }
    }

    #[tokio::test]
    async fn test_metastore_event_publisher() {
        let metastore = MetastoreEventPublisher::default_for_test().await;

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let subscription = metastore.event_broker.subscribe(TxSubscriber(tx));

        let index_id = "test-index";
        let index_uri = "ram:///indexes/test-index";
        let source_id = "test-source";
        let source_config = SourceConfig::for_test(source_id, SourceParams::void());

        metastore
            .create_index(IndexConfig::for_test(index_id, index_uri))
            .await
            .unwrap();

        metastore
            .add_source(
                index_id,
                SourceConfig::for_test(source_id, SourceParams::void()),
            )
            .await
            .unwrap();
        metastore
            .toggle_source(index_id, source_id, false)
            .await
            .unwrap();
        metastore.delete_source(index_id, source_id).await.unwrap();
        metastore.delete_index(index_id).await.unwrap();

        assert_eq!(
            rx.recv().await.unwrap(),
            MetastoreEvent::AddSource {
                index_id: index_id.to_string(),
                source_config,
            }
        );
        assert_eq!(
            rx.recv().await.unwrap(),
            MetastoreEvent::ToggleSource {
                index_id: index_id.to_string(),
                source_id: source_id.to_string(),
                enabled: false,
            }
        );
        assert_eq!(
            rx.recv().await.unwrap(),
            MetastoreEvent::DeleteSource {
                index_id: index_id.to_string(),
                source_id: source_id.to_string(),
            }
        );
        assert_eq!(
            rx.recv().await.unwrap(),
            MetastoreEvent::DeleteIndex {
                index_id: index_id.to_string(),
            }
        );
        subscription.cancel();
    }
}
