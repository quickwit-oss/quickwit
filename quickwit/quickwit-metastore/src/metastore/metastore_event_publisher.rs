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
use quickwit_proto::metastore::{
    AddSourceRequest, CreateIndexRequest, CreateIndexResponse, DeleteIndexRequest, DeleteQuery,
    DeleteSourceRequest, DeleteSplitsRequest, DeleteTask, EmptyResponse, IndexMetadataRequest,
    IndexMetadataResponse, ListDeleteTasksRequest, ListDeleteTasksResponse, ListIndexesRequest,
    ListIndexesResponse, ListSplitsRequest, ListSplitsResponse, MarkSplitsForDeletionRequest,
    PublishSplitsRequest, ResetSourceCheckpointRequest, StageSplitsRequest, ToggleSourceRequest,
    UpdateSplitsDeleteOpstampRequest,
};
use quickwit_proto::IndexUid;
use tracing::info;

use super::AddSourceRequestExt;
use crate::{ListSplitsQuery, Metastore, MetastoreResult, Split};

/// Metastore events dispatched to subscribers.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MetastoreEvent {
    /// Delete index event.
    DeleteIndex {
        /// Index ID of the deleted index.
        index_uid: IndexUid,
    },
    /// Add source event.
    AddSource {
        /// Index ID of the added source.
        index_uid: IndexUid,
        /// Source config of the added source.
        source_config: SourceConfig,
    },
    /// Toggle source events.
    ToggleSource {
        /// Index ID of the toggled source.
        index_uid: IndexUid,
        /// Source ID of the toggled source.
        source_id: String,
        /// Whether the source was enabled or not.
        enabled: bool,
    },
    /// Delete source event.
    DeleteSource {
        /// Index ID of the deleted source.
        index_uid: IndexUid,
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

    async fn create_index(
        &self,
        request: CreateIndexRequest,
    ) -> MetastoreResult<CreateIndexResponse> {
        self.underlying.create_index(request).await
    }

    async fn index_metadata(
        &self,
        request: IndexMetadataRequest,
    ) -> MetastoreResult<IndexMetadataResponse> {
        self.underlying.index_metadata(request).await
    }

    async fn list_indexes(
        &self,
        request: ListIndexesRequest,
    ) -> MetastoreResult<ListIndexesResponse> {
        self.underlying.list_indexes(request).await
    }

    async fn delete_index(&self, request: DeleteIndexRequest) -> MetastoreResult<EmptyResponse> {
        let event = MetastoreEvent::DeleteIndex {
            index_uid: request.index_uid.clone().into(),
        };
        let response = self.underlying.delete_index(request).await?;
        self.event_broker.publish(event);
        Ok(response)
    }

    // Split API

    async fn stage_splits(&self, request: StageSplitsRequest) -> MetastoreResult<EmptyResponse> {
        self.underlying.stage_splits(request).await
    }

    async fn publish_splits(
        &self,
        request: PublishSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.underlying.publish_splits(request).await
    }

    async fn list_splits(&self, request: ListSplitsRequest) -> MetastoreResult<ListSplitsResponse> {
        self.underlying.list_splits(request).await
    }

    async fn mark_splits_for_deletion(
        &self,
        request: MarkSplitsForDeletionRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.underlying.mark_splits_for_deletion(request).await
    }

    async fn delete_splits(&self, request: DeleteSplitsRequest) -> MetastoreResult<EmptyResponse> {
        self.underlying.delete_splits(request).await
    }

    // Source API

    async fn add_source(&self, request: AddSourceRequest) -> MetastoreResult<EmptyResponse> {
        let source_config = request.deserialize_source_config()?;
        let event = MetastoreEvent::AddSource {
            index_uid: request.index_uid.clone().into(),
            source_config,
        };
        let response = self.underlying.add_source(request).await?;
        self.event_broker.publish(event);
        Ok(response)
    }

    async fn toggle_source(&self, request: ToggleSourceRequest) -> MetastoreResult<EmptyResponse> {
        let event = MetastoreEvent::ToggleSource {
            index_uid: request.index_uid.clone().into(),
            source_id: request.source_id.clone(),
            enabled: request.enable,
        };
        let response = self.underlying.toggle_source(request).await?;
        self.event_broker.publish(event);
        Ok(response)
    }

    async fn reset_source_checkpoint(
        &self,
        request: ResetSourceCheckpointRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.underlying.reset_source_checkpoint(request).await
    }

    async fn delete_source(&self, request: DeleteSourceRequest) -> MetastoreResult<EmptyResponse> {
        let event = MetastoreEvent::DeleteSource {
            index_uid: request.index_uid.clone().into(),
            source_id: request.source_id.clone(),
        };
        let response = self.underlying.delete_source(request).await?;
        self.event_broker.publish(event);
        Ok(response)
    }

    // Delete task API
    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        self.underlying.create_delete_task(delete_query).await
    }

    async fn list_delete_tasks(
        &self,
        request: ListDeleteTasksRequest,
    ) -> MetastoreResult<ListDeleteTasksResponse> {
        self.underlying.list_delete_tasks(request).await
    }

    async fn last_delete_opstamp(&self, index_uid: IndexUid) -> MetastoreResult<u64> {
        self.underlying.last_delete_opstamp(index_uid).await
    }

    async fn update_splits_delete_opstamp(
        &self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.underlying.update_splits_delete_opstamp(request).await
    }
}

#[cfg(test)]
mod tests {

    use quickwit_common::pubsub::EventSubscriber;
    use quickwit_config::SourceParams;
    use quickwit_proto::DeleteIndexRequest;

    use super::*;
    use crate::metastore::CreateIndexRequestExt;
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
        let index_config = IndexConfig::for_test(index_id, index_uri);

        let create_index_request = CreateIndexRequest::try_from_index_config(index_config).unwrap();
        let create_index_response = metastore.create_index(create_index_request).await.unwrap();
        let index_uid: IndexUid = create_index_response.index_uid.into();

        let source_id = "test-source";
        let source_config = SourceConfig::for_test(source_id, SourceParams::void());
        let add_source_request =
            AddSourceRequest::try_from_source_config(index_uid.clone(), source_config).unwrap();

        metastore.add_source(add_source_request).await.unwrap();

        let toggle_source_request = ToggleSourceRequest {
            index_uid: index_uid.clone().into(),
            source_id: source_id.to_string(),
            enable: false,
        };
        metastore
            .toggle_source(toggle_source_request)
            .await
            .unwrap();

        let delete_source_request = DeleteSourceRequest {
            index_uid: index_uid.clone().into(),
            source_id: source_id.to_string(),
        };
        metastore
            .delete_source(delete_source_request)
            .await
            .unwrap();

        let delete_request = DeleteIndexRequest::new(index_uid.clone());
        metastore.delete_index(delete_request).await.unwrap();

        assert_eq!(
            rx.recv().await.unwrap(),
            MetastoreEvent::AddSource {
                index_uid: index_uid.clone(),
                source_config,
            }
        );
        assert_eq!(
            rx.recv().await.unwrap(),
            MetastoreEvent::ToggleSource {
                index_uid: index_uid.clone(),
                source_id: source_id.to_string(),
                enabled: false,
            }
        );
        assert_eq!(
            rx.recv().await.unwrap(),
            MetastoreEvent::DeleteSource {
                index_uid: index_uid.clone(),
                source_id: source_id.to_string(),
            }
        );
        assert_eq!(
            rx.recv().await.unwrap(),
            MetastoreEvent::DeleteIndex { index_uid }
        );
        subscription.cancel();
    }
}
