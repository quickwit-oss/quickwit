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
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, AskError, Handler, Mailbox, Universe,
};
use quickwit_config::{IndexConfig, SourceConfig};
use quickwit_ingest::IngesterPool;
use quickwit_metastore::Metastore;
use quickwit_proto::control_plane::{
    CloseShardsRequest, CloseShardsResponse, ControlPlaneError, ControlPlaneResult,
    GetOpenShardsRequest, GetOpenShardsResponse, NotifyIndexChangeRequest,
    NotifyIndexChangeResponse,
};
use quickwit_proto::metastore::events::{
    AddSourceEvent, CreateIndexEvent, DeleteIndexEvent, DeleteSourceEvent, ToggleSourceEvent,
};
use quickwit_proto::metastore::{
    serde_utils as metastore_serde_utils, AddSourceRequest, CreateIndexRequest,
    CreateIndexResponse, DeleteIndexRequest, DeleteSourceRequest, EmptyResponse,
    ToggleSourceRequest,
};
use quickwit_proto::{IndexUid, NodeId};
use tracing::debug;

use crate::ingest::IngestController;
use crate::scheduler::IndexingScheduler;
use crate::IndexerPool;

#[derive(Debug)]
pub struct ControlPlane {
    metastore: Arc<dyn Metastore>,
    // N.B.: The control plane is an actor and both the indexing scheduler and the ingest
    // controller are also actors. It might be simpler to have a single actor for the control
    // plane and replace message passing with function calls. The control plane would be in
    // charge of driving the control loop of controllers.
    indexing_scheduler_mailbox: Mailbox<IndexingScheduler>,
    indexing_scheduler_handle: ActorHandle<IndexingScheduler>,
    ingest_controller_mailbox: Mailbox<IngestController>,
    ingest_controller_handle: ActorHandle<IngestController>,
}

impl ControlPlane {
    pub fn spawn(
        universe: &Universe,
        cluster_id: String,
        self_node_id: NodeId,
        indexer_pool: IndexerPool,
        ingester_pool: IngesterPool,
        metastore: Arc<dyn Metastore>,
        replication_factor: usize,
    ) -> (Mailbox<Self>, ActorHandle<Self>) {
        let indexing_scheduler =
            IndexingScheduler::new(cluster_id, self_node_id, metastore.clone(), indexer_pool);
        let (indexing_scheduler_mailbox, indexing_scheduler_handle) =
            universe.spawn_builder().spawn(indexing_scheduler);
        let ingester_controller =
            IngestController::new(metastore.clone(), ingester_pool, replication_factor);
        let (ingest_controller_mailbox, ingest_controller_handle) =
            universe.spawn_builder().spawn(ingester_controller);
        let control_plane = Self {
            metastore,
            indexing_scheduler_mailbox,
            indexing_scheduler_handle,
            ingest_controller_mailbox,
            ingest_controller_handle,
        };
        universe.spawn_builder().spawn(control_plane)
    }
}

#[async_trait]
impl Actor for ControlPlane {
    type ObservableState = (
        <IndexingScheduler as Actor>::ObservableState,
        <IngestController as Actor>::ObservableState,
    );

    fn name(&self) -> String {
        "ControlPlane".to_string()
    }

    fn observable_state(&self) -> Self::ObservableState {
        (
            self.indexing_scheduler_handle.last_observation(),
            self.ingest_controller_handle.last_observation(),
        )
    }
}

macro_rules! handle_ask_res {
    ($ask_res:expr) => {
        match $ask_res {
            Ok(response) => response,
            Err(AskError::ErrorReply(error)) => return Ok(Err(error)),
            Err(error) => return Err(ActorExitStatus::Failure(anyhow::anyhow!(error).into())),
        }
    };
}

#[async_trait]
impl Handler<CreateIndexRequest> for ControlPlane {
    type Reply = ControlPlaneResult<CreateIndexResponse>;

    async fn handle(
        &mut self,
        request: CreateIndexRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let index_config: IndexConfig =
            match metastore_serde_utils::from_json_str(&request.index_config_json) {
                Ok(index_config) => index_config,
                Err(error) => {
                    return Ok(Err(ControlPlaneError::from(error)));
                }
            };
        let index_uid = match self.metastore.create_index(index_config).await {
            Ok(index_uid) => index_uid,
            Err(error) => {
                return Ok(Err(ControlPlaneError::from(error)));
            }
        };
        let event = CreateIndexEvent {
            index_uid: index_uid.clone(),
        };
        handle_ask_res!(
            ctx.ask_for_res(&self.ingest_controller_mailbox, event.clone())
                .await
        );
        let response = CreateIndexResponse {
            index_uid: index_uid.into(),
        };
        Ok(Ok(response))
    }
}

#[async_trait]
impl Handler<DeleteIndexRequest> for ControlPlane {
    type Reply = ControlPlaneResult<EmptyResponse>;

    async fn handle(
        &mut self,
        request: DeleteIndexRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let index_uid: IndexUid = request.index_uid.into();

        if let Err(error) = self.metastore.delete_index(index_uid.clone()).await {
            return Ok(Err(ControlPlaneError::from(error)));
        };
        let event = DeleteIndexEvent { index_uid };

        handle_ask_res!(
            ctx.ask_for_res(&self.ingest_controller_mailbox, event.clone())
                .await
        );

        let response = EmptyResponse {};
        Ok(Ok(response))
    }
}

#[async_trait]
impl Handler<AddSourceRequest> for ControlPlane {
    type Reply = ControlPlaneResult<EmptyResponse>;

    async fn handle(
        &mut self,
        request: AddSourceRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let index_uid: IndexUid = request.index_uid.into();
        let source_config: SourceConfig =
            match metastore_serde_utils::from_json_str(&request.source_config_json) {
                Ok(source_config) => source_config,
                Err(error) => {
                    return Ok(Err(ControlPlaneError::from(error)));
                }
            };
        let source_id = source_config.source_id.clone();
        let source_type = source_config.source_type();

        if let Err(error) = self
            .metastore
            .add_source(index_uid.clone(), source_config)
            .await
        {
            return Ok(Err(ControlPlaneError::from(error)));
        };
        let event = AddSourceEvent {
            index_uid,
            source_id,
            source_type,
        };
        handle_ask_res!(
            ctx.ask_for_res(&self.ingest_controller_mailbox, event)
                .await
        );
        // TODO: Notify indexing controller.
        let response = EmptyResponse {};
        Ok(Ok(response))
    }
}

#[async_trait]
impl Handler<ToggleSourceRequest> for ControlPlane {
    type Reply = ControlPlaneResult<EmptyResponse>;

    async fn handle(
        &mut self,
        request: ToggleSourceRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let index_uid: IndexUid = request.index_uid.into();

        if let Err(error) = self
            .metastore
            .toggle_source(index_uid.clone(), &request.source_id, request.enable)
            .await
        {
            return Ok(Err(ControlPlaneError::from(error)));
        };
        let _event = ToggleSourceEvent {
            index_uid,
            source_id: request.source_id,
            enabled: request.enable,
        };
        // TODO: Notify indexing controller.
        let response = EmptyResponse {};
        Ok(Ok(response))
    }
}

#[async_trait]
impl Handler<DeleteSourceRequest> for ControlPlane {
    type Reply = ControlPlaneResult<EmptyResponse>;

    async fn handle(
        &mut self,
        request: DeleteSourceRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let index_uid: IndexUid = request.index_uid.into();

        if let Err(error) = self
            .metastore
            .delete_source(index_uid.clone(), &request.source_id)
            .await
        {
            return Ok(Err(ControlPlaneError::from(error)));
        };
        let _event = DeleteSourceEvent {
            index_uid,
            source_id: request.source_id,
        };
        // TODO: Notify indexing controller.
        let response = EmptyResponse {};
        Ok(Ok(response))
    }
}

#[async_trait]
impl Handler<NotifyIndexChangeRequest> for ControlPlane {
    type Reply = ControlPlaneResult<NotifyIndexChangeResponse>;

    async fn handle(
        &mut self,
        request: NotifyIndexChangeRequest,
        _: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        debug!("Index change notification: schedule indexing plan.");
        // TODO: Switch from async (`send_message`) to sync (`ask_for_res`).
        self.indexing_scheduler_mailbox
            .send_message(request)
            .await
            .context("Error sending index change notification to index scheduler.")?;
        Ok(Ok(NotifyIndexChangeResponse {}))
    }
}

#[async_trait]
impl Handler<GetOpenShardsRequest> for ControlPlane {
    type Reply = ControlPlaneResult<GetOpenShardsResponse>;

    async fn handle(
        &mut self,
        request: GetOpenShardsRequest,
        _: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let response = handle_ask_res!(self.ingest_controller_mailbox.ask_for_res(request).await);
        Ok(Ok(response))
    }
}

#[async_trait]
impl Handler<CloseShardsRequest> for ControlPlane {
    type Reply = ControlPlaneResult<CloseShardsResponse>;

    async fn handle(
        &mut self,
        request: CloseShardsRequest,
        _: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let response = handle_ask_res!(self.ingest_controller_mailbox.ask_for_res(request).await);
        Ok(Ok(response))
    }
}

#[cfg(test)]
mod tests {
    use quickwit_config::{SourceParams, INGEST_SOURCE_ID};
    use quickwit_metastore::{IndexMetadata, MockMetastore};
    use quickwit_proto::control_plane::GetOpenShardsSubrequest;
    use quickwit_proto::ingest::Shard;
    use quickwit_proto::metastore::{ListShardsResponse, ListShardsSubresponse, SourceType};

    use super::*;

    #[tokio::test]
    async fn test_control_plane_create_index() {
        let universe = Universe::with_accelerated_time();

        let cluster_id = "test-cluster".to_string();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_create_index()
            .returning(|index_config| {
                assert_eq!(index_config.index_id, "test-index");
                assert_eq!(index_config.index_uri, "ram:///test-index");

                let index_uid: IndexUid = "test-index:0".into();
                Ok(index_uid)
            });
        mock_metastore
            .expect_list_indexes_metadatas()
            .returning(|_| Ok(Vec::new()));
        let metastore = Arc::new(mock_metastore);
        let replication_factor = 1;

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            cluster_id,
            self_node_id,
            indexer_pool,
            ingester_pool,
            metastore,
            replication_factor,
        );
        let index_config = IndexConfig::for_test("test-index", "ram:///test-index");
        let create_index_request = CreateIndexRequest {
            index_config_json: serde_json::to_string(&index_config).unwrap(),
        };
        let create_index_response = control_plane_mailbox
            .ask_for_res(create_index_request)
            .await
            .unwrap();
        assert_eq!(create_index_response.index_uid, "test-index:0");

        // TODO: Test that create index event is properly sent to ingest controller.

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_delete_index() {
        let universe = Universe::with_accelerated_time();

        let cluster_id = "test-cluster".to_string();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MockMetastore::default();
        mock_metastore.expect_delete_index().returning(|index_uid| {
            assert_eq!(index_uid.as_str(), "test-index:0");
            Ok(())
        });
        mock_metastore
            .expect_list_indexes_metadatas()
            .returning(|_| Ok(Vec::new()));
        let metastore = Arc::new(mock_metastore);
        let replication_factor = 1;

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            cluster_id,
            self_node_id,
            indexer_pool,
            ingester_pool,
            metastore,
            replication_factor,
        );
        let delete_index_request = DeleteIndexRequest {
            index_uid: "test-index:0".to_string(),
        };
        control_plane_mailbox
            .ask_for_res(delete_index_request)
            .await
            .unwrap();

        // TODO: Test that delete index event is properly sent to ingest controller.

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_add_source() {
        let universe = Universe::with_accelerated_time();

        let cluster_id = "test-cluster".to_string();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_add_source()
            .returning(|index_uid, source_config| {
                assert_eq!(index_uid.as_str(), "test-index:0");
                assert_eq!(source_config.source_id, "test-source");
                assert_eq!(source_config.source_type(), SourceType::Void);
                Ok(())
            });
        mock_metastore
            .expect_list_indexes_metadatas()
            .returning(|_| Ok(Vec::new()));
        let metastore = Arc::new(mock_metastore);
        let replication_factor = 1;

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            cluster_id,
            self_node_id,
            indexer_pool,
            ingester_pool,
            metastore,
            replication_factor,
        );
        let source_config = SourceConfig::for_test("test-source", SourceParams::void());
        let add_source_request = AddSourceRequest {
            index_uid: "test-index:0".to_string(),
            source_config_json: serde_json::to_string(&source_config).unwrap(),
        };
        control_plane_mailbox
            .ask_for_res(add_source_request)
            .await
            .unwrap();

        // TODO: Test that delete index event is properly sent to ingest controller.

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_toggle_source() {
        let universe = Universe::with_accelerated_time();

        let cluster_id = "test-cluster".to_string();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_toggle_source()
            .returning(|index_uid, source_id, enable| {
                assert_eq!(index_uid.as_str(), "test-index:0");
                assert_eq!(source_id, "test-source");
                assert!(enable);
                Ok(())
            });
        mock_metastore
            .expect_list_indexes_metadatas()
            .returning(|_| Ok(Vec::new()));
        let metastore = Arc::new(mock_metastore);
        let replication_factor = 1;

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            cluster_id,
            self_node_id,
            indexer_pool,
            ingester_pool,
            metastore,
            replication_factor,
        );
        let toggle_source_request = ToggleSourceRequest {
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
            enable: true,
        };
        control_plane_mailbox
            .ask_for_res(toggle_source_request)
            .await
            .unwrap();

        // TODO: Test that delete index event is properly sent to ingest controller.

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_delete_source() {
        let universe = Universe::with_accelerated_time();

        let cluster_id = "test-cluster".to_string();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_delete_source()
            .returning(|index_uid, source_id| {
                assert_eq!(index_uid.as_str(), "test-index:0");
                assert_eq!(source_id, "test-source");
                Ok(())
            });
        mock_metastore
            .expect_list_indexes_metadatas()
            .returning(|_| Ok(Vec::new()));
        let metastore = Arc::new(mock_metastore);
        let replication_factor = 1;

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            cluster_id,
            self_node_id,
            indexer_pool,
            ingester_pool,
            metastore,
            replication_factor,
        );
        let delete_source_request = DeleteSourceRequest {
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
        };
        control_plane_mailbox
            .ask_for_res(delete_source_request)
            .await
            .unwrap();

        // TODO: Test that delete index event is properly sent to ingest controller.

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_get_open_shards() {
        let universe = Universe::with_accelerated_time();

        let cluster_id = "test-cluster".to_string();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_list_indexes_metadatas()
            .returning(|_| {
                let mut index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
                let source_config = SourceConfig::for_test(INGEST_SOURCE_ID, SourceParams::void());
                index_metadata.add_source(source_config).unwrap();
                Ok(vec![index_metadata])
            });
        mock_metastore.expect_list_shards().returning(|request| {
            assert_eq!(request.subrequests.len(), 1);

            let subrequest = &request.subrequests[0];
            assert_eq!(subrequest.index_uid, "test-index:0");
            assert_eq!(subrequest.source_id, INGEST_SOURCE_ID);

            let subresponses = vec![ListShardsSubresponse {
                index_uid: "test-index:0".to_string(),
                source_id: INGEST_SOURCE_ID.to_string(),
                shards: vec![Shard {
                    index_uid: "test-index:0".to_string(),
                    source_id: INGEST_SOURCE_ID.to_string(),
                    shard_id: 1,
                    ..Default::default()
                }],
                next_shard_id: 2,
            }];
            let response = ListShardsResponse { subresponses };
            Ok(response)
        });
        let metastore = Arc::new(mock_metastore);
        let replication_factor = 1;

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            cluster_id,
            self_node_id,
            indexer_pool,
            ingester_pool,
            metastore,
            replication_factor,
        );
        let get_open_shards_request = GetOpenShardsRequest {
            subrequests: vec![GetOpenShardsSubrequest {
                index_id: "test-index".to_string(),
                source_id: INGEST_SOURCE_ID.to_string(),
            }],
            unavailable_ingesters: Vec::new(),
        };
        let get_open_shards_response = control_plane_mailbox
            .ask_for_res(get_open_shards_request)
            .await
            .unwrap();
        assert_eq!(get_open_shards_response.subresponses.len(), 1);

        let subresponse = &get_open_shards_response.subresponses[0];
        assert_eq!(subresponse.index_uid, "test-index:0");
        assert_eq!(subresponse.source_id, INGEST_SOURCE_ID);
        assert_eq!(subresponse.open_shards.len(), 1);
        assert_eq!(subresponse.open_shards[0].shard_id, 1);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_close_shards() {
        // TODO: Write test when the RPC is actually called by ingesters.
    }
}
