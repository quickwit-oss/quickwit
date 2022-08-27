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

use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_config::SourceConfig;
use quickwit_doc_mapper::tag_pruning::TagFilterAst;
use quickwit_proto::metastore_api::{DeleteQuery, DeleteTask};
use quickwit_proto::ControlPlaneGrpcClient;
use tracing::log::error;

use crate::checkpoint::IndexCheckpointDelta;
use crate::{IndexMetadata, Metastore, MetastoreResult, Split, SplitMetadata, SplitState};

/// Metastore which wraps a concrete metastore and triggers refresh indexing plan
/// events to the Control Plane when an {index, source} is {created, deleted}.
// TODO: forbid a metastore that wraps a gRPC client metastore.
pub struct MetastoreWithControlPlaneTriggers {
    metastore: Arc<dyn Metastore>,
    control_plane_client: ControlPlaneGrpcClient,
}

impl MetastoreWithControlPlaneTriggers {
    /// Create a metastore which triggers refresh indexing plan
    /// events to the Control Plane when an {index, source} is {created, deleted}.
    pub fn new(
        metastore: Arc<dyn Metastore>,
        control_plane_client: ControlPlaneGrpcClient,
    ) -> Self {
        Self {
            metastore,
            control_plane_client,
        }
    }
}

fn send_event_to_control_plane(mut control_plane_client: ControlPlaneGrpcClient, action: String) {
    tokio::spawn(async move {
        if let Err(error) = control_plane_client.send_index_event().await {
            error!(
                "Failed to send index event to the control plane when `{}`: `{}`.",
                action, error
            );
        }
    });
}

#[async_trait]
impl Metastore for MetastoreWithControlPlaneTriggers {
    async fn create_index(&self, index_metadata: IndexMetadata) -> MetastoreResult<()> {
        let action = format!("creating index `{}`", index_metadata.index_id);
        let _ = self.metastore.create_index(index_metadata).await?;
        send_event_to_control_plane(self.control_plane_client.clone(), action);
        Ok(())
    }

    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()> {
        let action = format!("deleting index `{}`", index_id);
        let _ = self.metastore.delete_index(index_id).await?;
        send_event_to_control_plane(self.control_plane_client.clone(), action);
        Ok(())
    }

    async fn stage_split(
        &self,
        index_id: &str,
        split_metadata: SplitMetadata,
    ) -> MetastoreResult<()> {
        self.metastore.stage_split(index_id, split_metadata).await
    }

    async fn publish_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
        checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    ) -> MetastoreResult<()> {
        self.metastore
            .publish_splits(
                index_id,
                split_ids,
                replaced_split_ids,
                checkpoint_delta_opt,
            )
            .await
    }

    async fn mark_splits_for_deletion<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        self.metastore
            .mark_splits_for_deletion(index_id, split_ids)
            .await
    }

    async fn delete_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        self.metastore.delete_splits(index_id, split_ids).await
    }

    async fn add_source(&self, index_id: &str, source: SourceConfig) -> MetastoreResult<()> {
        let action = format!(
            "creating source `{}` for index `{}`",
            source.source_id, index_id,
        );
        let _ = self.metastore.add_source(index_id, source).await?;
        send_event_to_control_plane(self.control_plane_client.clone(), action);
        Ok(())
    }

    async fn delete_source(&self, index_id: &str, source_id: &str) -> MetastoreResult<()> {
        let action = format!("deleting source `{}` for index `{}`", source_id, index_id,);
        let _ = self.metastore.delete_source(index_id, source_id).await?;
        send_event_to_control_plane(self.control_plane_client.clone(), action);
        Ok(())
    }

    async fn reset_source_checkpoint(
        &self,
        index_id: &str,
        source_id: &str,
    ) -> MetastoreResult<()> {
        self.metastore
            .reset_source_checkpoint(index_id, source_id)
            .await
    }

    async fn list_splits(
        &self,
        index_id: &str,
        state: SplitState,
        time_range_opt: Option<Range<i64>>,
        tags: Option<TagFilterAst>,
    ) -> MetastoreResult<Vec<Split>> {
        self.metastore
            .list_splits(index_id, state, time_range_opt, tags)
            .await
    }

    async fn list_all_splits(&self, index_id: &str) -> MetastoreResult<Vec<Split>> {
        self.metastore.list_all_splits(index_id).await
    }

    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        self.metastore.index_metadata(index_id).await
    }

    async fn list_indexes_metadatas(&self) -> MetastoreResult<Vec<IndexMetadata>> {
        self.metastore.list_indexes_metadatas().await
    }

    async fn list_stale_splits(
        &self,
        index_id: &str,
        delete_opstamp: u64,
        num_splits: usize,
    ) -> MetastoreResult<Vec<Split>> {
        self.metastore
            .list_stale_splits(index_id, delete_opstamp, num_splits)
            .await
    }

    fn uri(&self) -> &Uri {
        self.metastore.uri()
    }

    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.metastore.check_connectivity().await
    }

    async fn last_delete_opstamp(&self, index_id: &str) -> MetastoreResult<u64> {
        self.metastore.last_delete_opstamp(index_id).await
    }

    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        self.metastore.create_delete_task(delete_query).await
    }

    async fn update_splits_delete_opstamp<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        delete_opstamp: u64,
    ) -> MetastoreResult<()> {
        self.metastore
            .update_splits_delete_opstamp(index_id, split_ids, delete_opstamp)
            .await
    }

    async fn list_delete_tasks(
        &self,
        index_id: &str,
        opstamp_start: u64,
    ) -> MetastoreResult<Vec<DeleteTask>> {
        self.metastore
            .list_delete_tasks(index_id, opstamp_start)
            .await
    }
}

metastore_test_suite!(crate::MetastoreWithControlPlaneTriggers);

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use quickwit_config::{SourceConfig, SourceParams, VecSourceParams};
    use quickwit_proto::control_plane_api::control_plane_service_server::{
        ControlPlaneServiceServer, {self as grpc},
    };
    use quickwit_proto::control_plane_api::{
        RefreshIndexingPlanEventRequest, RefreshIndexingPlanEventResponse,
    };
    use quickwit_proto::tonic::transport::Server;
    use quickwit_proto::{tonic, ControlPlaneGrpcClient};
    use quickwit_storage::RamStorage;

    use crate::{IndexMetadata, Metastore, MetastoreWithControlPlaneTriggers, MockMetastore};

    #[cfg(test)]
    #[derive(Clone)]
    struct GrpcControlPlaneAdapterForTest {
        pub send_refresh_counter: Arc<AtomicU32>,
    }

    impl GrpcControlPlaneAdapterForTest {
        fn new() -> Self {
            Self {
                send_refresh_counter: Arc::new(AtomicU32::new(0)),
            }
        }
    }

    async fn start_control_plane_server_and_create_grpc_client(
        grpc_adapter: GrpcControlPlaneAdapterForTest,
    ) -> ControlPlaneGrpcClient {
        let (client, server) = tokio::io::duplex(1024);
        tokio::spawn(async move {
            Server::builder()
                .add_service(ControlPlaneServiceServer::new(grpc_adapter))
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(server)]))
                .await
        });
        ControlPlaneGrpcClient::from_duplex_stream(client)
            .await
            .unwrap()
    }

    #[cfg(test)]
    #[async_trait]
    impl grpc::ControlPlaneService for GrpcControlPlaneAdapterForTest {
        async fn send_refresh_indexing_plan_event(
            &self,
            _: tonic::Request<RefreshIndexingPlanEventRequest>,
        ) -> Result<tonic::Response<RefreshIndexingPlanEventResponse>, tonic::Status> {
            self.send_refresh_counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(tonic::Response::new(RefreshIndexingPlanEventResponse {}))
        }
    }

    #[async_trait]
    impl crate::tests::test_suite::DefaultForTest for MetastoreWithControlPlaneTriggers {
        async fn default_for_test() -> Self {
            use crate::FileBackedMetastore;
            let metastore = FileBackedMetastore::try_new(Arc::new(RamStorage::default()), None)
                .await
                .unwrap();
            let control_plane_client = start_control_plane_server_and_create_grpc_client(
                GrpcControlPlaneAdapterForTest::new(),
            )
            .await;
            MetastoreWithControlPlaneTriggers::new(Arc::new(metastore), control_plane_client)
        }
    }

    #[tokio::test]
    async fn test_metastore_triggers_control_plane_refresh_events() {
        let mut mocked_metastore = MockMetastore::new();
        let index_metadata =
            IndexMetadata::for_test("index-control-plane", "s3://metastore-control-plane");
        let source_config = SourceConfig {
            source_id: "source-control-plane".to_string(),
            num_pipelines: 0,
            source_params: SourceParams::Vec(VecSourceParams {
                docs: Vec::new(),
                batch_num_docs: 10,
                partition: "".to_string(),
            }),
        };
        mocked_metastore
            .expect_create_index()
            .returning(|_index_metadata: IndexMetadata| Ok(()));
        mocked_metastore
            .expect_delete_index()
            .returning(|_index: &str| Ok(()));
        mocked_metastore
            .expect_add_source()
            .returning(|_index: &str, _source_config: SourceConfig| Ok(()));
        mocked_metastore
            .expect_delete_source()
            .returning(|_index: &str, _source_id: &str| Ok(()));
        let grcp_adapter = GrpcControlPlaneAdapterForTest::new();
        let control_plane_client =
            start_control_plane_server_and_create_grpc_client(grcp_adapter.clone()).await;
        let metastore = MetastoreWithControlPlaneTriggers::new(
            Arc::new(mocked_metastore),
            control_plane_client,
        );

        assert!(metastore.create_index(index_metadata).await.is_ok());
        assert!(metastore.delete_index("index-control-plane").await.is_ok());
        assert!(metastore
            .add_source("index-control-plane", source_config)
            .await
            .is_ok());
        assert!(metastore
            .delete_source("index-control-plane", "source-control-plane")
            .await
            .is_ok());
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(
            grcp_adapter
                .send_refresh_counter
                .load(std::sync::atomic::Ordering::Relaxed),
            4
        );
    }
}
