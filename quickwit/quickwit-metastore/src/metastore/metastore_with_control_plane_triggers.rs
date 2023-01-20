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

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_config::{IndexConfig, SourceConfig, CLI_INGEST_SOURCE_ID};
use quickwit_grpc_clients::ControlPlaneGrpcClient;
use quickwit_proto::metastore_api::{DeleteQuery, DeleteTask};
use tracing::log::error;

use crate::checkpoint::IndexCheckpointDelta;
use crate::{IndexMetadata, ListSplitsQuery, Metastore, MetastoreResult, Split, SplitMetadata};

/// Metastore which wraps a concrete metastore and notifies the control plane by a gRPC rqeuest
/// when the following index changes occur:
/// - An index is deleted.
/// - A source, different from the INGEST CLI, is created.
/// - A source is deleted.
/// Note: we don't need to send an event to the control plane on an index creation.
/// A new index has no source and thus will not change the scheduling of indexing tasks.
// TODO(fmassot):
// - Forbid a `MetastoreWithControlPlaneTriggers` that wraps a gRPC client metastore.
// - We don't sent any data to the Control Plane. It could be nice to send the relevant data to the
//   control plane and let him decide to schedule or not indexing tasks.
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

// Send an event to the Control Plane. The `action` is just there for logging.
fn send_event_to_control_plane(
    mut control_plane_client: ControlPlaneGrpcClient,
    action: &'static str,
) {
    tokio::spawn(async move {
        if let Err(error) = control_plane_client.notify_index_change().await {
            error!("Failed to send index event to the control plane on `{action}`: `{error}`.");
        }
    });
}

#[async_trait]
impl Metastore for MetastoreWithControlPlaneTriggers {
    async fn create_index(&self, index_config: IndexConfig) -> MetastoreResult<()> {
        self.metastore.create_index(index_config).await
    }

    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()> {
        let _ = self.metastore.delete_index(index_id).await?;
        send_event_to_control_plane(self.control_plane_client.clone(), "delete-index");
        Ok(())
    }

    async fn stage_splits(
        &self,
        index_id: &str,
        split_metadata_list: Vec<SplitMetadata>,
    ) -> MetastoreResult<()> {
        self.metastore
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

    async fn list_splits<'a>(&self, query: ListSplitsQuery<'a>) -> MetastoreResult<Vec<Split>> {
        self.metastore.list_splits(query).await
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

    // Source API

    async fn add_source(&self, index_id: &str, source: SourceConfig) -> MetastoreResult<()> {
        let should_send_event_to_control_plane =
            ![CLI_INGEST_SOURCE_ID, "file"].contains(&source.source_id.as_str());
        let _ = self.metastore.add_source(index_id, source).await?;
        if should_send_event_to_control_plane {
            send_event_to_control_plane(self.control_plane_client.clone(), "create-source");
        }
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

    async fn toggle_source(
        &self,
        index_id: &str,
        source_id: &str,
        enable: bool,
    ) -> MetastoreResult<()> {
        self.metastore
            .toggle_source(index_id, source_id, enable)
            .await
    }

    async fn delete_source(&self, index_id: &str, source_id: &str) -> MetastoreResult<()> {
        let _ = self.metastore.delete_source(index_id, source_id).await?;
        send_event_to_control_plane(self.control_plane_client.clone(), "delete-source");
        Ok(())
    }

    // Delete tasks API

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

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use quickwit_config::{IndexConfig, SourceConfig, SourceParams, VecSourceParams};
    use quickwit_grpc_clients::{create_channel_from_duplex_stream, ControlPlaneGrpcClient};
    use quickwit_proto::control_plane_api::control_plane_service_server::{
        ControlPlaneServiceServer, {self as grpc},
    };
    use quickwit_proto::control_plane_api::{NotifyIndexChangeRequest, NotifyIndexChangeResponse};
    use quickwit_proto::tonic::transport::Server;
    use quickwit_storage::RamStorage;

    use super::MetastoreWithControlPlaneTriggers;
    use crate::{IndexMetadata, Metastore, MockMetastore};

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

    #[async_trait]
    impl grpc::ControlPlaneService for GrpcControlPlaneAdapterForTest {
        async fn notify_index_change(
            &self,
            _: quickwit_proto::tonic::Request<NotifyIndexChangeRequest>,
        ) -> Result<
            quickwit_proto::tonic::Response<NotifyIndexChangeResponse>,
            quickwit_proto::tonic::Status,
        > {
            self.send_refresh_counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(quickwit_proto::tonic::Response::new(
                NotifyIndexChangeResponse {},
            ))
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
        let channel = create_channel_from_duplex_stream(client).await.unwrap();
        ControlPlaneGrpcClient::new(channel)
    }

    metastore_test_suite!(super::MetastoreWithControlPlaneTriggers);

    #[tokio::test]
    async fn test_metastore_triggers_control_plane_refresh_events() {
        let mut mocked_metastore = MockMetastore::new();
        let index_metadata =
            IndexMetadata::for_test("index-control-plane", "s3://metastore-control-plane");
        let source_config = SourceConfig {
            source_id: "source-control-plane".to_string(),
            enabled: true,
            max_num_pipelines_per_indexer: 0,
            desired_num_pipelines: 1,
            source_params: SourceParams::Vec(VecSourceParams {
                docs: Vec::new(),
                batch_num_docs: 10,
                partition: "".to_string(),
            }),
            transform_config: None,
        };
        mocked_metastore
            .expect_create_index()
            .returning(|_index_config: IndexConfig| Ok(()));
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

        assert!(metastore
            .create_index(index_metadata.index_config.clone())
            .await
            .is_ok());
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
            3
        );
    }
}
