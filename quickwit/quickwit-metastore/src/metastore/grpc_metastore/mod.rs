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

mod grpc_adapter;

use std::error::Error;
use std::net::SocketAddr;

use anyhow::bail;
use async_trait::async_trait;
pub use grpc_adapter::GrpcMetastoreAdapter;
use itertools::Itertools;
use quickwit_common::tower::BalanceChannel;
use quickwit_common::uri::Uri as QuickwitUri;
use quickwit_config::{IndexConfig, SourceConfig};
use quickwit_proto::metastore_api::metastore_api_service_client::MetastoreApiServiceClient;
use quickwit_proto::metastore_api::{
    AddSourceRequest, CreateIndexRequest, DeleteIndexRequest, DeleteQuery, DeleteSourceRequest,
    DeleteSplitsRequest, DeleteTask, IndexMetadataRequest, LastDeleteOpstampRequest,
    ListAllSplitsRequest, ListDeleteTasksRequest, ListIndexesMetadatasRequest, ListSplitsRequest,
    ListStaleSplitsRequest, MarkSplitsForDeletionRequest, PublishSplitsRequest,
    ResetSourceCheckpointRequest, StageSplitsRequest, ToggleSourceRequest,
    UpdateSplitsDeleteOpstampRequest,
};
use quickwit_proto::tonic::codegen::InterceptedService;
use quickwit_proto::tonic::Status;
use quickwit_proto::{IndexUid, SpanContextInterceptor};
use tower::timeout::error::Elapsed;

use crate::checkpoint::IndexCheckpointDelta;
use crate::{
    IndexMetadata, ListSplitsQuery, Metastore, MetastoreError, MetastoreResult, Split,
    SplitMetadata,
};

// URI describing in a generic way the metastore services resource present in the cluster (=
// discovered by Quickwit gossip). This value is used to build the URI of `MetastoreGrpcClient` and
// is only useful for debugging.
const GRPC_METASTORE_BASE_URI: &str = "grpc://metastore.service.cluster";

type Transport = InterceptedService<BalanceChannel<SocketAddr>, SpanContextInterceptor>;
type MetastoreGrpcClientImpl = MetastoreApiServiceClient<Transport>;

/// The [`MetastoreGrpcClient`] sends gRPC requests to cluster members running a [`Metastore`]
/// service, those nodes will execute the queries on the metastore.
/// The [`MetastoreGrpcClient`] use tonic load balancer to balance requests between nodes and
/// listen to cluster live nodes changes to keep updated the list of available nodes.
#[derive(Clone)]
pub struct MetastoreGrpcClient {
    underlying: MetastoreGrpcClientImpl,
    balance_channel: BalanceChannel<SocketAddr>,
    // URI used to describe the metastore resource of form
    // `GRPC_METASTORE_BASE_URI:{grpc_advertise_port}`. This value is only useful for
    // debugging.
    uri: QuickwitUri,
}

impl MetastoreGrpcClient {
    /// Create a [`MetastoreGrpcClient`] that sends gRPC requests to nodes running
    /// [`Metastore`] service. It listens to cluster members changes to update the
    /// nodes.
    /// `grpc_advertise_port` is used only for building the `uri`.
    pub async fn from_balance_channel(
        balance_channel: BalanceChannel<SocketAddr>,
    ) -> anyhow::Result<Self> {
        let underlying = MetastoreApiServiceClient::with_interceptor(
            balance_channel.clone(),
            SpanContextInterceptor,
        );
        let uri = QuickwitUri::from_well_formed(GRPC_METASTORE_BASE_URI);
        Ok(Self {
            underlying,
            balance_channel,
            uri,
        })
    }

    /// Creates a [`MetastoreGrpcClient`] from a duplex stream client for testing purpose.
    #[cfg(any(test, feature = "testsuite"))]
    pub async fn from_duplex_stream(client: tokio::io::DuplexStream) -> anyhow::Result<Self> {
        use http::Uri;
        use quickwit_proto::tonic::transport::Endpoint;

        let mut client = Some(client);
        let channel = Endpoint::try_from("http://test.server")?
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                let client = client.take();
                async move {
                    client.ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::Other, "Client already taken")
                    })
                }
            }))
            .await?;
        let dummy_addr = "127.0.0.1:1234".parse::<SocketAddr>()?;
        let balance_channel = BalanceChannel::from_channel(dummy_addr, channel);
        let underlying = MetastoreApiServiceClient::with_interceptor(
            balance_channel.clone(),
            SpanContextInterceptor,
        );
        Ok(Self {
            underlying,
            balance_channel,
            uri: QuickwitUri::from_well_formed(GRPC_METASTORE_BASE_URI),
        })
    }
}

#[async_trait]
impl Metastore for MetastoreGrpcClient {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        if self.balance_channel.num_connections() == 0 {
            bail!("The metastore service is unavailable.");
        }
        Ok(())
    }

    fn uri(&self) -> &QuickwitUri {
        &self.uri
    }

    /// Creates an index.
    async fn create_index(&self, index_config: IndexConfig) -> MetastoreResult<IndexUid> {
        let index_config_serialized_json =
            serde_json::to_string(&index_config).map_err(|error| {
                MetastoreError::JsonSerializeError {
                    struct_name: "IndexConfig".to_string(),
                    message: error.to_string(),
                }
            })?;
        let request = CreateIndexRequest {
            index_config_serialized_json,
        };
        let inner_response = self
            .underlying
            .clone()
            .create_index(request)
            .await
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?
            .into_inner();
        let index_uid = inner_response.index_uid.into();
        Ok(index_uid)
    }

    /// List indexes.
    async fn list_indexes_metadatas(&self) -> MetastoreResult<Vec<IndexMetadata>> {
        let response = self
            .underlying
            .clone()
            .list_indexes_metadatas(ListIndexesMetadatasRequest {})
            .await
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        let indexes_metadatas =
            serde_json::from_str(&response.into_inner().indexes_metadatas_serialized_json)
                .map_err(|error| MetastoreError::JsonDeserializeError {
                    struct_name: "Vec<IndexMetadata>".to_string(),
                    message: error.to_string(),
                })?;
        Ok(indexes_metadatas)
    }

    /// Returns the [`IndexMetadata`] for a given index.
    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        let request = IndexMetadataRequest {
            index_id: index_id.to_string(),
        };
        let response = self
            .underlying
            .clone()
            .index_metadata(request)
            .await
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        let index_metadata = serde_json::from_str(
            &response.into_inner().index_metadata_serialized_json,
        )
        .map_err(|error| MetastoreError::JsonDeserializeError {
            struct_name: "IndexMetadata".to_string(),
            message: error.to_string(),
        })?;
        Ok(index_metadata)
    }

    /// Deletes an index.
    async fn delete_index(&self, index_uid: IndexUid) -> MetastoreResult<()> {
        let request = DeleteIndexRequest {
            index_uid: index_uid.to_string(),
        };
        self.underlying
            .clone()
            .delete_index(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    /// Stages several splits.
    async fn stage_splits(
        &self,
        index_uid: IndexUid,
        split_metadata_list: Vec<SplitMetadata>,
    ) -> MetastoreResult<()> {
        let split_metadata_list_serialized_json = serde_json::to_string(&split_metadata_list)
            .map_err(|error| MetastoreError::JsonSerializeError {
                struct_name: "Vec<SplitMetadata>".to_string(),
                message: error.to_string(),
            })?;
        let tonic_request = StageSplitsRequest {
            index_uid: index_uid.to_string(),
            split_metadata_list_serialized_json,
        };
        self.underlying
            .clone()
            .stage_splits(tonic_request)
            .await
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    /// Publishes a list of splits.
    async fn publish_splits<'a>(
        &self,
        index_uid: IndexUid,
        split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
        checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    ) -> MetastoreResult<()> {
        let split_ids_vec: Vec<String> = split_ids.iter().map(|split| split.to_string()).collect();
        let replaced_split_ids_vec: Vec<String> = replaced_split_ids
            .iter()
            .map(|split_id| split_id.to_string())
            .collect();
        let index_checkpoint_delta_serialized_json = checkpoint_delta_opt
            .map(|checkpoint_delta| serde_json::to_string(&checkpoint_delta))
            .transpose()
            .map_err(|error| MetastoreError::JsonSerializeError {
                struct_name: "IndexCheckpointDelta".to_string(),
                message: error.to_string(),
            })?;
        let request = PublishSplitsRequest {
            index_uid: index_uid.into(),
            split_ids: split_ids_vec,
            replaced_split_ids: replaced_split_ids_vec,
            index_checkpoint_delta_serialized_json,
        };
        self.underlying
            .clone()
            .publish_splits(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    /// Lists the splits.
    async fn list_splits(&self, query: ListSplitsQuery) -> MetastoreResult<Vec<Split>> {
        let filter_json =
            serde_json::to_string(&query).map_err(|error| MetastoreError::JsonSerializeError {
                struct_name: "ListSplitsQuery".to_string(),
                message: error.to_string(),
            })?;

        let request = ListSplitsRequest { filter_json };
        let response = self
            .underlying
            .clone()
            .list_splits(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        let splits: Vec<Split> =
            serde_json::from_str(&response.splits_serialized_json).map_err(|error| {
                MetastoreError::JsonDeserializeError {
                    struct_name: "Vec<Split>".to_string(),
                    message: error.to_string(),
                }
            })?;
        Ok(splits)
    }

    /// Lists all the splits without filtering.
    async fn list_all_splits(&self, index_uid: IndexUid) -> MetastoreResult<Vec<Split>> {
        let request = ListAllSplitsRequest {
            index_uid: index_uid.into(),
        };
        let response = self
            .underlying
            .clone()
            .list_all_splits(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        let splits: Vec<Split> =
            serde_json::from_str(&response.splits_serialized_json).map_err(|error| {
                MetastoreError::JsonDeserializeError {
                    struct_name: "Vec<Split>".to_string(),
                    message: error.to_string(),
                }
            })?;
        Ok(splits)
    }

    /// Marks a list of splits for deletion.
    async fn mark_splits_for_deletion<'a>(
        &self,
        index_uid: IndexUid,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        let split_ids_vec: Vec<String> = split_ids
            .iter()
            .map(|split_id| split_id.to_string())
            .collect();
        let request = MarkSplitsForDeletionRequest {
            index_uid: index_uid.into(),
            split_ids: split_ids_vec,
        };
        self.underlying
            .clone()
            .mark_splits_for_deletion(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    /// Deletes a list of splits.
    async fn delete_splits<'a>(
        &self,
        index_uid: IndexUid,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        let split_ids_vec: Vec<String> = split_ids
            .iter()
            .map(|split_id| split_id.to_string())
            .collect();
        let request = DeleteSplitsRequest {
            index_uid: index_uid.into(),
            split_ids: split_ids_vec,
        };
        self.underlying
            .clone()
            .delete_splits(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    /// Adds a source to a given index.
    async fn add_source(&self, index_uid: IndexUid, source: SourceConfig) -> MetastoreResult<()> {
        let source_config_serialized_json =
            serde_json::to_string(&source).map_err(|error| MetastoreError::JsonSerializeError {
                struct_name: "SourceConfig".to_string(),
                message: error.to_string(),
            })?;
        let request = AddSourceRequest {
            index_uid: index_uid.into(),
            source_config_serialized_json,
        };
        self.underlying
            .clone()
            .add_source(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    /// Toggles the source `enabled` field value.
    async fn toggle_source(
        &self,
        index_uid: IndexUid,
        source_id: &str,
        enable: bool,
    ) -> MetastoreResult<()> {
        let request = ToggleSourceRequest {
            index_uid: index_uid.into(),
            source_id: source_id.to_string(),
            enable,
        };
        self.underlying
            .clone()
            .toggle_source(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    /// Removes a source from a given index.
    async fn delete_source(&self, index_uid: IndexUid, source_id: &str) -> MetastoreResult<()> {
        let request = DeleteSourceRequest {
            index_uid: index_uid.into(),
            source_id: source_id.to_string(),
        };
        self.underlying
            .clone()
            .delete_source(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    /// Resets a source checkpoint.
    async fn reset_source_checkpoint(
        &self,
        index_uid: IndexUid,
        source_id: &str,
    ) -> MetastoreResult<()> {
        let request = ResetSourceCheckpointRequest {
            index_uid: index_uid.into(),
            source_id: source_id.to_string(),
        };
        self.underlying
            .clone()
            .reset_source_checkpoint(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    async fn last_delete_opstamp(&self, index_uid: IndexUid) -> MetastoreResult<u64> {
        let request = LastDeleteOpstampRequest {
            index_uid: index_uid.into(),
        };
        let response = self
            .underlying
            .clone()
            .last_delete_opstamp(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(response.last_delete_opstamp)
    }

    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        let response = self
            .underlying
            .clone()
            .create_delete_task(delete_query)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(response)
    }

    async fn update_splits_delete_opstamp<'a>(
        &self,
        index_uid: IndexUid,
        split_ids: &[&'a str],
        delete_opstamp: u64,
    ) -> MetastoreResult<()> {
        let split_ids_vec: Vec<String> = split_ids
            .iter()
            .map(|split_id| split_id.to_string())
            .collect();
        let request = UpdateSplitsDeleteOpstampRequest {
            index_uid: index_uid.into(),
            split_ids: split_ids_vec,
            delete_opstamp,
        };
        self.underlying
            .clone()
            .update_splits_delete_opstamp(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    async fn list_delete_tasks(
        &self,
        index_uid: IndexUid,
        opstamp_start: u64,
    ) -> MetastoreResult<Vec<DeleteTask>> {
        let request = ListDeleteTasksRequest {
            index_uid: index_uid.into(),
            opstamp_start,
        };
        let response = self
            .underlying
            .clone()
            .list_delete_tasks(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        let delete_tasks: Vec<DeleteTask> = response
            .delete_tasks
            .into_iter()
            .map(DeleteTask::from)
            .collect_vec();
        Ok(delete_tasks)
    }

    async fn list_stale_splits(
        &self,
        index_uid: IndexUid,
        delete_opstamp: u64,
        num_splits: usize,
    ) -> MetastoreResult<Vec<Split>> {
        let request = ListStaleSplitsRequest {
            index_uid: index_uid.into(),
            delete_opstamp,
            num_splits: num_splits as u64,
        };
        let response = self
            .underlying
            .clone()
            .list_stale_splits(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        let splits: Vec<Split> =
            serde_json::from_str(&response.splits_serialized_json).map_err(|error| {
                MetastoreError::JsonDeserializeError {
                    struct_name: "Vec<Split>".to_string(),
                    message: error.to_string(),
                }
            })?;
        Ok(splits)
    }
}

/// Parse tonic error and returns [`MetastoreError`].
pub fn parse_grpc_error(grpc_error: &Status) -> MetastoreError {
    // TODO: we want to process network related errors so that we
    // return the right `MetastoreError`. We do it for the
    // channel timeout error to help the user debug this kind of
    // situation.
    let elapsed_error_opt = grpc_error
        .source()
        .and_then(|error| error.downcast_ref::<Elapsed>());

    if elapsed_error_opt.is_some() {
        return MetastoreError::ConnectionError {
            message: "gRPC request timeout triggered by the channel timeout. This can happens \
                      when tonic channel has no registered endpoints."
                .to_string(),
        };
    }

    serde_json::from_str(grpc_error.message()).unwrap_or_else(|_| MetastoreError::InternalError {
        message: grpc_error.message().to_string(),
        cause: "".to_string(),
    })
}

#[cfg(test)]
#[async_trait]
impl crate::tests::test_suite::DefaultForTest for MetastoreGrpcClient {
    async fn default_for_test() -> Self {
        use std::sync::Arc;

        use quickwit_proto::metastore_api::metastore_api_service_server::MetastoreApiServiceServer;
        use quickwit_proto::tonic::transport::Server;
        use quickwit_storage::RamStorage;

        use crate::FileBackedMetastore;

        let metastore = FileBackedMetastore::try_new(Arc::new(RamStorage::default()), None)
            .await
            .unwrap();
        let (client, server) = tokio::io::duplex(1024);
        let grpc_adapter = GrpcMetastoreAdapter::from(Arc::new(metastore) as Arc<dyn Metastore>);
        tokio::spawn(async move {
            Server::builder()
                .add_service(MetastoreApiServiceServer::new(grpc_adapter))
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(server)]))
                .await
        });
        MetastoreGrpcClient::from_duplex_stream(client)
            .await
            .unwrap()
    }
}

metastore_test_suite!(crate::MetastoreGrpcClient);
