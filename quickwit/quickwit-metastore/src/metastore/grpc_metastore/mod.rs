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

mod grpc_adapter;

use std::collections::HashSet;
use std::error::Error;
use std::net::SocketAddr;
use std::ops::Range;
use std::time::Duration;

use async_trait::async_trait;
pub use grpc_adapter::GrpcMetastoreAdapter;
use http::Uri;
use itertools::Itertools;
use quickwit_cluster::ClusterMember;
use quickwit_common::uri::Uri as QuickwitUri;
use quickwit_config::service::QuickwitService;
use quickwit_config::SourceConfig;
use quickwit_doc_mapper::tag_pruning::TagFilterAst;
use quickwit_proto::metastore_api::metastore_api_service_client::MetastoreApiServiceClient;
use quickwit_proto::metastore_api::{
    AddSourceRequest, CreateIndexRequest, DeleteIndexRequest, DeleteQuery, DeleteSourceRequest,
    DeleteSplitsRequest, DeleteTask, IndexMetadataRequest, LastDeleteOpstampRequest,
    ListAllSplitsRequest, ListDeleteTasksRequest, ListIndexesMetadatasRequest, ListSplitsRequest,
    ListStaleSplitsRequest, MarkSplitsForDeletionRequest, PublishSplitsRequest,
    ResetSourceCheckpointRequest, StageSplitRequest, ToggleSourceRequest,
    UpdateSplitsDeleteOpstampRequest,
};
use quickwit_proto::tonic::transport::{Channel, Endpoint};
use quickwit_proto::tonic::Status;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;
use tower::discover::Change;
use tower::service_fn;
use tower::timeout::error::Elapsed;
use tower::timeout::Timeout;
use tracing::{error, info};

use crate::checkpoint::IndexCheckpointDelta;
use crate::{
    IndexMetadata, Metastore, MetastoreError, MetastoreResult, Split, SplitMetadata, SplitState,
};

const CLIENT_TIMEOUT_DURATION: Duration = if cfg!(test) {
    Duration::from_millis(100)
} else {
    Duration::from_secs(5)
};

/// The [`MetastoreGrpcClient`] sends gRPC requests to cluster members running a [`Metastore`]
/// service, those nodes will execute the queries on the metastore.
/// The [`MetastoreGrpcClient`] use tonic load balancer to balance requests between nodes and
/// listen to cluster live nodes changes to keep updated the list of available nodes.
#[derive(Clone)]
pub struct MetastoreGrpcClient(MetastoreApiServiceClient<Timeout<Channel>>);

impl MetastoreGrpcClient {
    /// Create a [`MetastoreGrpcClient`] that sends gRPC requests to nodes running
    /// [`Metastore`] service. It listens to cluster members changes to update the
    /// nodes.
    pub async fn create_and_update_from_members(
        mut members_watch_channel: WatchStream<Vec<ClusterMember>>,
    ) -> anyhow::Result<Self> {
        // Create a balance channel whose endpoint can be updated thanks to a sender.
        let (channel, channel_tx) = Channel::balance_channel(10);

        // A request send to to a channel with no endpoint will hang. To avoid a blocking request, a
        // timeout is added to the channel.
        // TODO: ideally, we want to implement our own `Channel::balance_channel` to
        // properly raise a timeout error.
        let timeout_channel = Timeout::new(channel, CLIENT_TIMEOUT_DURATION);

        let mut grpc_addresses_in_use = HashSet::new();

        // Watch for cluster members changes and dynamically update channel endpoint.
        tokio::spawn(async move {
            while let Some(new_members) = members_watch_channel.next().await {
                let new_grpc_addresses = get_metastore_grpc_addresses(&new_members);
                update_channel_endpoints(&new_grpc_addresses, &grpc_addresses_in_use, &channel_tx)
                    .await?; // <- Fails if the channel is closed. In this case we can stop the loop.
                grpc_addresses_in_use = new_grpc_addresses;
            }
            Result::<_, anyhow::Error>::Ok(())
        });

        Ok(Self(MetastoreApiServiceClient::new(timeout_channel)))
    }

    /// Creates a [`MetastoreService`] from a duplex stream client for testing purpose.
    #[doc(hidden)]
    pub async fn from_duplex_stream(client: tokio::io::DuplexStream) -> anyhow::Result<Self> {
        let mut client = Some(client);
        let channel = Endpoint::try_from("http://test.server")?
            .connect_with_connector(service_fn(move |_: Uri| {
                let client = client.take();
                async move {
                    client.ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::Other, "Client already taken")
                    })
                }
            }))
            .await?;
        let client = MetastoreApiServiceClient::new(Timeout::new(channel, CLIENT_TIMEOUT_DURATION));
        Ok(Self(client))
    }
}

#[async_trait]
impl Metastore for MetastoreGrpcClient {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        // TODO: https://github.com/quickwit-oss/quickwit/issues/1879
        Ok(())
    }

    fn uri(&self) -> &QuickwitUri {
        unimplemented!()
    }

    /// Creates an index.
    async fn create_index(&self, index_metadata: IndexMetadata) -> MetastoreResult<()> {
        let index_metadata_serialized_json =
            serde_json::to_string(&index_metadata).map_err(|error| {
                MetastoreError::JsonSerializeError {
                    name: "IndexMetadata".to_string(),
                    message: error.to_string(),
                }
            })?;
        let request = CreateIndexRequest {
            index_metadata_serialized_json,
        };
        self.0
            .clone()
            .create_index(request)
            .await
            .map(|_| ())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))
    }

    /// List indexes.
    async fn list_indexes_metadatas(&self) -> MetastoreResult<Vec<IndexMetadata>> {
        let response = self
            .0
            .clone()
            .list_indexes_metadatas(ListIndexesMetadatasRequest {})
            .await
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        let indexes_metadatas =
            serde_json::from_str(&response.into_inner().indexes_metadatas_serialized_json)
                .map_err(|error| MetastoreError::JsonDeserializeError {
                    name: "Vec<IndexMetadata>".to_string(),
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
            .0
            .clone()
            .index_metadata(request)
            .await
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        let index_metadata = serde_json::from_str(
            &response.into_inner().index_metadata_serialized_json,
        )
        .map_err(|error| MetastoreError::JsonDeserializeError {
            name: "IndexMetadata".to_string(),
            message: error.to_string(),
        })?;
        Ok(index_metadata)
    }

    /// Deletes an index.
    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()> {
        let request = DeleteIndexRequest {
            index_id: index_id.to_string(),
        };
        self.0
            .clone()
            .delete_index(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    /// Stages a split.
    async fn stage_split(
        &self,
        index_id: &str,
        split_metadata: SplitMetadata,
    ) -> MetastoreResult<()> {
        let split_metadata_serialized_json =
            serde_json::to_string(&split_metadata).map_err(|error| {
                MetastoreError::JsonSerializeError {
                    name: "SplitMetadata".to_string(),
                    message: error.to_string(),
                }
            })?;
        let tonic_request = StageSplitRequest {
            index_id: index_id.to_string(),
            split_metadata_serialized_json,
        };
        self.0
            .clone()
            .stage_split(tonic_request)
            .await
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    /// Publishes a list of splits.
    async fn publish_splits<'a>(
        &self,
        index_id: &str,
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
                name: "IndexCheckpointDelta".to_string(),
                message: error.to_string(),
            })?;
        let request = PublishSplitsRequest {
            index_id: index_id.to_string(),
            split_ids: split_ids_vec,
            replaced_split_ids: replaced_split_ids_vec,
            index_checkpoint_delta_serialized_json,
        };
        self.0
            .clone()
            .publish_splits(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    /// Lists the splits.
    async fn list_splits(
        &self,
        index_id: &str,
        split_state: SplitState,
        time_range: Option<Range<i64>>,
        tags: Option<TagFilterAst>,
    ) -> MetastoreResult<Vec<Split>> {
        let tags_serialized_json = tags
            .map(|tags_filter| serde_json::to_string(&tags_filter))
            .transpose()
            .map_err(|error| MetastoreError::JsonSerializeError {
                name: "TagFilterAst".to_string(),
                message: error.to_string(),
            })?;
        let request = ListSplitsRequest {
            index_id: index_id.to_string(),
            split_state: split_state.as_str().to_string(),
            time_range_start: time_range.as_ref().map(|range| range.start),
            time_range_end: time_range.as_ref().map(|range| range.end),
            tags_serialized_json,
        };
        let response = self
            .0
            .clone()
            .list_splits(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        let splits: Vec<Split> =
            serde_json::from_str(&response.splits_serialized_json).map_err(|error| {
                MetastoreError::JsonDeserializeError {
                    name: "Vec<Split>".to_string(),
                    message: error.to_string(),
                }
            })?;
        Ok(splits)
    }

    /// Lists all the splits without filtering.
    async fn list_all_splits(&self, index_id: &str) -> MetastoreResult<Vec<Split>> {
        let request = ListAllSplitsRequest {
            index_id: index_id.to_string(),
        };
        let response = self
            .0
            .clone()
            .list_all_splits(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        let splits: Vec<Split> =
            serde_json::from_str(&response.splits_serialized_json).map_err(|error| {
                MetastoreError::JsonDeserializeError {
                    name: "Vec<Split>".to_string(),
                    message: error.to_string(),
                }
            })?;
        Ok(splits)
    }

    /// Marks a list of splits for deletion.
    async fn mark_splits_for_deletion<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        let split_ids_vec: Vec<String> = split_ids
            .iter()
            .map(|split_id| split_id.to_string())
            .collect();
        let request = MarkSplitsForDeletionRequest {
            index_id: index_id.to_string(),
            split_ids: split_ids_vec,
        };
        self.0
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
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        let split_ids_vec: Vec<String> = split_ids
            .iter()
            .map(|split_id| split_id.to_string())
            .collect();
        let request = DeleteSplitsRequest {
            index_id: index_id.to_string(),
            split_ids: split_ids_vec,
        };
        self.0
            .clone()
            .delete_splits(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    /// Adds a source to a given index.
    async fn add_source(&self, index_id: &str, source: SourceConfig) -> MetastoreResult<()> {
        let source_config_serialized_json =
            serde_json::to_string(&source).map_err(|error| MetastoreError::JsonSerializeError {
                name: "SourceConfig".to_string(),
                message: error.to_string(),
            })?;
        let request = AddSourceRequest {
            index_id: index_id.to_string(),
            source_config_serialized_json,
        };
        self.0
            .clone()
            .add_source(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    /// Toggle source `enabled` field value.
    async fn toggle_source(
        &self,
        index_id: &str,
        source_id: &str,
        enable: bool,
    ) -> MetastoreResult<()> {
        let request = ToggleSourceRequest {
            index_id: index_id.to_string(),
            source_id: source_id.to_string(),
            enable,
        };
        self.0
            .clone()
            .toggle_source(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    /// Removes a source from a given index.
    async fn delete_source(&self, index_id: &str, source_id: &str) -> MetastoreResult<()> {
        let request = DeleteSourceRequest {
            index_id: index_id.to_string(),
            source_id: source_id.to_string(),
        };
        self.0
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
        index_id: &str,
        source_id: &str,
    ) -> MetastoreResult<()> {
        let request = ResetSourceCheckpointRequest {
            index_id: index_id.to_string(),
            source_id: source_id.to_string(),
        };
        self.0
            .clone()
            .reset_source_checkpoint(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    async fn last_delete_opstamp(&self, index_id: &str) -> MetastoreResult<u64> {
        let request = LastDeleteOpstampRequest {
            index_id: index_id.to_string(),
        };
        let response = self
            .0
            .clone()
            .last_delete_opstamp(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(response.last_delete_opstamp)
    }

    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        let response = self
            .0
            .clone()
            .create_delete_task(delete_query)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(response)
    }

    async fn update_splits_delete_opstamp<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        delete_opstamp: u64,
    ) -> MetastoreResult<()> {
        let split_ids_vec: Vec<String> = split_ids
            .iter()
            .map(|split_id| split_id.to_string())
            .collect();
        let request = UpdateSplitsDeleteOpstampRequest {
            index_id: index_id.to_string(),
            split_ids: split_ids_vec,
            delete_opstamp,
        };
        self.0
            .clone()
            .update_splits_delete_opstamp(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    async fn list_delete_tasks(
        &self,
        index_id: &str,
        opstamp_start: u64,
    ) -> MetastoreResult<Vec<DeleteTask>> {
        let request = ListDeleteTasksRequest {
            index_id: index_id.to_string(),
            opstamp_start,
        };
        let response = self
            .0
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
        index_id: &str,
        delete_opstamp: u64,
        num_splits: usize,
    ) -> MetastoreResult<Vec<Split>> {
        let request = ListStaleSplitsRequest {
            index_id: index_id.to_string(),
            delete_opstamp,
            num_splits: num_splits as u64,
        };
        let response = self
            .0
            .clone()
            .list_stale_splits(request)
            .await
            .map(|tonic_response| tonic_response.into_inner())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        let splits: Vec<Split> =
            serde_json::from_str(&response.splits_serialized_json).map_err(|error| {
                MetastoreError::JsonDeserializeError {
                    name: "Vec<Split>".to_string(),
                    message: error.to_string(),
                }
            })?;
        Ok(splits)
    }
}

fn get_metastore_grpc_addresses(members: &[ClusterMember]) -> HashSet<SocketAddr> {
    members
        .iter()
        .filter(|member| {
            member
                .available_services
                .contains(&QuickwitService::Metastore)
        })
        .map(|member| member.grpc_advertise_addr)
        .collect()
}

/// Updates channel endpoints by:
/// - Sending `Change::Insert` grpc addresses not already present in `grpc_addresses_in_use`.
/// - Sending `Change::Remove` event on grpc addresses present in `grpc_addresses_in_use` but not in
///   `updated_members`.
async fn update_channel_endpoints(
    new_grpc_addresses: &HashSet<SocketAddr>,
    grpc_addresses_in_use: &HashSet<SocketAddr>,
    endpoint_channel_rx: &Sender<Change<SocketAddr, Endpoint>>,
) -> anyhow::Result<()> {
    if new_grpc_addresses.is_empty() {
        error!("No Metastore service is available in the cluster.");
    }

    let leaving_grpc_addresses = grpc_addresses_in_use
        .difference(new_grpc_addresses)
        .into_iter()
        .collect_vec();
    if !leaving_grpc_addresses.is_empty() {
        info!(
            addresses=?leaving_grpc_addresses,
            "Removing gRPC addresses from `MetastoreGrpcClient`.",
        );
    }

    for leaving_grpc_address in leaving_grpc_addresses {
        endpoint_channel_rx
            .send(Change::Remove(*leaving_grpc_address))
            .await?;
    }

    let new_grpc_addresses = new_grpc_addresses
        .difference(grpc_addresses_in_use)
        .into_iter()
        .collect_vec();
    if !new_grpc_addresses.is_empty() {
        info!(
            addresses=?new_grpc_addresses,
            "Adding gRPC addresses to `MetastoreGrpcClient`.",
        );
    }

    for new_grpc_address in new_grpc_addresses {
        let new_grpc_uri_result = Uri::builder()
            .scheme("http")
            .authority(new_grpc_address.to_string().as_str())
            .path_and_query("/")
            .build();
        if let Ok(new_grpc_uri) = new_grpc_uri_result {
            let new_grpc_endpoint = Endpoint::from(new_grpc_uri);
            endpoint_channel_rx
                .send(Change::Insert(*new_grpc_address, new_grpc_endpoint))
                .await?;
        } else {
            error!(
                "Cannot build `Uri` from socket address `{}`, address ignored.",
                new_grpc_address
            );
        }
    }

    Ok(())
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::sync::Arc;

    use quickwit_cluster::ClusterMember;
    use quickwit_config::service::QuickwitService;
    use quickwit_proto::metastore_api::metastore_api_service_server::MetastoreApiServiceServer;
    use quickwit_proto::tonic::transport::Server;
    use tokio::sync::watch;
    use tokio_stream::wrappers::WatchStream;

    use super::grpc_adapter::GrpcMetastoreAdapter;
    use super::{IndexMetadata, Metastore, MetastoreError, MetastoreGrpcClient};
    use crate::MockMetastore;

    pub async fn create_duplex_stream_server_and_client(
        mock_metastore: Arc<dyn Metastore>,
    ) -> anyhow::Result<Arc<dyn Metastore>> {
        let (client, server) = tokio::io::duplex(1024);
        let grpc_adapter = GrpcMetastoreAdapter::from(mock_metastore);
        tokio::spawn(async move {
            Server::builder()
                .add_service(MetastoreApiServiceServer::new(grpc_adapter))
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(server)]))
                .await
        });
        let metastore = MetastoreGrpcClient::from_duplex_stream(client).await?;
        Ok(Arc::new(metastore))
    }

    async fn start_grpc_server(
        address: SocketAddr,
        metastore: Arc<dyn Metastore>,
    ) -> anyhow::Result<()> {
        let grpc_adpater = GrpcMetastoreAdapter::from(metastore);
        tokio::spawn(async move {
            Server::builder()
                .add_service(MetastoreApiServiceServer::new(grpc_adpater))
                .serve(address)
                .await?;
            Result::<_, anyhow::Error>::Ok(())
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_metastore_service_grpc_with_one_metastore_service() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let mut metastore = MockMetastore::new();
        let index_id = "test-index";
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        let metastore_service_grpc_port = quickwit_common::net::find_available_tcp_port()?;
        let metastore_service_grpc_addr: SocketAddr =
            ([127, 0, 0, 1], metastore_service_grpc_port).into();
        let searcher_grpc_port = quickwit_common::net::find_available_tcp_port()?;
        let searcher_grpc_addr: SocketAddr = ([127, 0, 0, 1], searcher_grpc_port).into();
        start_grpc_server(metastore_service_grpc_addr, Arc::new(metastore)).await?;

        let metastore_service_member = ClusterMember::new(
            "1".to_string(),
            0,
            metastore_service_grpc_addr,
            HashSet::from([QuickwitService::Metastore, QuickwitService::Indexer]),
            metastore_service_grpc_addr,
        );
        let searcher_member = ClusterMember::new(
            "2".to_string(),
            0,
            searcher_grpc_addr,
            HashSet::from([QuickwitService::Searcher]),
            searcher_grpc_addr,
        );
        let (members_tx, members_rx) =
            watch::channel::<Vec<ClusterMember>>(vec![metastore_service_member.clone()]);
        let watch_members = WatchStream::new(members_rx);
        let metastore_client = MetastoreGrpcClient::create_and_update_from_members(watch_members)
            .await
            .unwrap();

        // gRPC service should send request on the running server.
        let result = metastore_client.index_metadata(index_id).await;
        assert!(result.is_ok());

        // Send empty vec to signal that there is no more control plane in the cluster.
        let _ = members_tx.send(Vec::new());
        let err = metastore_client.index_metadata(index_id).await.unwrap_err();
        assert!(
            matches!(err, MetastoreError::ConnectionError { message } if message.starts_with("gRPC request timeout triggered by the channel timeout"))
        );

        // Send control plan member and check it's working again.
        let _ = members_tx.send(vec![metastore_service_member.clone()]);
        let result = metastore_client.index_metadata(index_id).await;
        assert!(result.is_ok());

        // Send searcher member only.
        let _ = members_tx.send(vec![searcher_member.clone()]);
        let result = metastore_client.index_metadata(index_id).await;
        assert!(result.is_err());

        // Send control plane + searcher members.
        let _ = members_tx.send(vec![
            metastore_service_member.clone(),
            searcher_member.clone(),
        ]);
        let result = metastore_client.index_metadata(index_id).await;
        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_metastore_grpc_client_with_multiple_metastore_services() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let mut metastore = MockMetastore::new();
        let index_id = "test-index";
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });

        let grpc_port_1 = quickwit_common::net::find_available_tcp_port()?;
        let grpc_addr_1: SocketAddr = ([127, 0, 0, 1], grpc_port_1).into();
        let grpc_addr_2: SocketAddr = ([127, 0, 0, 1], grpc_port_1 + 1).into();
        let grpc_addr_3: SocketAddr = ([127, 0, 0, 1], grpc_port_1 - 1).into();
        // Only one grpc socket will work.
        start_grpc_server(grpc_addr_1, Arc::new(metastore)).await?;

        let metastore_member_1 = ClusterMember::new(
            "1".to_string(),
            0,
            grpc_addr_1,
            HashSet::from([QuickwitService::Metastore]),
            grpc_addr_1,
        );
        let metastore_member_2 = ClusterMember::new(
            "2".to_string(),
            0,
            grpc_addr_2,
            HashSet::from([QuickwitService::Metastore]),
            grpc_addr_2,
        );
        let metastore_member_3 = ClusterMember::new(
            "3".to_string(),
            0,
            grpc_addr_3,
            HashSet::from([QuickwitService::Metastore]),
            grpc_addr_3,
        );
        let (members_tx, members_rx) =
            watch::channel::<Vec<ClusterMember>>(vec![metastore_member_1.clone()]);
        let watch_members = WatchStream::new(members_rx);
        let metastore_client = MetastoreGrpcClient::create_and_update_from_members(watch_members)
            .await
            .unwrap();

        let result = metastore_client.index_metadata(index_id).await;
        assert!(result.is_ok());

        // Send the three metastore members
        let _ = members_tx.send(vec![metastore_member_2.clone(), metastore_member_3.clone()]);
        let err = metastore_client.index_metadata(index_id).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("tcp connect error: Connection refused"));

        // Send the running metastore member.
        let _ = members_tx.send(vec![metastore_member_1.clone()]);
        let result = metastore_client.index_metadata(index_id).await;
        assert!(result.is_ok());

        Ok(())
    }

    // Testing a few methods of the gRPC service should be sufficient as it's only a wrapper on the
    // gRPC client.
    #[tokio::test]
    async fn test_grpc_metastore_service_index_metadata() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        let index_id = "test-index";
        metastore
            .expect_index_metadata()
            .return_once(|index_id: &str| {
                assert_eq!(index_id, "test-index");
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore
            .expect_list_all_splits()
            .return_once(|index_id: &str| {
                assert_eq!(index_id, "test-index");
                Ok(Vec::new())
            });
        let metastore_client = create_duplex_stream_server_and_client(Arc::new(metastore))
            .await
            .unwrap();

        let index_metadata_result = metastore_client.index_metadata(index_id).await;
        assert!(index_metadata_result.is_ok());
        let list_all_splits_result = metastore_client.list_all_splits(index_id).await;
        assert!(list_all_splits_result.is_ok());

        Ok(())
    }
}
