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
use std::ops::Sub;
use std::time::Duration;

use async_trait::async_trait;
pub use grpc_adapter::GrpcMetastoreAdapter;
use http::Uri;
use itertools::Itertools;
use quickwit_cluster::ClusterMember;
use quickwit_common::uri::Uri as QuickwitUri;
use quickwit_config::service::QuickwitService;
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
use quickwit_proto::tonic::transport::{Channel, Endpoint};
use quickwit_proto::tonic::Status;
use quickwit_proto::SpanContextInterceptor;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;
use tower::discover::Change;
use tower::timeout::error::Elapsed;
use tower::timeout::Timeout;
use tracing::{error, info};

use crate::checkpoint::IndexCheckpointDelta;
use crate::{
    IndexMetadata, ListSplitsQuery, Metastore, MetastoreError, MetastoreResult, Split,
    SplitMetadata,
};

const CLIENT_TIMEOUT_DURATION: Duration = if cfg!(test) {
    Duration::from_millis(100)
} else {
    Duration::from_secs(30)
};

// URI describing in a generic way the metastore services resource present in the cluster (=
// discovered by Quickwit gossip). This value is used to build the URI of `MetastoreGrpcClient` and
// is only useful for debugging.
const GRPC_METASTORE_BASE_URI: &str = "grpc://metastore.service.cluster";

/// The [`MetastoreGrpcClient`] sends gRPC requests to cluster members running a [`Metastore`]
/// service, those nodes will execute the queries on the metastore.
/// The [`MetastoreGrpcClient`] use tonic load balancer to balance requests between nodes and
/// listen to cluster live nodes changes to keep updated the list of available nodes.
#[derive(Clone)]
pub struct MetastoreGrpcClient {
    underlying:
        MetastoreApiServiceClient<InterceptedService<Timeout<Channel>, SpanContextInterceptor>>,
    pool_size_rx: watch::Receiver<usize>,
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
    pub async fn create_and_update_from_members(
        grpc_advertise_port: u16,
        mut members_watch_channel: WatchStream<Vec<ClusterMember>>,
    ) -> anyhow::Result<Self> {
        // Create a balance channel whose endpoint can be updated thanks to a sender.
        let (channel, channel_tx) = Channel::balance_channel(10);

        // A request send to to a channel with no endpoint will hang. To avoid a blocking request, a
        // timeout is added to the channel.
        // TODO: ideally, we want to implement our own `Channel::balance_channel` to
        // properly raise a timeout error.
        let timeout_channel = Timeout::new(channel, CLIENT_TIMEOUT_DURATION);
        let (pool_size_tx, pool_size_rx) = watch::channel(0);

        // Watch for cluster members changes and dynamically update channel endpoint.
        tokio::spawn({
            let mut current_grpc_address_pool = HashSet::new();
            async move {
                while let Some(new_members) = members_watch_channel.next().await {
                    let new_grpc_address_pool = get_metastore_grpc_addresses(&new_members);
                    update_channel_endpoints(
                        &new_grpc_address_pool,
                        &current_grpc_address_pool,
                        &channel_tx,
                    )
                    .await?; // <- Fails if the channel is closed. In this case we can stop the loop.
                    current_grpc_address_pool = new_grpc_address_pool;
                    // TODO: Expose number of metastore servers in the pool as a Prometheus metric.
                    pool_size_tx.send(current_grpc_address_pool.len())?;
                }
                Result::<_, anyhow::Error>::Ok(())
            }
        });
        let underlying =
            MetastoreApiServiceClient::with_interceptor(timeout_channel, SpanContextInterceptor);
        let uri = QuickwitUri::from_well_formed(format!(
            "{}:{}",
            GRPC_METASTORE_BASE_URI, grpc_advertise_port
        ));
        Ok(Self {
            uri,
            underlying,
            pool_size_rx,
        })
    }

    /// Creates a [`MetastoreGrpcClient`] from a duplex stream client for testing purpose.
    #[cfg(any(test, feature = "testsuite"))]
    pub async fn from_duplex_stream(client: tokio::io::DuplexStream) -> anyhow::Result<Self> {
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
        let timeout_channel = Timeout::new(channel, CLIENT_TIMEOUT_DURATION);
        let underlying =
            MetastoreApiServiceClient::with_interceptor(timeout_channel, SpanContextInterceptor);
        let (_pool_size_tx, pool_size_rx) = watch::channel(1);
        Ok(Self {
            underlying,
            pool_size_rx,
            uri: QuickwitUri::from_well_formed(GRPC_METASTORE_BASE_URI),
        })
    }
}

#[async_trait]
impl Metastore for MetastoreGrpcClient {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        if *self.pool_size_rx.borrow() == 0 {
            return Err(anyhow::anyhow!("No metastore server in the pool."));
        }
        Ok(())
    }

    fn uri(&self) -> &QuickwitUri {
        &self.uri
    }

    /// Creates an index.
    async fn create_index(&self, index_config: IndexConfig) -> MetastoreResult<()> {
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
        self.underlying
            .clone()
            .create_index(request)
            .await
            .map(|_| ())
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))
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
    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()> {
        let request = DeleteIndexRequest {
            index_id: index_id.to_string(),
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
        index_id: &str,
        split_metadata_list: Vec<SplitMetadata>,
    ) -> MetastoreResult<()> {
        let split_metadata_list_serialized_json = serde_json::to_string(&split_metadata_list)
            .map_err(|error| MetastoreError::JsonSerializeError {
                struct_name: "Vec<SplitMetadata>".to_string(),
                message: error.to_string(),
            })?;
        let tonic_request = StageSplitsRequest {
            index_id: index_id.to_string(),
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
                struct_name: "IndexCheckpointDelta".to_string(),
                message: error.to_string(),
            })?;
        let request = PublishSplitsRequest {
            index_id: index_id.to_string(),
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
    async fn list_splits<'a>(&self, query: ListSplitsQuery<'a>) -> MetastoreResult<Vec<Split>> {
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
    async fn list_all_splits(&self, index_id: &str) -> MetastoreResult<Vec<Split>> {
        let request = ListAllSplitsRequest {
            index_id: index_id.to_string(),
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
        self.underlying
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
                struct_name: "SourceConfig".to_string(),
                message: error.to_string(),
            })?;
        let request = AddSourceRequest {
            index_id: index_id.to_string(),
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
        index_id: &str,
        source_id: &str,
        enable: bool,
    ) -> MetastoreResult<()> {
        let request = ToggleSourceRequest {
            index_id: index_id.to_string(),
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
    async fn delete_source(&self, index_id: &str, source_id: &str) -> MetastoreResult<()> {
        let request = DeleteSourceRequest {
            index_id: index_id.to_string(),
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
        index_id: &str,
        source_id: &str,
    ) -> MetastoreResult<()> {
        let request = ResetSourceCheckpointRequest {
            index_id: index_id.to_string(),
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

    async fn last_delete_opstamp(&self, index_id: &str) -> MetastoreResult<u64> {
        let request = LastDeleteOpstampRequest {
            index_id: index_id.to_string(),
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
        index_id: &str,
        opstamp_start: u64,
    ) -> MetastoreResult<Vec<DeleteTask>> {
        let request = ListDeleteTasksRequest {
            index_id: index_id.to_string(),
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

fn get_metastore_grpc_addresses(members: &[ClusterMember]) -> HashSet<SocketAddr> {
    members
        .iter()
        .filter(|member| {
            member
                .enabled_services
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
    new_grpc_address_pool: &HashSet<SocketAddr>,
    current_grpc_address_pool: &HashSet<SocketAddr>,
    channel_endpoint_tx: &Sender<Change<SocketAddr, Endpoint>>,
) -> anyhow::Result<()> {
    if new_grpc_address_pool.is_empty() {
        error!("No metastore servers available in the cluster.");
    }
    let leaving_grpc_addresses = current_grpc_address_pool.sub(new_grpc_address_pool);
    if !leaving_grpc_addresses.is_empty() {
        info!(
            // TODO: Log node IDs along with the addresses.
            server_addresses=?leaving_grpc_addresses,
            "Removing metastore servers from client pool.",
        );
        for leaving_grpc_address in leaving_grpc_addresses {
            channel_endpoint_tx
                .send(Change::Remove(leaving_grpc_address))
                .await?;
        }
    }
    let new_grpc_addresses = new_grpc_address_pool.sub(current_grpc_address_pool);
    if !new_grpc_addresses.is_empty() {
        info!(
            // TODO: Log node IDs along with the addresses.
            server_addresses=?new_grpc_addresses,
            "Adding metastore servers to client pool.",
        );
        for new_grpc_address in new_grpc_addresses {
            let new_grpc_uri = Uri::builder()
                .scheme("http")
                .authority(new_grpc_address.to_string())
                .path_and_query("/")
                .build()
                .expect("Failed to build URI. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.");
            let new_grpc_endpoint = Endpoint::from(new_grpc_uri);
            channel_endpoint_tx
                .send(Change::Insert(new_grpc_address, new_grpc_endpoint))
                .await?;
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
        tokio::spawn(async move {
            let grpc_adpater = GrpcMetastoreAdapter::from(metastore);
            let service = MetastoreApiServiceServer::new(grpc_adpater);
            Server::builder()
                .add_service(service)
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
            HashSet::from([QuickwitService::Metastore, QuickwitService::Indexer]),
            metastore_service_grpc_addr,
            metastore_service_grpc_addr,
        );
        let searcher_member = ClusterMember::new(
            "2".to_string(),
            0,
            HashSet::from([QuickwitService::Searcher]),
            searcher_grpc_addr,
            searcher_grpc_addr,
        );
        let (members_tx, members_rx) =
            watch::channel::<Vec<ClusterMember>>(vec![metastore_service_member.clone()]);
        let watch_members = WatchStream::new(members_rx);
        let mut metastore_client =
            MetastoreGrpcClient::create_and_update_from_members(1, watch_members)
                .await
                .unwrap();

        assert_eq!(
            metastore_client.uri().to_string(),
            "grpc://metastore.service.cluster:1"
        );
        // gRPC service should send request on the running server.
        metastore_client.pool_size_rx.changed().await.unwrap();
        assert_eq!(*metastore_client.pool_size_rx.borrow(), 1);
        metastore_client.check_connectivity().await.unwrap();
        metastore_client.index_metadata(index_id).await.unwrap();

        // Send empty vec to signal that there is no more control plane in the cluster.
        members_tx.send(Vec::new()).unwrap();
        metastore_client.pool_size_rx.changed().await.unwrap();
        assert_eq!(*metastore_client.pool_size_rx.borrow(), 0);
        metastore_client.check_connectivity().await.unwrap_err();
        let error = metastore_client.index_metadata(index_id).await.unwrap_err();
        assert!(
            matches!(error, MetastoreError::ConnectionError { message } if message.starts_with("gRPC request timeout triggered by the channel timeout"))
        );

        // Send control plan member and check it's working again.
        members_tx
            .send(vec![metastore_service_member.clone()])
            .unwrap();
        metastore_client.index_metadata(index_id).await.unwrap();

        // Send searcher member only.
        members_tx.send(vec![searcher_member.clone()]).unwrap();
        metastore_client.index_metadata(index_id).await.unwrap_err();

        // Send control plane + searcher members.
        members_tx
            .send(vec![metastore_service_member, searcher_member])
            .unwrap();
        metastore_client.index_metadata(index_id).await.unwrap();
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
        let metastore = Arc::new(metastore);

        let grpc_port_1 = quickwit_common::net::find_available_tcp_port()?;
        let grpc_addr_1: SocketAddr = ([127, 0, 0, 1], grpc_port_1).into();
        let grpc_addr_2: SocketAddr = ([127, 0, 0, 1], 1234).into();
        let grpc_addr_3: SocketAddr = ([127, 0, 0, 1], 4567).into();

        // Only start metastore member 1.
        start_grpc_server(grpc_addr_1, metastore.clone()).await?;

        let metastore_member_1 = ClusterMember::new(
            "1".to_string(),
            0,
            HashSet::from([QuickwitService::Metastore]),
            grpc_addr_1,
            grpc_addr_1,
        );
        let metastore_member_2 = ClusterMember::new(
            "2".to_string(),
            0,
            HashSet::from([QuickwitService::Metastore]),
            grpc_addr_2,
            grpc_addr_2,
        );
        let metastore_member_3 = ClusterMember::new(
            "3".to_string(),
            0,
            HashSet::from([QuickwitService::Metastore]),
            grpc_addr_3,
            grpc_addr_3,
        );
        let (members_tx, members_rx) =
            watch::channel::<Vec<ClusterMember>>(vec![metastore_member_1.clone()]);
        let watch_members = WatchStream::new(members_rx);
        let mut metastore_client =
            MetastoreGrpcClient::create_and_update_from_members(1, watch_members)
                .await
                .unwrap();

        metastore_client.pool_size_rx.changed().await.unwrap();
        assert_eq!(*metastore_client.pool_size_rx.borrow(), 1);
        metastore_client.index_metadata(index_id).await.unwrap();

        // Send two unavailable metastore members.
        members_tx
            .send(vec![metastore_member_2, metastore_member_3])
            .unwrap();

        metastore_client.pool_size_rx.changed().await.unwrap();
        assert_eq!(*metastore_client.pool_size_rx.borrow(), 2);

        let error = metastore_client.index_metadata(index_id).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("tcp connect error: Connection refused"));

        // Send running metastore member.
        members_tx.send(vec![metastore_member_1]).unwrap();

        metastore_client.pool_size_rx.changed().await.unwrap();
        assert_eq!(*metastore_client.pool_size_rx.borrow(), 1);
        metastore_client.index_metadata(index_id).await.unwrap();
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

        metastore_client.index_metadata(index_id).await.unwrap();
        metastore_client.list_all_splits(index_id).await.unwrap();
        Ok(())
    }
}
