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

use std::error::Error;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use http::Uri;
use itertools::Itertools;
use quickwit_cluster::{Member, QuickwitService};
use quickwit_config::SourceConfig;
use quickwit_metastore::checkpoint::IndexCheckpointDelta;
use quickwit_metastore::{
    IndexMetadata, Metastore, MetastoreError, MetastoreResult, SplitMetadata, SplitState,
};
use quickwit_proto::metastore_api::metastore_api_service_client::MetastoreApiServiceClient;
use quickwit_proto::metastore_api::{
    AddSourceRequest, CreateIndexRequest, CreateIndexResponse, DeleteIndexRequest,
    DeleteIndexResponse, DeleteSourceRequest, DeleteSplitsRequest, IndexMetadataRequest,
    IndexMetadataResponse, ListAllSplitsRequest, ListIndexesMetadatasRequest,
    ListIndexesMetadatasResponse, ListSplitsRequest, ListSplitsResponse,
    MarkSplitsForDeletionRequest, PublishSplitsRequest, SourceResponse, SplitResponse,
    StageSplitRequest,
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
use tracing::{debug, error, info};

const CLIENT_TIMEOUT_DURATION: Duration = if cfg!(test) {
    Duration::from_millis(0)
} else {
    Duration::from_secs(5)
};

/// The [`MetastoreService`] is responsible for executing index CRUD operations
/// and provide two implementations:
/// - a `Local` implementation that directly calls the [`Metastore`] methods.
/// - a `gRPC` implementation that send gRPC requests to the Control Plane on which a `Local`
///   [`MetastoreService`] is running. This inner gRPC client can listen to cluster members changes
///   in order to update the channel endpoint.
///
/// What it does not do currently:
/// - Taking care of deleting splits on the storage, this is currently done either by the garbage
///   collector or by using dedicated functions like `delete_index`.
/// What it will do soon:
/// - The `Local` implementation is meant to send events to the future `IndexPlanner` and at the end
///   informs the different indexers that an index has been created/updated.
#[derive(Clone)]
enum MetastoreServiceImpl {
    Local(Arc<dyn Metastore>),
    Grpc(MetastoreApiServiceClient<Timeout<Channel>>),
}

#[derive(Clone)]
pub struct MetastoreService(MetastoreServiceImpl);

impl MetastoreService {
    pub fn from_metastore(metastore: Arc<dyn Metastore>) -> Self {
        Self(MetastoreServiceImpl::Local(metastore))
    }

    pub fn is_local(&self) -> bool {
        match &self.0 {
            MetastoreServiceImpl::Local(_) => true,
            MetastoreServiceImpl::Grpc(_) => false,
        }
    }

    /// Create a gRPC [`MetastoreService`] that send gRPC requests to the member that provides
    /// a Control Plane service. It listens to cluster members changes to update the channel
    /// endpoint.
    // TODO: in order to update
    pub async fn create_and_update_grpc_service_from_members(
        current_members: &[Member],
        mut members_watch_channel: WatchStream<Vec<Member>>,
    ) -> anyhow::Result<Self> {
        // Create a channel whose endpoint can be updated thanks to a sender.
        // A capacity of 1 is sufficient as we want only one Control Plane endpoint at a given time.
        // It will change in a near future where we will have Control Plane high availability.
        let (channel, channel_tx) = Channel::balance_channel(1);

        // A request on a channel with no endpoint will hang. To avoid a blocking request, a timeout
        // is added to the channel.
        // TODO: ideally, we want to implement our own `Channel::balance_channel` to
        // properly do this job and generate the right errors when there is no control plane.
        let timeout_channel = Timeout::new(channel, CLIENT_TIMEOUT_DURATION);
        let mut current_grpc_address_in_use: Option<SocketAddr> = None;
        update_client_grpc_address(
            &current_members,
            &mut current_grpc_address_in_use,
            &channel_tx,
        )
        .await?;

        // Watch for cluster members changes and dynamically update channel endpoint.
        tokio::spawn(async move {
            while let Some(new_members) = members_watch_channel.next().await {
                update_client_grpc_address(
                    &new_members,
                    &mut current_grpc_address_in_use,
                    &channel_tx,
                )
                .await?; // <- Fails if the channel is closed. In this case we can stop the loop.
            }
            Result::<_, anyhow::Error>::Ok(())
        });

        Ok(Self(MetastoreServiceImpl::Grpc(
            MetastoreApiServiceClient::new(timeout_channel),
        )))
    }

    /// Creates an [`MetastoreService`] from a duplex stream client for testing purpose.
    #[doc(hidden)]
    pub async fn from_duplex_stream(client: tokio::io::DuplexStream) -> anyhow::Result<Self> {
        let mut client = Some(client);
        let channel = Endpoint::try_from("http://test.server")?
            .connect_with_connector(service_fn(move |_: Uri| {
                let client = client.take();
                async move {
                    if let Some(client) = client {
                        Ok(client)
                    } else {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Client already taken",
                        ))
                    }
                }
            }))
            .await?;
        let client = MetastoreApiServiceClient::new(Timeout::new(channel, CLIENT_TIMEOUT_DURATION));
        Ok(Self(MetastoreServiceImpl::Grpc(client)))
    }

    /// Creates an index.
    pub async fn create_index(
        &mut self,
        request: CreateIndexRequest,
    ) -> MetastoreResult<CreateIndexResponse> {
        match &mut self.0 {
            MetastoreServiceImpl::Local(metastore) => {
                let index_metadata: IndexMetadata = serde_json::from_str(
                    &request.index_metadata_serialized_json,
                )
                .map_err(|error| MetastoreError::InternalError {
                    message: "Cannot deserialized incoming `IndexMetadata`.".to_string(),
                    cause: error.to_string(),
                })?;
                metastore.create_index(index_metadata).await?;
                Ok(CreateIndexResponse {})
            }
            MetastoreServiceImpl::Grpc(client) => client
                .create_index(request)
                .await
                .map(|tonic_response| tonic_response.into_inner())
                .map_err(|tonic_error| parse_grpc_error(&tonic_error)),
        }
    }

    /// List indexes.
    pub async fn list_indexes_metadatas(
        &mut self,
        request: ListIndexesMetadatasRequest,
    ) -> MetastoreResult<ListIndexesMetadatasResponse> {
        match &mut self.0 {
            MetastoreServiceImpl::Local(metastore) => {
                let indexes_metadatas = metastore.list_indexes_metadatas().await?;
                let indexes_metadatas_serialized_json = serde_json::to_string(&indexes_metadatas)
                    .map_err(|error| {
                    MetastoreError::InternalError {
                        message: "Cannot serialized `IndexMetadata`s returned by the metastore."
                            .to_string(),
                        cause: error.to_string(),
                    }
                })?;
                Ok(ListIndexesMetadatasResponse {
                    indexes_metadatas_serialized_json,
                })
            }
            MetastoreServiceImpl::Grpc(client) => client
                .list_indexes_metadatas(request)
                .await
                .map(|tonic_response| tonic_response.into_inner())
                .map_err(|tonic_error| parse_grpc_error(&tonic_error)),
        }
    }

    /// Returns the [`IndexMetadata`] for a given index.
    pub async fn index_metadata(
        &mut self,
        request: IndexMetadataRequest,
    ) -> MetastoreResult<IndexMetadataResponse> {
        match &mut self.0 {
            MetastoreServiceImpl::Local(metastore) => {
                let index_metadata = metastore.index_metadata(&request.index_id).await?;
                let index_metadata_serialized_json = serde_json::to_string(&index_metadata)
                    .map_err(|error| MetastoreError::InternalError {
                        message: "Cannot serialized `IndexMetadata` returned by the metastore."
                            .to_string(),
                        cause: error.to_string(),
                    })?;
                Ok(IndexMetadataResponse {
                    index_metadata_serialized_json,
                })
            }
            MetastoreServiceImpl::Grpc(client) => client
                .index_metadata(request)
                .await
                .map(|tonic_response| tonic_response.into_inner())
                .map_err(|tonic_error| parse_grpc_error(&tonic_error)),
        }
    }

    /// Deletes an index.
    pub async fn delete_index(
        &mut self,
        request: DeleteIndexRequest,
    ) -> MetastoreResult<DeleteIndexResponse> {
        match &mut self.0 {
            MetastoreServiceImpl::Local(metastore) => {
                metastore.delete_index(&request.index_id).await?;
                Ok(DeleteIndexResponse {})
            }
            MetastoreServiceImpl::Grpc(client) => client
                .delete_index(request)
                .await
                .map(|tonic_response| tonic_response.into_inner())
                .map_err(|tonic_error| parse_grpc_error(&tonic_error)),
        }
    }

    /// Stages a split.
    pub async fn stage_split(
        &mut self,
        request: StageSplitRequest,
    ) -> MetastoreResult<SplitResponse> {
        match &mut self.0 {
            MetastoreServiceImpl::Local(metastore) => {
                let split_metadata: SplitMetadata = serde_json::from_str(
                    &request.split_metadata_serialized_json,
                )
                .map_err(|error| MetastoreError::InternalError {
                    message: "Cannot deserialized incoming `SplitMetadata`.".to_string(),
                    cause: error.to_string(),
                })?;
                metastore
                    .stage_split(&request.index_id, split_metadata)
                    .await?;
                Ok(SplitResponse {})
            }
            MetastoreServiceImpl::Grpc(client) => client
                .stage_split(request)
                .await
                .map(|tonic_response| tonic_response.into_inner())
                .map_err(|tonic_error| parse_grpc_error(&tonic_error)),
        }
    }

    /// Publishes a list of splits.
    pub async fn publish_splits(
        &mut self,
        request: PublishSplitsRequest,
    ) -> MetastoreResult<SplitResponse> {
        match &mut self.0 {
            MetastoreServiceImpl::Local(metastore) => {
                let index_checkpoint_delta_opt = request
                    .index_checkpoint_delta_serialized_json
                    .map(|value| serde_json::from_str::<IndexCheckpointDelta>(&value))
                    .transpose()
                    .map_err(|error| MetastoreError::InternalError {
                        message: "Cannot deserialized incoming `CheckpointDelta`.".to_string(),
                        cause: error.to_string(),
                    })?;
                let split_ids = request
                    .split_ids
                    .iter()
                    .map(|split_id| split_id.as_str())
                    .collect_vec();
                let replaced_split_ids = request
                    .replaced_split_ids
                    .iter()
                    .map(|split_id| split_id.as_str())
                    .collect_vec();
                metastore
                    .publish_splits(
                        &request.index_id,
                        &split_ids,
                        &replaced_split_ids,
                        index_checkpoint_delta_opt,
                    )
                    .await?;
                Ok(SplitResponse {})
            }
            MetastoreServiceImpl::Grpc(client) => client
                .publish_splits(request)
                .await
                .map(|tonic_response| tonic_response.into_inner())
                .map_err(|tonic_error| parse_grpc_error(&tonic_error)),
        }
    }

    /// Lists the splits.
    pub async fn list_splits(
        &mut self,
        request: ListSplitsRequest,
    ) -> MetastoreResult<ListSplitsResponse> {
        match &mut self.0 {
            MetastoreServiceImpl::Local(metastore) => {
                let split_state = SplitState::from_str(&request.split_state).map_err(|cause| {
                    MetastoreError::InternalError {
                        message: "Cannot deserialized incoming `SplitState`.".to_string(),
                        cause,
                    }
                })?;
                // TODO: add time range and tags.
                let splits = metastore
                    .list_splits(&request.index_id, split_state, None, None)
                    .await?;
                let splits_serialized_json = serde_json::to_string(&splits).map_err(|error| {
                    MetastoreError::InternalError {
                        message: "Cannot serialized `Vec<Split>` returned by the metastore."
                            .to_string(),
                        cause: error.to_string(),
                    }
                })?;
                Ok(ListSplitsResponse {
                    splits_serialized_json,
                })
            }
            MetastoreServiceImpl::Grpc(client) => client
                .list_splits(request)
                .await
                .map(|tonic_response| tonic_response.into_inner())
                .map_err(|tonic_error| parse_grpc_error(&tonic_error)),
        }
    }

    /// Lists all the splits without filtering.
    pub async fn list_all_splits(
        &mut self,
        request: ListAllSplitsRequest,
    ) -> MetastoreResult<ListSplitsResponse> {
        match &mut self.0 {
            MetastoreServiceImpl::Local(metastore) => {
                let splits = metastore.list_all_splits(&request.index_id).await?;
                let splits_serialized_json = serde_json::to_string(&splits).map_err(|error| {
                    MetastoreError::InternalError {
                        message: "Cannot serialized `Vec<Split>` returned by the metastore."
                            .to_string(),
                        cause: error.to_string(),
                    }
                })?;
                Ok(ListSplitsResponse {
                    splits_serialized_json,
                })
            }
            MetastoreServiceImpl::Grpc(client) => client
                .list_all_splits(request)
                .await
                .map(|tonic_response| tonic_response.into_inner())
                .map_err(|tonic_error| parse_grpc_error(&tonic_error)),
        }
    }

    /// Marks a list of splits for deletion.
    pub async fn mark_splits_for_deletion(
        &mut self,
        request: MarkSplitsForDeletionRequest,
    ) -> MetastoreResult<SplitResponse> {
        match &mut self.0 {
            MetastoreServiceImpl::Local(metastore) => {
                let split_ids = request
                    .split_ids
                    .iter()
                    .map(|split_id| split_id.as_str())
                    .collect_vec();
                metastore
                    .mark_splits_for_deletion(&request.index_id, &split_ids)
                    .await?;
                Ok(SplitResponse {})
            }
            MetastoreServiceImpl::Grpc(client) => client
                .mark_splits_for_deletion(request)
                .await
                .map(|tonic_response| tonic_response.into_inner())
                .map_err(|tonic_error| parse_grpc_error(&tonic_error)),
        }
    }

    /// Deletes a list of splits.
    pub async fn delete_splits(
        &mut self,
        request: DeleteSplitsRequest,
    ) -> MetastoreResult<SplitResponse> {
        match &mut self.0 {
            MetastoreServiceImpl::Local(metastore) => {
                let split_ids = request
                    .split_ids
                    .iter()
                    .map(|split_id| split_id.as_str())
                    .collect_vec();
                metastore
                    .delete_splits(&request.index_id, &split_ids)
                    .await?;
                Ok(SplitResponse {})
            }
            MetastoreServiceImpl::Grpc(client) => client
                .delete_splits(request)
                .await
                .map(|tonic_response| tonic_response.into_inner())
                .map_err(|tonic_error| parse_grpc_error(&tonic_error)),
        }
    }

    /// Adds a source to a given index.
    pub async fn add_source(
        &mut self,
        request: AddSourceRequest,
    ) -> MetastoreResult<SourceResponse> {
        match &mut self.0 {
            MetastoreServiceImpl::Local(metastore) => {
                let source_config: SourceConfig = serde_json::from_str(
                    &request.source_config_serialized_json,
                )
                .map_err(|error| MetastoreError::InternalError {
                    message: "Cannot deserialized incoming `SourceConfig`.".to_string(),
                    cause: error.to_string(),
                })?;
                metastore
                    .add_source(&request.index_id, source_config)
                    .await?;
                Ok(SourceResponse {})
            }
            MetastoreServiceImpl::Grpc(client) => client
                .add_source(request)
                .await
                .map(|tonic_response| tonic_response.into_inner())
                .map_err(|tonic_error| parse_grpc_error(&tonic_error)),
        }
    }

    /// Removes a source from a given index.
    pub async fn delete_source(
        &mut self,
        request: DeleteSourceRequest,
    ) -> MetastoreResult<SourceResponse> {
        match &mut self.0 {
            MetastoreServiceImpl::Local(metastore) => {
                metastore
                    .delete_source(&request.index_id, &request.source_id)
                    .await?;
                Ok(SourceResponse {})
            }
            MetastoreServiceImpl::Grpc(client) => client
                .delete_source(request)
                .await
                .map(|tonic_response| tonic_response.into_inner())
                .map_err(|tonic_error| parse_grpc_error(&tonic_error)),
        }
    }
}

/// Updates `current_grpc_address_opt` gRPC address by:
/// - Selecting the first control plane member ordered by socket address. Note that today, we expect
///   to have only one Control Plane.
/// - If the selected socket address is different from the current one:
///   - Updating `current_grpc_address_opt`.
///   - Sending the endpoint with the updated gRPC address to `endpoint_channel_rx`.
async fn update_client_grpc_address(
    members: &[Member],
    current_grpc_address_opt: &mut Option<SocketAddr>,
    endpoint_channel_rx: &Sender<Change<SocketAddr, Endpoint>>,
) -> anyhow::Result<()> {
    let mut members_grpc_addresses = members
        .iter()
        .filter(|member| {
            member
                .available_services
                .contains(&QuickwitService::ControlPlane)
        })
        .map(|member| member.grpc_address)
        .collect_vec();

    if members_grpc_addresses.is_empty() {
        error!("No Control Plane service is available in the cluster.");
        if let Some(grpc_address) = current_grpc_address_opt.take() {
            debug!("Removing outdated grpc address from `MetastoreService`.");
            endpoint_channel_rx
                .send(Change::Remove(grpc_address))
                .await?;
        }
        return Ok(());
    }

    // TODO: should we consider that more than one control plane is
    // a very bad situation and forbid any request to a Control Plane node?
    if members_grpc_addresses.len() > 1 {
        error!(
            "There is more than one Control Plane service members,
             this is not currently supported. Only the first ordered
             by gRPC address will be used."
        );
    }

    // Sort addresses in order to be consistent when choosing the first one.
    members_grpc_addresses.sort();
    let new_grpc_address = members_grpc_addresses[0];
    let new_grpc_uri = Uri::builder()
        .scheme("http")
        .authority(new_grpc_address.to_string().as_str())
        .path_and_query("/")
        .build()?;
    let new_grpc_endpoint = Endpoint::from(new_grpc_uri);

    if let Some(current_grpc_address) = current_grpc_address_opt {
        // gRPC address has changed.
        if current_grpc_address.to_string() != new_grpc_address.to_string() {
            info!(
                "Update `MetastoreService` client's gRPC address `{}`.",
                new_grpc_address
            );
            endpoint_channel_rx
                .send(Change::Remove(*current_grpc_address))
                .await?;
            endpoint_channel_rx
                .send(Change::Insert(new_grpc_address, new_grpc_endpoint))
                .await?;
            *current_grpc_address_opt = Some(new_grpc_address);
        }
    } else {
        info!(
            "Add new gRPC address `{}` to `MetastoreService` client.",
            new_grpc_address
        );
        endpoint_channel_rx
            .send(Change::Insert(new_grpc_address, new_grpc_endpoint))
            .await?;
        *current_grpc_address_opt = Some(new_grpc_address);
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
