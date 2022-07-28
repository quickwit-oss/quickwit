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

use std::net::SocketAddr;
use std::ops::Range;
use std::sync::Arc;

use http::Uri;
use quickwit_cluster::{Cluster, QuickwitService};
use quickwit_metastore::checkpoint::CheckpointDelta;
use quickwit_metastore::{IndexMetadata, Split, SplitMetadata, SplitState};
use quickwit_proto::{
    tonic, IndexMetadataRequest, ListAllSplitsRequest, ListIndexesMetadatasRequest,
    ListSplitsRequest, MarkSplitsForDeletionRequest, PublishSplitsRequest, ReplaceSplitsRequest,
    StageSplitRequest,
};
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use tower::discover::Change;
use tower::service_fn;

use crate::error::IndexManagementError;

/// Create an [`IndexManagementClient`] with SocketAddr as an argument.
/// It will try to reconnect to the node automatically.
pub async fn create_index_management_client(
    grpc_addr: SocketAddr,
) -> anyhow::Result<IndexManagementClient> {
    let uri = Uri::builder()
        .scheme("http")
        .authority(grpc_addr.to_string().as_str())
        .path_and_query("/")
        .build()?;
    // Create a channel with connect_lazy to automatically reconnect to the node.
    let channel = Endpoint::from(uri).connect_lazy();
    let client = IndexManagementClient::from_grpc_client(
        quickwit_proto::index_management_service_client::IndexManagementServiceClient::new(channel),
    );
    Ok(client)
}

fn create_grpc_endpoint(grpc_addr: SocketAddr) -> anyhow::Result<Endpoint> {
    let uri = Uri::builder()
        .scheme("http")
        .authority(grpc_addr.to_string().as_str())
        .path_and_query("/")
        .build()?;
    Ok(Endpoint::from(uri))
}

/// Parse tonic error and returns `IndexServiceError`.
pub fn parse_grpc_error(grpc_error: &tonic::Status) -> IndexManagementError {
    // TODO: parse correctly IndexServiceError message.
    IndexManagementError::InternalError(grpc_error.message().to_string())
}

/// IndexManagementService gRPC client.
#[derive(Clone)]
pub struct IndexManagementClient {
    grpc_client:
        quickwit_proto::index_management_service_client::IndexManagementServiceClient<Channel>,
}

/// Sends endpoint changes in the `channel_rx` and udpates `current_grpc_address_in_use`
/// if some change are detected with the provided `members_grpc_addresses`. The applied rules are:
/// - if `members_grpc_addresses` is empty => remove
/// - if there is at least one address in `members_grpc_addresses` => take the first one and update
///   if necessary `current_grpc_address_in_use` and send Insert/Remove events to the channel.
async fn update_client_grpc_address_if_needed(
    members_grpc_addresses: &[SocketAddr],
    current_grpc_address_in_use: &mut Option<SocketAddr>,
    channel_rx: &Sender<Change<SocketAddr, Endpoint>>,
) -> anyhow::Result<()> {
    if members_grpc_addresses.len() < 1 {
        tracing::error!("No Control Plane service is available in the cluster.");
        if let Some(grpc_address) = current_grpc_address_in_use.take() {
            tracing::debug!("Removing outdated grpc address from `IndexManagementClient`.");
            channel_rx.send(Change::Remove(grpc_address)).await?;
        }
    } else if members_grpc_addresses.len() >= 1 {
        if members_grpc_addresses.len() == 2 {
            tracing::error!(
                "There is more than one control plane service. `IndexManagementClient` will use \
                 the first in the list."
            );
        }
        if let Ok(endpoint) = create_grpc_endpoint(members_grpc_addresses[0]) {
            if let Some(current_grpc_address) = current_grpc_address_in_use {
                if current_grpc_address.to_string() != members_grpc_addresses[0].to_string() {
                    channel_rx
                        .send(Change::Remove(current_grpc_address.clone()))
                        .await?;
                    tracing::info!(
                        "Add endpoint with gRPC address `{}` from `IndexManagementClient`.",
                        members_grpc_addresses[0]
                    );
                    channel_rx
                        .send(Change::Insert(members_grpc_addresses[0], endpoint))
                        .await?;
                    *current_grpc_address_in_use = Some(members_grpc_addresses[0]);
                }
            } else {
                tracing::info!(
                    "Add endpoint with gRPC address `{}` from `IndexManagementClient`.",
                    members_grpc_addresses[0]
                );
                channel_rx
                    .send(Change::Insert(members_grpc_addresses[0], endpoint))
                    .await?;
                *current_grpc_address_in_use = Some(members_grpc_addresses[0]);
            }
        } else {
            tracing::error!(
                "Cannot create an endpoint with gRPC address `{}`.",
                members_grpc_addresses[0]
            );
        }
    }
    Ok(())
}

impl IndexManagementClient {
    /// Creates an [`IndexManagementClient`] and monitor in the background Control Plane gRPC
    /// address changes to dynamically update the client's gRPC address.
    pub async fn create_and_update_from_cluster(cluster: Arc<Cluster>) -> anyhow::Result<Self> {
        let (channel, channel_rx) = Channel::balance_channel(1);
        // Keep the currently used gRPC address to track changes and potentially update the channel.
        let mut current_grpc_address_in_use: Option<SocketAddr> = None;
        let members_grpc_addresses = cluster
            .members_grpc_addresses_for_service(QuickwitService::ControlPlane)
            .await?;
        update_client_grpc_address_if_needed(
            &members_grpc_addresses,
            &mut current_grpc_address_in_use,
            &channel_rx,
        )
        .await?;

        // Watch for cluster members changes and dynamically update channel endpoint.
        let mut members_watch_channel = cluster.member_change_watcher();
        tokio::spawn(async move {
            while (members_watch_channel.next().await).is_some() {
                if let Ok(members_grpc_addresses) = cluster
                    .members_grpc_addresses_for_service(QuickwitService::ControlPlane)
                    .await
                {
                    update_client_grpc_address_if_needed(
                        &members_grpc_addresses,
                        &mut current_grpc_address_in_use,
                        &channel_rx,
                    )
                    .await?;
                } else {
                    tracing::error!(
                        "Cannot update `IndexManagementClient` gRPC address: an error happens on \
                         retrieving gRPC members addresses from cluster."
                    );
                }
            }
            Result::<(), anyhow::Error>::Ok(())
        });

        Ok(Self {
            grpc_client:
                quickwit_proto::index_management_service_client::IndexManagementServiceClient::new(
                    channel,
                ),
        })
    }

    /// Creates an [`IndexManagementClient`] from a gRPC tonic client.
    pub fn from_grpc_client(
        client: quickwit_proto::index_management_service_client::IndexManagementServiceClient<
            Channel,
        >,
    ) -> Self {
        Self {
            grpc_client: client,
        }
    }

    /// Creates an [`IndexManagementClient`] from a duplex stream client for testing purpose.
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
        Ok(Self {
            grpc_client:
                quickwit_proto::index_management_service_client::IndexManagementServiceClient::new(
                    channel,
                ),
        })
    }

    /// Gets index metadata.
    pub async fn index_metadata(
        &mut self,
        index_id: &str,
    ) -> Result<IndexMetadata, IndexManagementError> {
        let tonic_request = Request::new(IndexMetadataRequest {
            index_id: index_id.to_string(),
        });
        let tonic_response = self
            .grpc_client
            .index_metadata(tonic_request)
            .await
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        let index_metadata =
            serde_json::from_str(&tonic_response.into_inner().index_metadata_serialized_json)
                .map_err(|error| IndexManagementError::InternalError(error.to_string()))?;
        Ok(index_metadata)
    }

    /// Gets all indexes metadatas.
    pub async fn list_indexes_metadatas(
        &mut self,
    ) -> Result<Vec<IndexMetadata>, IndexManagementError> {
        let tonic_request = Request::new(ListIndexesMetadatasRequest {});
        let tonic_response = self
            .grpc_client
            .list_indexes_metadatas(tonic_request)
            .await
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        let indexes_metadatas = serde_json::from_str(
            &tonic_response
                .into_inner()
                .indexes_metadatas_serialized_json,
        )
        .map_err(|error| IndexManagementError::InternalError(error.to_string()))?;
        Ok(indexes_metadatas)
    }

    /// Gets all splits.
    pub async fn list_all_splits(
        &mut self,
        index_id: &str,
    ) -> Result<Vec<SplitMetadata>, IndexManagementError> {
        let tonic_request = Request::new(ListAllSplitsRequest {
            index_id: index_id.to_string(),
        });
        let tonic_response = self
            .grpc_client
            .list_all_splits(tonic_request)
            .await
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        let splits_metadatas =
            serde_json::from_str(&tonic_response.into_inner().splits_serialized_json)
                .map_err(|error| IndexManagementError::InternalError(error.to_string()))?;
        Ok(splits_metadatas)
    }

    /// Gets splits.
    pub async fn list_splits(
        &mut self,
        index_id: &str,
        split_state: SplitState,
        _time_range: Option<Range<i64>>,
    ) -> Result<Vec<Split>, IndexManagementError> {
        // TODO: add tags filter, range.
        let tonic_request = Request::new(ListSplitsRequest {
            index_id: index_id.to_string(),
            split_state: split_state.to_string(),
            time_range_start: None,
            time_range_end: None,
            tags_serialized_json: None,
        });
        let tonic_response = self
            .grpc_client
            .list_splits(tonic_request)
            .await
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        let splits = serde_json::from_str(&tonic_response.into_inner().splits_serialized_json)
            .map_err(|error| IndexManagementError::InternalError(error.to_string()))?;
        Ok(splits)
    }

    /// Stages split.
    pub async fn stage_split(
        &mut self,
        inded_id: &str,
        split_metadata: &SplitMetadata,
    ) -> Result<(), IndexManagementError> {
        let split_metadata_serialized_json = serde_json::to_string(split_metadata)
            .map_err(|error| IndexManagementError::InternalError(error.to_string()))?;
        let tonic_request = Request::new(StageSplitRequest {
            index_id: inded_id.to_string(),
            split_metadata_serialized_json,
        });
        let _ = self
            .grpc_client
            .stage_split(tonic_request)
            .await
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    /// Publishes splits.
    pub async fn publish_splits(
        &mut self,
        index_id: String,
        source_id: String,
        split_ids: Vec<String>,
        checkpoint_delta: CheckpointDelta,
    ) -> Result<(), IndexManagementError> {
        let checkpoint_delta_serialized_json = serde_json::to_string(&checkpoint_delta)
            .map_err(|error| IndexManagementError::InternalError(error.to_string()))?;
        let tonic_request = Request::new(PublishSplitsRequest {
            index_id,
            source_id,
            split_ids,
            checkpoint_delta_serialized_json,
        });
        let _ = self
            .grpc_client
            .publish_splits(tonic_request)
            .await
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    /// Replaces splits.
    pub async fn replace_splits(
        &mut self,
        index_id: String,
        new_split_ids: Vec<String>,
        replaced_split_ids: Vec<String>,
    ) -> Result<(), IndexManagementError> {
        let tonic_request = Request::new(ReplaceSplitsRequest {
            index_id,
            new_split_ids,
            replaced_split_ids,
        });
        let _ = self
            .grpc_client
            .replace_splits(tonic_request)
            .await
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }

    /// Stages split.
    pub async fn mark_splits_for_deletion(
        &mut self,
        index_id: String,
        split_ids: Vec<String>,
    ) -> Result<(), IndexManagementError> {
        let tonic_request = Request::new(MarkSplitsForDeletionRequest {
            index_id,
            split_ids,
        });
        let _ = self
            .grpc_client
            .mark_splits_for_deletion(tonic_request)
            .await
            .map_err(|tonic_error| parse_grpc_error(&tonic_error))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_actors::Universe;
    use quickwit_metastore::{IndexMetadata, MockMetastore};

    use crate::test_utils::create_index_management_client_for_test;

    #[tokio::test]
    async fn test_index_management_client() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let mut mock_metastore = MockMetastore::default();
        mock_metastore.expect_index_metadata().returning(move |_| {
            Ok(IndexMetadata::for_test(
                "test-index",
                "ram:///indexes/test-index",
            ))
        });
        let universe = Universe::new();
        let mut service_client =
            create_index_management_client_for_test(Arc::new(mock_metastore), &universe).await?;
        let response = service_client.index_metadata("my-index").await;
        assert!(response.is_ok());
        Ok(())
    }
}
