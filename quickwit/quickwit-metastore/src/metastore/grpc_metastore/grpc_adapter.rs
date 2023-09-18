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

use async_trait::async_trait;
use itertools::Itertools;
use quickwit_config::IndexConfig;
use quickwit_proto::metastore::{
    serde_utils as metastore_serde_utils, AcquireShardsRequest, AcquireShardsResponse,
    AddSourceRequest, CloseShardsRequest, CloseShardsResponse, CreateIndexRequest,
    CreateIndexResponse, DeleteIndexRequest, DeleteQuery, DeleteShardsRequest,
    DeleteShardsResponse, DeleteSourceRequest, DeleteSplitsRequest, DeleteTask, EmptyResponse,
    IndexMetadataRequest, IndexMetadataResponse, LastDeleteOpstampRequest,
    LastDeleteOpstampResponse, ListAllSplitsRequest, ListDeleteTasksRequest,
    ListDeleteTasksResponse, ListIndexesMetadatasRequest, ListIndexesMetadatasResponse,
    ListShardsRequest, ListShardsResponse, ListSplitsRequest, ListSplitsResponse,
    ListStaleSplitsRequest, MarkSplitsForDeletionRequest, MetastoreError, MetastoreService,
    OpenShardsRequest, OpenShardsResponse, PublishSplitsRequest, ResetSourceCheckpointRequest,
    StageSplitsRequest, ToggleSourceRequest, UpdateSplitsDeleteOpstampRequest,
    UpdateSplitsDeleteOpstampResponse,
};
use quickwit_proto::tonic::{Request, Response, Status};
use quickwit_proto::{set_parent_span_from_request_metadata, tonic};
use tracing::instrument;

use crate::{ListSplitsQuery, Metastore};

#[allow(missing_docs)]
#[derive(Clone)]
pub struct GrpcMetastoreAdapter(Arc<dyn Metastore>);

impl From<Arc<dyn Metastore>> for GrpcMetastoreAdapter {
    fn from(metastore: Arc<dyn Metastore>) -> Self {
        Self(metastore)
    }
}

#[async_trait]
impl MetastoreService for GrpcMetastoreAdapter {
    #[instrument(skip(self, request))]
    async fn create_index(
        &self,
        request: tonic::Request<CreateIndexRequest>,
    ) -> Result<tonic::Response<CreateIndexResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let create_index_request = request.into_inner();
        let index_config = serde_json::from_str::<IndexConfig>(
            &create_index_request.index_config_json,
        )
        .map_err(|error| MetastoreError::JsonDeserializeError {
            struct_name: "IndexConfig".to_string(),
            message: error.to_string(),
        })?;
        let create_index_reply =
            self.0
                .create_index(index_config)
                .await
                .map(|index_uid| CreateIndexResponse {
                    index_uid: index_uid.to_string(),
                })?;
        Ok(tonic::Response::new(create_index_reply))
    }

    #[instrument(skip(self, request))]
    async fn index_metadata(
        &self,
        request: tonic::Request<IndexMetadataRequest>,
    ) -> Result<tonic::Response<IndexMetadataResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let index_metadata_request = request.into_inner();
        let index_metadata = self
            .0
            .index_metadata(&index_metadata_request.index_id)
            .await?;
        let index_metadata_reply = serde_json::to_string(&index_metadata)
            .map(|index_metadata_serialized_json| IndexMetadataResponse {
                index_metadata_serialized_json,
            })
            .map_err(|error| MetastoreError::JsonSerializeError {
                struct_name: "IndexMetadata".to_string(),
                message: error.to_string(),
            })?;
        Ok(tonic::Response::new(index_metadata_reply))
    }

    #[instrument(skip(self, request))]
    async fn list_indexes_metadatas(
        &self,
        request: tonic::Request<ListIndexesMetadatasRequest>,
    ) -> Result<tonic::Response<ListIndexesMetadatasResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let query = serde_json::from_str(&request.into_inner().query_json).map_err(|error| {
            MetastoreError::JsonSerializeError {
                struct_name: "ListIndexesQuery".to_string(),
                message: error.to_string(),
            }
        })?;
        let indexes_metadatas = self.0.list_indexes_metadatas(query).await?;
        let list_indexes_metadatas_reply = serde_json::to_string(&indexes_metadatas)
            .map(
                |indexes_metadatas_serialized_json| ListIndexesMetadatasResponse {
                    indexes_metadatas_serialized_json,
                },
            )
            .map_err(|error| MetastoreError::JsonSerializeError {
                struct_name: "Vec<IndexMetadata>".to_string(),
                message: error.to_string(),
            })?;
        Ok(tonic::Response::new(list_indexes_metadatas_reply))
    }

    #[instrument(skip(self, request))]
    async fn delete_index(
        &self,
        request: tonic::Request<DeleteIndexRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let delete_request = request.into_inner();
        let delete_reply = self
            .0
            .delete_index(delete_request.index_uid.into())
            .await
            .map(|_| EmptyResponse {})?;
        Ok(tonic::Response::new(delete_reply))
    }

    #[instrument(skip(self, request))]
    async fn list_all_splits(
        &self,
        request: tonic::Request<ListAllSplitsRequest>,
    ) -> Result<tonic::Response<ListSplitsResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let list_all_splits_request = request.into_inner();
        let splits = self
            .0
            .list_all_splits(list_all_splits_request.index_uid.into())
            .await?;
        let list_all_splits_reply = serde_json::to_string(&splits)
            .map(|splits_serialized_json| ListSplitsResponse {
                splits_serialized_json,
            })
            .map_err(|error| MetastoreError::JsonSerializeError {
                struct_name: "Vec<Split>".to_string(),
                message: error.to_string(),
            })?;
        Ok(tonic::Response::new(list_all_splits_reply))
    }

    #[instrument(skip(self, request))]
    async fn list_splits(
        &self,
        request: tonic::Request<ListSplitsRequest>,
    ) -> Result<tonic::Response<ListSplitsResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let list_splits_request = request.into_inner();
        let query: ListSplitsQuery = serde_json::from_str(&list_splits_request.query_json)
            .map_err(|error| MetastoreError::JsonDeserializeError {
                struct_name: "ListSplitsQuery".to_string(),
                message: error.to_string(),
            })?;

        let splits = self.0.list_splits(query).await?;
        let list_splits_reply = serde_json::to_string(&splits)
            .map(|splits_serialized_json| ListSplitsResponse {
                splits_serialized_json,
            })
            .map_err(|error| MetastoreError::JsonSerializeError {
                struct_name: "Vec<Split>".to_string(),
                message: error.to_string(),
            })?;
        Ok(tonic::Response::new(list_splits_reply))
    }

    #[instrument(skip(self, request))]
    async fn stage_splits(
        &self,
        request: Request<StageSplitsRequest>,
    ) -> Result<Response<EmptyResponse>, Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let stage_split_request = request.into_inner();
        let split_metadata_list =
            serde_json::from_str(&stage_split_request.split_metadata_list_serialized_json)
                .map_err(|error| MetastoreError::JsonDeserializeError {
                    struct_name: "Vec<SplitMetadata>".to_string(),
                    message: error.to_string(),
                })?;
        self.0
            .stage_splits(stage_split_request.index_uid.into(), split_metadata_list)
            .await?;
        Ok(tonic::Response::new(EmptyResponse {}))
    }

    #[instrument(skip(self, request))]
    async fn publish_splits(
        &self,
        request: tonic::Request<PublishSplitsRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let publish_request = request.into_inner();
        let split_ids = publish_request
            .staged_split_ids
            .iter()
            .map(|split_id| split_id.as_str())
            .collect_vec();
        let replaced_split_ids = publish_request
            .replaced_split_ids
            .iter()
            .map(|split_id| split_id.as_str())
            .collect_vec();
        let checkpoint_delta_opt = publish_request
            .index_checkpoint_delta_json_opt
            .as_deref()
            .map(metastore_serde_utils::from_json_str)
            .transpose()?;
        let publish_splits_reply = self
            .0
            .publish_splits(
                publish_request.index_uid.into(),
                &split_ids,
                &replaced_split_ids,
                checkpoint_delta_opt,
                publish_request.publish_token_opt,
            )
            .await
            .map(|_| EmptyResponse {})?;
        Ok(tonic::Response::new(publish_splits_reply))
    }

    #[instrument(skip(self, request))]
    async fn mark_splits_for_deletion(
        &self,
        request: tonic::Request<MarkSplitsForDeletionRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let mark_splits_for_deletion_request = request.into_inner();
        let split_ids = mark_splits_for_deletion_request
            .split_ids
            .iter()
            .map(|split_id| split_id.as_str())
            .collect_vec();
        let mark_splits_for_deletion_reply = self
            .0
            .mark_splits_for_deletion(
                mark_splits_for_deletion_request.index_uid.into(),
                &split_ids,
            )
            .await
            .map(|_| EmptyResponse {})?;
        Ok(tonic::Response::new(mark_splits_for_deletion_reply))
    }

    #[instrument(skip(self, request))]
    async fn delete_splits(
        &self,
        request: tonic::Request<DeleteSplitsRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let delete_splits_request = request.into_inner();
        let split_ids = delete_splits_request
            .split_ids
            .iter()
            .map(|split_id| split_id.as_str())
            .collect_vec();
        let delete_splits_reply = self
            .0
            .delete_splits(delete_splits_request.index_uid.into(), &split_ids)
            .await
            .map(|_| EmptyResponse {})?;
        Ok(tonic::Response::new(delete_splits_reply))
    }

    #[instrument(skip(self, request))]
    async fn add_source(
        &self,
        request: tonic::Request<AddSourceRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let add_source_request = request.into_inner();
        let source_config =
            serde_json::from_str(&add_source_request.source_config_json).map_err(|error| {
                MetastoreError::JsonDeserializeError {
                    struct_name: "SourceConfig".to_string(),
                    message: error.to_string(),
                }
            })?;
        let add_source_reply = self
            .0
            .add_source(add_source_request.index_uid.into(), source_config)
            .await
            .map(|_| EmptyResponse {})?;
        Ok(tonic::Response::new(add_source_reply))
    }

    #[instrument(skip(self, request))]
    async fn toggle_source(
        &self,
        request: tonic::Request<ToggleSourceRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let toggle_source_request = request.into_inner();
        let toggle_source_reply = self
            .0
            .toggle_source(
                toggle_source_request.index_uid.into(),
                &toggle_source_request.source_id,
                toggle_source_request.enable,
            )
            .await
            .map(|_| EmptyResponse {})?;
        Ok(tonic::Response::new(toggle_source_reply))
    }

    #[instrument(skip(self, request))]
    async fn delete_source(
        &self,
        request: tonic::Request<DeleteSourceRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let delete_source_request = request.into_inner();
        let delete_source_reply = self
            .0
            .delete_source(
                delete_source_request.index_uid.into(),
                &delete_source_request.source_id,
            )
            .await
            .map(|_| EmptyResponse {})?;
        Ok(tonic::Response::new(delete_source_reply))
    }

    #[instrument(skip(self, request))]
    async fn reset_source_checkpoint(
        &self,
        request: tonic::Request<ResetSourceCheckpointRequest>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let request = request.into_inner();
        let reply = self
            .0
            .reset_source_checkpoint(request.index_uid.into(), &request.source_id)
            .await
            .map(|_| EmptyResponse {})?;
        Ok(tonic::Response::new(reply))
    }

    #[instrument(skip(self, request))]
    async fn last_delete_opstamp(
        &self,
        request: tonic::Request<LastDeleteOpstampRequest>,
    ) -> Result<tonic::Response<LastDeleteOpstampResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let request = request.into_inner();
        let last_delete_opstamp = self.0.last_delete_opstamp(request.index_uid.into()).await?;
        let last_opstamp_reply = LastDeleteOpstampResponse {
            last_delete_opstamp,
        };
        Ok(tonic::Response::new(last_opstamp_reply))
    }

    #[instrument(skip(self, request))]
    async fn create_delete_task(
        &self,
        request: tonic::Request<DeleteQuery>,
    ) -> Result<tonic::Response<DeleteTask>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let request = request.into_inner();
        let delete_task = self.0.create_delete_task(request).await?;
        Ok(tonic::Response::new(delete_task))
    }

    #[instrument(skip(self, request))]
    async fn update_splits_delete_opstamp(
        &self,
        request: tonic::Request<UpdateSplitsDeleteOpstampRequest>,
    ) -> Result<tonic::Response<UpdateSplitsDeleteOpstampResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let request = request.into_inner();
        let split_ids = request
            .split_ids
            .iter()
            .map(|split_id| split_id.as_str())
            .collect_vec();
        let reply = self
            .0
            .update_splits_delete_opstamp(
                request.index_uid.into(),
                &split_ids,
                request.delete_opstamp,
            )
            .await
            .map(|_| UpdateSplitsDeleteOpstampResponse {})?;
        Ok(tonic::Response::new(reply))
    }

    #[instrument(skip(self, request))]
    async fn list_delete_tasks(
        &self,
        request: tonic::Request<ListDeleteTasksRequest>,
    ) -> Result<tonic::Response<ListDeleteTasksResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let request = request.into_inner();
        let delete_tasks = self
            .0
            .list_delete_tasks(request.index_uid.into(), request.opstamp_start)
            .await?
            .into_iter()
            .map(DeleteTask::from)
            .collect_vec();
        let reply = ListDeleteTasksResponse { delete_tasks };
        Ok(tonic::Response::new(reply))
    }

    #[instrument(skip(self, request))]
    async fn list_stale_splits(
        &self,
        request: tonic::Request<ListStaleSplitsRequest>,
    ) -> Result<tonic::Response<ListSplitsResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let request = request.into_inner();
        let splits = self
            .0
            .list_stale_splits(
                request.index_uid.into(),
                request.delete_opstamp,
                request.num_splits as usize,
            )
            .await?;
        let reply = serde_json::to_string(&splits)
            .map(|splits_serialized_json| ListSplitsResponse {
                splits_serialized_json,
            })
            .map_err(|error| MetastoreError::JsonSerializeError {
                struct_name: "Vec<Split>".to_string(),
                message: error.to_string(),
            })?;
        Ok(tonic::Response::new(reply))
    }

    // Shard API:
    // - `open_shards`
    // - `acquire_shards`
    // - `close_shards`
    // - `list_shards`
    // - `delete_shards`

    #[instrument(skip(self, request))]
    async fn open_shards(
        &self,
        request: tonic::Request<OpenShardsRequest>,
    ) -> Result<tonic::Response<OpenShardsResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let request = request.into_inner();
        let response = self.0.open_shards(request).await?;
        Ok(tonic::Response::new(response))
    }

    #[instrument(skip(self, request))]
    async fn acquire_shards(
        &self,
        request: tonic::Request<AcquireShardsRequest>,
    ) -> Result<tonic::Response<AcquireShardsResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let request = request.into_inner();
        let response = self.0.acquire_shards(request).await?;
        Ok(tonic::Response::new(response))
    }

    #[instrument(skip(self, request))]
    async fn close_shards(
        &self,
        request: tonic::Request<CloseShardsRequest>,
    ) -> Result<tonic::Response<CloseShardsResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let request = request.into_inner();
        let response = self.0.close_shards(request).await?;
        Ok(tonic::Response::new(response))
    }

    #[instrument(skip(self, request))]
    async fn list_shards(
        &self,
        request: tonic::Request<ListShardsRequest>,
    ) -> Result<tonic::Response<ListShardsResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let request = request.into_inner();
        let response = self.0.list_shards(request).await?;
        Ok(tonic::Response::new(response))
    }

    #[instrument(skip(self, request))]
    async fn delete_shards(
        &self,
        request: tonic::Request<DeleteShardsRequest>,
    ) -> Result<tonic::Response<DeleteShardsResponse>, tonic::Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let request = request.into_inner();
        let response = self.0.delete_shards(request).await?;
        Ok(tonic::Response::new(response))
    }
}
