#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateIndexRequest {
    #[prost(string, tag="1")]
    pub index_metadata_serialized_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateIndexResponse {
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListIndexesMetadatasRequest {
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListIndexesMetadatasResponse {
    #[prost(string, tag="1")]
    pub indexes_metadatas_serialized_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteIndexRequest {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteTasksResponse {
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteIndexResponse {
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexMetadataRequest {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexMetadataResponse {
    #[prost(string, tag="1")]
    pub index_metadata_serialized_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListAllSplitsRequest {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListSplitsRequest {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub split_state: ::prost::alloc::string::String,
    #[prost(int64, optional, tag="3")]
    pub time_range_start: ::core::option::Option<i64>,
    #[prost(int64, optional, tag="4")]
    pub time_range_end: ::core::option::Option<i64>,
    #[prost(string, optional, tag="5")]
    pub tags_serialized_json: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListSplitsResponse {
    #[prost(string, tag="1")]
    pub splits_serialized_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StageSplitRequest {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub split_metadata_serialized_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PublishSplitsRequest {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="2")]
    pub split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag="3")]
    pub replaced_split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, optional, tag="4")]
    pub index_checkpoint_delta_serialized_json: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MarkSplitsForDeletionRequest {
    #[prost(string, tag="2")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="3")]
    pub split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSplitsRequest {
    #[prost(string, tag="2")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="3")]
    pub split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SplitResponse {
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddSourceRequest {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub source_config_serialized_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ToggleSourceRequest {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(bool, tag="3")]
    pub enable: bool,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSourceRequest {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub source_id: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResetSourceCheckpointRequest {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub source_id: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SourceResponse {
    #[prost(bool, tag="1")]
    pub has_changed: bool,
}
// /
// / Delete tasks.
// /

#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteTask {
    #[prost(int64, tag="1")]
    pub create_timestamp: i64,
    #[prost(uint64, tag="2")]
    pub opstamp: u64,
    #[prost(message, optional, tag="3")]
    pub delete_query: ::core::option::Option<DeleteQuery>,
}
#[derive(Serialize, Deserialize)]
#[serde(default)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteQuery {
    /// / Index ID.
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
    /// / If set, restrict search to documents with a `timestamp >= start_timestamp`.
    #[prost(int64, optional, tag="2")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_timestamp: ::core::option::Option<i64>,
    /// / If set, restrict search to documents with a `timestamp < end_timestamp``.
    #[prost(int64, optional, tag="3")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_timestamp: ::core::option::Option<i64>,
    /// / Query text. The query language is that of tantivy.
    #[prost(string, tag="4")]
    pub query: ::prost::alloc::string::String,
    /// / Search fields.
    #[prost(string, repeated, tag="5")]
    pub search_fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateSplitsDeleteOpstampRequest {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="2")]
    pub split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(uint64, tag="3")]
    pub delete_opstamp: u64,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateSplitsDeleteOpstampResponse {
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LastDeleteOpstampRequest {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LastDeleteOpstampResponse {
    #[prost(uint64, tag="1")]
    pub last_delete_opstamp: u64,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListStaleSplitsRequest {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(uint64, tag="2")]
    pub delete_opstamp: u64,
    #[prost(uint64, tag="3")]
    pub num_splits: u64,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListDeleteTasksRequest {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(uint64, tag="2")]
    pub opstamp_start: u64,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListDeleteTasksResponse {
    #[prost(message, repeated, tag="1")]
    pub delete_tasks: ::prost::alloc::vec::Vec<DeleteTask>,
}
/// Generated client implementations.
pub mod metastore_api_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct MetastoreApiServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl MetastoreApiServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> MetastoreApiServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> MetastoreApiServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            MetastoreApiServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Creates an index.
        pub async fn create_index(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateIndexRequest>,
        ) -> Result<tonic::Response<super::CreateIndexResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_metastore_api.MetastoreApiService/create_index",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Gets an index metadata.
        pub async fn index_metadata(
            &mut self,
            request: impl tonic::IntoRequest<super::IndexMetadataRequest>,
        ) -> Result<tonic::Response<super::IndexMetadataResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_metastore_api.MetastoreApiService/index_metadata",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Gets an indexes metadatas.
        pub async fn list_indexes_metadatas(
            &mut self,
            request: impl tonic::IntoRequest<super::ListIndexesMetadatasRequest>,
        ) -> Result<
            tonic::Response<super::ListIndexesMetadatasResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_metastore_api.MetastoreApiService/list_indexes_metadatas",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Deletes an index
        pub async fn delete_index(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteIndexRequest>,
        ) -> Result<tonic::Response<super::DeleteIndexResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_metastore_api.MetastoreApiService/delete_index",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Gets all splits from index.
        pub async fn list_all_splits(
            &mut self,
            request: impl tonic::IntoRequest<super::ListAllSplitsRequest>,
        ) -> Result<tonic::Response<super::ListSplitsResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_metastore_api.MetastoreApiService/list_all_splits",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Gets splits from index.
        pub async fn list_splits(
            &mut self,
            request: impl tonic::IntoRequest<super::ListSplitsRequest>,
        ) -> Result<tonic::Response<super::ListSplitsResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_metastore_api.MetastoreApiService/list_splits",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Stages split.
        pub async fn stage_split(
            &mut self,
            request: impl tonic::IntoRequest<super::StageSplitRequest>,
        ) -> Result<tonic::Response<super::SplitResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_metastore_api.MetastoreApiService/stage_split",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Publishes split.
        pub async fn publish_splits(
            &mut self,
            request: impl tonic::IntoRequest<super::PublishSplitsRequest>,
        ) -> Result<tonic::Response<super::SplitResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_metastore_api.MetastoreApiService/publish_splits",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Marks splits for deletion.
        pub async fn mark_splits_for_deletion(
            &mut self,
            request: impl tonic::IntoRequest<super::MarkSplitsForDeletionRequest>,
        ) -> Result<tonic::Response<super::SplitResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_metastore_api.MetastoreApiService/mark_splits_for_deletion",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Deletes splits.
        pub async fn delete_splits(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteSplitsRequest>,
        ) -> Result<tonic::Response<super::SplitResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_metastore_api.MetastoreApiService/delete_splits",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Adds source.
        pub async fn add_source(
            &mut self,
            request: impl tonic::IntoRequest<super::AddSourceRequest>,
        ) -> Result<tonic::Response<super::SourceResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_metastore_api.MetastoreApiService/add_source",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Toggles source.
        pub async fn toggle_source(
            &mut self,
            request: impl tonic::IntoRequest<super::ToggleSourceRequest>,
        ) -> Result<tonic::Response<super::SourceResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_metastore_api.MetastoreApiService/toggle_source",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Removes source.
        pub async fn delete_source(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteSourceRequest>,
        ) -> Result<tonic::Response<super::SourceResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_metastore_api.MetastoreApiService/delete_source",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Resets source checkpoint.
        pub async fn reset_source_checkpoint(
            &mut self,
            request: impl tonic::IntoRequest<super::ResetSourceCheckpointRequest>,
        ) -> Result<tonic::Response<super::SourceResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_metastore_api.MetastoreApiService/reset_source_checkpoint",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Gets last opstamp for a given `index_id`.
        pub async fn last_delete_opstamp(
            &mut self,
            request: impl tonic::IntoRequest<super::LastDeleteOpstampRequest>,
        ) -> Result<tonic::Response<super::LastDeleteOpstampResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_metastore_api.MetastoreApiService/last_delete_opstamp",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Creates a delete task.
        pub async fn create_delete_task(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteQuery>,
        ) -> Result<tonic::Response<super::DeleteTask>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_metastore_api.MetastoreApiService/create_delete_task",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Updates splits `delete_opstamp`.
        pub async fn update_splits_delete_opstamp(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateSplitsDeleteOpstampRequest>,
        ) -> Result<
            tonic::Response<super::UpdateSplitsDeleteOpstampResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_metastore_api.MetastoreApiService/update_splits_delete_opstamp",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Lists delete tasks with `delete_task.opstamp` > `opstamp_start` for a given `index_id`.
        pub async fn list_delete_tasks(
            &mut self,
            request: impl tonic::IntoRequest<super::ListDeleteTasksRequest>,
        ) -> Result<tonic::Response<super::ListDeleteTasksResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_metastore_api.MetastoreApiService/list_delete_tasks",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        //// Lists splits with `split.delete_opstamp` < `delete_opstamp` for a given `index_id`.
        pub async fn list_stale_splits(
            &mut self,
            request: impl tonic::IntoRequest<super::ListStaleSplitsRequest>,
        ) -> Result<tonic::Response<super::ListSplitsResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/quickwit_metastore_api.MetastoreApiService/list_stale_splits",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod metastore_api_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with MetastoreApiServiceServer.
    #[async_trait]
    pub trait MetastoreApiService: Send + Sync + 'static {
        /// Creates an index.
        async fn create_index(
            &self,
            request: tonic::Request<super::CreateIndexRequest>,
        ) -> Result<tonic::Response<super::CreateIndexResponse>, tonic::Status>;
        /// Gets an index metadata.
        async fn index_metadata(
            &self,
            request: tonic::Request<super::IndexMetadataRequest>,
        ) -> Result<tonic::Response<super::IndexMetadataResponse>, tonic::Status>;
        /// Gets an indexes metadatas.
        async fn list_indexes_metadatas(
            &self,
            request: tonic::Request<super::ListIndexesMetadatasRequest>,
        ) -> Result<tonic::Response<super::ListIndexesMetadatasResponse>, tonic::Status>;
        /// Deletes an index
        async fn delete_index(
            &self,
            request: tonic::Request<super::DeleteIndexRequest>,
        ) -> Result<tonic::Response<super::DeleteIndexResponse>, tonic::Status>;
        /// Gets all splits from index.
        async fn list_all_splits(
            &self,
            request: tonic::Request<super::ListAllSplitsRequest>,
        ) -> Result<tonic::Response<super::ListSplitsResponse>, tonic::Status>;
        /// Gets splits from index.
        async fn list_splits(
            &self,
            request: tonic::Request<super::ListSplitsRequest>,
        ) -> Result<tonic::Response<super::ListSplitsResponse>, tonic::Status>;
        /// Stages split.
        async fn stage_split(
            &self,
            request: tonic::Request<super::StageSplitRequest>,
        ) -> Result<tonic::Response<super::SplitResponse>, tonic::Status>;
        /// Publishes split.
        async fn publish_splits(
            &self,
            request: tonic::Request<super::PublishSplitsRequest>,
        ) -> Result<tonic::Response<super::SplitResponse>, tonic::Status>;
        /// Marks splits for deletion.
        async fn mark_splits_for_deletion(
            &self,
            request: tonic::Request<super::MarkSplitsForDeletionRequest>,
        ) -> Result<tonic::Response<super::SplitResponse>, tonic::Status>;
        /// Deletes splits.
        async fn delete_splits(
            &self,
            request: tonic::Request<super::DeleteSplitsRequest>,
        ) -> Result<tonic::Response<super::SplitResponse>, tonic::Status>;
        /// Adds source.
        async fn add_source(
            &self,
            request: tonic::Request<super::AddSourceRequest>,
        ) -> Result<tonic::Response<super::SourceResponse>, tonic::Status>;
        /// Toggles source.
        async fn toggle_source(
            &self,
            request: tonic::Request<super::ToggleSourceRequest>,
        ) -> Result<tonic::Response<super::SourceResponse>, tonic::Status>;
        /// Removes source.
        async fn delete_source(
            &self,
            request: tonic::Request<super::DeleteSourceRequest>,
        ) -> Result<tonic::Response<super::SourceResponse>, tonic::Status>;
        /// Resets source checkpoint.
        async fn reset_source_checkpoint(
            &self,
            request: tonic::Request<super::ResetSourceCheckpointRequest>,
        ) -> Result<tonic::Response<super::SourceResponse>, tonic::Status>;
        /// Gets last opstamp for a given `index_id`.
        async fn last_delete_opstamp(
            &self,
            request: tonic::Request<super::LastDeleteOpstampRequest>,
        ) -> Result<tonic::Response<super::LastDeleteOpstampResponse>, tonic::Status>;
        /// Creates a delete task.
        async fn create_delete_task(
            &self,
            request: tonic::Request<super::DeleteQuery>,
        ) -> Result<tonic::Response<super::DeleteTask>, tonic::Status>;
        /// Updates splits `delete_opstamp`.
        async fn update_splits_delete_opstamp(
            &self,
            request: tonic::Request<super::UpdateSplitsDeleteOpstampRequest>,
        ) -> Result<
            tonic::Response<super::UpdateSplitsDeleteOpstampResponse>,
            tonic::Status,
        >;
        /// Lists delete tasks with `delete_task.opstamp` > `opstamp_start` for a given `index_id`.
        async fn list_delete_tasks(
            &self,
            request: tonic::Request<super::ListDeleteTasksRequest>,
        ) -> Result<tonic::Response<super::ListDeleteTasksResponse>, tonic::Status>;
        //// Lists splits with `split.delete_opstamp` < `delete_opstamp` for a given `index_id`.
        async fn list_stale_splits(
            &self,
            request: tonic::Request<super::ListStaleSplitsRequest>,
        ) -> Result<tonic::Response<super::ListSplitsResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct MetastoreApiServiceServer<T: MetastoreApiService> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: MetastoreApiService> MetastoreApiServiceServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for MetastoreApiServiceServer<T>
    where
        T: MetastoreApiService,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/quickwit_metastore_api.MetastoreApiService/create_index" => {
                    #[allow(non_camel_case_types)]
                    struct create_indexSvc<T: MetastoreApiService>(pub Arc<T>);
                    impl<
                        T: MetastoreApiService,
                    > tonic::server::UnaryService<super::CreateIndexRequest>
                    for create_indexSvc<T> {
                        type Response = super::CreateIndexResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateIndexRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).create_index(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = create_indexSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_metastore_api.MetastoreApiService/index_metadata" => {
                    #[allow(non_camel_case_types)]
                    struct index_metadataSvc<T: MetastoreApiService>(pub Arc<T>);
                    impl<
                        T: MetastoreApiService,
                    > tonic::server::UnaryService<super::IndexMetadataRequest>
                    for index_metadataSvc<T> {
                        type Response = super::IndexMetadataResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::IndexMetadataRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).index_metadata(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = index_metadataSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_metastore_api.MetastoreApiService/list_indexes_metadatas" => {
                    #[allow(non_camel_case_types)]
                    struct list_indexes_metadatasSvc<T: MetastoreApiService>(pub Arc<T>);
                    impl<
                        T: MetastoreApiService,
                    > tonic::server::UnaryService<super::ListIndexesMetadatasRequest>
                    for list_indexes_metadatasSvc<T> {
                        type Response = super::ListIndexesMetadatasResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListIndexesMetadatasRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).list_indexes_metadatas(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = list_indexes_metadatasSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_metastore_api.MetastoreApiService/delete_index" => {
                    #[allow(non_camel_case_types)]
                    struct delete_indexSvc<T: MetastoreApiService>(pub Arc<T>);
                    impl<
                        T: MetastoreApiService,
                    > tonic::server::UnaryService<super::DeleteIndexRequest>
                    for delete_indexSvc<T> {
                        type Response = super::DeleteIndexResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteIndexRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).delete_index(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = delete_indexSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_metastore_api.MetastoreApiService/list_all_splits" => {
                    #[allow(non_camel_case_types)]
                    struct list_all_splitsSvc<T: MetastoreApiService>(pub Arc<T>);
                    impl<
                        T: MetastoreApiService,
                    > tonic::server::UnaryService<super::ListAllSplitsRequest>
                    for list_all_splitsSvc<T> {
                        type Response = super::ListSplitsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListAllSplitsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).list_all_splits(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = list_all_splitsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_metastore_api.MetastoreApiService/list_splits" => {
                    #[allow(non_camel_case_types)]
                    struct list_splitsSvc<T: MetastoreApiService>(pub Arc<T>);
                    impl<
                        T: MetastoreApiService,
                    > tonic::server::UnaryService<super::ListSplitsRequest>
                    for list_splitsSvc<T> {
                        type Response = super::ListSplitsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListSplitsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).list_splits(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = list_splitsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_metastore_api.MetastoreApiService/stage_split" => {
                    #[allow(non_camel_case_types)]
                    struct stage_splitSvc<T: MetastoreApiService>(pub Arc<T>);
                    impl<
                        T: MetastoreApiService,
                    > tonic::server::UnaryService<super::StageSplitRequest>
                    for stage_splitSvc<T> {
                        type Response = super::SplitResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StageSplitRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).stage_split(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = stage_splitSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_metastore_api.MetastoreApiService/publish_splits" => {
                    #[allow(non_camel_case_types)]
                    struct publish_splitsSvc<T: MetastoreApiService>(pub Arc<T>);
                    impl<
                        T: MetastoreApiService,
                    > tonic::server::UnaryService<super::PublishSplitsRequest>
                    for publish_splitsSvc<T> {
                        type Response = super::SplitResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PublishSplitsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).publish_splits(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = publish_splitsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_metastore_api.MetastoreApiService/mark_splits_for_deletion" => {
                    #[allow(non_camel_case_types)]
                    struct mark_splits_for_deletionSvc<T: MetastoreApiService>(
                        pub Arc<T>,
                    );
                    impl<
                        T: MetastoreApiService,
                    > tonic::server::UnaryService<super::MarkSplitsForDeletionRequest>
                    for mark_splits_for_deletionSvc<T> {
                        type Response = super::SplitResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::MarkSplitsForDeletionRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).mark_splits_for_deletion(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = mark_splits_for_deletionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_metastore_api.MetastoreApiService/delete_splits" => {
                    #[allow(non_camel_case_types)]
                    struct delete_splitsSvc<T: MetastoreApiService>(pub Arc<T>);
                    impl<
                        T: MetastoreApiService,
                    > tonic::server::UnaryService<super::DeleteSplitsRequest>
                    for delete_splitsSvc<T> {
                        type Response = super::SplitResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteSplitsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).delete_splits(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = delete_splitsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_metastore_api.MetastoreApiService/add_source" => {
                    #[allow(non_camel_case_types)]
                    struct add_sourceSvc<T: MetastoreApiService>(pub Arc<T>);
                    impl<
                        T: MetastoreApiService,
                    > tonic::server::UnaryService<super::AddSourceRequest>
                    for add_sourceSvc<T> {
                        type Response = super::SourceResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AddSourceRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).add_source(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = add_sourceSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_metastore_api.MetastoreApiService/toggle_source" => {
                    #[allow(non_camel_case_types)]
                    struct toggle_sourceSvc<T: MetastoreApiService>(pub Arc<T>);
                    impl<
                        T: MetastoreApiService,
                    > tonic::server::UnaryService<super::ToggleSourceRequest>
                    for toggle_sourceSvc<T> {
                        type Response = super::SourceResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ToggleSourceRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).toggle_source(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = toggle_sourceSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_metastore_api.MetastoreApiService/delete_source" => {
                    #[allow(non_camel_case_types)]
                    struct delete_sourceSvc<T: MetastoreApiService>(pub Arc<T>);
                    impl<
                        T: MetastoreApiService,
                    > tonic::server::UnaryService<super::DeleteSourceRequest>
                    for delete_sourceSvc<T> {
                        type Response = super::SourceResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteSourceRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).delete_source(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = delete_sourceSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_metastore_api.MetastoreApiService/reset_source_checkpoint" => {
                    #[allow(non_camel_case_types)]
                    struct reset_source_checkpointSvc<T: MetastoreApiService>(
                        pub Arc<T>,
                    );
                    impl<
                        T: MetastoreApiService,
                    > tonic::server::UnaryService<super::ResetSourceCheckpointRequest>
                    for reset_source_checkpointSvc<T> {
                        type Response = super::SourceResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ResetSourceCheckpointRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).reset_source_checkpoint(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = reset_source_checkpointSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_metastore_api.MetastoreApiService/last_delete_opstamp" => {
                    #[allow(non_camel_case_types)]
                    struct last_delete_opstampSvc<T: MetastoreApiService>(pub Arc<T>);
                    impl<
                        T: MetastoreApiService,
                    > tonic::server::UnaryService<super::LastDeleteOpstampRequest>
                    for last_delete_opstampSvc<T> {
                        type Response = super::LastDeleteOpstampResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LastDeleteOpstampRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).last_delete_opstamp(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = last_delete_opstampSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_metastore_api.MetastoreApiService/create_delete_task" => {
                    #[allow(non_camel_case_types)]
                    struct create_delete_taskSvc<T: MetastoreApiService>(pub Arc<T>);
                    impl<
                        T: MetastoreApiService,
                    > tonic::server::UnaryService<super::DeleteQuery>
                    for create_delete_taskSvc<T> {
                        type Response = super::DeleteTask;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteQuery>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).create_delete_task(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = create_delete_taskSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_metastore_api.MetastoreApiService/update_splits_delete_opstamp" => {
                    #[allow(non_camel_case_types)]
                    struct update_splits_delete_opstampSvc<T: MetastoreApiService>(
                        pub Arc<T>,
                    );
                    impl<
                        T: MetastoreApiService,
                    > tonic::server::UnaryService<
                        super::UpdateSplitsDeleteOpstampRequest,
                    > for update_splits_delete_opstampSvc<T> {
                        type Response = super::UpdateSplitsDeleteOpstampResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::UpdateSplitsDeleteOpstampRequest,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).update_splits_delete_opstamp(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = update_splits_delete_opstampSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_metastore_api.MetastoreApiService/list_delete_tasks" => {
                    #[allow(non_camel_case_types)]
                    struct list_delete_tasksSvc<T: MetastoreApiService>(pub Arc<T>);
                    impl<
                        T: MetastoreApiService,
                    > tonic::server::UnaryService<super::ListDeleteTasksRequest>
                    for list_delete_tasksSvc<T> {
                        type Response = super::ListDeleteTasksResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListDeleteTasksRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).list_delete_tasks(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = list_delete_tasksSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/quickwit_metastore_api.MetastoreApiService/list_stale_splits" => {
                    #[allow(non_camel_case_types)]
                    struct list_stale_splitsSvc<T: MetastoreApiService>(pub Arc<T>);
                    impl<
                        T: MetastoreApiService,
                    > tonic::server::UnaryService<super::ListStaleSplitsRequest>
                    for list_stale_splitsSvc<T> {
                        type Response = super::ListSplitsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListStaleSplitsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).list_stale_splits(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = list_stale_splitsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: MetastoreApiService> Clone for MetastoreApiServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: MetastoreApiService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: MetastoreApiService> tonic::server::NamedService
    for MetastoreApiServiceServer<T> {
        const NAME: &'static str = "quickwit_metastore_api.MetastoreApiService";
    }
}
