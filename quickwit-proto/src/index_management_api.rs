#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateIndexRequest {
    #[prost(string, tag="1")]
    pub index_config_serialized_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateIndexResponse {
    #[prost(string, tag="1")]
    pub index_metadata_serialized_json: ::prost::alloc::string::String,
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
pub struct DeleteIndexResponse {
    #[prost(message, repeated, tag="1")]
    pub file_entries: ::prost::alloc::vec::Vec<FileEntry>,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileEntry {
    #[prost(string, tag="1")]
    pub file_name: ::prost::alloc::string::String,
    #[prost(uint64, tag="2")]
    pub file_size_in_bytes: u64,
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
    #[prost(uint64, optional, tag="3")]
    pub time_range_start: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag="4")]
    pub time_range_end: ::core::option::Option<u64>,
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
    #[prost(string, tag="2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="3")]
    pub split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag="4")]
    pub checkpoint_delta_serialized_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplaceSplitsRequest {
    #[prost(string, tag="2")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="3")]
    pub new_split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag="4")]
    pub replaced_split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
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
pub struct DeleteSourceRequest {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SourceResponse {
}
/// Generated client implementations.
pub mod index_management_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct IndexManagementServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl IndexManagementServiceClient<tonic::transport::Channel> {
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
    impl<T> IndexManagementServiceClient<T>
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
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> IndexManagementServiceClient<InterceptedService<T, F>>
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
            IndexManagementServiceClient::new(
                InterceptedService::new(inner, interceptor),
            )
        }
        /// Compress requests with `gzip`.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        /// Enable decompressing responses with `gzip`.
        #[must_use]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        /// Create an index
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
                "/index_management_api.IndexManagementService/create_index",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Get an index metadata.
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
                "/index_management_api.IndexManagementService/index_metadata",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Get an indexes metadatas.
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
                "/index_management_api.IndexManagementService/list_indexes_metadatas",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Delete an index
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
                "/index_management_api.IndexManagementService/delete_index",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Get all splits from index.
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
                "/index_management_api.IndexManagementService/list_all_splits",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Get splits from index.
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
                "/index_management_api.IndexManagementService/list_splits",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Stage split.
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
                "/index_management_api.IndexManagementService/stage_split",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Publish split.
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
                "/index_management_api.IndexManagementService/publish_splits",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Replace split.
        pub async fn replace_splits(
            &mut self,
            request: impl tonic::IntoRequest<super::ReplaceSplitsRequest>,
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
                "/index_management_api.IndexManagementService/replace_splits",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Mark splits for deletion.
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
                "/index_management_api.IndexManagementService/mark_splits_for_deletion",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod index_management_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with IndexManagementServiceServer.
    #[async_trait]
    pub trait IndexManagementService: Send + Sync + 'static {
        /// Create an index
        async fn create_index(
            &self,
            request: tonic::Request<super::CreateIndexRequest>,
        ) -> Result<tonic::Response<super::CreateIndexResponse>, tonic::Status>;
        /// Get an index metadata.
        async fn index_metadata(
            &self,
            request: tonic::Request<super::IndexMetadataRequest>,
        ) -> Result<tonic::Response<super::IndexMetadataResponse>, tonic::Status>;
        /// Get an indexes metadatas.
        async fn list_indexes_metadatas(
            &self,
            request: tonic::Request<super::ListIndexesMetadatasRequest>,
        ) -> Result<tonic::Response<super::ListIndexesMetadatasResponse>, tonic::Status>;
        /// Delete an index
        async fn delete_index(
            &self,
            request: tonic::Request<super::DeleteIndexRequest>,
        ) -> Result<tonic::Response<super::DeleteIndexResponse>, tonic::Status>;
        /// Get all splits from index.
        async fn list_all_splits(
            &self,
            request: tonic::Request<super::ListAllSplitsRequest>,
        ) -> Result<tonic::Response<super::ListSplitsResponse>, tonic::Status>;
        /// Get splits from index.
        async fn list_splits(
            &self,
            request: tonic::Request<super::ListSplitsRequest>,
        ) -> Result<tonic::Response<super::ListSplitsResponse>, tonic::Status>;
        /// Stage split.
        async fn stage_split(
            &self,
            request: tonic::Request<super::StageSplitRequest>,
        ) -> Result<tonic::Response<super::SplitResponse>, tonic::Status>;
        /// Publish split.
        async fn publish_splits(
            &self,
            request: tonic::Request<super::PublishSplitsRequest>,
        ) -> Result<tonic::Response<super::SplitResponse>, tonic::Status>;
        /// Replace split.
        async fn replace_splits(
            &self,
            request: tonic::Request<super::ReplaceSplitsRequest>,
        ) -> Result<tonic::Response<super::SplitResponse>, tonic::Status>;
        /// Mark splits for deletion.
        async fn mark_splits_for_deletion(
            &self,
            request: tonic::Request<super::MarkSplitsForDeletionRequest>,
        ) -> Result<tonic::Response<super::SplitResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct IndexManagementServiceServer<T: IndexManagementService> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: IndexManagementService> IndexManagementServiceServer<T> {
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
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>>
    for IndexManagementServiceServer<T>
    where
        T: IndexManagementService,
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
                "/index_management_api.IndexManagementService/create_index" => {
                    #[allow(non_camel_case_types)]
                    struct create_indexSvc<T: IndexManagementService>(pub Arc<T>);
                    impl<
                        T: IndexManagementService,
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
                "/index_management_api.IndexManagementService/index_metadata" => {
                    #[allow(non_camel_case_types)]
                    struct index_metadataSvc<T: IndexManagementService>(pub Arc<T>);
                    impl<
                        T: IndexManagementService,
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
                "/index_management_api.IndexManagementService/list_indexes_metadatas" => {
                    #[allow(non_camel_case_types)]
                    struct list_indexes_metadatasSvc<T: IndexManagementService>(
                        pub Arc<T>,
                    );
                    impl<
                        T: IndexManagementService,
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
                "/index_management_api.IndexManagementService/delete_index" => {
                    #[allow(non_camel_case_types)]
                    struct delete_indexSvc<T: IndexManagementService>(pub Arc<T>);
                    impl<
                        T: IndexManagementService,
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
                "/index_management_api.IndexManagementService/list_all_splits" => {
                    #[allow(non_camel_case_types)]
                    struct list_all_splitsSvc<T: IndexManagementService>(pub Arc<T>);
                    impl<
                        T: IndexManagementService,
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
                "/index_management_api.IndexManagementService/list_splits" => {
                    #[allow(non_camel_case_types)]
                    struct list_splitsSvc<T: IndexManagementService>(pub Arc<T>);
                    impl<
                        T: IndexManagementService,
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
                "/index_management_api.IndexManagementService/stage_split" => {
                    #[allow(non_camel_case_types)]
                    struct stage_splitSvc<T: IndexManagementService>(pub Arc<T>);
                    impl<
                        T: IndexManagementService,
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
                "/index_management_api.IndexManagementService/publish_splits" => {
                    #[allow(non_camel_case_types)]
                    struct publish_splitsSvc<T: IndexManagementService>(pub Arc<T>);
                    impl<
                        T: IndexManagementService,
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
                "/index_management_api.IndexManagementService/replace_splits" => {
                    #[allow(non_camel_case_types)]
                    struct replace_splitsSvc<T: IndexManagementService>(pub Arc<T>);
                    impl<
                        T: IndexManagementService,
                    > tonic::server::UnaryService<super::ReplaceSplitsRequest>
                    for replace_splitsSvc<T> {
                        type Response = super::SplitResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ReplaceSplitsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).replace_splits(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = replace_splitsSvc(inner);
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
                "/index_management_api.IndexManagementService/mark_splits_for_deletion" => {
                    #[allow(non_camel_case_types)]
                    struct mark_splits_for_deletionSvc<T: IndexManagementService>(
                        pub Arc<T>,
                    );
                    impl<
                        T: IndexManagementService,
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
    impl<T: IndexManagementService> Clone for IndexManagementServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: IndexManagementService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: IndexManagementService> tonic::transport::NamedService
    for IndexManagementServiceServer<T> {
        const NAME: &'static str = "index_management_api.IndexManagementService";
    }
}
