#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetIndexMetadataRequest {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetIndexMetadataResponse {
    #[prost(string, tag="1")]
    pub index_metadata_serialized_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSplitsMetadatasRequest {
    /// TODO: add filter options
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSplitsMetadatasResponse {
    #[prost(string, tag="1")]
    pub splits_metadatas_serialized_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StageSplitRequest {
    #[prost(string, tag="1")]
    pub publish_token: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub split_metadata_serialized_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PublishSplitRequest {
    #[prost(string, tag="1")]
    pub publish_token: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub split_metadata_serialized_json: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplaceSplitsRequest {
    #[prost(string, tag="1")]
    pub publish_token: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="3")]
    pub new_split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag="4")]
    pub replaced_split_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SplitResponse {
}
/// Generated client implementations.
pub mod index_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct IndexServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl IndexServiceClient<tonic::transport::Channel> {
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
    impl<T> IndexServiceClient<T>
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
        ) -> IndexServiceClient<InterceptedService<T, F>>
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
            IndexServiceClient::new(InterceptedService::new(inner, interceptor))
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
        /// Get an index.
        pub async fn get_index_metadata(
            &mut self,
            request: impl tonic::IntoRequest<super::GetIndexMetadataRequest>,
        ) -> Result<tonic::Response<super::GetIndexMetadataResponse>, tonic::Status> {
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
                "/index_api.IndexService/get_index_metadata",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Get all splits from index.
        pub async fn get_splits_metadatas(
            &mut self,
            request: impl tonic::IntoRequest<super::GetSplitsMetadatasRequest>,
        ) -> Result<tonic::Response<super::GetSplitsMetadatasResponse>, tonic::Status> {
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
                "/index_api.IndexService/get_splits_metadatas",
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
                "/index_api.IndexService/stage_split",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Publish split.
        pub async fn publish_split(
            &mut self,
            request: impl tonic::IntoRequest<super::PublishSplitRequest>,
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
                "/index_api.IndexService/publish_split",
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
                "/index_api.IndexService/replace_splits",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod index_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with IndexServiceServer.
    #[async_trait]
    pub trait IndexService: Send + Sync + 'static {
        /// Get an index.
        async fn get_index_metadata(
            &self,
            request: tonic::Request<super::GetIndexMetadataRequest>,
        ) -> Result<tonic::Response<super::GetIndexMetadataResponse>, tonic::Status>;
        /// Get all splits from index.
        async fn get_splits_metadatas(
            &self,
            request: tonic::Request<super::GetSplitsMetadatasRequest>,
        ) -> Result<tonic::Response<super::GetSplitsMetadatasResponse>, tonic::Status>;
        /// Stage split.
        async fn stage_split(
            &self,
            request: tonic::Request<super::StageSplitRequest>,
        ) -> Result<tonic::Response<super::SplitResponse>, tonic::Status>;
        /// Publish split.
        async fn publish_split(
            &self,
            request: tonic::Request<super::PublishSplitRequest>,
        ) -> Result<tonic::Response<super::SplitResponse>, tonic::Status>;
        /// Replace split.
        async fn replace_splits(
            &self,
            request: tonic::Request<super::ReplaceSplitsRequest>,
        ) -> Result<tonic::Response<super::SplitResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct IndexServiceServer<T: IndexService> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: IndexService> IndexServiceServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for IndexServiceServer<T>
    where
        T: IndexService,
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
                "/index_api.IndexService/get_index_metadata" => {
                    #[allow(non_camel_case_types)]
                    struct get_index_metadataSvc<T: IndexService>(pub Arc<T>);
                    impl<
                        T: IndexService,
                    > tonic::server::UnaryService<super::GetIndexMetadataRequest>
                    for get_index_metadataSvc<T> {
                        type Response = super::GetIndexMetadataResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetIndexMetadataRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).get_index_metadata(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = get_index_metadataSvc(inner);
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
                "/index_api.IndexService/get_splits_metadatas" => {
                    #[allow(non_camel_case_types)]
                    struct get_splits_metadatasSvc<T: IndexService>(pub Arc<T>);
                    impl<
                        T: IndexService,
                    > tonic::server::UnaryService<super::GetSplitsMetadatasRequest>
                    for get_splits_metadatasSvc<T> {
                        type Response = super::GetSplitsMetadatasResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetSplitsMetadatasRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).get_splits_metadatas(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = get_splits_metadatasSvc(inner);
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
                "/index_api.IndexService/stage_split" => {
                    #[allow(non_camel_case_types)]
                    struct stage_splitSvc<T: IndexService>(pub Arc<T>);
                    impl<
                        T: IndexService,
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
                "/index_api.IndexService/publish_split" => {
                    #[allow(non_camel_case_types)]
                    struct publish_splitSvc<T: IndexService>(pub Arc<T>);
                    impl<
                        T: IndexService,
                    > tonic::server::UnaryService<super::PublishSplitRequest>
                    for publish_splitSvc<T> {
                        type Response = super::SplitResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PublishSplitRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).publish_split(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = publish_splitSvc(inner);
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
                "/index_api.IndexService/replace_splits" => {
                    #[allow(non_camel_case_types)]
                    struct replace_splitsSvc<T: IndexService>(pub Arc<T>);
                    impl<
                        T: IndexService,
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
    impl<T: IndexService> Clone for IndexServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: IndexService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: IndexService> tonic::transport::NamedService for IndexServiceServer<T> {
        const NAME: &'static str = "index_api.IndexService";
    }
}
