#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyIndexingPlanRequest {
    #[prost(message, repeated, tag="1")]
    pub indexing_tasks: ::prost::alloc::vec::Vec<IndexingTask>,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyIndexingPlanResponse {
    /// TODO: define returned errors, one for each failed task?
    #[prost(string, repeated, tag="1")]
    pub errors: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Serialize, Deserialize)]
#[derive(Eq, Hash)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexingTask {
    /// / Index ID of the task.
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
    /// / Source ID of the task.
    #[prost(string, tag="2")]
    pub source_id: ::prost::alloc::string::String,
    /// / Indexing pipeline ordinal of the task on the (index_id, source_id) with
    /// / 0 <= pipeline_idx < num_pipelines.
    ///
    /// / Task source partitions. Not needed right now as we only offer
    /// / indexing distribution with Kafka and we rely on Kafka consumer
    /// / groups for that.
    /// /repeated string partitions = 4;
    #[prost(uint64, tag="3")]
    pub pipeline_ord: u64,
}
/// Generated client implementations.
pub mod indexing_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct IndexingServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl IndexingServiceClient<tonic::transport::Channel> {
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
    impl<T> IndexingServiceClient<T>
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
        ) -> IndexingServiceClient<InterceptedService<T, F>>
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
            IndexingServiceClient::new(InterceptedService::new(inner, interceptor))
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
        //// Apply a physical plan on the node.
        pub async fn apply_indexing_plan(
            &mut self,
            request: impl tonic::IntoRequest<super::ApplyIndexingPlanRequest>,
        ) -> Result<tonic::Response<super::ApplyIndexingPlanResponse>, tonic::Status> {
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
                "/quickwit_indexing_api.IndexingService/applyIndexingPlan",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod indexing_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with IndexingServiceServer.
    #[async_trait]
    pub trait IndexingService: Send + Sync + 'static {
        //// Apply a physical plan on the node.
        async fn apply_indexing_plan(
            &self,
            request: tonic::Request<super::ApplyIndexingPlanRequest>,
        ) -> Result<tonic::Response<super::ApplyIndexingPlanResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct IndexingServiceServer<T: IndexingService> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: IndexingService> IndexingServiceServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for IndexingServiceServer<T>
    where
        T: IndexingService,
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
                "/quickwit_indexing_api.IndexingService/applyIndexingPlan" => {
                    #[allow(non_camel_case_types)]
                    struct applyIndexingPlanSvc<T: IndexingService>(pub Arc<T>);
                    impl<
                        T: IndexingService,
                    > tonic::server::UnaryService<super::ApplyIndexingPlanRequest>
                    for applyIndexingPlanSvc<T> {
                        type Response = super::ApplyIndexingPlanResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ApplyIndexingPlanRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).apply_indexing_plan(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = applyIndexingPlanSvc(inner);
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
    impl<T: IndexingService> Clone for IndexingServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: IndexingService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: IndexingService> tonic::server::NamedService for IndexingServiceServer<T> {
        const NAME: &'static str = "quickwit_indexing_api.IndexingService";
    }
}
