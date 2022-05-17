#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueueExistsRequest {
    #[prost(string, tag="1")]
    pub queue_id: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateQueueRequest {
    #[prost(string, tag="1")]
    pub queue_id: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateQueueIfNotExistsRequest {
    #[prost(string, tag="1")]
    pub queue_id: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropQueueRequest {
    #[prost(string, tag="1")]
    pub queue_id: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestRequest {
    #[prost(message, repeated, tag="1")]
    pub doc_batches: ::prost::alloc::vec::Vec<DocBatch>,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestResponse {
    #[prost(uint64, tag="1")]
    pub num_ingested_docs: u64,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchRequest {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(uint64, optional, tag="2")]
    pub start_after: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag="3")]
    pub num_bytes_limit: ::core::option::Option<u64>,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchResponse {
    #[prost(uint64, optional, tag="1")]
    pub first_position: ::core::option::Option<u64>,
    #[prost(message, optional, tag="2")]
    pub doc_batch: ::core::option::Option<DocBatch>,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DocBatch {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(bytes="vec", tag="2")]
    pub concat_docs: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, repeated, tag="3")]
    pub doc_lens: ::prost::alloc::vec::Vec<u64>,
}
//// Suggest to truncate the queue.
////
//// This function allows the queue to remove all records up to and
//// including `up_to_offset_included`.
////
//// The role of this truncation is to release memory and disk space.
////
//// There are no guarantees that the record will effectively be removed.
//// Nothing might happen, or the truncation might be partial.
////
//// In other words, truncating from a position, and fetching records starting
//// earlier than this position can yield undefined result:
//// the truncated records may or may not be returned.
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SuggestTruncateRequest {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(uint64, tag="2")]
    pub up_to_position_included: u64,
}
#[derive(Serialize, Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TailRequest {
    #[prost(string, tag="1")]
    pub index_id: ::prost::alloc::string::String,
}
/// Generated client implementations.
pub mod ingest_api_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct IngestApiServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl IngestApiServiceClient<tonic::transport::Channel> {
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
    impl<T> IngestApiServiceClient<T>
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
        ) -> IngestApiServiceClient<InterceptedService<T, F>>
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
            IngestApiServiceClient::new(InterceptedService::new(inner, interceptor))
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
        //// Ingests document in a given queue.
        ////
        //// Upon any kind of error, the client should
        //// - retry to get at least once delivery.
        //// - not retry to get at most once delivery.
        ////
        //// Exactly once delivery is not supported yet.
        pub async fn ingest(
            &mut self,
            request: impl tonic::IntoRequest<super::IngestRequest>,
        ) -> Result<tonic::Response<super::IngestResponse>, tonic::Status> {
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
                "/quickwit_ingest_api.IngestAPIService/Ingest",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        //// Fetches record from a given queue.
        ////
        //// Records are returned in order.
        ////
        //// The returned `FetchResponse` object is meant to be read with the
        //// `crate::iter_records` function.
        ////
        //// Fetching does not necessarily return all of the available records.
        //// If returning all records would exceed `FETCH_PAYLOAD_LIMIT` (2MB),
        //// the reponse will be partial.
        pub async fn fetch(
            &mut self,
            request: impl tonic::IntoRequest<super::FetchRequest>,
        ) -> Result<tonic::Response<super::FetchResponse>, tonic::Status> {
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
                "/quickwit_ingest_api.IngestAPIService/Fetch",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        //// Returns a batch containing the last records.
        ////
        //// It returns the last documents, from the newest
        //// to the oldest, and stops as soon as `FETCH_PAYLOAD_LIMIT` (2MB)
        //// is exceeded.
        pub async fn tail(
            &mut self,
            request: impl tonic::IntoRequest<super::TailRequest>,
        ) -> Result<tonic::Response<super::FetchResponse>, tonic::Status> {
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
                "/quickwit_ingest_api.IngestAPIService/Tail",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod ingest_api_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with IngestApiServiceServer.
    #[async_trait]
    pub trait IngestApiService: Send + Sync + 'static {
        //// Ingests document in a given queue.
        ////
        //// Upon any kind of error, the client should
        //// - retry to get at least once delivery.
        //// - not retry to get at most once delivery.
        ////
        //// Exactly once delivery is not supported yet.
        async fn ingest(
            &self,
            request: tonic::Request<super::IngestRequest>,
        ) -> Result<tonic::Response<super::IngestResponse>, tonic::Status>;
        //// Fetches record from a given queue.
        ////
        //// Records are returned in order.
        ////
        //// The returned `FetchResponse` object is meant to be read with the
        //// `crate::iter_records` function.
        ////
        //// Fetching does not necessarily return all of the available records.
        //// If returning all records would exceed `FETCH_PAYLOAD_LIMIT` (2MB),
        //// the reponse will be partial.
        async fn fetch(
            &self,
            request: tonic::Request<super::FetchRequest>,
        ) -> Result<tonic::Response<super::FetchResponse>, tonic::Status>;
        //// Returns a batch containing the last records.
        ////
        //// It returns the last documents, from the newest
        //// to the oldest, and stops as soon as `FETCH_PAYLOAD_LIMIT` (2MB)
        //// is exceeded.
        async fn tail(
            &self,
            request: tonic::Request<super::TailRequest>,
        ) -> Result<tonic::Response<super::FetchResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct IngestApiServiceServer<T: IngestApiService> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: IngestApiService> IngestApiServiceServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for IngestApiServiceServer<T>
    where
        T: IngestApiService,
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
                "/quickwit_ingest_api.IngestAPIService/Ingest" => {
                    #[allow(non_camel_case_types)]
                    struct IngestSvc<T: IngestApiService>(pub Arc<T>);
                    impl<
                        T: IngestApiService,
                    > tonic::server::UnaryService<super::IngestRequest>
                    for IngestSvc<T> {
                        type Response = super::IngestResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::IngestRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).ingest(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = IngestSvc(inner);
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
                "/quickwit_ingest_api.IngestAPIService/Fetch" => {
                    #[allow(non_camel_case_types)]
                    struct FetchSvc<T: IngestApiService>(pub Arc<T>);
                    impl<
                        T: IngestApiService,
                    > tonic::server::UnaryService<super::FetchRequest> for FetchSvc<T> {
                        type Response = super::FetchResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FetchRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).fetch(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = FetchSvc(inner);
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
                "/quickwit_ingest_api.IngestAPIService/Tail" => {
                    #[allow(non_camel_case_types)]
                    struct TailSvc<T: IngestApiService>(pub Arc<T>);
                    impl<
                        T: IngestApiService,
                    > tonic::server::UnaryService<super::TailRequest> for TailSvc<T> {
                        type Response = super::FetchResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::TailRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).tail(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = TailSvc(inner);
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
    impl<T: IngestApiService> Clone for IngestApiServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: IngestApiService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: IngestApiService> tonic::transport::NamedService
    for IngestApiServiceServer<T> {
        const NAME: &'static str = "quickwit_ingest_api.IngestAPIService";
    }
}
