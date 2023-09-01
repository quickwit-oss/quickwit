#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyIndexingPlanRequest {
    #[prost(message, repeated, tag = "1")]
    pub indexing_tasks: ::prost::alloc::vec::Vec<IndexingTask>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexingTask {
    /// / The tasks's index UID.
    #[prost(string, tag = "1")]
    pub index_uid: ::prost::alloc::string::String,
    /// / The task's source ID.
    #[prost(string, tag = "2")]
    pub source_id: ::prost::alloc::string::String,
    /// / The shards assigned to the indexer.
    #[prost(uint64, repeated, tag = "3")]
    pub shard_ids: ::prost::alloc::vec::Vec<u64>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyIndexingPlanResponse {}
/// BEGIN quickwit-codegen
use tower::{Layer, Service, ServiceExt};
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait::async_trait]
pub trait IndexingService: std::fmt::Debug + dyn_clone::DynClone + Send + Sync + 'static {
    async fn apply_indexing_plan(
        &mut self,
        request: ApplyIndexingPlanRequest,
    ) -> crate::indexing::IndexingResult<ApplyIndexingPlanResponse>;
}
dyn_clone::clone_trait_object!(IndexingService);
#[cfg(any(test, feature = "testsuite"))]
impl Clone for MockIndexingService {
    fn clone(&self) -> Self {
        MockIndexingService::new()
    }
}
#[derive(Debug, Clone)]
pub struct IndexingServiceClient {
    inner: Box<dyn IndexingService>,
}
impl IndexingServiceClient {
    pub fn new<T>(instance: T) -> Self
    where
        T: IndexingService,
    {
        Self { inner: Box::new(instance) }
    }
    pub fn as_grpc_service(
        &self,
    ) -> indexing_service_grpc_server::IndexingServiceGrpcServer<
        IndexingServiceGrpcServerAdapter,
    > {
        let adapter = IndexingServiceGrpcServerAdapter::new(self.clone());
        indexing_service_grpc_server::IndexingServiceGrpcServer::new(adapter)
    }
    pub fn from_channel<C>(channel: C) -> Self
    where
        C: tower::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<hyper::Body>,
                Error = quickwit_common::tower::BoxError,
            > + std::fmt::Debug + Clone + Send + Sync + 'static,
        <C as tower::Service<
            http::Request<tonic::body::BoxBody>,
        >>::Future: std::future::Future<
                Output = Result<
                    http::Response<hyper::Body>,
                    quickwit_common::tower::BoxError,
                >,
            > + Send + 'static,
    {
        IndexingServiceClient::new(
            IndexingServiceGrpcClientAdapter::new(
                indexing_service_grpc_client::IndexingServiceGrpcClient::new(channel),
            ),
        )
    }
    pub fn from_mailbox<A>(mailbox: quickwit_actors::Mailbox<A>) -> Self
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        IndexingServiceMailbox<A>: IndexingService,
    {
        IndexingServiceClient::new(IndexingServiceMailbox::new(mailbox))
    }
    pub fn tower() -> IndexingServiceTowerBlockBuilder {
        IndexingServiceTowerBlockBuilder::default()
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn mock() -> MockIndexingService {
        MockIndexingService::new()
    }
}
#[async_trait::async_trait]
impl IndexingService for IndexingServiceClient {
    async fn apply_indexing_plan(
        &mut self,
        request: ApplyIndexingPlanRequest,
    ) -> crate::indexing::IndexingResult<ApplyIndexingPlanResponse> {
        self.inner.apply_indexing_plan(request).await
    }
}
#[cfg(any(test, feature = "testsuite"))]
pub mod indexing_service_mock {
    use super::*;
    #[derive(Debug, Clone)]
    struct MockIndexingServiceWrapper {
        inner: std::sync::Arc<tokio::sync::Mutex<MockIndexingService>>,
    }
    #[async_trait::async_trait]
    impl IndexingService for MockIndexingServiceWrapper {
        async fn apply_indexing_plan(
            &mut self,
            request: super::ApplyIndexingPlanRequest,
        ) -> crate::indexing::IndexingResult<super::ApplyIndexingPlanResponse> {
            self.inner.lock().await.apply_indexing_plan(request).await
        }
    }
    impl From<MockIndexingService> for IndexingServiceClient {
        fn from(mock: MockIndexingService) -> Self {
            let mock_wrapper = MockIndexingServiceWrapper {
                inner: std::sync::Arc::new(tokio::sync::Mutex::new(mock)),
            };
            IndexingServiceClient::new(mock_wrapper)
        }
    }
}
pub type BoxFuture<T, E> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>,
>;
impl tower::Service<ApplyIndexingPlanRequest> for Box<dyn IndexingService> {
    type Response = ApplyIndexingPlanResponse;
    type Error = crate::indexing::IndexingError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ApplyIndexingPlanRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.apply_indexing_plan(request).await };
        Box::pin(fut)
    }
}
/// A tower block is a set of towers. Each tower is stack of layers (middlewares) that are applied to a service.
#[derive(Debug)]
struct IndexingServiceTowerBlock {
    apply_indexing_plan_svc: quickwit_common::tower::BoxService<
        ApplyIndexingPlanRequest,
        ApplyIndexingPlanResponse,
        crate::indexing::IndexingError,
    >,
}
impl Clone for IndexingServiceTowerBlock {
    fn clone(&self) -> Self {
        Self {
            apply_indexing_plan_svc: self.apply_indexing_plan_svc.clone(),
        }
    }
}
#[async_trait::async_trait]
impl IndexingService for IndexingServiceTowerBlock {
    async fn apply_indexing_plan(
        &mut self,
        request: ApplyIndexingPlanRequest,
    ) -> crate::indexing::IndexingResult<ApplyIndexingPlanResponse> {
        self.apply_indexing_plan_svc.ready().await?.call(request).await
    }
}
#[derive(Debug, Default)]
pub struct IndexingServiceTowerBlockBuilder {
    #[allow(clippy::type_complexity)]
    apply_indexing_plan_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IndexingService>,
            ApplyIndexingPlanRequest,
            ApplyIndexingPlanResponse,
            crate::indexing::IndexingError,
        >,
    >,
}
impl IndexingServiceTowerBlockBuilder {
    pub fn shared_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IndexingService>> + Clone + Send + Sync + 'static,
        L::Service: tower::Service<
                ApplyIndexingPlanRequest,
                Response = ApplyIndexingPlanResponse,
                Error = crate::indexing::IndexingError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ApplyIndexingPlanRequest>>::Future: Send + 'static,
    {
        self
            .apply_indexing_plan_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn apply_indexing_plan_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IndexingService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                ApplyIndexingPlanRequest,
                Response = ApplyIndexingPlanResponse,
                Error = crate::indexing::IndexingError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<ApplyIndexingPlanRequest>>::Future: Send + 'static,
    {
        self
            .apply_indexing_plan_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn build<T>(self, instance: T) -> IndexingServiceClient
    where
        T: IndexingService,
    {
        self.build_from_boxed(Box::new(instance))
    }
    pub fn build_from_channel<T, C>(self, channel: C) -> IndexingServiceClient
    where
        C: tower::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<hyper::Body>,
                Error = quickwit_common::tower::BoxError,
            > + std::fmt::Debug + Clone + Send + Sync + 'static,
        <C as tower::Service<
            http::Request<tonic::body::BoxBody>,
        >>::Future: std::future::Future<
                Output = Result<
                    http::Response<hyper::Body>,
                    quickwit_common::tower::BoxError,
                >,
            > + Send + 'static,
    {
        self.build_from_boxed(Box::new(IndexingServiceClient::from_channel(channel)))
    }
    pub fn build_from_mailbox<A>(
        self,
        mailbox: quickwit_actors::Mailbox<A>,
    ) -> IndexingServiceClient
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        IndexingServiceMailbox<A>: IndexingService,
    {
        self.build_from_boxed(Box::new(IndexingServiceClient::from_mailbox(mailbox)))
    }
    fn build_from_boxed(
        self,
        boxed_instance: Box<dyn IndexingService>,
    ) -> IndexingServiceClient {
        let apply_indexing_plan_svc = if let Some(layer) = self.apply_indexing_plan_layer
        {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let tower_block = IndexingServiceTowerBlock {
            apply_indexing_plan_svc,
        };
        IndexingServiceClient::new(tower_block)
    }
}
#[derive(Debug, Clone)]
struct MailboxAdapter<A: quickwit_actors::Actor, E> {
    inner: quickwit_actors::Mailbox<A>,
    phantom: std::marker::PhantomData<E>,
}
impl<A, E> std::ops::Deref for MailboxAdapter<A, E>
where
    A: quickwit_actors::Actor,
{
    type Target = quickwit_actors::Mailbox<A>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
#[derive(Debug)]
pub struct IndexingServiceMailbox<A: quickwit_actors::Actor> {
    inner: MailboxAdapter<A, crate::indexing::IndexingError>,
}
impl<A: quickwit_actors::Actor> IndexingServiceMailbox<A> {
    pub fn new(instance: quickwit_actors::Mailbox<A>) -> Self {
        let inner = MailboxAdapter {
            inner: instance,
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A: quickwit_actors::Actor> Clone for IndexingServiceMailbox<A> {
    fn clone(&self) -> Self {
        let inner = MailboxAdapter {
            inner: self.inner.clone(),
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A, M, T, E> tower::Service<M> for IndexingServiceMailbox<A>
where
    A: quickwit_actors::Actor
        + quickwit_actors::DeferableReplyHandler<M, Reply = Result<T, E>> + Send
        + 'static,
    M: std::fmt::Debug + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Debug + Send + 'static,
    crate::indexing::IndexingError: From<quickwit_actors::AskError<E>>,
{
    type Response = T;
    type Error = crate::indexing::IndexingError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        //! This does not work with balance middlewares such as `tower::balance::pool::Pool` because
        //! this always returns `Poll::Ready`. The fix is to acquire a permit from the
        //! mailbox in `poll_ready` and consume it in `call`.
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, message: M) -> Self::Future {
        let mailbox = self.inner.clone();
        let fut = async move {
            mailbox.ask_for_res(message).await.map_err(|error| error.into())
        };
        Box::pin(fut)
    }
}
#[async_trait::async_trait]
impl<A> IndexingService for IndexingServiceMailbox<A>
where
    A: quickwit_actors::Actor + std::fmt::Debug,
    IndexingServiceMailbox<
        A,
    >: tower::Service<
        ApplyIndexingPlanRequest,
        Response = ApplyIndexingPlanResponse,
        Error = crate::indexing::IndexingError,
        Future = BoxFuture<ApplyIndexingPlanResponse, crate::indexing::IndexingError>,
    >,
{
    async fn apply_indexing_plan(
        &mut self,
        request: ApplyIndexingPlanRequest,
    ) -> crate::indexing::IndexingResult<ApplyIndexingPlanResponse> {
        self.call(request).await
    }
}
#[derive(Debug, Clone)]
pub struct IndexingServiceGrpcClientAdapter<T> {
    inner: T,
}
impl<T> IndexingServiceGrpcClientAdapter<T> {
    pub fn new(instance: T) -> Self {
        Self { inner: instance }
    }
}
#[async_trait::async_trait]
impl<T> IndexingService
for IndexingServiceGrpcClientAdapter<
    indexing_service_grpc_client::IndexingServiceGrpcClient<T>,
>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + std::fmt::Debug + Clone + Send
        + Sync + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError>
        + Send,
    T::Future: Send,
{
    async fn apply_indexing_plan(
        &mut self,
        request: ApplyIndexingPlanRequest,
    ) -> crate::indexing::IndexingResult<ApplyIndexingPlanResponse> {
        self.inner
            .apply_indexing_plan(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
}
#[derive(Debug)]
pub struct IndexingServiceGrpcServerAdapter {
    inner: Box<dyn IndexingService>,
}
impl IndexingServiceGrpcServerAdapter {
    pub fn new<T>(instance: T) -> Self
    where
        T: IndexingService,
    {
        Self { inner: Box::new(instance) }
    }
}
#[async_trait::async_trait]
impl indexing_service_grpc_server::IndexingServiceGrpc
for IndexingServiceGrpcServerAdapter {
    async fn apply_indexing_plan(
        &self,
        request: tonic::Request<ApplyIndexingPlanRequest>,
    ) -> Result<tonic::Response<ApplyIndexingPlanResponse>, tonic::Status> {
        self.inner
            .clone()
            .apply_indexing_plan(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
}
/// Generated client implementations.
pub mod indexing_service_grpc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct IndexingServiceGrpcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl IndexingServiceGrpcClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> IndexingServiceGrpcClient<T>
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
        ) -> IndexingServiceGrpcClient<InterceptedService<T, F>>
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
            IndexingServiceGrpcClient::new(InterceptedService::new(inner, interceptor))
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
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        /// / Apply an indexing plan on the node.
        pub async fn apply_indexing_plan(
            &mut self,
            request: impl tonic::IntoRequest<super::ApplyIndexingPlanRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ApplyIndexingPlanResponse>,
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
                "/quickwit.indexing.IndexingService/ApplyIndexingPlan",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.indexing.IndexingService",
                        "ApplyIndexingPlan",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod indexing_service_grpc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with IndexingServiceGrpcServer.
    #[async_trait]
    pub trait IndexingServiceGrpc: Send + Sync + 'static {
        /// / Apply an indexing plan on the node.
        async fn apply_indexing_plan(
            &self,
            request: tonic::Request<super::ApplyIndexingPlanRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ApplyIndexingPlanResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct IndexingServiceGrpcServer<T: IndexingServiceGrpc> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: IndexingServiceGrpc> IndexingServiceGrpcServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
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
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for IndexingServiceGrpcServer<T>
    where
        T: IndexingServiceGrpc,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/quickwit.indexing.IndexingService/ApplyIndexingPlan" => {
                    #[allow(non_camel_case_types)]
                    struct ApplyIndexingPlanSvc<T: IndexingServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IndexingServiceGrpc,
                    > tonic::server::UnaryService<super::ApplyIndexingPlanRequest>
                    for ApplyIndexingPlanSvc<T> {
                        type Response = super::ApplyIndexingPlanResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ApplyIndexingPlanRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).apply_indexing_plan(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ApplyIndexingPlanSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
    impl<T: IndexingServiceGrpc> Clone for IndexingServiceGrpcServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: IndexingServiceGrpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: IndexingServiceGrpc> tonic::server::NamedService
    for IndexingServiceGrpcServer<T> {
        const NAME: &'static str = "quickwit.indexing.IndexingService";
    }
}
