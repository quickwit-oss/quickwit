#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueueExistsRequest {
    #[prost(string, tag = "1")]
    pub queue_id: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateQueueRequest {
    #[prost(string, tag = "1")]
    pub queue_id: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateQueueIfNotExistsRequest {
    #[prost(string, tag = "1")]
    pub queue_id: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropQueueRequest {
    #[prost(string, tag = "1")]
    pub queue_id: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestRequest {
    #[prost(message, repeated, tag = "1")]
    pub doc_batches: ::prost::alloc::vec::Vec<DocBatch>,
    #[prost(uint32, tag = "2")]
    pub commit: u32,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestResponse {
    #[prost(uint64, tag = "1")]
    pub num_docs_for_processing: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchRequest {
    #[prost(string, tag = "1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(uint64, optional, tag = "2")]
    pub start_after: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "3")]
    pub num_bytes_limit: ::core::option::Option<u64>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchResponse {
    #[prost(uint64, optional, tag = "1")]
    pub first_position: ::core::option::Option<u64>,
    #[prost(message, optional, tag = "2")]
    pub doc_batch: ::core::option::Option<DocBatch>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DocBatch {
    #[prost(string, tag = "1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(bytes = "bytes", tag = "2")]
    #[schema(value_type = String, format = Binary)]
    pub concat_docs: ::prost::bytes::Bytes,
    #[prost(uint64, repeated, tag = "3")]
    pub doc_lens: ::prost::alloc::vec::Vec<u64>,
}
/// / Suggest to truncate the queue.
/// /
/// / This function allows the queue to remove all records up to and
/// / including `up_to_offset_included`.
/// /
/// / The role of this truncation is to release memory and disk space.
/// /
/// / There are no guarantees that the record will effectively be removed.
/// / Nothing might happen, or the truncation might be partial.
/// /
/// / In other words, truncating from a position, and fetching records starting
/// / earlier than this position can yield undefined result:
/// / the truncated records may or may not be returned.
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SuggestTruncateRequest {
    #[prost(string, tag = "1")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub up_to_position_included: u64,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TailRequest {
    #[prost(string, tag = "1")]
    pub index_id: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListQueuesRequest {}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListQueuesResponse {
    #[prost(string, repeated, tag = "1")]
    pub queues: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// BEGIN quickwit-codegen
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait::async_trait]
pub trait IngestService: std::fmt::Debug + dyn_clone::DynClone + Send + Sync + 'static {
    async fn ingest(&mut self, request: IngestRequest) -> crate::Result<IngestResponse>;
    async fn fetch(&mut self, request: FetchRequest) -> crate::Result<FetchResponse>;
    async fn tail(&mut self, request: TailRequest) -> crate::Result<FetchResponse>;
}
dyn_clone::clone_trait_object!(IngestService);
#[cfg(any(test, feature = "testsuite"))]
impl Clone for MockIngestService {
    fn clone(&self) -> Self {
        MockIngestService::new()
    }
}
#[derive(Debug, Clone)]
pub struct IngestServiceClient {
    inner: Box<dyn IngestService>,
}
impl IngestServiceClient {
    pub fn new<T>(instance: T) -> Self
    where
        T: IngestService,
    {
        Self { inner: Box::new(instance) }
    }
    pub fn from_channel(
        channel: tower::timeout::Timeout<tonic::transport::Channel>,
    ) -> Self {
        IngestServiceClient::new(
            IngestServiceGrpcClientAdapter::new(
                ingest_service_grpc_client::IngestServiceGrpcClient::new(channel),
            ),
        )
    }
    pub fn from_mailbox<A>(mailbox: quickwit_actors::Mailbox<A>) -> Self
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + Sync + 'static,
        IngestServiceMailbox<A>: IngestService,
    {
        IngestServiceClient::new(IngestServiceMailbox::new(mailbox))
    }
    pub fn tower() -> IngestServiceTowerBlockBuilder {
        IngestServiceTowerBlockBuilder::default()
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn mock() -> MockIngestService {
        MockIngestService::new()
    }
}
#[async_trait::async_trait]
impl IngestService for IngestServiceClient {
    async fn ingest(&mut self, request: IngestRequest) -> crate::Result<IngestResponse> {
        self.inner.ingest(request).await
    }
    async fn fetch(&mut self, request: FetchRequest) -> crate::Result<FetchResponse> {
        self.inner.fetch(request).await
    }
    async fn tail(&mut self, request: TailRequest) -> crate::Result<FetchResponse> {
        self.inner.tail(request).await
    }
}
#[cfg(any(test, feature = "testsuite"))]
impl From<MockIngestService> for IngestServiceClient {
    fn from(mock: MockIngestService) -> Self {
        IngestServiceClient::new(mock)
    }
}
pub type BoxFuture<T, E> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>,
>;
impl tower::Service<IngestRequest> for Box<dyn IngestService> {
    type Response = IngestResponse;
    type Error = crate::IngestServiceError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: IngestRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.ingest(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<FetchRequest> for Box<dyn IngestService> {
    type Response = FetchResponse;
    type Error = crate::IngestServiceError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: FetchRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.fetch(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<TailRequest> for Box<dyn IngestService> {
    type Response = FetchResponse;
    type Error = crate::IngestServiceError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: TailRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.tail(request).await };
        Box::pin(fut)
    }
}
/// A tower block is a set of towers. Each tower is stack of layers (middlewares) that are applied to a service.
#[derive(Debug)]
struct IngestServiceTowerBlock {
    ingest_svc: quickwit_common::tower::BoxService<
        IngestRequest,
        IngestResponse,
        crate::IngestServiceError,
    >,
    fetch_svc: quickwit_common::tower::BoxService<
        FetchRequest,
        FetchResponse,
        crate::IngestServiceError,
    >,
    tail_svc: quickwit_common::tower::BoxService<
        TailRequest,
        FetchResponse,
        crate::IngestServiceError,
    >,
}
impl Clone for IngestServiceTowerBlock {
    fn clone(&self) -> Self {
        Self {
            ingest_svc: self.ingest_svc.clone(),
            fetch_svc: self.fetch_svc.clone(),
            tail_svc: self.tail_svc.clone(),
        }
    }
}
#[async_trait::async_trait]
impl IngestService for IngestServiceTowerBlock {
    async fn ingest(&mut self, request: IngestRequest) -> crate::Result<IngestResponse> {
        self.ingest_svc.ready().await?.call(request).await
    }
    async fn fetch(&mut self, request: FetchRequest) -> crate::Result<FetchResponse> {
        self.fetch_svc.ready().await?.call(request).await
    }
    async fn tail(&mut self, request: TailRequest) -> crate::Result<FetchResponse> {
        self.tail_svc.ready().await?.call(request).await
    }
}
#[derive(Debug, Default)]
pub struct IngestServiceTowerBlockBuilder {
    ingest_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngestService>,
            IngestRequest,
            IngestResponse,
            crate::IngestServiceError,
        >,
    >,
    fetch_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngestService>,
            FetchRequest,
            FetchResponse,
            crate::IngestServiceError,
        >,
    >,
    tail_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngestService>,
            TailRequest,
            FetchResponse,
            crate::IngestServiceError,
        >,
    >,
}
impl IngestServiceTowerBlockBuilder {
    pub fn shared_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngestService>> + Clone + Send + Sync + 'static,
        L::Service: Service<
                IngestRequest,
                Response = IngestResponse,
                Error = crate::IngestServiceError,
            > + Clone + Send + Sync + 'static,
        <L::Service as Service<IngestRequest>>::Future: Send + 'static,
        L::Service: Service<
                FetchRequest,
                Response = FetchResponse,
                Error = crate::IngestServiceError,
            > + Clone + Send + Sync + 'static,
        <L::Service as Service<FetchRequest>>::Future: Send + 'static,
        L::Service: Service<
                TailRequest,
                Response = FetchResponse,
                Error = crate::IngestServiceError,
            > + Clone + Send + Sync + 'static,
        <L::Service as Service<TailRequest>>::Future: Send + 'static,
    {
        self.ingest_layer = Some(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.fetch_layer = Some(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.tail_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn ingest_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngestService>> + Send + Sync + 'static,
        L::Service: Service<
                IngestRequest,
                Response = IngestResponse,
                Error = crate::IngestServiceError,
            > + Clone + Send + Sync + 'static,
        <L::Service as Service<IngestRequest>>::Future: Send + 'static,
    {
        self.ingest_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn fetch_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngestService>> + Send + Sync + 'static,
        L::Service: Service<
                FetchRequest,
                Response = FetchResponse,
                Error = crate::IngestServiceError,
            > + Clone + Send + Sync + 'static,
        <L::Service as Service<FetchRequest>>::Future: Send + 'static,
    {
        self.fetch_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn tail_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngestService>> + Send + Sync + 'static,
        L::Service: Service<
                TailRequest,
                Response = FetchResponse,
                Error = crate::IngestServiceError,
            > + Clone + Send + Sync + 'static,
        <L::Service as Service<TailRequest>>::Future: Send + 'static,
    {
        self.tail_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn build<T>(self, instance: T) -> IngestServiceClient
    where
        T: IngestService,
    {
        self.build_from_boxed(Box::new(instance))
    }
    pub fn build_from_channel<T>(
        self,
        channel: tower::timeout::Timeout<tonic::transport::Channel>,
    ) -> IngestServiceClient {
        self.build_from_boxed(Box::new(IngestServiceClient::from_channel(channel)))
    }
    pub fn build_from_mailbox<A>(
        self,
        mailbox: quickwit_actors::Mailbox<A>,
    ) -> IngestServiceClient
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + Sync + 'static,
        IngestServiceMailbox<A>: IngestService,
    {
        self.build_from_boxed(Box::new(IngestServiceClient::from_mailbox(mailbox)))
    }
    fn build_from_boxed(
        self,
        boxed_instance: Box<dyn IngestService>,
    ) -> IngestServiceClient {
        let ingest_svc = if let Some(layer) = self.ingest_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let fetch_svc = if let Some(layer) = self.fetch_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let tail_svc = if let Some(layer) = self.tail_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let tower_block = IngestServiceTowerBlock {
            ingest_svc,
            fetch_svc,
            tail_svc,
        };
        IngestServiceClient::new(tower_block)
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
pub struct IngestServiceMailbox<A: quickwit_actors::Actor> {
    inner: MailboxAdapter<A, crate::IngestServiceError>,
}
impl<A: quickwit_actors::Actor> IngestServiceMailbox<A> {
    pub fn new(instance: quickwit_actors::Mailbox<A>) -> Self {
        let inner = MailboxAdapter {
            inner: instance,
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A: quickwit_actors::Actor> Clone for IngestServiceMailbox<A> {
    fn clone(&self) -> Self {
        let inner = MailboxAdapter {
            inner: self.inner.clone(),
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
use tower::{Layer, Service, ServiceExt};
impl<A, M, T, E> tower::Service<M> for IngestServiceMailbox<A>
where
    A: quickwit_actors::Actor
        + quickwit_actors::DeferableReplyHandler<M, Reply = Result<T, E>> + Send + Sync
        + 'static,
    M: std::fmt::Debug + Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: std::fmt::Debug + Send + Sync + 'static,
    crate::IngestServiceError: From<quickwit_actors::AskError<E>>,
{
    type Response = T;
    type Error = crate::IngestServiceError;
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
impl<A> IngestService for IngestServiceMailbox<A>
where
    A: quickwit_actors::Actor + std::fmt::Debug + Send + Sync + 'static,
    IngestServiceMailbox<
        A,
    >: tower::Service<
            IngestRequest,
            Response = IngestResponse,
            Error = crate::IngestServiceError,
            Future = BoxFuture<IngestResponse, crate::IngestServiceError>,
        >
        + tower::Service<
            FetchRequest,
            Response = FetchResponse,
            Error = crate::IngestServiceError,
            Future = BoxFuture<FetchResponse, crate::IngestServiceError>,
        >
        + tower::Service<
            TailRequest,
            Response = FetchResponse,
            Error = crate::IngestServiceError,
            Future = BoxFuture<FetchResponse, crate::IngestServiceError>,
        >,
{
    async fn ingest(&mut self, request: IngestRequest) -> crate::Result<IngestResponse> {
        self.call(request).await
    }
    async fn fetch(&mut self, request: FetchRequest) -> crate::Result<FetchResponse> {
        self.call(request).await
    }
    async fn tail(&mut self, request: TailRequest) -> crate::Result<FetchResponse> {
        self.call(request).await
    }
}
#[derive(Debug, Clone)]
pub struct IngestServiceGrpcClientAdapter<T> {
    inner: T,
}
impl<T> IngestServiceGrpcClientAdapter<T> {
    pub fn new(instance: T) -> Self {
        Self { inner: instance }
    }
}
#[async_trait::async_trait]
impl<T> IngestService
for IngestServiceGrpcClientAdapter<
    ingest_service_grpc_client::IngestServiceGrpcClient<T>,
>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + std::fmt::Debug + Clone + Send
        + Sync + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError>
        + Send,
    T::Future: Send,
{
    async fn ingest(&mut self, request: IngestRequest) -> crate::Result<IngestResponse> {
        self.inner
            .ingest(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn fetch(&mut self, request: FetchRequest) -> crate::Result<FetchResponse> {
        self.inner
            .fetch(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn tail(&mut self, request: TailRequest) -> crate::Result<FetchResponse> {
        self.inner
            .tail(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
}
#[derive(Debug)]
pub struct IngestServiceGrpcServerAdapter {
    inner: Box<dyn IngestService>,
}
impl IngestServiceGrpcServerAdapter {
    pub fn new<T>(instance: T) -> Self
    where
        T: IngestService,
    {
        Self { inner: Box::new(instance) }
    }
}
#[async_trait::async_trait]
impl ingest_service_grpc_server::IngestServiceGrpc for IngestServiceGrpcServerAdapter {
    async fn ingest(
        &self,
        request: tonic::Request<IngestRequest>,
    ) -> Result<tonic::Response<IngestResponse>, tonic::Status> {
        self.inner
            .clone()
            .ingest(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(Into::into)
    }
    async fn fetch(
        &self,
        request: tonic::Request<FetchRequest>,
    ) -> Result<tonic::Response<FetchResponse>, tonic::Status> {
        self.inner
            .clone()
            .fetch(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(Into::into)
    }
    async fn tail(
        &self,
        request: tonic::Request<TailRequest>,
    ) -> Result<tonic::Response<FetchResponse>, tonic::Status> {
        self.inner
            .clone()
            .tail(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(Into::into)
    }
}
/// Generated client implementations.
pub mod ingest_service_grpc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct IngestServiceGrpcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl IngestServiceGrpcClient<tonic::transport::Channel> {
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
    impl<T> IngestServiceGrpcClient<T>
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
        ) -> IngestServiceGrpcClient<InterceptedService<T, F>>
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
            IngestServiceGrpcClient::new(InterceptedService::new(inner, interceptor))
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
        /// / Ingests document in a given queue.
        /// /
        /// / Upon any kind of error, the client should
        /// / - retry to get at least once delivery.
        /// / - not retry to get at most once delivery.
        /// /
        /// / Exactly once delivery is not supported yet.
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
                "/ingest_service.IngestService/Ingest",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// / Fetches record from a given queue.
        /// /
        /// / Records are returned in order.
        /// /
        /// / The returned `FetchResponse` object is meant to be read with the
        /// / `crate::iter_records` function.
        /// /
        /// / Fetching does not necessarily return all of the available records.
        /// / If returning all records would exceed `FETCH_PAYLOAD_LIMIT` (2MB),
        /// / the response will be partial.
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
                "/ingest_service.IngestService/Fetch",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// / Returns a batch containing the last records.
        /// /
        /// / It returns the last documents, from the newest
        /// / to the oldest, and stops as soon as `FETCH_PAYLOAD_LIMIT` (2MB)
        /// / is exceeded.
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
                "/ingest_service.IngestService/Tail",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod ingest_service_grpc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with IngestServiceGrpcServer.
    #[async_trait]
    pub trait IngestServiceGrpc: Send + Sync + 'static {
        /// / Ingests document in a given queue.
        /// /
        /// / Upon any kind of error, the client should
        /// / - retry to get at least once delivery.
        /// / - not retry to get at most once delivery.
        /// /
        /// / Exactly once delivery is not supported yet.
        async fn ingest(
            &self,
            request: tonic::Request<super::IngestRequest>,
        ) -> Result<tonic::Response<super::IngestResponse>, tonic::Status>;
        /// / Fetches record from a given queue.
        /// /
        /// / Records are returned in order.
        /// /
        /// / The returned `FetchResponse` object is meant to be read with the
        /// / `crate::iter_records` function.
        /// /
        /// / Fetching does not necessarily return all of the available records.
        /// / If returning all records would exceed `FETCH_PAYLOAD_LIMIT` (2MB),
        /// / the response will be partial.
        async fn fetch(
            &self,
            request: tonic::Request<super::FetchRequest>,
        ) -> Result<tonic::Response<super::FetchResponse>, tonic::Status>;
        /// / Returns a batch containing the last records.
        /// /
        /// / It returns the last documents, from the newest
        /// / to the oldest, and stops as soon as `FETCH_PAYLOAD_LIMIT` (2MB)
        /// / is exceeded.
        async fn tail(
            &self,
            request: tonic::Request<super::TailRequest>,
        ) -> Result<tonic::Response<super::FetchResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct IngestServiceGrpcServer<T: IngestServiceGrpc> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: IngestServiceGrpc> IngestServiceGrpcServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for IngestServiceGrpcServer<T>
    where
        T: IngestServiceGrpc,
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
                "/ingest_service.IngestService/Ingest" => {
                    #[allow(non_camel_case_types)]
                    struct IngestSvc<T: IngestServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngestServiceGrpc,
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
                "/ingest_service.IngestService/Fetch" => {
                    #[allow(non_camel_case_types)]
                    struct FetchSvc<T: IngestServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngestServiceGrpc,
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
                "/ingest_service.IngestService/Tail" => {
                    #[allow(non_camel_case_types)]
                    struct TailSvc<T: IngestServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngestServiceGrpc,
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
    impl<T: IngestServiceGrpc> Clone for IngestServiceGrpcServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: IngestServiceGrpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: IngestServiceGrpc> tonic::server::NamedService
    for IngestServiceGrpcServer<T> {
        const NAME: &'static str = "ingest_service.IngestService";
    }
}
