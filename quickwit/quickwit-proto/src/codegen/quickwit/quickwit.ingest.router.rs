#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestRequestV2 {
    #[prost(message, repeated, tag = "1")]
    pub subrequests: ::prost::alloc::vec::Vec<IngestSubrequest>,
    #[prost(enumeration = "super::CommitTypeV2", tag = "2")]
    pub commit_type: i32,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestSubrequest {
    /// The subrequest ID is used to identify the various subrequests and responses
    /// (ingest, persist, replicate) at play during the ingest and replication
    /// process.
    #[prost(uint32, tag = "1")]
    pub subrequest_id: u32,
    #[prost(string, tag = "2")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "4")]
    pub doc_batch: ::core::option::Option<super::DocBatchV2>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestResponseV2 {
    #[prost(message, repeated, tag = "1")]
    pub successes: ::prost::alloc::vec::Vec<IngestSuccess>,
    #[prost(message, repeated, tag = "2")]
    pub failures: ::prost::alloc::vec::Vec<IngestFailure>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestSuccess {
    #[prost(uint32, tag = "1")]
    pub subrequest_id: u32,
    #[prost(string, tag = "2")]
    pub index_uid: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "4")]
    pub shard_id: u64,
    /// Replication position inclusive.
    #[prost(message, optional, tag = "5")]
    pub replication_position_inclusive: ::core::option::Option<crate::types::Position>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IngestFailure {
    #[prost(uint32, tag = "1")]
    pub subrequest_id: u32,
    #[prost(string, tag = "2")]
    pub index_id: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(enumeration = "IngestFailureReason", tag = "5")]
    pub reason: i32,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum IngestFailureReason {
    Unspecified = 0,
    NoShardsAvailable = 1,
}
impl IngestFailureReason {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            IngestFailureReason::Unspecified => "INGEST_FAILURE_REASON_UNSPECIFIED",
            IngestFailureReason::NoShardsAvailable => {
                "INGEST_FAILURE_REASON_NO_SHARDS_AVAILABLE"
            }
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "INGEST_FAILURE_REASON_UNSPECIFIED" => Some(Self::Unspecified),
            "INGEST_FAILURE_REASON_NO_SHARDS_AVAILABLE" => Some(Self::NoShardsAvailable),
            _ => None,
        }
    }
}
/// BEGIN quickwit-codegen
use tower::{Layer, Service, ServiceExt};
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait::async_trait]
pub trait IngestRouterService: std::fmt::Debug + dyn_clone::DynClone + Send + Sync + 'static {
    /// Ingests batches of documents for one or multiple indexes.
    /// TODO: Describe error cases and how to handle them.
    async fn ingest(
        &mut self,
        request: IngestRequestV2,
    ) -> crate::ingest::IngestV2Result<IngestResponseV2>;
}
dyn_clone::clone_trait_object!(IngestRouterService);
#[cfg(any(test, feature = "testsuite"))]
impl Clone for MockIngestRouterService {
    fn clone(&self) -> Self {
        MockIngestRouterService::new()
    }
}
#[derive(Debug, Clone)]
pub struct IngestRouterServiceClient {
    inner: Box<dyn IngestRouterService>,
}
impl IngestRouterServiceClient {
    pub fn new<T>(instance: T) -> Self
    where
        T: IngestRouterService,
    {
        #[cfg(any(test, feature = "testsuite"))]
        assert!(
            std::any::TypeId::of:: < T > () != std::any::TypeId::of:: <
            MockIngestRouterService > (),
            "`MockIngestRouterService` must be wrapped in a `MockIngestRouterServiceWrapper`. Use `MockIngestRouterService::from(mock)` to instantiate the client."
        );
        Self { inner: Box::new(instance) }
    }
    pub fn as_grpc_service(
        &self,
    ) -> ingest_router_service_grpc_server::IngestRouterServiceGrpcServer<
        IngestRouterServiceGrpcServerAdapter,
    > {
        let adapter = IngestRouterServiceGrpcServerAdapter::new(self.clone());
        ingest_router_service_grpc_server::IngestRouterServiceGrpcServer::new(adapter)
    }
    pub fn from_channel(
        addr: std::net::SocketAddr,
        channel: tonic::transport::Channel,
    ) -> Self {
        let (_, connection_keys_watcher) = tokio::sync::watch::channel(
            std::collections::HashSet::from_iter([addr]),
        );
        let adapter = IngestRouterServiceGrpcClientAdapter::new(
            ingest_router_service_grpc_client::IngestRouterServiceGrpcClient::new(
                channel,
            ),
            connection_keys_watcher,
        );
        Self::new(adapter)
    }
    pub fn from_balance_channel(
        balance_channel: quickwit_common::tower::BalanceChannel<std::net::SocketAddr>,
    ) -> IngestRouterServiceClient {
        let connection_keys_watcher = balance_channel.connection_keys_watcher();
        let adapter = IngestRouterServiceGrpcClientAdapter::new(
            ingest_router_service_grpc_client::IngestRouterServiceGrpcClient::new(
                balance_channel,
            ),
            connection_keys_watcher,
        );
        Self::new(adapter)
    }
    pub fn from_mailbox<A>(mailbox: quickwit_actors::Mailbox<A>) -> Self
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        IngestRouterServiceMailbox<A>: IngestRouterService,
    {
        IngestRouterServiceClient::new(IngestRouterServiceMailbox::new(mailbox))
    }
    pub fn tower() -> IngestRouterServiceTowerBlockBuilder {
        IngestRouterServiceTowerBlockBuilder::default()
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn mock() -> MockIngestRouterService {
        MockIngestRouterService::new()
    }
}
#[async_trait::async_trait]
impl IngestRouterService for IngestRouterServiceClient {
    async fn ingest(
        &mut self,
        request: IngestRequestV2,
    ) -> crate::ingest::IngestV2Result<IngestResponseV2> {
        self.inner.ingest(request).await
    }
}
#[cfg(any(test, feature = "testsuite"))]
pub mod ingest_router_service_mock {
    use super::*;
    #[derive(Debug, Clone)]
    struct MockIngestRouterServiceWrapper {
        inner: std::sync::Arc<tokio::sync::Mutex<MockIngestRouterService>>,
    }
    #[async_trait::async_trait]
    impl IngestRouterService for MockIngestRouterServiceWrapper {
        async fn ingest(
            &mut self,
            request: super::IngestRequestV2,
        ) -> crate::ingest::IngestV2Result<super::IngestResponseV2> {
            self.inner.lock().await.ingest(request).await
        }
    }
    impl From<MockIngestRouterService> for IngestRouterServiceClient {
        fn from(mock: MockIngestRouterService) -> Self {
            let mock_wrapper = MockIngestRouterServiceWrapper {
                inner: std::sync::Arc::new(tokio::sync::Mutex::new(mock)),
            };
            IngestRouterServiceClient::new(mock_wrapper)
        }
    }
}
pub type BoxFuture<T, E> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>,
>;
impl tower::Service<IngestRequestV2> for Box<dyn IngestRouterService> {
    type Response = IngestResponseV2;
    type Error = crate::ingest::IngestV2Error;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: IngestRequestV2) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.ingest(request).await };
        Box::pin(fut)
    }
}
/// A tower block is a set of towers. Each tower is stack of layers (middlewares) that are applied to a service.
#[derive(Debug)]
struct IngestRouterServiceTowerBlock {
    inner: Box<dyn IngestRouterService>,
    ingest_svc: quickwit_common::tower::BoxService<
        IngestRequestV2,
        IngestResponseV2,
        crate::ingest::IngestV2Error,
    >,
}
impl Clone for IngestRouterServiceTowerBlock {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            ingest_svc: self.ingest_svc.clone(),
        }
    }
}
#[async_trait::async_trait]
impl IngestRouterService for IngestRouterServiceTowerBlock {
    async fn ingest(
        &mut self,
        request: IngestRequestV2,
    ) -> crate::ingest::IngestV2Result<IngestResponseV2> {
        self.ingest_svc.ready().await?.call(request).await
    }
}
#[derive(Debug, Default)]
pub struct IngestRouterServiceTowerBlockBuilder {
    #[allow(clippy::type_complexity)]
    ingest_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn IngestRouterService>,
            IngestRequestV2,
            IngestResponseV2,
            crate::ingest::IngestV2Error,
        >,
    >,
}
impl IngestRouterServiceTowerBlockBuilder {
    pub fn shared_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngestRouterService>> + Clone + Send + Sync + 'static,
        L::Service: tower::Service<
                IngestRequestV2,
                Response = IngestResponseV2,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<IngestRequestV2>>::Future: Send + 'static,
    {
        self.ingest_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn ingest_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn IngestRouterService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                IngestRequestV2,
                Response = IngestResponseV2,
                Error = crate::ingest::IngestV2Error,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<IngestRequestV2>>::Future: Send + 'static,
    {
        self.ingest_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn build<T>(self, instance: T) -> IngestRouterServiceClient
    where
        T: IngestRouterService,
    {
        self.build_from_boxed(Box::new(instance))
    }
    pub fn build_from_channel(
        self,
        addr: std::net::SocketAddr,
        channel: tonic::transport::Channel,
    ) -> IngestRouterServiceClient {
        self.build_from_boxed(
            Box::new(IngestRouterServiceClient::from_channel(addr, channel)),
        )
    }
    pub fn build_from_balance_channel(
        self,
        balance_channel: quickwit_common::tower::BalanceChannel<std::net::SocketAddr>,
    ) -> IngestRouterServiceClient {
        self.build_from_boxed(
            Box::new(IngestRouterServiceClient::from_balance_channel(balance_channel)),
        )
    }
    pub fn build_from_mailbox<A>(
        self,
        mailbox: quickwit_actors::Mailbox<A>,
    ) -> IngestRouterServiceClient
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        IngestRouterServiceMailbox<A>: IngestRouterService,
    {
        self.build_from_boxed(Box::new(IngestRouterServiceMailbox::new(mailbox)))
    }
    fn build_from_boxed(
        self,
        boxed_instance: Box<dyn IngestRouterService>,
    ) -> IngestRouterServiceClient {
        let ingest_svc = if let Some(layer) = self.ingest_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let tower_block = IngestRouterServiceTowerBlock {
            inner: boxed_instance.clone(),
            ingest_svc,
        };
        IngestRouterServiceClient::new(tower_block)
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
pub struct IngestRouterServiceMailbox<A: quickwit_actors::Actor> {
    inner: MailboxAdapter<A, crate::ingest::IngestV2Error>,
}
impl<A: quickwit_actors::Actor> IngestRouterServiceMailbox<A> {
    pub fn new(instance: quickwit_actors::Mailbox<A>) -> Self {
        let inner = MailboxAdapter {
            inner: instance,
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A: quickwit_actors::Actor> Clone for IngestRouterServiceMailbox<A> {
    fn clone(&self) -> Self {
        let inner = MailboxAdapter {
            inner: self.inner.clone(),
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A, M, T, E> tower::Service<M> for IngestRouterServiceMailbox<A>
where
    A: quickwit_actors::Actor
        + quickwit_actors::DeferableReplyHandler<M, Reply = Result<T, E>> + Send
        + 'static,
    M: std::fmt::Debug + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Debug + Send + 'static,
    crate::ingest::IngestV2Error: From<quickwit_actors::AskError<E>>,
{
    type Response = T;
    type Error = crate::ingest::IngestV2Error;
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
impl<A> IngestRouterService for IngestRouterServiceMailbox<A>
where
    A: quickwit_actors::Actor + std::fmt::Debug,
    IngestRouterServiceMailbox<
        A,
    >: tower::Service<
        IngestRequestV2,
        Response = IngestResponseV2,
        Error = crate::ingest::IngestV2Error,
        Future = BoxFuture<IngestResponseV2, crate::ingest::IngestV2Error>,
    >,
{
    async fn ingest(
        &mut self,
        request: IngestRequestV2,
    ) -> crate::ingest::IngestV2Result<IngestResponseV2> {
        self.call(request).await
    }
}
#[derive(Debug, Clone)]
pub struct IngestRouterServiceGrpcClientAdapter<T> {
    inner: T,
    #[allow(dead_code)]
    connection_addrs_rx: tokio::sync::watch::Receiver<
        std::collections::HashSet<std::net::SocketAddr>,
    >,
}
impl<T> IngestRouterServiceGrpcClientAdapter<T> {
    pub fn new(
        instance: T,
        connection_addrs_rx: tokio::sync::watch::Receiver<
            std::collections::HashSet<std::net::SocketAddr>,
        >,
    ) -> Self {
        Self {
            inner: instance,
            connection_addrs_rx,
        }
    }
}
#[async_trait::async_trait]
impl<T> IngestRouterService
for IngestRouterServiceGrpcClientAdapter<
    ingest_router_service_grpc_client::IngestRouterServiceGrpcClient<T>,
>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + std::fmt::Debug + Clone + Send
        + Sync + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError>
        + Send,
    T::Future: Send,
{
    async fn ingest(
        &mut self,
        request: IngestRequestV2,
    ) -> crate::ingest::IngestV2Result<IngestResponseV2> {
        self.inner
            .ingest(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
}
#[derive(Debug)]
pub struct IngestRouterServiceGrpcServerAdapter {
    inner: Box<dyn IngestRouterService>,
}
impl IngestRouterServiceGrpcServerAdapter {
    pub fn new<T>(instance: T) -> Self
    where
        T: IngestRouterService,
    {
        Self { inner: Box::new(instance) }
    }
}
#[async_trait::async_trait]
impl ingest_router_service_grpc_server::IngestRouterServiceGrpc
for IngestRouterServiceGrpcServerAdapter {
    async fn ingest(
        &self,
        request: tonic::Request<IngestRequestV2>,
    ) -> Result<tonic::Response<IngestResponseV2>, tonic::Status> {
        self.inner
            .clone()
            .ingest(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
}
/// Generated client implementations.
pub mod ingest_router_service_grpc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct IngestRouterServiceGrpcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl IngestRouterServiceGrpcClient<tonic::transport::Channel> {
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
    impl<T> IngestRouterServiceGrpcClient<T>
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
        ) -> IngestRouterServiceGrpcClient<InterceptedService<T, F>>
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
            IngestRouterServiceGrpcClient::new(
                InterceptedService::new(inner, interceptor),
            )
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
        /// Ingests batches of documents for one or multiple indexes.
        /// TODO: Describe error cases and how to handle them.
        pub async fn ingest(
            &mut self,
            request: impl tonic::IntoRequest<super::IngestRequestV2>,
        ) -> std::result::Result<
            tonic::Response<super::IngestResponseV2>,
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
                "/quickwit.ingest.router.IngestRouterService/Ingest",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.ingest.router.IngestRouterService",
                        "Ingest",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod ingest_router_service_grpc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with IngestRouterServiceGrpcServer.
    #[async_trait]
    pub trait IngestRouterServiceGrpc: Send + Sync + 'static {
        /// Ingests batches of documents for one or multiple indexes.
        /// TODO: Describe error cases and how to handle them.
        async fn ingest(
            &self,
            request: tonic::Request<super::IngestRequestV2>,
        ) -> std::result::Result<
            tonic::Response<super::IngestResponseV2>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct IngestRouterServiceGrpcServer<T: IngestRouterServiceGrpc> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: IngestRouterServiceGrpc> IngestRouterServiceGrpcServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>>
    for IngestRouterServiceGrpcServer<T>
    where
        T: IngestRouterServiceGrpc,
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
                "/quickwit.ingest.router.IngestRouterService/Ingest" => {
                    #[allow(non_camel_case_types)]
                    struct IngestSvc<T: IngestRouterServiceGrpc>(pub Arc<T>);
                    impl<
                        T: IngestRouterServiceGrpc,
                    > tonic::server::UnaryService<super::IngestRequestV2>
                    for IngestSvc<T> {
                        type Response = super::IngestResponseV2;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::IngestRequestV2>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).ingest(request).await };
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
                        let method = IngestSvc(inner);
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
    impl<T: IngestRouterServiceGrpc> Clone for IngestRouterServiceGrpcServer<T> {
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
    impl<T: IngestRouterServiceGrpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: IngestRouterServiceGrpc> tonic::server::NamedService
    for IngestRouterServiceGrpcServer<T> {
        const NAME: &'static str = "quickwit.ingest.router.IngestRouterService";
    }
}
