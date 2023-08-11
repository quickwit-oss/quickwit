#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NotifySplitsChangeRequest {
    #[prost(message, repeated, tag = "1")]
    pub splits_change: ::prost::alloc::vec::Vec<SplitsChangeNotification>,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NotifySplitsChangeResponse {}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SplitsChangeNotification {
    /// / Storage uri.
    #[prost(string, tag = "1")]
    pub storage_uri: ::prost::alloc::string::String,
    /// / Index id.
    #[prost(string, tag = "2")]
    pub index_id: ::prost::alloc::string::String,
    /// / Split ID.
    #[prost(string, tag = "3")]
    pub split_id: ::prost::alloc::string::String,
}
/// BEGIN quickwit-codegen
use tower::{Layer, Service, ServiceExt};
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait::async_trait]
pub trait CacheStorageService: std::fmt::Debug + dyn_clone::DynClone + Send + Sync + 'static {
    async fn notify_split_change(
        &mut self,
        request: NotifySplitsChangeRequest,
    ) -> crate::cache_storage::Result<NotifySplitsChangeResponse>;
}
dyn_clone::clone_trait_object!(CacheStorageService);
#[cfg(any(test, feature = "testsuite"))]
impl Clone for MockCacheStorageService {
    fn clone(&self) -> Self {
        MockCacheStorageService::new()
    }
}
#[derive(Debug, Clone)]
pub struct CacheStorageServiceClient {
    inner: Box<dyn CacheStorageService>,
}
impl CacheStorageServiceClient {
    pub fn new<T>(instance: T) -> Self
    where
        T: CacheStorageService,
    {
        Self { inner: Box::new(instance) }
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
        CacheStorageServiceClient::new(
            CacheStorageServiceGrpcClientAdapter::new(
                cache_storage_service_grpc_client::CacheStorageServiceGrpcClient::new(
                    channel,
                ),
            ),
        )
    }
    pub fn from_mailbox<A>(mailbox: quickwit_actors::Mailbox<A>) -> Self
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        CacheStorageServiceMailbox<A>: CacheStorageService,
    {
        CacheStorageServiceClient::new(CacheStorageServiceMailbox::new(mailbox))
    }
    pub fn tower() -> CacheStorageServiceTowerBlockBuilder {
        CacheStorageServiceTowerBlockBuilder::default()
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn mock() -> MockCacheStorageService {
        MockCacheStorageService::new()
    }
}
#[async_trait::async_trait]
impl CacheStorageService for CacheStorageServiceClient {
    async fn notify_split_change(
        &mut self,
        request: NotifySplitsChangeRequest,
    ) -> crate::cache_storage::Result<NotifySplitsChangeResponse> {
        self.inner.notify_split_change(request).await
    }
}
#[cfg(any(test, feature = "testsuite"))]
pub mod mock {
    use super::*;
    #[derive(Debug, Clone)]
    struct MockCacheStorageServiceWrapper {
        inner: std::sync::Arc<tokio::sync::Mutex<MockCacheStorageService>>,
    }
    #[async_trait::async_trait]
    impl CacheStorageService for MockCacheStorageServiceWrapper {
        async fn notify_split_change(
            &mut self,
            request: NotifySplitsChangeRequest,
        ) -> crate::cache_storage::Result<NotifySplitsChangeResponse> {
            self.inner.lock().await.notify_split_change(request).await
        }
    }
    impl From<MockCacheStorageService> for CacheStorageServiceClient {
        fn from(mock: MockCacheStorageService) -> Self {
            let mock_wrapper = MockCacheStorageServiceWrapper {
                inner: std::sync::Arc::new(tokio::sync::Mutex::new(mock)),
            };
            CacheStorageServiceClient::new(mock_wrapper)
        }
    }
}
pub type BoxFuture<T, E> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>,
>;
impl tower::Service<NotifySplitsChangeRequest> for Box<dyn CacheStorageService> {
    type Response = NotifySplitsChangeResponse;
    type Error = crate::cache_storage::CacheStorageError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: NotifySplitsChangeRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.notify_split_change(request).await };
        Box::pin(fut)
    }
}
/// A tower block is a set of towers. Each tower is stack of layers (middlewares) that are applied to a service.
#[derive(Debug)]
struct CacheStorageServiceTowerBlock {
    notify_split_change_svc: quickwit_common::tower::BoxService<
        NotifySplitsChangeRequest,
        NotifySplitsChangeResponse,
        crate::cache_storage::CacheStorageError,
    >,
}
impl Clone for CacheStorageServiceTowerBlock {
    fn clone(&self) -> Self {
        Self {
            notify_split_change_svc: self.notify_split_change_svc.clone(),
        }
    }
}
#[async_trait::async_trait]
impl CacheStorageService for CacheStorageServiceTowerBlock {
    async fn notify_split_change(
        &mut self,
        request: NotifySplitsChangeRequest,
    ) -> crate::cache_storage::Result<NotifySplitsChangeResponse> {
        self.notify_split_change_svc.ready().await?.call(request).await
    }
}
#[derive(Debug, Default)]
pub struct CacheStorageServiceTowerBlockBuilder {
    #[allow(clippy::type_complexity)]
    notify_split_change_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn CacheStorageService>,
            NotifySplitsChangeRequest,
            NotifySplitsChangeResponse,
            crate::cache_storage::CacheStorageError,
        >,
    >,
}
impl CacheStorageServiceTowerBlockBuilder {
    pub fn shared_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn CacheStorageService>> + Clone + Send + Sync + 'static,
        L::Service: tower::Service<
                NotifySplitsChangeRequest,
                Response = NotifySplitsChangeResponse,
                Error = crate::cache_storage::CacheStorageError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            NotifySplitsChangeRequest,
        >>::Future: Send + 'static,
    {
        self
            .notify_split_change_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn notify_split_change_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn CacheStorageService>> + Send + Sync + 'static,
        L::Service: tower::Service<
                NotifySplitsChangeRequest,
                Response = NotifySplitsChangeResponse,
                Error = crate::cache_storage::CacheStorageError,
            > + Clone + Send + Sync + 'static,
        <L::Service as tower::Service<
            NotifySplitsChangeRequest,
        >>::Future: Send + 'static,
    {
        self
            .notify_split_change_layer = Some(
            quickwit_common::tower::BoxLayer::new(layer),
        );
        self
    }
    pub fn build<T>(self, instance: T) -> CacheStorageServiceClient
    where
        T: CacheStorageService,
    {
        self.build_from_boxed(Box::new(instance))
    }
    pub fn build_from_channel<T, C>(self, channel: C) -> CacheStorageServiceClient
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
        self.build_from_boxed(Box::new(CacheStorageServiceClient::from_channel(channel)))
    }
    pub fn build_from_mailbox<A>(
        self,
        mailbox: quickwit_actors::Mailbox<A>,
    ) -> CacheStorageServiceClient
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + 'static,
        CacheStorageServiceMailbox<A>: CacheStorageService,
    {
        self.build_from_boxed(Box::new(CacheStorageServiceClient::from_mailbox(mailbox)))
    }
    fn build_from_boxed(
        self,
        boxed_instance: Box<dyn CacheStorageService>,
    ) -> CacheStorageServiceClient {
        let notify_split_change_svc = if let Some(layer) = self.notify_split_change_layer
        {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let tower_block = CacheStorageServiceTowerBlock {
            notify_split_change_svc,
        };
        CacheStorageServiceClient::new(tower_block)
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
pub struct CacheStorageServiceMailbox<A: quickwit_actors::Actor> {
    inner: MailboxAdapter<A, crate::cache_storage::CacheStorageError>,
}
impl<A: quickwit_actors::Actor> CacheStorageServiceMailbox<A> {
    pub fn new(instance: quickwit_actors::Mailbox<A>) -> Self {
        let inner = MailboxAdapter {
            inner: instance,
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A: quickwit_actors::Actor> Clone for CacheStorageServiceMailbox<A> {
    fn clone(&self) -> Self {
        let inner = MailboxAdapter {
            inner: self.inner.clone(),
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A, M, T, E> tower::Service<M> for CacheStorageServiceMailbox<A>
where
    A: quickwit_actors::Actor
        + quickwit_actors::DeferableReplyHandler<M, Reply = Result<T, E>> + Send
        + 'static,
    M: std::fmt::Debug + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Debug + Send + 'static,
    crate::cache_storage::CacheStorageError: From<quickwit_actors::AskError<E>>,
{
    type Response = T;
    type Error = crate::cache_storage::CacheStorageError;
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
impl<A> CacheStorageService for CacheStorageServiceMailbox<A>
where
    A: quickwit_actors::Actor + std::fmt::Debug,
    CacheStorageServiceMailbox<
        A,
    >: tower::Service<
        NotifySplitsChangeRequest,
        Response = NotifySplitsChangeResponse,
        Error = crate::cache_storage::CacheStorageError,
        Future = BoxFuture<
            NotifySplitsChangeResponse,
            crate::cache_storage::CacheStorageError,
        >,
    >,
{
    async fn notify_split_change(
        &mut self,
        request: NotifySplitsChangeRequest,
    ) -> crate::cache_storage::Result<NotifySplitsChangeResponse> {
        self.call(request).await
    }
}
#[derive(Debug, Clone)]
pub struct CacheStorageServiceGrpcClientAdapter<T> {
    inner: T,
}
impl<T> CacheStorageServiceGrpcClientAdapter<T> {
    pub fn new(instance: T) -> Self {
        Self { inner: instance }
    }
}
#[async_trait::async_trait]
impl<T> CacheStorageService
for CacheStorageServiceGrpcClientAdapter<
    cache_storage_service_grpc_client::CacheStorageServiceGrpcClient<T>,
>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + std::fmt::Debug + Clone + Send
        + Sync + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError>
        + Send,
    T::Future: Send,
{
    async fn notify_split_change(
        &mut self,
        request: NotifySplitsChangeRequest,
    ) -> crate::cache_storage::Result<NotifySplitsChangeResponse> {
        self.inner
            .notify_split_change(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
}
#[derive(Debug)]
pub struct CacheStorageServiceGrpcServerAdapter {
    inner: Box<dyn CacheStorageService>,
}
impl CacheStorageServiceGrpcServerAdapter {
    pub fn new<T>(instance: T) -> Self
    where
        T: CacheStorageService,
    {
        Self { inner: Box::new(instance) }
    }
}
#[async_trait::async_trait]
impl cache_storage_service_grpc_server::CacheStorageServiceGrpc
for CacheStorageServiceGrpcServerAdapter {
    async fn notify_split_change(
        &self,
        request: tonic::Request<NotifySplitsChangeRequest>,
    ) -> Result<tonic::Response<NotifySplitsChangeResponse>, tonic::Status> {
        self.inner
            .clone()
            .notify_split_change(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(|error| error.into())
    }
}
/// Generated client implementations.
pub mod cache_storage_service_grpc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct CacheStorageServiceGrpcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl CacheStorageServiceGrpcClient<tonic::transport::Channel> {
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
    impl<T> CacheStorageServiceGrpcClient<T>
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
        ) -> CacheStorageServiceGrpcClient<InterceptedService<T, F>>
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
            CacheStorageServiceGrpcClient::new(
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
        /// / Apply an indexing plan on the node.
        pub async fn notify_split_change(
            &mut self,
            request: impl tonic::IntoRequest<super::NotifySplitsChangeRequest>,
        ) -> std::result::Result<
            tonic::Response<super::NotifySplitsChangeResponse>,
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
                "/quickwit.cache_storage.CacheStorageService/notifySplitChange",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "quickwit.cache_storage.CacheStorageService",
                        "notifySplitChange",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod cache_storage_service_grpc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with CacheStorageServiceGrpcServer.
    #[async_trait]
    pub trait CacheStorageServiceGrpc: Send + Sync + 'static {
        /// / Apply an indexing plan on the node.
        async fn notify_split_change(
            &self,
            request: tonic::Request<super::NotifySplitsChangeRequest>,
        ) -> std::result::Result<
            tonic::Response<super::NotifySplitsChangeResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct CacheStorageServiceGrpcServer<T: CacheStorageServiceGrpc> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: CacheStorageServiceGrpc> CacheStorageServiceGrpcServer<T> {
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
    for CacheStorageServiceGrpcServer<T>
    where
        T: CacheStorageServiceGrpc,
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
                "/quickwit.cache_storage.CacheStorageService/notifySplitChange" => {
                    #[allow(non_camel_case_types)]
                    struct notifySplitChangeSvc<T: CacheStorageServiceGrpc>(pub Arc<T>);
                    impl<
                        T: CacheStorageServiceGrpc,
                    > tonic::server::UnaryService<super::NotifySplitsChangeRequest>
                    for notifySplitChangeSvc<T> {
                        type Response = super::NotifySplitsChangeResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::NotifySplitsChangeRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).notify_split_change(request).await
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
                        let method = notifySplitChangeSvc(inner);
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
    impl<T: CacheStorageServiceGrpc> Clone for CacheStorageServiceGrpcServer<T> {
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
    impl<T: CacheStorageServiceGrpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: CacheStorageServiceGrpc> tonic::server::NamedService
    for CacheStorageServiceGrpcServer<T> {
        const NAME: &'static str = "quickwit.cache_storage.CacheStorageService";
    }
}
