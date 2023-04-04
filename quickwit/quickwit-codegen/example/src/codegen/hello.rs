#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HelloRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HelloResponse {
    #[prost(string, tag = "1")]
    pub message: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GoodbyeRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GoodbyeResponse {
    #[prost(string, tag = "1")]
    pub message: ::prost::alloc::string::String,
}
/// BEGIN quickwit-codegen
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait::async_trait]
pub trait Hello: std::fmt::Debug + dyn_clone::DynClone + Send + Sync + 'static {
    async fn hello(
        &mut self,
        request: HelloRequest,
    ) -> crate::HelloResult<HelloResponse>;
    async fn goodbye(
        &mut self,
        request: GoodbyeRequest,
    ) -> crate::HelloResult<GoodbyeResponse>;
}
dyn_clone::clone_trait_object!(Hello);
#[cfg(any(test, feature = "testsuite"))]
impl Clone for MockHello {
    fn clone(&self) -> Self {
        MockHello::new()
    }
}
#[derive(Debug, Clone)]
pub struct HelloClient {
    inner: Box<dyn Hello>,
}
impl HelloClient {
    pub fn new<T>(instance: T) -> Self
    where
        T: Hello,
    {
        Self { inner: Box::new(instance) }
    }
    pub fn from_channel(
        channel: tower::timeout::Timeout<tonic::transport::Channel>,
    ) -> Self {
        HelloClient::new(
            HelloGrpcClientAdapter::new(hello_grpc_client::HelloGrpcClient::new(channel)),
        )
    }
    pub fn from_mailbox<A>(mailbox: quickwit_actors::Mailbox<A>) -> Self
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + Sync + 'static,
        HelloMailbox<A>: Hello,
    {
        HelloClient::new(HelloMailbox::new(mailbox))
    }
    pub fn tower() -> HelloTowerBlockBuilder {
        HelloTowerBlockBuilder::default()
    }
    #[cfg(any(test, feature = "testsuite"))]
    pub fn mock() -> MockHello {
        MockHello::new()
    }
}
#[async_trait::async_trait]
impl Hello for HelloClient {
    async fn hello(
        &mut self,
        request: HelloRequest,
    ) -> crate::HelloResult<HelloResponse> {
        self.inner.hello(request).await
    }
    async fn goodbye(
        &mut self,
        request: GoodbyeRequest,
    ) -> crate::HelloResult<GoodbyeResponse> {
        self.inner.goodbye(request).await
    }
}
#[cfg(any(test, feature = "testsuite"))]
impl From<MockHello> for HelloClient {
    fn from(mock: MockHello) -> Self {
        HelloClient::new(mock)
    }
}
pub type BoxFuture<T, E> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'static>,
>;
impl tower::Service<HelloRequest> for Box<dyn Hello> {
    type Response = HelloResponse;
    type Error = crate::HelloError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: HelloRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.hello(request).await };
        Box::pin(fut)
    }
}
impl tower::Service<GoodbyeRequest> for Box<dyn Hello> {
    type Response = GoodbyeResponse;
    type Error = crate::HelloError;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: GoodbyeRequest) -> Self::Future {
        let mut svc = self.clone();
        let fut = async move { svc.goodbye(request).await };
        Box::pin(fut)
    }
}
/// A tower block is a set of towers. Each tower is stack of layers (middlewares) that are applied to a service.
#[derive(Debug)]
struct HelloTowerBlock {
    hello_svc: quickwit_common::tower::BoxService<
        HelloRequest,
        HelloResponse,
        crate::HelloError,
    >,
    goodbye_svc: quickwit_common::tower::BoxService<
        GoodbyeRequest,
        GoodbyeResponse,
        crate::HelloError,
    >,
}
impl Clone for HelloTowerBlock {
    fn clone(&self) -> Self {
        Self {
            hello_svc: self.hello_svc.clone(),
            goodbye_svc: self.goodbye_svc.clone(),
        }
    }
}
#[async_trait::async_trait]
impl Hello for HelloTowerBlock {
    async fn hello(
        &mut self,
        request: HelloRequest,
    ) -> crate::HelloResult<HelloResponse> {
        self.hello_svc.ready().await?.call(request).await
    }
    async fn goodbye(
        &mut self,
        request: GoodbyeRequest,
    ) -> crate::HelloResult<GoodbyeResponse> {
        self.goodbye_svc.ready().await?.call(request).await
    }
}
#[derive(Debug, Default)]
pub struct HelloTowerBlockBuilder {
    hello_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn Hello>,
            HelloRequest,
            HelloResponse,
            crate::HelloError,
        >,
    >,
    goodbye_layer: Option<
        quickwit_common::tower::BoxLayer<
            Box<dyn Hello>,
            GoodbyeRequest,
            GoodbyeResponse,
            crate::HelloError,
        >,
    >,
}
impl HelloTowerBlockBuilder {
    pub fn shared_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn Hello>> + Clone + Send + Sync + 'static,
        L::Service: Service<
                HelloRequest,
                Response = HelloResponse,
                Error = crate::HelloError,
            > + Clone + Send + Sync + 'static,
        <L::Service as Service<HelloRequest>>::Future: Send + 'static,
        L::Service: Service<
                GoodbyeRequest,
                Response = GoodbyeResponse,
                Error = crate::HelloError,
            > + Clone + Send + Sync + 'static,
        <L::Service as Service<GoodbyeRequest>>::Future: Send + 'static,
    {
        self.hello_layer = Some(quickwit_common::tower::BoxLayer::new(layer.clone()));
        self.goodbye_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn hello_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn Hello>> + Send + Sync + 'static,
        L::Service: Service<
                HelloRequest,
                Response = HelloResponse,
                Error = crate::HelloError,
            > + Clone + Send + Sync + 'static,
        <L::Service as Service<HelloRequest>>::Future: Send + 'static,
    {
        self.hello_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn goodbye_layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<Box<dyn Hello>> + Send + Sync + 'static,
        L::Service: Service<
                GoodbyeRequest,
                Response = GoodbyeResponse,
                Error = crate::HelloError,
            > + Clone + Send + Sync + 'static,
        <L::Service as Service<GoodbyeRequest>>::Future: Send + 'static,
    {
        self.goodbye_layer = Some(quickwit_common::tower::BoxLayer::new(layer));
        self
    }
    pub fn build<T>(self, instance: T) -> HelloClient
    where
        T: Hello,
    {
        self.build_from_boxed(Box::new(instance))
    }
    pub fn build_from_channel<T>(
        self,
        channel: tower::timeout::Timeout<tonic::transport::Channel>,
    ) -> HelloClient {
        self.build_from_boxed(Box::new(HelloClient::from_channel(channel)))
    }
    pub fn build_from_mailbox<A>(
        self,
        mailbox: quickwit_actors::Mailbox<A>,
    ) -> HelloClient
    where
        A: quickwit_actors::Actor + std::fmt::Debug + Send + Sync + 'static,
        HelloMailbox<A>: Hello,
    {
        self.build_from_boxed(Box::new(HelloClient::from_mailbox(mailbox)))
    }
    fn build_from_boxed(self, boxed_instance: Box<dyn Hello>) -> HelloClient {
        let hello_svc = if let Some(layer) = self.hello_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let goodbye_svc = if let Some(layer) = self.goodbye_layer {
            layer.layer(boxed_instance.clone())
        } else {
            quickwit_common::tower::BoxService::new(boxed_instance.clone())
        };
        let tower_block = HelloTowerBlock {
            hello_svc,
            goodbye_svc,
        };
        HelloClient::new(tower_block)
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
pub struct HelloMailbox<A: quickwit_actors::Actor> {
    inner: MailboxAdapter<A, crate::HelloError>,
}
impl<A: quickwit_actors::Actor> HelloMailbox<A> {
    pub fn new(instance: quickwit_actors::Mailbox<A>) -> Self {
        let inner = MailboxAdapter {
            inner: instance,
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
impl<A: quickwit_actors::Actor> Clone for HelloMailbox<A> {
    fn clone(&self) -> Self {
        let inner = MailboxAdapter {
            inner: self.inner.clone(),
            phantom: std::marker::PhantomData,
        };
        Self { inner }
    }
}
use tower::{Layer, Service, ServiceExt};
impl<A, M, T, E> tower::Service<M> for HelloMailbox<A>
where
    A: quickwit_actors::Actor
        + quickwit_actors::DeferableReplyHandler<M, Reply = Result<T, E>> + Send + Sync
        + 'static,
    M: std::fmt::Debug + Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: std::fmt::Debug + Send + Sync + 'static,
    crate::HelloError: From<quickwit_actors::AskError<E>>,
{
    type Response = T;
    type Error = crate::HelloError;
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
impl<A> Hello for HelloMailbox<A>
where
    A: quickwit_actors::Actor + std::fmt::Debug + Send + Sync + 'static,
    HelloMailbox<
        A,
    >: tower::Service<
            HelloRequest,
            Response = HelloResponse,
            Error = crate::HelloError,
            Future = BoxFuture<HelloResponse, crate::HelloError>,
        >
        + tower::Service<
            GoodbyeRequest,
            Response = GoodbyeResponse,
            Error = crate::HelloError,
            Future = BoxFuture<GoodbyeResponse, crate::HelloError>,
        >,
{
    async fn hello(
        &mut self,
        request: HelloRequest,
    ) -> crate::HelloResult<HelloResponse> {
        self.call(request).await
    }
    async fn goodbye(
        &mut self,
        request: GoodbyeRequest,
    ) -> crate::HelloResult<GoodbyeResponse> {
        self.call(request).await
    }
}
#[derive(Debug, Clone)]
pub struct HelloGrpcClientAdapter<T> {
    inner: T,
}
impl<T> HelloGrpcClientAdapter<T> {
    pub fn new(instance: T) -> Self {
        Self { inner: instance }
    }
}
#[async_trait::async_trait]
impl<T> Hello for HelloGrpcClientAdapter<hello_grpc_client::HelloGrpcClient<T>>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + std::fmt::Debug + Clone + Send
        + Sync + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError>
        + Send,
    T::Future: Send,
{
    async fn hello(
        &mut self,
        request: HelloRequest,
    ) -> crate::HelloResult<HelloResponse> {
        self.inner
            .hello(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
    async fn goodbye(
        &mut self,
        request: GoodbyeRequest,
    ) -> crate::HelloResult<GoodbyeResponse> {
        self.inner
            .goodbye(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| error.into())
    }
}
#[derive(Debug)]
pub struct HelloGrpcServerAdapter {
    inner: Box<dyn Hello>,
}
impl HelloGrpcServerAdapter {
    pub fn new<T>(instance: T) -> Self
    where
        T: Hello,
    {
        Self { inner: Box::new(instance) }
    }
}
#[async_trait::async_trait]
impl hello_grpc_server::HelloGrpc for HelloGrpcServerAdapter {
    async fn hello(
        &self,
        request: tonic::Request<HelloRequest>,
    ) -> Result<tonic::Response<HelloResponse>, tonic::Status> {
        self.inner
            .clone()
            .hello(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(Into::into)
    }
    async fn goodbye(
        &self,
        request: tonic::Request<GoodbyeRequest>,
    ) -> Result<tonic::Response<GoodbyeResponse>, tonic::Status> {
        self.inner
            .clone()
            .goodbye(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(Into::into)
    }
}
/// Generated client implementations.
pub mod hello_grpc_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct HelloGrpcClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl HelloGrpcClient<tonic::transport::Channel> {
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
    impl<T> HelloGrpcClient<T>
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
        ) -> HelloGrpcClient<InterceptedService<T, F>>
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
            HelloGrpcClient::new(InterceptedService::new(inner, interceptor))
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
        pub async fn hello(
            &mut self,
            request: impl tonic::IntoRequest<super::HelloRequest>,
        ) -> Result<tonic::Response<super::HelloResponse>, tonic::Status> {
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
            let path = http::uri::PathAndQuery::from_static("/hello.Hello/Hello");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn goodbye(
            &mut self,
            request: impl tonic::IntoRequest<super::GoodbyeRequest>,
        ) -> Result<tonic::Response<super::GoodbyeResponse>, tonic::Status> {
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
            let path = http::uri::PathAndQuery::from_static("/hello.Hello/Goodbye");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod hello_grpc_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with HelloGrpcServer.
    #[async_trait]
    pub trait HelloGrpc: Send + Sync + 'static {
        async fn hello(
            &self,
            request: tonic::Request<super::HelloRequest>,
        ) -> Result<tonic::Response<super::HelloResponse>, tonic::Status>;
        async fn goodbye(
            &self,
            request: tonic::Request<super::GoodbyeRequest>,
        ) -> Result<tonic::Response<super::GoodbyeResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct HelloGrpcServer<T: HelloGrpc> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: HelloGrpc> HelloGrpcServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for HelloGrpcServer<T>
    where
        T: HelloGrpc,
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
                "/hello.Hello/Hello" => {
                    #[allow(non_camel_case_types)]
                    struct HelloSvc<T: HelloGrpc>(pub Arc<T>);
                    impl<T: HelloGrpc> tonic::server::UnaryService<super::HelloRequest>
                    for HelloSvc<T> {
                        type Response = super::HelloResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::HelloRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).hello(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = HelloSvc(inner);
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
                "/hello.Hello/Goodbye" => {
                    #[allow(non_camel_case_types)]
                    struct GoodbyeSvc<T: HelloGrpc>(pub Arc<T>);
                    impl<T: HelloGrpc> tonic::server::UnaryService<super::GoodbyeRequest>
                    for GoodbyeSvc<T> {
                        type Response = super::GoodbyeResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GoodbyeRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).goodbye(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GoodbyeSvc(inner);
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
    impl<T: HelloGrpc> Clone for HelloGrpcServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: HelloGrpc> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: HelloGrpc> tonic::server::NamedService for HelloGrpcServer<T> {
        const NAME: &'static str = "hello.Hello";
    }
}
